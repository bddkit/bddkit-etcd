//! Named watches. A task in the plugin's runtime reads the stream of one watch
//! into a buffer; the assertions only read that buffer, so the host's polling
//! waits for an event and the plugin never sleeps. The buffer is the whole
//! history of the watch: nothing consumes it, so a retried attempt sees the
//! same events and the new ones.
//!
//! A watch belongs to the instance that opened it and ends with the scenario
//! (`Watches::stop_all`). Until a stream that broke can be resumed, a break is
//! a terminal state, like a cancellation by the server or a full buffer: it
//! is kept with its reason, and the next assertion on the watch fails with it.

use std::time::Duration;

use etcd_client::{EventType, WatchResponse};
use serde_json::{Value, json};

use crate::check::{columns, is_slot, lossy, miss};
use crate::client::request_failure;
use crate::error::{Class, StepError};
use crate::evidence::{Record, shown};
use crate::node::{revision, variable};
use crate::ops::Request;
use crate::steps::Step;

/// How many of the last events a diagnostic shows.
const SHOWN_EVENTS: usize = 10;

/// How long a watch that replays from a revision waits for the server to turn
/// it down. A compacted revision is not refused in the answer to `create`: the
/// server confirms the watch first, and cancels it from the loop that syncs
/// the watchers, about 100 ms later.
// ponytail: a timer, not a protocol exchange; a server slower than this still
// ends the watch, and the first assertion on it says why.
const REPLAY_GRACE: Duration = Duration::from_millis(500);

const COLUMNS: [&str; 6] = [
    "type",
    "key",
    "value",
    "prev_value",
    "mod_revision",
    "version",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Put,
    Delete,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Put => "PUT",
            Self::Delete => "DELETE",
        }
    }

    fn parse(text: &str) -> Result<Self, String> {
        match text {
            "PUT" => Ok(Self::Put),
            "DELETE" => Ok(Self::Delete),
            other => Err(format!(
                "unknown event type \"{other}\"; the types are PUT and DELETE"
            )),
        }
    }
}

/// An event as the server sent it. The judging works on this and never on the
/// client's type, so it can be tested without a server.
#[derive(Debug, Clone)]
pub struct Event {
    kind: Kind,
    key: Vec<u8>,
    value: Vec<u8>,
    /// Empty when the key had no previous value.
    prev_value: Vec<u8>,
    mod_revision: i64,
    version: i64,
}

impl Event {
    fn from_client(event: &etcd_client::Event) -> Result<Self, String> {
        let item = event.kv().ok_or("an event arrived with no key-value")?;
        Ok(Self {
            kind: match event.event_type() {
                EventType::Put => Kind::Put,
                EventType::Delete => Kind::Delete,
            },
            key: item.key().to_vec(),
            value: item.value().to_vec(),
            prev_value: event
                .prev_kv()
                .map(|prev| prev.value().to_vec())
                .unwrap_or_default(),
            mod_revision: item.mod_revision(),
            version: item.version(),
        })
    }

    fn shown(&self) -> Value {
        json!({
            "type": self.kind.name(),
            "key": shown(&self.key),
            "value": shown(&self.value),
            "prev_value": shown(&self.prev_value),
            "mod_revision": self.mod_revision,
            "version": self.version,
        })
    }
}

/// A value that is about to be quoted goes to the host's `scan` first.
fn quoted(bytes: &[u8], record: &mut Record) -> String {
    let text = lossy(bytes);
    record.scan_text(&text);
    text
}

struct Failure {
    class: Class,
    message: String,
}

enum Ending {
    /// By the tester: what was received stays readable.
    Cancelled,
    Failed(Failure),
}

/// Everything the task writes and the assertions read.
pub struct State {
    target: String,
    limit: usize,
    /// The revision of the answer that confirmed the watch.
    revision: i64,
    events: Vec<Event>,
    violation: Option<String>,
    ending: Option<Ending>,
}

impl State {
    fn new(target: String, limit: usize, revision: i64) -> Self {
        Self {
            target,
            limit,
            revision,
            events: Vec::new(),
            violation: None,
            ending: None,
        }
    }

    fn stopped(&self) -> bool {
        self.violation.is_some() || self.ending.is_some()
    }

    fn fail(&mut self, class: Class, message: impl Into<String>) {
        if self.ending.is_none() {
            self.ending = Some(Ending::Failed(Failure {
                class,
                message: message.into(),
            }));
        }
    }

    fn failure(&self) -> Option<&Failure> {
        match &self.ending {
            Some(Ending::Failed(failure)) => Some(failure),
            _ => None,
        }
    }

    /// Why `event` cannot follow what has arrived: an event the server sent
    /// twice, or one older than the last. For the server under test either is
    /// a defect.
    fn disorder(&self, event: &Event) -> Option<String> {
        let last = self.events.last()?;
        let at = self.events.len() + 1;
        let key = lossy(&event.key);
        if event.mod_revision < last.mod_revision {
            return Some(format!(
                "event {at} ({} of \"{key}\") has mod_revision {}, after an event with mod_revision {}",
                event.kind.name(),
                event.mod_revision,
                last.mod_revision
            ));
        }
        let twice = self
            .events
            .iter()
            .rev()
            .take_while(|seen| seen.mod_revision == event.mod_revision)
            .any(|seen| seen.key == event.key);
        twice.then(|| {
            format!(
                "event {at} arrived twice: \"{key}\" at mod_revision {}",
                event.mod_revision
            )
        })
    }

    /// What the stream delivered. Nothing is dropped: a full buffer stops the
    /// watch.
    fn accept(&mut self, events: Vec<Event>) {
        for event in events {
            if self.stopped() {
                return;
            }
            if let Some(why) = self.disorder(&event) {
                self.violation = Some(why);
                return;
            }
            if self.events.len() >= self.limit {
                self.fail(
                    Class::Assertion,
                    format!(
                        "the watch holds watch_buffer_events ({}) events and another one arrived; nothing is dropped, so the watch stopped: raise watch_buffer_events or watch a narrower prefix",
                        self.limit
                    ),
                );
                return;
            }
            self.events.push(event);
        }
    }

    /// One message of the stream. `false` when the task is to stop.
    fn apply(
        &mut self,
        message: Result<Option<WatchResponse>, etcd_client::Error>,
        tls: bool,
    ) -> bool {
        let response = match message {
            Ok(Some(response)) => response,
            Ok(None) => {
                self.fail(Class::DbConnection, "the server closed the watch stream");
                return false;
            }
            Err(error) => {
                // A request that failed says what the server answered; it is not
                // a refused password.
                let failure = request_failure(&error, tls);
                self.fail(
                    failure.class,
                    format!("the watch stream broke: {}", failure.message),
                );
                return false;
            }
        };
        if response.canceled() {
            self.fail(
                Class::DbStatement,
                format!(
                    "the server cancelled the watch: {}",
                    cancel_reason(&response)
                ),
            );
            return false;
        }
        let events: Result<Vec<Event>, String> =
            response.events().iter().map(Event::from_client).collect();
        match events {
            Ok(events) => self.accept(events),
            Err(why) => self.violation = Some(why),
        }
        !self.stopped()
    }

    fn describe(&self) -> Value {
        let state = match &self.ending {
            None => "running".to_string(),
            Some(Ending::Cancelled) => "cancelled".to_string(),
            Some(Ending::Failed(failure)) => format!("stopped: {}", failure.message),
        };
        let first = self.events.len().saturating_sub(SHOWN_EVENTS);
        json!({
            "watching": self.target,
            "state": state,
            "violation": self.violation,
            "created_at_revision": self.revision,
            "events": self.events.len(),
            "last_revision": self.events.last().map(|event| event.mod_revision),
            "last_events": self.events[first..].iter().map(Event::shown).collect::<Vec<_>>(),
        })
    }

    /// The values of the events a diagnostic shows, for the host's `scan`.
    fn scan(&self) -> Vec<Value> {
        let first = self.events.len().saturating_sub(SHOWN_EVENTS);
        let mut record = Record::new("Watch", "", "", Value::Null);
        for event in &self.events[first..] {
            quoted(&event.value, &mut record);
            quoted(&event.prev_value, &mut record);
        }
        record.scan().to_vec()
    }
}

fn cancel_reason(response: &WatchResponse) -> String {
    let reason = response.cancel_reason();
    match (reason.is_empty(), response.compact_revision()) {
        (true, 0) => "no reason given".to_string(),
        (true, compacted) => format!("revision {compacted} was compacted"),
        (false, 0) => reason.to_string(),
        (false, compacted) => format!("{reason} (compacted at revision {compacted})"),
    }
}

// ---- what a step asks ----

/// One row of a `received events:` table: only the columns the header names
/// are judged.
#[derive(Debug, Default)]
pub struct Row {
    kind: Option<Kind>,
    key: Option<String>,
    value: Option<String>,
    prev_value: Option<String>,
    mod_revision: Option<i64>,
    version: Option<i64>,
}

#[derive(Debug)]
pub enum Expect {
    Put { key: String, value: String },
    Delete { key: String },
    Count(u64),
    Table(Vec<Row>),
}

#[derive(Debug)]
pub struct Create {
    name: String,
    key: String,
    prefix: bool,
    from: Option<i64>,
}

#[derive(Debug)]
pub enum Watch {
    Create(Create),
    Cancel(String),
    Check { name: String, expect: Expect },
}

fn whole(cell: &str) -> Result<i64, String> {
    cell.parse()
        .map_err(|_| format!("\"{cell}\" is not a whole number"))
}

fn rows(table: Option<&[Vec<String>]>) -> Result<Vec<Row>, String> {
    let at = columns(table, &COLUMNS, 0)?;
    let table = table.unwrap_or_default();
    if table.len() < 2 {
        return Err("the table has no rows; to expect no events write: etcd watch \"<name>\" should have received 0 events".to_string());
    }
    table[1..]
        .iter()
        .map(|row| {
            let cell = |column: usize| at[column].map(|i| row[i].as_str());
            Ok(Row {
                kind: cell(0).map(Kind::parse).transpose()?,
                key: cell(1).map(str::to_string),
                value: cell(2).map(str::to_string),
                prev_value: cell(3).map(str::to_string),
                mod_revision: cell(4).map(whole).transpose()?,
                version: cell(5).map(whole).transpose()?,
            })
        })
        .collect()
}

fn watch_revision(text: &str) -> Result<i64, StepError> {
    match revision(text)? {
        0 => Err(StepError::input(
            "revision 0 means \"from now on\" to etcd; write the step without \"from revision\"",
        )),
        rev => Ok(rev),
    }
}

fn nonempty(text: &str, what: &str) -> Result<String, StepError> {
    if text.is_empty() {
        return Err(StepError::input(format!("the {what} is empty")));
    }
    Ok(text.to_string())
}

/// Everything that can be refused without a request, refused here.
pub fn plan(step: Step, request: &Request) -> Result<Watch, StepError> {
    let name = |index: usize| variable(request.arg(index)?);
    Ok(match step {
        Step::WatchKey | Step::WatchKeyFrom | Step::WatchPrefix | Step::WatchPrefixFrom => {
            let prefix = matches!(step, Step::WatchPrefix | Step::WatchPrefixFrom);
            let from = matches!(step, Step::WatchKeyFrom | Step::WatchPrefixFrom);
            Watch::Create(Create {
                name: name(1)?,
                key: nonempty(request.arg(0)?, if prefix { "prefix" } else { "key" })?,
                prefix,
                from: if from {
                    Some(watch_revision(request.arg(2)?)?)
                } else {
                    None
                },
            })
        }
        Step::WatchCancel => Watch::Cancel(name(0)?),
        Step::WatchedPut => Watch::Check {
            name: name(0)?,
            expect: Expect::Put {
                key: request.arg(1)?.to_string(),
                value: request.arg(2)?.to_string(),
            },
        },
        Step::WatchedDelete => Watch::Check {
            name: name(0)?,
            expect: Expect::Delete {
                key: request.arg(1)?.to_string(),
            },
        },
        Step::WatchedCount => Watch::Check {
            name: name(0)?,
            expect: Expect::Count(request.arg(1)?.parse().map_err(|_| {
                StepError::input(format!("\"{}\" is not a count", request.args[1]))
            })?),
        },
        Step::WatchedEvents => Watch::Check {
            name: name(0)?,
            expect: Expect::Table(rows(request.table.as_deref()).map_err(StepError::input)?),
        },
        Step::Put
        | Step::PutDocString
        | Step::Get
        | Step::GetSerializable
        | Step::DeleteKey
        | Step::DeletePrefix
        | Step::DeleteAll
        | Step::Exists
        | Step::Absent
        | Step::HasValue
        | Step::HasValueDoc
        | Step::Matches
        | Step::ContainsJson
        | Step::EqualsJson
        | Step::HasFields
        | Step::Count
        | Step::KeysBe
        | Step::KeysContain
        | Step::Leader
        | Step::NotLeader
        | Step::Reached
        | Step::ComputeHash
        | Step::HashIs
        | Step::AcceptPut
        | Step::RefusePut
        | Step::AcceptRead
        | Step::AcceptReadSerializable
        | Step::RefuseRead
        | Step::RefuseReadSerializable
        | Step::RefuseCompact => return Err(StepError::input("not a watch step")),
    })
}

/// `bddkit_validate_step`: what a literal already says. A value that holds a
/// `<<slot>>` is left to the step.
pub fn validate(step: Step, request: &Request) -> Result<(), String> {
    let literal = |index: usize| request.args.get(index).filter(|a| !is_slot(a));
    let name_at = match step {
        Step::WatchKey | Step::WatchPrefix | Step::WatchKeyFrom | Step::WatchPrefixFrom => 1,
        _ => 0,
    };
    if let Some(name) = literal(name_at) {
        variable(name).map_err(|e| e.message)?;
    }
    match step {
        Step::WatchKey | Step::WatchKeyFrom => {
            literal(0).map_or(Ok(()), |k| nonempty(k, "key").map(drop))
        }
        Step::WatchPrefix | Step::WatchPrefixFrom => {
            literal(0).map_or(Ok(()), |p| nonempty(p, "prefix").map(drop))
        }
        _ => Ok(()),
    }
    .map_err(|e| e.message)?;
    if matches!(step, Step::WatchKeyFrom | Step::WatchPrefixFrom) {
        literal(2)
            .map_or(Ok(()), |rev| watch_revision(rev).map(drop))
            .map_err(|e| e.message)?;
    }
    if step == Step::WatchedEvents {
        let table = request.table.as_deref();
        // A header that holds a slot is judged when the step runs.
        if !table
            .and_then(<[_]>::first)
            .is_some_and(|header| header.iter().any(|cell| is_slot(cell)))
        {
            columns(table, &COLUMNS, 0)?;
            // A cell that holds a slot is judged when the step runs.
            if !table.is_some_and(|t| t.iter().flatten().any(|cell| is_slot(cell))) {
                rows(table)?;
            }
        }
    }
    Ok(())
}

pub fn record_for(watch: &Watch, instance: &str) -> Record {
    match watch {
        Watch::Create(create) => {
            let mut request =
                json!({"name": create.name, "from_revision": create.from, "prev_kv": true});
            request[if create.prefix { "prefix" } else { "key" }] = json!(create.key);
            Record::new("Watch", &create.name, instance, request)
        }
        Watch::Cancel(name) => Record::new("CancelWatch", name, instance, json!({"name": name})),
        Watch::Check { name, .. } => {
            Record::new("CheckWatch", name, instance, json!({"name": name}))
        }
    }
}

// ---- judging ----

impl Row {
    /// What differs between the event and the row, in the columns the row has.
    fn differences(&self, event: &Event, record: &mut Record) -> Vec<String> {
        let mut wrong = Vec::new();
        let mut text = |name: &str, actual: &[u8], expected: Option<&String>| {
            if let Some(expected) = expected.filter(|e| e.as_bytes() != actual) {
                wrong.push(format!(
                    "{name} is \"{}\", expected \"{expected}\"",
                    quoted(actual, record)
                ));
            }
        };
        text("key", &event.key, self.key.as_ref());
        text("value", &event.value, self.value.as_ref());
        text("prev_value", &event.prev_value, self.prev_value.as_ref());
        if let Some(kind) = self.kind.filter(|kind| *kind != event.kind) {
            wrong.push(format!(
                "type is {}, expected {}",
                event.kind.name(),
                kind.name()
            ));
        }
        for (name, actual, expected) in [
            ("mod_revision", event.mod_revision, self.mod_revision),
            ("version", event.version, self.version),
        ] {
            if let Some(expected) = expected.filter(|e| *e != actual) {
                wrong.push(format!("{name} is {actual}, expected {expected}"));
            }
        }
        wrong
    }
}

impl State {
    /// The assertion over the history. A watch that cannot be trusted any more
    /// (an event out of order, a stream that ended) fails every assertion on
    /// it, final; so does a history that already differs from what is
    /// expected. A history that is still a beginning of it is `not_yet`.
    fn judge(&self, name: &str, expect: &Expect, record: &mut Record) -> Result<(), StepError> {
        if let Some(why) = &self.violation {
            return Err(StepError::assertion(format!(
                "etcd watch \"{name}\": {why}"
            )));
        }
        if let Some(failure) = self.failure() {
            return Err(StepError::new(
                failure.class,
                format!("etcd watch \"{name}\" stopped: {}", failure.message),
            ));
        }
        for event in &self.events[self.events.len().saturating_sub(SHOWN_EVENTS)..] {
            quoted(&event.value, record);
            quoted(&event.prev_value, record);
            record.keys.push(event.shown());
        }
        let held = self.events.len();
        let judged = match expect {
            Expect::Put { key, value } => self.any(name, Kind::Put, key, Some(value)),
            Expect::Delete { key } => self.any(name, Kind::Delete, key, None),
            Expect::Count(want) => match u64::try_from(held).unwrap_or(u64::MAX).cmp(want) {
                std::cmp::Ordering::Equal => Ok(()),
                std::cmp::Ordering::Less => Err(miss(format!(
                    "etcd watch \"{name}\" has received {held} events, expected {want}"
                ))),
                std::cmp::Ordering::Greater => Err(StepError::assertion(format!(
                    "etcd watch \"{name}\" has received {held} events, expected {want}, and an event does not go away"
                ))),
            },
            Expect::Table(rows) => self.table(name, rows, record),
        };
        // A cancelled watch receives nothing more: what it lacks it will
        // always lack, and waiting for it would only run the polling out.
        match judged {
            Err(error) if error.retry && matches!(self.ending, Some(Ending::Cancelled)) => {
                Err(StepError::assertion(format!(
                    "{}, and the watch is cancelled, so nothing more will arrive",
                    error.message
                )))
            }
            other => other,
        }
    }

    fn any(
        &self,
        name: &str,
        kind: Kind,
        key: &str,
        value: Option<&String>,
    ) -> Result<(), StepError> {
        let found = self.events.iter().any(|event| {
            event.kind == kind
                && event.key == key.as_bytes()
                && value.is_none_or(|value| event.value == value.as_bytes())
        });
        if found {
            return Ok(());
        }
        let with = value.map_or(String::new(), |value| format!(" with value \"{value}\""));
        Err(miss(format!(
            "etcd watch \"{name}\" has received {} events, none is a {} of \"{key}\"{with}",
            self.events.len(),
            kind.name()
        )))
    }

    fn table(&self, name: &str, rows: &[Row], record: &mut Record) -> Result<(), StepError> {
        for (index, (event, row)) in self.events.iter().zip(rows).enumerate() {
            let wrong = row.differences(event, record);
            if !wrong.is_empty() {
                return Err(StepError::assertion(format!(
                    "etcd watch \"{name}\": event {} differs from the table, and an event does not go away: {}",
                    index + 1,
                    wrong.join("; ")
                )));
            }
        }
        let held = self.events.len();
        if held > rows.len() {
            let extra = &self.events[rows.len()];
            return Err(StepError::assertion(format!(
                "etcd watch \"{name}\" has received {held} events, the table has {}; the first extra one is a {} of \"{}\"",
                rows.len(),
                extra.kind.name(),
                lossy(&extra.key)
            )));
        }
        if held < rows.len() {
            return Err(miss(format!(
                "etcd watch \"{name}\" has received {held} events, the table has {}",
                rows.len()
            )));
        }
        Ok(())
    }
}

mod run;

pub use run::{Watches, perform};

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str, value: &str, rev: i64) -> Event {
        Event {
            kind: Kind::Put,
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            prev_value: Vec::new(),
            mod_revision: rev,
            version: 1,
        }
    }

    fn delete(key: &str, rev: i64) -> Event {
        Event {
            kind: Kind::Delete,
            key: key.as_bytes().to_vec(),
            value: Vec::new(),
            prev_value: b"old".to_vec(),
            mod_revision: rev,
            version: 0,
        }
    }

    fn state(events: Vec<Event>) -> State {
        let mut state = State::new("prefix \"/p/\"".to_string(), 100, 1);
        state.accept(events);
        state
    }

    fn row(kind: Kind, key: &str, value: Option<&str>) -> Row {
        Row {
            kind: Some(kind),
            key: Some(key.to_string()),
            value: value.map(str::to_string),
            ..Row::default()
        }
    }

    fn record() -> Record {
        Record::new("CheckWatch", "w", "main", json!({}))
    }

    fn judged(state: &State, expect: &Expect) -> Result<(), StepError> {
        state.judge("w", expect, &mut record())
    }

    #[test]
    fn a_history_that_is_a_beginning_of_the_table_is_not_yet_and_any_other_is_final() {
        let table = |n: usize| {
            Expect::Table(
                [
                    row(Kind::Put, "/p/a", Some("1")),
                    row(Kind::Delete, "/p/a", None),
                ]
                .into_iter()
                .take(n)
                .collect(),
            )
        };
        let none = state(vec![]);
        assert!(judged(&none, &table(2)).expect_err("short").retry);
        let one = state(vec![put("/p/a", "1", 5)]);
        assert!(judged(&one, &table(2)).expect_err("short").retry);
        let both = state(vec![put("/p/a", "1", 5), delete("/p/a", 6)]);
        assert!(judged(&both, &table(2)).is_ok());
        // One event too many: it will not go away.
        let extra = judged(&both, &table(1)).expect_err("extra");
        assert!(!extra.retry && extra.message.contains("first extra one is a DELETE"));
        // A different event in the range of the table.
        let other = state(vec![put("/p/a", "2", 5)]);
        let different = judged(&other, &table(2)).expect_err("different");
        assert!(!different.retry, "{different}");
        assert!(different.message.contains("value is \"2\", expected \"1\""));
    }

    #[test]
    fn a_count_is_not_yet_below_and_final_above() {
        let two = state(vec![put("/p/a", "1", 5), put("/p/b", "1", 6)]);
        assert!(judged(&two, &Expect::Count(2)).is_ok());
        assert!(judged(&two, &Expect::Count(3)).expect_err("fewer").retry);
        assert!(!judged(&two, &Expect::Count(1)).expect_err("more").retry);
    }

    #[test]
    fn a_put_or_a_delete_is_found_anywhere_in_the_history_and_waited_for_otherwise() {
        let history = state(vec![
            put("/p/a", "1", 5),
            delete("/p/b", 6),
            put("/p/a", "2", 7),
        ]);
        let found = |kind, key: &str, value: Option<&str>| {
            history.any("w", kind, key, value.map(str::to_string).as_ref())
        };
        assert!(found(Kind::Put, "/p/a", Some("1")).is_ok());
        assert!(found(Kind::Put, "/p/a", Some("2")).is_ok());
        assert!(found(Kind::Delete, "/p/b", None).is_ok());
        for missing in [
            found(Kind::Put, "/p/a", Some("3")),
            found(Kind::Delete, "/p/a", None),
            found(Kind::Put, "/p/b", Some("")),
        ] {
            assert!(missing.expect_err("absent").retry);
        }
    }

    #[test]
    fn an_event_out_of_order_or_seen_twice_fails_every_later_assertion() {
        let mut backwards = state(vec![put("/p/a", "1", 5), put("/p/b", "1", 4)]);
        let error = judged(&backwards, &Expect::Count(2)).expect_err("violation");
        assert!(!error.retry && error.class == Class::Assertion);
        assert!(error.message.contains("mod_revision 4, after"), "{error}");
        // The history stops growing once it cannot be trusted.
        backwards.accept(vec![put("/p/c", "1", 9)]);
        assert_eq!(backwards.events.len(), 1);
        let twice = state(vec![put("/p/a", "1", 5), put("/p/a", "1", 5)]);
        assert!(
            twice
                .violation
                .as_deref()
                .is_some_and(|v| v.contains("arrived twice"))
        );
        // The same key at a later revision, and another key at the same one, are fine.
        let fine = state(vec![
            put("/p/a", "1", 5),
            put("/p/b", "1", 5),
            put("/p/a", "2", 6),
        ]);
        assert!(fine.violation.is_none());
    }

    #[test]
    fn a_full_buffer_stops_the_watch_and_names_the_limit() {
        let mut full = State::new("key \"/k\"".to_string(), 2, 1);
        full.accept(vec![
            put("/k", "1", 2),
            put("/k", "2", 3),
            put("/k", "3", 4),
        ]);
        assert_eq!(full.events.len(), 2, "nothing is dropped, nothing is added");
        let error = judged(&full, &Expect::Count(2)).expect_err("stopped");
        assert!(
            !error.retry && error.message.contains("watch_buffer_events (2)"),
            "{error}"
        );
    }

    #[test]
    fn an_ended_watch_is_final_for_the_assertion_whatever_the_class_of_its_reason() {
        let mut broke = state(vec![]);
        broke.fail(Class::DbConnection, "the watch stream broke: gone");
        let error = judged(&broke, &Expect::Count(0)).expect_err("stopped");
        assert!(!error.retry);
        assert_eq!(error.class, Class::DbConnection);
        // A cancelled watch still answers from what it received.
        let mut cancelled = state(vec![put("/p/a", "1", 5)]);
        cancelled.ending = Some(Ending::Cancelled);
        assert!(judged(&cancelled, &Expect::Count(1)).is_ok());
    }

    #[test]
    fn the_columns_of_a_table_are_the_ones_it_names() {
        let table = |rows: &[&[&str]]| -> Vec<Vec<String>> {
            rows.iter()
                .map(|r| r.iter().map(ToString::to_string).collect())
                .collect()
        };
        let ok = table(&[
            &["mod_revision", "type", "prev_value"],
            &["7", "DELETE", "old"],
        ]);
        let rows = rows(Some(&ok)).expect("rows");
        assert_eq!(rows[0].mod_revision, Some(7));
        assert_eq!(rows[0].kind, Some(Kind::Delete));
        assert!(rows[0].key.is_none());
        assert!(
            rows[0]
                .differences(&delete("/k", 7), &mut record())
                .is_empty()
        );
        assert_eq!(
            rows[0].differences(&delete("/k", 8), &mut record()),
            ["mod_revision is 8, expected 7"]
        );
        for (bad, said) in [
            (
                table(&[&["key", "rev"], &["/k", "1"]]),
                "unknown table column \"rev\"",
            ),
            (table(&[&["type"], &["put"]]), "unknown event type \"put\""),
            (table(&[&["version"], &["x"]]), "not a whole number"),
            (table(&[&["key"]]), "no rows"),
        ] {
            let error = super::rows(Some(&bad)).expect_err("refused");
            assert!(error.contains(said), "{error}");
        }
    }

    #[test]
    fn a_literal_that_cannot_work_is_refused_before_the_run_and_a_slot_is_not() {
        let validate = |step, args: &[&str]| validate(step, &Request::for_test(args));
        assert!(validate(Step::WatchKey, &["/k", "cfg"]).is_ok());
        assert!(validate(Step::WatchKey, &["", "cfg"]).is_err());
        assert!(validate(Step::WatchKey, &["<<k>>", "<<n>>"]).is_ok());
        assert!(validate(Step::WatchPrefix, &["", "cfg"]).is_err());
        assert!(validate(Step::WatchKey, &["/k", "not a name"]).is_err());
        assert!(validate(Step::WatchCancel, &["bad-name"]).is_err());
        assert!(validate(Step::WatchKeyFrom, &["/k", "cfg", "0"]).is_err());
        assert!(validate(Step::WatchKeyFrom, &["/k", "cfg", "5"]).is_ok());
    }

    #[test]
    fn a_name_the_instance_does_not_know_is_input_and_never_needs_a_server() {
        let watches = Watches::default();
        assert_eq!(
            watches.state("nope").err().map(|e| e.class),
            Some(Class::Input)
        );
    }

    #[test]
    fn a_stream_that_ends_or_breaks_stops_the_watch_and_a_refusal_is_not_a_bad_password() {
        let mut closed = state(vec![]);
        assert!(!closed.apply(Ok(None), false));
        assert_eq!(closed.failure().expect("ended").class, Class::DbConnection);
        let mut refused = state(vec![]);
        let status = tonic::Status::new(tonic::Code::InvalidArgument, "etcdserver: nope");
        assert!(!refused.apply(Err(etcd_client::Error::GRpcStatus(status)), false));
        let failure = refused.failure().expect("ended");
        assert_eq!(failure.class, Class::DbStatement);
        assert!(
            failure.message.contains("answered with an error"),
            "{}",
            failure.message
        );
        assert!(
            !failure.message.contains("credentials"),
            "{}",
            failure.message
        );
    }

    #[test]
    fn a_cancelled_watch_turns_a_miss_into_a_final_failure() {
        let mut cancelled = state(vec![put("/p/a", "1", 5)]);
        cancelled.ending = Some(Ending::Cancelled);
        let error = judged(&cancelled, &Expect::Count(2)).expect_err("never");
        assert!(
            !error.retry && error.message.contains("nothing more will arrive"),
            "{error}"
        );
        assert!(judged(&cancelled, &Expect::Delete { key: "/p/a".into() }).is_err());
    }
}
