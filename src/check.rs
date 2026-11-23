//! The assertions over keys. Each attempt reads the server again and judges
//! what it read, so a polled assertion sees a key written after it started.
//! A mismatch is `not_yet` (class `assertion`); a value that cannot be judged
//! as text, or a step that cannot run as written, is `fatal`. A failure of the
//! server itself is made `not_yet` by the caller, see `StepError::polled`.

use std::collections::HashMap;

use etcd_client::{Client, GetOptions, KeyValue};
use regex::Regex;
use serde_json::{Value, json};

use crate::client::request_failure;
use crate::error::{Class, StepError};
use crate::evidence::{Record, base64, shown};
use crate::ops::{Request, doc_value};
use crate::steps::Step;

/// How many keys of a listing the diagnostic names.
const SHOWN_KEYS: usize = 50;

#[derive(Debug, Clone, Copy)]
pub enum Field {
    Version,
    CreateRevision,
    ModRevision,
    Lease,
}

impl Field {
    fn parse(name: &str) -> Result<Self, String> {
        match name {
            "version" => Ok(Self::Version),
            "create_revision" => Ok(Self::CreateRevision),
            "mod_revision" => Ok(Self::ModRevision),
            "lease" => Ok(Self::Lease),
            other => Err(format!(
                "unknown field \"{other}\"; the fields are version, create_revision, mod_revision and lease"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::CreateRevision => "create_revision",
            Self::ModRevision => "mod_revision",
            Self::Lease => "lease",
        }
    }

    fn of(self, item: &Seen) -> i64 {
        match self {
            Self::Version => item.version,
            Self::CreateRevision => item.create_revision,
            Self::ModRevision => item.mod_revision,
            Self::Lease => item.lease,
        }
    }
}

/// A key as the server returned it. The judging below works on this and never
/// on the client's type, so it can be tested without a server.
pub struct Seen<'a> {
    key: &'a [u8],
    value: &'a [u8],
    version: i64,
    create_revision: i64,
    mod_revision: i64,
    lease: i64,
}

impl<'a> From<&'a KeyValue> for Seen<'a> {
    fn from(item: &'a KeyValue) -> Self {
        Self {
            key: item.key(),
            value: item.value(),
            version: item.version(),
            create_revision: item.create_revision(),
            mod_revision: item.mod_revision(),
            lease: item.lease(),
        }
    }
}

/// One row of a `key` / `value` table; with no `value` column only the key
/// is judged.
#[derive(Debug)]
pub struct Row {
    key: String,
    value: Option<String>,
}

/// What a check asks of the one key it reads.
#[derive(Debug)]
pub enum Judge {
    Exists,
    Absent,
    Value(String),
    Matches(Regex),
    Json { expected: Value, strict: bool },
    Fields(Vec<(Field, i64)>),
}

#[derive(Debug)]
pub enum Check {
    Key {
        key: String,
        judge: Judge,
    },
    Count {
        prefix: String,
        expected: u64,
    },
    List {
        prefix: String,
        rows: Vec<Row>,
        exact: bool,
    },
}

pub(crate) fn is_slot(text: &str) -> bool {
    text.contains("<<")
}

fn regex(pattern: &str) -> Result<Regex, String> {
    Regex::new(pattern).map_err(|e| format!("invalid regular expression: {e}"))
}

fn json_doc(docstring: Option<&str>) -> Result<Value, String> {
    let source = docstring.ok_or("the step needs a doc string with the JSON")?;
    bddkit_crate_json::parse_expected(source).map_err(|e| e.to_string())
}

fn number(cell: &str) -> Result<i64, String> {
    cell.parse()
        .map_err(|_| format!("\"{cell}\" is not a whole number"))
}

/// Where each wanted column sits in the header row, the first `required` of
/// them being mandatory. A header the step does not know and a repeated one
/// are refused.
pub(crate) fn columns(
    table: Option<&[Vec<String>]>,
    wanted: &[&str],
    required: usize,
) -> Result<Vec<Option<usize>>, String> {
    let header = table
        .and_then(<[_]>::first)
        .ok_or("the step needs a table")?;
    if let Some(unknown) = header.iter().find(|h| !wanted.contains(&h.as_str())) {
        return Err(format!(
            "unknown table column \"{unknown}\"; the columns are {}",
            wanted.join(" and ")
        ));
    }
    let at: Vec<_> = wanted
        .iter()
        .map(|name| header.iter().position(|h| h == name))
        .collect();
    if header.len() != at.iter().flatten().count() {
        return Err("a column is repeated in the table header".to_string());
    }
    if let Some(missing) = at[..required].iter().position(Option::is_none) {
        return Err(format!("the table needs a \"{}\" column", wanted[missing]));
    }
    Ok(at)
}

fn fields(table: Option<&[Vec<String>]>) -> Result<Vec<(Field, i64)>, String> {
    let at = columns(table, &["field", "value"], 2)?;
    let (name, value) = (at[0].unwrap_or_default(), at[1].unwrap_or_default());
    let rows = &table.unwrap_or_default()[1..];
    if rows.is_empty() {
        return Err("the table has no rows".to_string());
    }
    rows.iter()
        .map(|row| Ok((Field::parse(&row[name])?, number(&row[value])?)))
        .collect()
}

fn list_rows(table: Option<&[Vec<String>]>) -> Result<Vec<Row>, String> {
    let at = columns(table, &["key", "value"], 1)?;
    let key = at[0].unwrap_or_default();
    Ok(table.unwrap_or_default()[1..]
        .iter()
        .map(|row| Row {
            key: row[key].clone(),
            value: at[1].map(|value| row[value].clone()),
        })
        .collect())
}

const EMPTY_PREFIX: &str = "the prefix is empty, and the server refuses a read of the whole keyspace; write the prefix of the keys you mean";

fn key_arg(request: &Request) -> Result<String, StepError> {
    match request.arg(0)? {
        "" => Err(StepError::input("the key is empty")),
        key => Ok(key.to_string()),
    }
}

fn prefix_arg(request: &Request, index: usize) -> Result<String, StepError> {
    match request.arg(index)? {
        "" => Err(StepError::input(EMPTY_PREFIX)),
        prefix => Ok(prefix.to_string()),
    }
}

/// Everything that can be refused without a request, refused here.
pub fn plan(step: Step, request: &Request) -> Result<Check, StepError> {
    let table = request.table.as_deref();
    let input = StepError::input;
    let on_key = |judge| {
        Ok(Check::Key {
            key: key_arg(request)?,
            judge,
        })
    };
    match step {
        Step::Exists => on_key(Judge::Exists),
        Step::Absent => on_key(Judge::Absent),
        Step::HasValue => on_key(Judge::Value(request.arg(1)?.to_string())),
        Step::HasValueDoc => on_key(Judge::Value(doc_value(request.doc()?).to_string())),
        Step::Matches => on_key(Judge::Matches(regex(request.arg(1)?).map_err(input)?)),
        Step::ContainsJson | Step::EqualsJson => on_key(Judge::Json {
            expected: json_doc(request.docstring.as_deref()).map_err(input)?,
            strict: step == Step::EqualsJson,
        }),
        Step::HasFields => on_key(Judge::Fields(fields(table).map_err(input)?)),
        Step::Count => Ok(Check::Count {
            expected: request
                .arg(0)?
                .parse()
                .map_err(|_| input(format!("\"{}\" is not a count", request.args[0])))?,
            prefix: prefix_arg(request, 1)?,
        }),
        Step::KeysBe | Step::KeysContain => {
            let rows = list_rows(table).map_err(input)?;
            if step == Step::KeysContain && rows.is_empty() {
                return Err(StepError::input(EMPTY_CONTAIN));
            }
            Ok(Check::List {
                prefix: prefix_arg(request, 0)?,
                rows,
                exact: step == Step::KeysBe,
            })
        }
        Step::Put
        | Step::PutDocString
        | Step::Get
        | Step::DeleteKey
        | Step::DeletePrefix
        | Step::DeleteAll
        | Step::GetSerializable
        | Step::ComputeHash
        | Step::Leader
        | Step::NotLeader
        | Step::Reached
        | Step::HashIs
        | Step::AcceptPut
        | Step::RefusePut
        | Step::AcceptRead
        | Step::AcceptReadSerializable
        | Step::RefuseRead
        | Step::RefuseReadSerializable
        | Step::RefuseCompact
        | Step::WatchKey
        | Step::WatchPrefix
        | Step::WatchKeyFrom
        | Step::WatchPrefixFrom
        | Step::WatchCancel
        | Step::WatchedPut
        | Step::WatchedDelete
        | Step::WatchedCount
        | Step::WatchedEvents
        | Step::WatchResumed
        | Step::WatchResumedTimes => Err(StepError::input("not an assertion")),
    }
}

const EMPTY_CONTAIN: &str = "the table has no rows, so there is nothing to check";

/// `bddkit_validate_step` for an assertion: what a literal already says. A
/// value that holds a `<<slot>>` is left to the step.
pub fn validate(step: Step, request: &Request) -> Result<(), String> {
    let literal = |index: usize| request.args.get(index).filter(|a| !is_slot(a));
    if step.reads_one_key() && literal(0).is_some_and(String::is_empty) {
        return Err("the key is empty".to_string());
    }
    let prefix = match step {
        Step::Count => literal(1),
        Step::KeysBe | Step::KeysContain => literal(0),
        _ => None,
    };
    if prefix.is_some_and(String::is_empty) {
        return Err(EMPTY_PREFIX.to_string());
    }
    match step {
        Step::Matches => literal(1).map_or(Ok(()), |p| regex(p).map(drop)),
        Step::ContainsJson | Step::EqualsJson => match request.docstring.as_deref() {
            Some(doc) if is_slot(doc) => Ok(()),
            doc => json_doc(doc).map(drop),
        },
        Step::HasValueDoc => request.doc().map(drop).map_err(|e| e.message),
        Step::HasFields | Step::KeysBe | Step::KeysContain => validate_table(step, request),
        _ => Ok(()),
    }
}

fn validate_table(step: Step, request: &Request) -> Result<(), String> {
    let table = request.table.as_deref();
    let wanted = if step == Step::HasFields {
        ["field", "value"]
    } else {
        ["key", "value"]
    };
    let header = table.and_then(<[_]>::first);
    if header.is_some_and(|h| h.iter().any(|c| is_slot(c))) {
        return Ok(());
    }
    let at = columns(table, &wanted, if step == Step::HasFields { 2 } else { 1 })?;
    let rows = &table.unwrap_or_default()[1..];
    match step {
        Step::KeysContain if rows.is_empty() => return Err(EMPTY_CONTAIN.to_string()),
        Step::HasFields => {}
        _ => return Ok(()),
    }
    if rows.is_empty() {
        return Err("the table has no rows".to_string());
    }
    let (name, value) = (at[0].unwrap_or_default(), at[1].unwrap_or_default());
    for row in rows {
        if !is_slot(&row[name]) {
            Field::parse(&row[name])?;
        }
        if !is_slot(&row[value]) {
            number(&row[value])?;
        }
    }
    Ok(())
}

pub fn record_for(check: &Check, instance: &str) -> Record {
    let (target, request) = match check {
        Check::Key { key, .. } => (key.clone(), json!({"key": key})),
        Check::Count { prefix, .. } => (
            format!("{prefix} (prefix)"),
            json!({"prefix": prefix, "count_only": true}),
        ),
        Check::List { prefix, .. } => (format!("{prefix} (prefix)"), json!({"prefix": prefix})),
    };
    Record::new("Get", &target, instance, request)
}

pub(crate) fn miss(message: String) -> StepError {
    StepError::not_yet(Class::Assertion, message)
}

pub(crate) fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A value as text. One that is not UTF-8 cannot be judged, and will not
/// become UTF-8 on a later attempt: that is `fatal`.
fn text<'a>(item: &Seen<'a>, record: &mut Record) -> Result<&'a str, StepError> {
    let text = std::str::from_utf8(item.value).map_err(|_| {
        StepError::assertion(format!(
            "the value of etcd key \"{}\" is not UTF-8, which a text check cannot read (base64: {})",
            lossy(item.key),
            base64(item.value)
        ))
    })?;
    // Before any message quotes it.
    record.scan_text(text);
    Ok(text)
}

pub async fn perform(
    client: &Client,
    tls: bool,
    check: &Check,
    record: &mut Record,
) -> Result<(), StepError> {
    let mut kv = client.kv_client();
    let read = |e| request_failure(&e, tls);
    match check {
        Check::Count { prefix, expected } => {
            let options = GetOptions::new().with_prefix().with_count_only();
            let response = kv.get(prefix.as_str(), Some(options)).await.map_err(read)?;
            record.answered(response.header());
            let found = u64::try_from(response.count()).unwrap_or_default();
            if found == *expected {
                return Ok(());
            }
            Err(miss(format!(
                "etcd holds {found} keys with prefix \"{prefix}\", expected {expected}"
            )))
        }
        Check::List {
            prefix,
            rows,
            exact,
        } => {
            let mut options = GetOptions::new().with_prefix();
            if rows.iter().all(|row| row.value.is_none()) {
                options = options.with_keys_only();
            }
            let response = kv.get(prefix.as_str(), Some(options)).await.map_err(read)?;
            record.answered(response.header());
            let seen: Vec<Seen> = response.kvs().iter().map(Seen::from).collect();
            list(prefix, &seen, rows, *exact, record)
        }
        Check::Key { key, judge } => {
            // The metadata comes with a keys-only read; the value is fetched
            // only for a check that reads it.
            let keys_only = matches!(judge, Judge::Exists | Judge::Absent | Judge::Fields(_));
            let options = keys_only.then(|| GetOptions::new().with_keys_only());
            let response = kv.get(key.as_str(), options).await.map_err(read)?;
            record.answered(response.header());
            let item = response.kvs().first().map(Seen::from);
            if let Some(item) = &item {
                record.keys.push(json!({"key": shown(item.key)}));
            }
            one(judge, key, item.as_ref(), record)
        }
    }
}

/// The checks that read one key.
fn one(
    judge: &Judge,
    key: &str,
    item: Option<&Seen>,
    record: &mut Record,
) -> Result<(), StepError> {
    let Some(item) = item else {
        return match judge {
            Judge::Absent => Ok(()),
            _ => Err(miss(format!("etcd key \"{key}\" does not exist"))),
        };
    };
    match judge {
        Judge::Absent => Err(miss(format!("etcd key \"{key}\" exists"))),
        Judge::Exists => Ok(()),
        Judge::Value(expected) => {
            let actual = text(item, record)?;
            if actual == expected {
                return Ok(());
            }
            Err(miss(format!(
                "etcd key \"{key}\" has value \"{actual}\", expected \"{expected}\""
            )))
        }
        Judge::Matches(regex) => {
            let actual = text(item, record)?;
            if regex.is_match(actual) {
                return Ok(());
            }
            Err(miss(format!(
                "the value of etcd key \"{key}\" (\"{actual}\") does not match /{regex}/"
            )))
        }
        Judge::Json { expected, strict } => {
            let actual = text(item, record)?;
            let parsed: Value = serde_json::from_str(actual).map_err(|e| {
                miss(format!(
                    "the value of etcd key \"{key}\" is not valid JSON: {e}"
                ))
            })?;
            let verdict = if *strict {
                bddkit_crate_json::matcher::equals(&parsed, expected)
            } else {
                bddkit_crate_json::matcher::contains(&parsed, expected)
            };
            verdict.map_err(|mismatch| miss(mismatch.to_string()))
        }
        Judge::Fields(rows) => {
            let wrong: Vec<String> = rows
                .iter()
                .filter(|(field, want)| field.of(item) != *want)
                .map(|(field, want)| {
                    format!("{} is {}, expected {want}", field.name(), field.of(item))
                })
                .collect();
            if wrong.is_empty() {
                return Ok(());
            }
            Err(miss(format!("etcd key \"{key}\": {}", wrong.join("; "))))
        }
    }
}

/// Does `item` hold the row's value? A row with no `value` column judges the
/// key alone.
fn holds(item: &Seen, row: &Row, record: &mut Record) -> Result<(), StepError> {
    let Some(expected) = &row.value else {
        return Ok(());
    };
    let actual = text(item, record)?;
    if actual == expected {
        return Ok(());
    }
    Err(miss(format!(
        "etcd key \"{}\" has value \"{actual}\", expected \"{expected}\"",
        row.key
    )))
}

fn list(
    prefix: &str,
    items: &[Seen],
    rows: &[Row],
    exact: bool,
    record: &mut Record,
) -> Result<(), StepError> {
    record.keys.extend(
        items
            .iter()
            .take(SHOWN_KEYS)
            .map(|item| json!({"key": shown(item.key)})),
    );
    if items.len() > SHOWN_KEYS {
        record
            .keys
            .push(json!({"note": format!("{} more keys not shown", items.len() - SHOWN_KEYS)}));
    }
    if !exact {
        let by_key: HashMap<&[u8], &Seen> = items.iter().map(|i| (i.key, i)).collect();
        for row in rows {
            let Some(item) = by_key.get(row.key.as_bytes()) else {
                return Err(miss(format!(
                    "etcd key \"{}\" is not among the keys with prefix \"{prefix}\"",
                    row.key
                )));
            };
            holds(item, row, record)?;
        }
        return Ok(());
    }
    for (index, (item, row)) in items.iter().zip(rows).enumerate() {
        if item.key != row.key.as_bytes() {
            return Err(miss(format!(
                "key {} under \"{prefix}\" is \"{}\", expected \"{}\"",
                index + 1,
                lossy(item.key),
                row.key
            )));
        }
        holds(item, row, record)?;
    }
    if items.len() == rows.len() {
        return Ok(());
    }
    let (which, name) = match items.get(rows.len()) {
        Some(extra) => ("extra", lossy(extra.key)),
        None => ("missing", rows[items.len()].key.clone()),
    };
    Err(miss(format!(
        "etcd holds {} keys with prefix \"{prefix}\", expected {}; first {which} key: \"{name}\"",
        items.len(),
        rows.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
        rows.iter()
            .map(|r| r.iter().map(ToString::to_string).collect())
            .collect()
    }

    #[test]
    fn the_header_picks_the_columns_in_any_order_and_refuses_the_rest() {
        let t = table(&[&["value", "key"]]);
        assert_eq!(
            columns(Some(&t), &["key", "value"], 1).unwrap(),
            [Some(1), Some(0)]
        );
        let t = table(&[&["key"]]);
        assert_eq!(
            columns(Some(&t), &["key", "value"], 1).unwrap(),
            [Some(0), None]
        );
        let unknown = table(&[&["key", "val"]]);
        assert!(
            columns(Some(&unknown), &["key", "value"], 1)
                .unwrap_err()
                .contains("unknown table column \"val\"")
        );
        let repeated = table(&[&["key", "key"]]);
        assert!(
            columns(Some(&repeated), &["key", "value"], 1)
                .unwrap_err()
                .contains("repeated")
        );
        let no_key = table(&[&["value"]]);
        assert!(
            columns(Some(&no_key), &["key", "value"], 1)
                .unwrap_err()
                .contains("needs a \"key\" column")
        );
        let one = table(&[&["field"]]);
        assert!(
            columns(Some(&one), &["field", "value"], 2)
                .unwrap_err()
                .contains("\"value\" column")
        );
        assert!(
            columns(None, &["key"], 1)
                .unwrap_err()
                .contains("needs a table")
        );
    }

    #[test]
    fn a_field_table_reads_names_and_whole_numbers() {
        let t = table(&[&["value", "field"], &["-1", "lease"], &["7", "version"]]);
        let rows = fields(Some(&t)).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(matches!(rows[0], (Field::Lease, -1)));
        let bad = table(&[&["field", "value"], &["lease", "1.5"]]);
        assert!(
            fields(Some(&bad))
                .unwrap_err()
                .contains("not a whole number")
        );
        let empty = table(&[&["field", "value"]]);
        assert!(fields(Some(&empty)).unwrap_err().contains("no rows"));
    }

    #[test]
    fn a_list_table_without_a_value_column_judges_keys_alone() {
        let t = table(&[&["key"], &["/a"], &["/b"]]);
        let rows = list_rows(Some(&t)).unwrap();
        assert!(rows.iter().all(|r| r.value.is_none()));
        let t = table(&[&["key", "value"], &["/a", ""]]);
        assert_eq!(list_rows(Some(&t)).unwrap()[0].value.as_deref(), Some(""));
    }

    fn seen<'a>(key: &'a str, value: &'a [u8]) -> Seen<'a> {
        Seen {
            key: key.as_bytes(),
            value,
            version: 2,
            create_revision: 5,
            mod_revision: 9,
            lease: 0,
        }
    }

    fn row(key: &str, value: Option<&str>) -> Row {
        Row {
            key: key.to_string(),
            value: value.map(ToString::to_string),
        }
    }

    fn record() -> Record {
        Record::new("Get", "/p/", "main", json!({}))
    }

    fn message(result: Result<(), StepError>) -> String {
        let error = result.expect_err("a mismatch");
        assert!(error.retry, "a mismatch is asked again: {error}");
        assert_eq!(error.class, Class::Assertion);
        error.message
    }

    #[test]
    fn an_exact_listing_follows_the_order_and_names_the_first_difference() {
        let items = [seen("/p/a", b"1"), seen("/p/b", b"2")];
        let rows = [row("/p/a", Some("1")), row("/p/b", Some("2"))];
        assert!(list("/p/", &items, &rows, true, &mut record()).is_ok());
        let swapped = [row("/p/b", Some("2")), row("/p/a", Some("1"))];
        let said = message(list("/p/", &items, &swapped, true, &mut record()));
        assert!(
            said.contains("key 1 under \"/p/\" is \"/p/a\", expected \"/p/b\""),
            "{said}"
        );
        let fewer = message(list("/p/", &items, &rows[..1], true, &mut record()));
        assert!(
            fewer.contains("holds 2 keys") && fewer.contains("first extra key: \"/p/b\""),
            "{fewer}"
        );
        let more = [row("/p/a", None), row("/p/b", None), row("/p/c", None)];
        let missing = message(list("/p/", &items, &more, true, &mut record()));
        assert!(missing.contains("first missing key: \"/p/c\""), "{missing}");
        let empty = message(list("/p/", &items, &[], true, &mut record()));
        assert!(empty.contains("expected 0"), "{empty}");
        assert!(list("/p/", &[], &[], true, &mut record()).is_ok());
    }

    #[test]
    fn a_subset_listing_ignores_order_and_extra_keys() {
        let items = [seen("/p/a", b"1"), seen("/p/b", b"2"), seen("/p/c", b"3")];
        let rows = [row("/p/c", Some("3")), row("/p/a", None)];
        assert!(list("/p/", &items, &rows, false, &mut record()).is_ok());
        let absent = message(list(
            "/p/",
            &items,
            &[row("/p/z", None)],
            false,
            &mut record(),
        ));
        assert!(absent.contains("is not among"), "{absent}");
        let wrong = message(list(
            "/p/",
            &items,
            &[row("/p/a", Some("9"))],
            false,
            &mut record(),
        ));
        assert!(wrong.contains("has value \"1\", expected \"9\""), "{wrong}");
    }

    #[test]
    fn a_value_that_is_not_utf8_is_fatal_only_where_the_value_is_judged() {
        let binary = [seen("/p/a", &[0xff, 0xfe])];
        // No `value` column: the key alone is judged, the bytes are not read.
        assert!(list("/p/", &binary, &[row("/p/a", None)], true, &mut record()).is_ok());
        let error = list(
            "/p/",
            &binary,
            &[row("/p/a", Some("x"))],
            true,
            &mut record(),
        )
        .expect_err("fatal");
        assert!(!error.retry);
        assert_eq!(error.class, Class::Assertion);
        assert!(error.message.contains("not UTF-8") && error.message.contains("//4="));
    }

    #[test]
    fn every_wrong_field_is_named_at_once() {
        let item = seen("/p/a", b"");
        let rows = vec![
            (Field::Version, 2),
            (Field::CreateRevision, 1),
            (Field::ModRevision, 9),
            (Field::Lease, 7),
        ];
        let said = message(one(
            &Judge::Fields(rows),
            "/p/a",
            Some(&item),
            &mut record(),
        ));
        assert!(
            said.contains("create_revision is 5, expected 1; lease is 0, expected 7"),
            "{said}"
        );
        assert!(!said.contains("version is"), "{said}");
    }

    #[test]
    fn a_missing_key_is_not_yet_for_everything_but_the_absence_check() {
        let said = message(one(&Judge::Value("x".into()), "/k", None, &mut record()));
        assert!(said.contains("does not exist"));
        assert!(one(&Judge::Absent, "/k", None, &mut record()).is_ok());
        assert!(one(&Judge::Exists, "/k", Some(&seen("/k", b"")), &mut record()).is_ok());
    }
}
