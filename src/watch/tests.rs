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
    let mut state = State::new("prefix \"/p/\"".to_string(), 100, 1, None);
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
    let mut full = State::new("key \"/k\"".to_string(), 2, 1, None);
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
fn a_stream_that_ends_or_breaks_is_opened_again_and_a_refusal_is_not_a_bad_password() {
    let mut closed = state(vec![]);
    assert_eq!(closed.apply(Ok(None), false), Next::Reopen);
    assert!(closed.failure().is_none(), "a break is not final");
    assert_eq!(
        closed.broken.as_ref().expect("down").class,
        Class::DbConnection
    );
    let mut refused = state(vec![]);
    let status = tonic::Status::new(tonic::Code::InvalidArgument, "etcdserver: nope");
    let error = etcd_client::Error::GRpcStatus(status);
    assert_eq!(refused.apply(Err(error), false), Next::Reopen);
    assert!(refused.failure().is_none());
    let broken = refused.broken.as_ref().expect("down");
    assert_eq!(broken.class, Class::DbStatement);
    assert!(
        broken.message.contains("answered with an error"),
        "{}",
        broken.message
    );
    assert!(
        !broken.message.contains("credentials"),
        "{}",
        broken.message
    );
    assert_eq!(refused.last_error.as_deref(), Some(broken.message.as_str()));
}

#[test]
fn only_a_compaction_or_a_permission_ends_a_watch_for_good_and_a_missing_leader_does_not() {
    assert!(cancel_is_final(
        12,
        "etcdserver: mvcc: required revision has been compacted"
    ));
    assert!(cancel_is_final(0, "etcdserver: permission denied"));
    assert!(!cancel_is_final(0, "etcdserver: no leader"));
}

#[test]
fn a_watch_that_ended_reads_and_counts_nothing_more() {
    let mut ended = state(vec![put("/p/a", "1", 5)]);
    ended.fail(Class::DbStatement, "the server cancelled the watch: gone");
    assert_eq!(ended.apply(Ok(None), false), Next::Stop);
    assert!(ended.broken.is_none() && ended.resumes == 0);
    assert_eq!(ended.describe()["reconnecting"], false);
}

#[test]
fn a_resume_starts_after_the_last_event_and_else_where_the_watch_began() {
    assert_eq!(
        state(vec![]).resume_from(),
        2,
        "the confirmation's revision + 1"
    );
    assert_eq!(state(vec![put("/p/a", "1", 5)]).resume_from(), 6);
    let mut replay = State::new("key \"/k\"".to_string(), 100, 9, Some(4));
    assert_eq!(replay.resume_from(), 4, "the replay may not have arrived");
    replay.accept(vec![put("/k", "1", 4)]);
    assert_eq!(replay.resume_from(), 5);
}

#[test]
fn the_resumes_are_not_yet_below_and_final_above_and_a_miss_says_the_stream_is_down() {
    let mut watch = state(vec![]);
    let at_least = Expect::Resumed(None);
    let twice = Expect::Resumed(Some(2));
    assert!(judged(&watch, &at_least).expect_err("never broke").retry);
    watch.broke(Class::DbConnection, "the server closed the watch stream");
    let down = judged(&watch, &at_least).expect_err("still down");
    assert!(down.retry && down.class == Class::DbConnection, "{down}");
    assert!(down.message.contains("being opened again"), "{down}");
    // The server confirmed a new stream.
    watch.resumes = 1;
    watch.broken = None;
    assert!(judged(&watch, &at_least).is_ok());
    assert!(judged(&watch, &Expect::Resumed(Some(1))).is_ok());
    assert!(judged(&watch, &twice).expect_err("fewer").retry);
    watch.resumes = 3;
    let more = judged(&watch, &twice).expect_err("more");
    assert!(!more.retry && more.class == Class::Assertion, "{more}");
    // What the watch kept of the break is in its context.
    assert_eq!(watch.describe()["resumes"], 3);
    assert_eq!(watch.describe()["reconnecting"], false);
    assert!(watch.describe()["last_error"].is_string());
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
