//! The watch steps through the exports, the way the host dispatches them, on a
//! live server at `BDDKIT_TEST_ETCD` (or `http://127.0.0.1:2379`).
//!
//! Every test owns a prefix. The one that compacts the server takes the write
//! side of `SERVER`, the others the read side: a compaction would turn a
//! replay of another test into a refusal.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use std::sync::RwLock;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bddkit_etcd::*;
use common::{Handle, block_on, call, cstr, etcd_endpoint};
use serde_json::{Value, json};

static SERVER: RwLock<()> = RwLock::new(());

const PUT: u32 = 0;
const DELETE_KEY: u32 = 3;
const DELETE_PREFIX: u32 = 4;
const WATCH_KEY: u32 = 30;
const WATCH_PREFIX: u32 = 31;
const WATCH_KEY_FROM: u32 = 32;
const WATCH_PREFIX_FROM: u32 = 33;
const CANCEL: u32 = 34;
const RECEIVED_PUT: u32 = 35;
const RECEIVED_DELETE: u32 = 36;
const RECEIVED_COUNT: u32 = 37;
const RECEIVED_EVENTS: u32 = 38;

fn prefix(test: &str) -> String {
    format!("/bddkit-etcd/watch-{test}/{}/", std::process::id())
}

fn passed(reply: &Value) -> &Value {
    assert_eq!(reply["status"], "passed", "{reply}");
    &reply["vars"]
}

fn revision(handle: &Handle, step: u32, args: &[&str]) -> i64 {
    passed(&handle.run(step, args))["etcd_revision"]
        .as_str()
        .expect("etcd_revision")
        .parse()
        .expect("a number")
}

fn events(handle: &Handle, name: &str, table: &[&[&str]]) -> Value {
    handle.send(RECEIVED_EVENTS, &[name], None, Some(table), false)
}

/// The assertion, asked again the way the host's polling asks it, until it
/// passes or the time is up; the last answer.
fn eventually(mut ask: impl FnMut() -> Value) -> Value {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let reply = ask();
        if reply["status"] == "passed" || Instant::now() > until {
            return reply;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn tasks(handle: &Handle) -> usize {
    watch_tasks(handle.0)
        .expect("handle")
        .load(Ordering::SeqCst)
}

#[test]
fn a_write_in_the_step_right_after_the_watch_is_in_its_history() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("race"));
    // The window between "subscribed" and "listening" must not exist.
    for round in 1..=20 {
        let name = format!("w{round}");
        let vars = passed(&handle.run(WATCH_KEY, &[&key, &name])).clone();
        let at: i64 = vars[format!("etcd_watch_{name}_revision")]
            .as_str()
            .expect("the revision of the confirmation")
            .parse()
            .expect("a number");
        let written = revision(&handle, PUT, &[&key, "v"]);
        assert!(written > at, "{written} after {at}");
        let reply = eventually(|| handle.run(RECEIVED_PUT, &[&name, &key, "v"]));
        assert_eq!(reply["status"], "passed", "round {round}: {reply}");
    }
    passed(&handle.run(DELETE_KEY, &[&key]));
}

#[test]
fn a_key_watch_and_a_prefix_watch_see_puts_and_deletes_with_the_previous_value() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let base = prefix("kinds");
    let (a, b) = (format!("{base}a"), format!("{base}b"));
    passed(&handle.run(WATCH_KEY, &[&a, "one"]));
    passed(&handle.run(WATCH_PREFIX, &[&base, "all"]));
    let first = revision(&handle, PUT, &[&a, "1"]);
    let second = revision(&handle, PUT, &[&b, "x"]);
    let third = revision(&handle, PUT, &[&a, "2"]);
    passed(&handle.run(DELETE_KEY, &[&a]));
    let (first, second, third) = (first.to_string(), second.to_string(), third.to_string());

    let reply = eventually(|| handle.run(RECEIVED_COUNT, &["all", "4"]));
    assert_eq!(reply["status"], "passed", "{reply}");
    let table: &[&[&str]] = &[
        &[
            "type",
            "key",
            "value",
            "prev_value",
            "mod_revision",
            "version",
        ],
        &["PUT", &a, "1", "", &first, "1"],
        &["PUT", &b, "x", "", &second, "1"],
        &["PUT", &a, "2", "1", &third, "2"],
    ];
    // The prefix watch holds a fourth event, so the whole table is too short.
    let short = events(&handle, "all", table);
    assert_eq!(short["status"], "fatal", "{short}");
    assert!(
        short["error"]
            .as_str()
            .unwrap()
            .contains("first extra one is a DELETE")
    );
    let header_only = events(&handle, "all", &table[..1]);
    assert_eq!(header_only["class"], "input", "{header_only}");

    // The key watch saw the writes to its key only.
    let reply = eventually(|| handle.run(RECEIVED_COUNT, &["one", "3"]));
    assert_eq!(reply["status"], "passed", "{reply}");
    assert_eq!(
        handle.run(RECEIVED_PUT, &["one", &a, "2"])["status"],
        "passed"
    );
    assert_eq!(
        handle.run(RECEIVED_DELETE, &["one", &a])["status"],
        "passed"
    );
    assert_eq!(
        handle.run(RECEIVED_DELETE, &["all", &b])["status"],
        "not_yet"
    );
    assert_eq!(
        handle.run(RECEIVED_PUT, &["one", &b, "x"])["status"],
        "not_yet"
    );
    let ordered: &[&[&str]] = &[
        &["type", "key", "value", "prev_value"],
        &["PUT", &a, "1", ""],
        &["PUT", &a, "2", "1"],
        &["DELETE", &a, "", "2"],
    ];
    assert_eq!(events(&handle, "one", ordered)["status"], "passed");
    // Fewer rows than events is final; more rows is a wait.
    let fewer = events(&handle, "one", &ordered[..3]);
    assert_eq!(fewer["status"], "fatal", "{fewer}");
    let extra: &[&str] = &["PUT", &a, "3", ""];
    let mut more = ordered.to_vec();
    more.push(extra);
    let more = events(&handle, "one", &more);
    assert_eq!(more["status"], "not_yet", "{more}");
    passed(&handle.run(DELETE_PREFIX, &[&base]));
}

#[test]
fn from_revision_replays_the_writes_made_before_the_watch() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let base = prefix("replay");
    let key = format!("{base}k");
    let first = revision(&handle, PUT, &[&key, "old"]);
    revision(&handle, PUT, &[&key, "newer"]);
    passed(&handle.run(WATCH_KEY_FROM, &[&key, "since", &first.to_string()]));
    passed(&handle.run(
        WATCH_PREFIX_FROM,
        &[&base, "since_prefix", &first.to_string()],
    ));
    for name in ["since", "since_prefix"] {
        let table: &[&[&str]] = &[
            &["type", "value", "prev_value"],
            &["PUT", "old", ""],
            &["PUT", "newer", "old"],
        ];
        let reply = eventually(|| events(&handle, name, table));
        assert_eq!(reply["status"], "passed", "{name}: {reply}");
    }
    passed(&handle.run(DELETE_KEY, &[&key]));
}

#[test]
fn a_watch_from_a_compacted_revision_fails_the_step() {
    let _server = SERVER.write().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("compacted"));
    let old = revision(&handle, PUT, &[&key, "1"]);
    let now = revision(&handle, PUT, &[&key, "2"]);
    block_on(async {
        let client = etcd_client::Client::connect([etcd_endpoint()], None)
            .await
            .expect("connect");
        client
            .kv_client()
            .compact(now, None)
            .await
            .expect("compact");
    });
    let reply = handle.run(WATCH_KEY_FROM, &[&key, "gone", &old.to_string()]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement");
    assert!(
        reply["error"].as_str().unwrap().contains("compacted"),
        "{reply}"
    );
    // The name was not taken by the watch that never opened.
    assert_eq!(handle.run(CANCEL, &["gone"])["class"], "input");
    // From the compaction itself the replay is still possible.
    passed(&handle.run(WATCH_KEY_FROM, &[&key, "kept", &now.to_string()]));
    let reply = eventually(|| handle.run(RECEIVED_PUT, &["kept", &key, "2"]));
    assert_eq!(reply["status"], "passed", "{reply}");
    passed(&handle.run(DELETE_KEY, &[&key]));
}

#[test]
fn a_full_buffer_ends_the_watch_and_names_the_setting() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::new(&json!({"endpoints": etcd_endpoint(), "watch_buffer_events": 3}));
    let base = prefix("full");
    passed(&handle.run(WATCH_PREFIX, &[&base, "small"]));
    for n in 0..4 {
        passed(&handle.run(PUT, &[&format!("{base}{n}"), "v"]));
    }
    let reply = eventually(|| {
        let reply = handle.run(RECEIVED_COUNT, &["small", "3"]);
        // Still filling: the third event has not arrived, or the fourth has.
        if reply["status"] == "passed" {
            json!({"status": "wait"})
        } else {
            reply
        }
    });
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert!(
        reply["error"]
            .as_str()
            .unwrap()
            .contains("watch_buffer_events (3)"),
        "{reply}"
    );
    // Nothing was dropped to make room: the three events are still there.
    let context = handle.context();
    let shown = context["diagnostics"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert!(
        shown["content"].as_str().unwrap().contains("\"events\": 3"),
        "{shown}"
    );
    passed(&handle.run(DELETE_PREFIX, &[&base]));
}

#[test]
fn two_handles_with_the_same_watch_name_do_not_see_each_others_events() {
    let _server = SERVER.read().unwrap();
    let (one, two) = (Handle::live(), Handle::live());
    let base = prefix("isolated");
    let (a, b) = (format!("{base}a"), format!("{base}b"));
    passed(&one.run(WATCH_KEY, &[&a, "same"]));
    passed(&two.run(WATCH_KEY, &[&b, "same"]));
    passed(&one.run(PUT, &[&a, "1"]));
    passed(&two.run(PUT, &[&b, "2"]));
    assert_eq!(
        eventually(|| one.run(RECEIVED_PUT, &["same", &a, "1"]))["status"],
        "passed"
    );
    assert_eq!(
        eventually(|| two.run(RECEIVED_PUT, &["same", &b, "2"]))["status"],
        "passed"
    );
    assert_eq!(one.run(RECEIVED_COUNT, &["same", "1"])["status"], "passed");
    assert_eq!(
        two.run(RECEIVED_PUT, &["same", &a, "1"])["status"],
        "not_yet"
    );
    passed(&one.run(DELETE_KEY, &[&a]));
    passed(&two.run(DELETE_KEY, &[&b]));
}

#[test]
fn no_watch_task_outlives_a_reset_or_a_drop() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let base = prefix("tasks");
    passed(&handle.run(WATCH_PREFIX, &[&base, "one"]));
    passed(&handle.run(WATCH_KEY, &[&format!("{base}k"), "two"]));
    assert_eq!(tasks(&handle), 2);
    assert_eq!(call(bddkit_reset_scenario(handle.0))["ok"], true);
    assert_eq!(tasks(&handle), 0, "after the scenario boundary");
    // The names are free again, and the old history is gone.
    passed(&handle.run(WATCH_PREFIX, &[&base, "one"]));
    assert_eq!(
        handle.run(RECEIVED_COUNT, &["one", "0"])["status"],
        "passed"
    );
    assert_eq!(tasks(&handle), 1);
    let counter = watch_tasks(handle.0).expect("handle");
    drop(handle);
    assert_eq!(counter.load(Ordering::SeqCst), 0, "after the drop");
}

#[test]
fn a_watch_outlives_the_clients_request_timeout() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::new(&json!({"endpoints": etcd_endpoint(), "timeout_secs": 1}));
    let key = format!("{}k", prefix("timeout"));
    passed(&handle.run(WATCH_KEY, &[&key, "long"]));
    // Three times the limit of one request.
    std::thread::sleep(Duration::from_secs(3));
    passed(&handle.run(PUT, &[&key, "late"]));
    let reply = eventually(|| handle.run(RECEIVED_PUT, &["long", &key, "late"]));
    assert_eq!(reply["status"], "passed", "{reply}");
    passed(&handle.run(DELETE_KEY, &[&key]));
}

#[test]
fn names_are_checked_before_any_server_is_asked() {
    let _server = SERVER.read().unwrap();
    // Nothing listens there: a step that needed a connection would be a
    // `db_connection` failure.
    let down = Handle::new(&json!({"endpoints": "http://127.0.0.1:1"}));
    for (step, args) in [
        (RECEIVED_COUNT, vec!["nope", "1"]),
        (RECEIVED_PUT, vec!["nope", "/k", "v"]),
        (CANCEL, vec!["nope"]),
    ] {
        let reply = down.run(step, &args);
        assert_eq!(reply["status"], "fatal", "{reply}");
        assert_eq!(reply["class"], "input", "{reply}");
    }
    let unreachable = down.run(WATCH_KEY, &["/k", "w"]);
    assert_eq!(unreachable["class"], "db_connection", "{unreachable}");
    let handle = Handle::live();
    let key = format!("{}k", prefix("names"));
    passed(&handle.run(WATCH_KEY, &[&key, "taken"]));
    let again = handle.run(WATCH_PREFIX, &[&key, "taken"]);
    assert_eq!(
        (again["status"].as_str(), again["class"].as_str()),
        (Some("fatal"), Some("input"))
    );
    assert_eq!(handle.run(WATCH_KEY, &["", "x"])["class"], "input");
    assert_eq!(
        handle.run(WATCH_KEY, &[&key, "not a name"])["class"],
        "input"
    );
}

#[test]
fn a_cancelled_watch_keeps_its_history_and_receives_nothing_more() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("cancel"));
    let opened = handle.run(WATCH_KEY, &[&key, "w"]);
    assert_eq!(
        opened["diagnostics"][0]["title"], "etcd Watch w",
        "{opened}"
    );
    passed(&opened);
    passed(&handle.run(PUT, &[&key, "1"]));
    assert_eq!(
        eventually(|| handle.run(RECEIVED_COUNT, &["w", "1"]))["status"],
        "passed"
    );
    let cancelled = handle.run(CANCEL, &["w"]);
    assert_eq!(
        cancelled["diagnostics"][0]["title"], "etcd CancelWatch w",
        "{cancelled}"
    );
    passed(&cancelled);
    assert_eq!(tasks(&handle), 0);
    // Nothing more can arrive, so a miss is final, not a wait.
    assert_eq!(handle.run(RECEIVED_COUNT, &["w", "2"])["status"], "fatal");
    passed(&handle.run(PUT, &[&key, "2"]));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(handle.run(RECEIVED_COUNT, &["w", "1"])["status"], "passed");
    assert_eq!(
        handle.run(CANCEL, &["w"])["class"],
        "input",
        "cancelled once"
    );
    assert_eq!(
        handle.run(WATCH_KEY, &[&key, "w"])["class"],
        "input",
        "the name stays taken"
    );
    passed(&handle.run(DELETE_KEY, &[&key]));
}

#[test]
fn a_table_that_cannot_work_is_refused_before_the_run() {
    let validate =
        |step: u32, request: Value| call(bddkit_validate_step(step, cstr(&request).as_ptr()));
    let table = |rows: Value| json!({"args": ["w"], "table": rows});
    assert_eq!(
        validate(
            RECEIVED_EVENTS,
            table(json!([["type", "key"], ["PUT", "/k"]]))
        )["ok"],
        true
    );
    let unknown = validate(
        RECEIVED_EVENTS,
        table(json!([["type", "revision"], ["PUT", "1"]])),
    );
    assert_eq!(unknown["ok"], false, "{unknown}");
    assert!(
        unknown["error"]
            .as_str()
            .unwrap()
            .contains("unknown table column \"revision\"")
    );
    let kind = validate(RECEIVED_EVENTS, table(json!([["type"], ["put"]])));
    assert_eq!(kind["ok"], false, "{kind}");
    // A slot is judged when the step runs.
    assert_eq!(
        validate(RECEIVED_EVENTS, table(json!([["type"], ["<<t>>"]])))["ok"],
        true
    );
    assert_eq!(
        validate(WATCH_KEY, json!({"args": ["/k", "bad name"]}))["ok"],
        false
    );
}

#[test]
fn a_failing_step_dump_shows_every_watch() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("context"));
    passed(&handle.run(WATCH_KEY, &[&key, "shown"]));
    passed(&handle.run(PUT, &[&key, "{\"password\":\"hunter2hunter2\"}"]));
    assert_eq!(
        eventually(|| handle.run(RECEIVED_COUNT, &["shown", "1"]))["status"],
        "passed"
    );
    let context = handle.context();
    let watch = context["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["title"] == "etcd watch shown")
        .unwrap_or_else(|| panic!("no watch in {context}"));
    let dumped: Value = serde_json::from_str(watch["content"].as_str().unwrap()).unwrap();
    assert_eq!(dumped["state"], "running");
    assert_eq!(dumped["events"], 1);
    assert_eq!(dumped["last_events"][0]["type"], "PUT");
    // The value goes to the host's scan before it is shown.
    assert!(!context["scan"].as_array().unwrap().is_empty(), "{context}");
    passed(&handle.run(DELETE_KEY, &[&key]));
}
