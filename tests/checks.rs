//! The key assertions through the exports, the way the host dispatches them,
//! on a live server at `BDDKIT_TEST_ETCD` (or `http://127.0.0.1:2379`). One
//! reply is one attempt: that a polled assertion runs out in the right class
//! is proven end to end in `e2e.rs`, where the host does the polling.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use bddkit_etcd::*;
use common::{Handle, call, cstr};
use serde_json::{Value, json};

const PUT: u32 = 0;
const DELETE_PREFIX: u32 = 4;
const EXISTS: u32 = 6;
const ABSENT: u32 = 7;
const HAS_VALUE: u32 = 8;
const HAS_VALUE_DOC: u32 = 9;
const MATCHES: u32 = 10;
const CONTAINS_JSON: u32 = 11;
const EQUALS_JSON: u32 = 12;
const HAS_FIELDS: u32 = 13;
const COUNT: u32 = 14;
const KEYS_BE: u32 = 15;
const KEYS_CONTAIN: u32 = 16;

fn prefix(test: &str) -> String {
    format!("/bddkit-etcd/checks/{test}/{}/", std::process::id())
}

/// A prefix with the keys `(name, value)` under it.
fn seeded(test: &str, keys: &[(&str, &str)]) -> (Handle, String) {
    let handle = Handle::live();
    let base = prefix(test);
    handle.run(DELETE_PREFIX, &[&base]);
    for (name, value) in keys {
        let reply = handle.run(PUT, &[&format!("{base}{name}"), value]);
        assert_eq!(reply["status"], "passed", "{reply}");
    }
    (handle, base)
}

fn passes(reply: &Value) {
    assert_eq!(reply["status"], "passed", "{reply}");
}

/// `not_yet`, class `assertion`, with a message holding `fragment`.
fn not_yet(reply: &Value, fragment: &str) {
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "assertion", "{reply}");
    let error = reply["error"].as_str().expect("error");
    assert!(error.contains(fragment), "{fragment}\n{error}");
}

fn fatal(reply: &Value, class: &str, fragment: &str) {
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], class, "{reply}");
    let error = reply["error"].as_str().expect("error");
    assert!(error.contains(fragment), "{fragment}\n{error}");
}

fn validated(step: u32, request: &Value) -> Value {
    call(bddkit_validate_step(step, cstr(request).as_ptr()))
}

fn refusal(step: u32, request: &Value) -> String {
    let reply = validated(step, request);
    assert_eq!(reply["ok"], false, "{reply}");
    reply["error"].as_str().expect("error").to_string()
}

#[test]
fn a_key_exists_or_does_not_and_the_attempt_after_a_write_sees_it() {
    let (handle, base) = seeded("exists", &[("k", "v")]);
    let (here, gone) = (format!("{base}k"), format!("{base}gone"));
    passes(&handle.run(EXISTS, &[&here]));
    passes(&handle.run(ABSENT, &[&gone]));
    not_yet(&handle.run(EXISTS, &[&gone]), "does not exist");
    not_yet(&handle.run(ABSENT, &[&here]), "exists");
    // The same step, asked again after the key was written: it passes.
    handle.run(PUT, &[&gone, "now"]);
    passes(&handle.run(EXISTS, &[&gone]));
    fatal(&handle.run(EXISTS, &[""]), "input", "the key is empty");
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn a_value_is_compared_as_exact_text_and_a_missing_key_is_not_yet() {
    let (handle, base) = seeded("value", &[("k", "30s")]);
    let key = format!("{base}k");
    passes(&handle.run(HAS_VALUE, &[&key, "30s"]));
    not_yet(
        &handle.run(HAS_VALUE, &[&key, "30"]),
        "has value \"30s\", expected \"30\"",
    );
    // A missing key is `not_yet`, never the fatal of `I get`: the key may
    // arrive while the host polls.
    not_yet(
        &handle.run(HAS_VALUE, &[&format!("{base}later"), "x"]),
        "does not exist",
    );
    let doc = format!("{base}doc");
    handle.dispatch(1, &[&doc], Some("\nline 1\n  line 2\n"), false);
    passes(&handle.dispatch(HAS_VALUE_DOC, &[&doc], Some("\nline 1\n  line 2\n"), false));
    not_yet(
        &handle.dispatch(HAS_VALUE_DOC, &[&doc], Some("\nline 1\n"), false),
        "expected \"line 1\"",
    );
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn a_value_that_is_not_utf8_is_fatal_for_every_text_check() {
    let (handle, base) = seeded("binary", &[]);
    let key = format!("{base}binary");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let mut client = etcd_client::Client::connect([common::etcd_endpoint()], None)
            .await
            .expect("connect");
        client
            .put(key.as_str(), vec![0xff, 0xfe], None)
            .await
            .expect("put");
    });
    passes(&handle.run(EXISTS, &[&key]));
    for (step, args) in [
        (HAS_VALUE, vec![key.as_str(), "x"]),
        (MATCHES, vec![key.as_str(), "x"]),
    ] {
        let reply = handle.run(step, &args);
        fatal(&reply, "assertion", "not UTF-8");
        assert!(reply["error"].as_str().unwrap().contains("//4="), "{reply}");
    }
    fatal(
        &handle.dispatch(CONTAINS_JSON, &[&key], Some("{}"), false),
        "assertion",
        "not UTF-8",
    );
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn a_regular_expression_is_not_anchored_and_a_bad_one_is_refused_early() {
    let (handle, base) = seeded("regex", &[("k", "build 42 done")]);
    let key = format!("{base}k");
    passes(&handle.run(MATCHES, &[&key, r"\d+"]));
    passes(&handle.run(MATCHES, &[&key, "^build"]));
    not_yet(&handle.run(MATCHES, &[&key, "^done"]), "does not match");
    // Literal: stopped before the first request. From a slot: the step.
    let request =
        |pattern: &str| json!({"args": ["/k", pattern], "docstring": null, "table": null});
    assert!(refusal(MATCHES, &request("(")).contains("invalid regular expression"));
    assert_eq!(validated(MATCHES, &request("<<pattern>>"))["ok"], true);
    fatal(
        &handle.run(MATCHES, &[&key, "("]),
        "input",
        "invalid regular expression",
    );
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn json_contains_is_loose_and_equals_is_strict() {
    let (handle, base) = seeded("json", &[("k", r#"{"id": 1, "name": "x", "extra": true}"#)]);
    let key = format!("{base}k");
    let json = |step, doc: &str| handle.dispatch(step, &[&key], Some(doc), false);
    passes(&json(CONTAINS_JSON, r#"{"id": 1}"#));
    passes(&json(CONTAINS_JSON, r#"{"name": "@regExp(/^x$/)"}"#));
    not_yet(&json(CONTAINS_JSON, r#"{"id": 2}"#), "root.id");
    passes(&json(
        EQUALS_JSON,
        r#"{"id": 1, "name": "x", "extra": true}"#,
    ));
    not_yet(&json(EQUALS_JSON, r#"{"id": 1}"#), "root");
    // A stored value that is not JSON may change: `not_yet`.
    handle.run(PUT, &[&key, "plain"]);
    not_yet(&json(CONTAINS_JSON, "{}"), "not valid JSON");
    // The expected JSON that is not JSON is the tester's: `input`.
    fatal(
        &json(CONTAINS_JSON, "{nope"),
        "input",
        "expected JSON is invalid",
    );
    let request = |doc: &str| json!({"args": ["/k"], "docstring": doc, "table": null});
    assert!(refusal(CONTAINS_JSON, &request("{nope")).contains("expected JSON is invalid"));
    assert_eq!(validated(EQUALS_JSON, &request("<<doc>>"))["ok"], true);
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn the_metadata_of_a_key_is_compared_field_by_field() {
    let (handle, base) = seeded("fields", &[("k", "a")]);
    let key = format!("{base}k");
    let first = handle.run(PUT, &[&key, "b"]);
    let revision = first["vars"]["etcd_revision"].as_str().unwrap().to_string();
    let table = |rows: &[&[&str]]| {
        let mut all: Vec<&[&str]> = vec![&["field", "value"]];
        all.extend_from_slice(rows);
        handle.send(HAS_FIELDS, &[&key], None, Some(&all), false)
    };
    passes(&table(&[
        &["version", "2"],
        &["mod_revision", &revision],
        &["lease", "0"],
    ]));
    let wrong = table(&[
        &["version", "1"],
        &["lease", "0"],
        &["create_revision", "1"],
    ]);
    not_yet(&wrong, "version is 2, expected 1");
    not_yet(&wrong, "create_revision is");
    fatal(
        &table(&[&["colour", "1"]]),
        "input",
        "unknown field \"colour\"",
    );
    fatal(
        &table(&[&["version", "two"]]),
        "input",
        "not a whole number",
    );
    fatal(&table(&[]), "input", "no rows");
    let bad_header = handle.send(HAS_FIELDS, &[&key], None, Some(&[&["field", "val"]]), false);
    fatal(&bad_header, "input", "unknown table column \"val\"");
    // Literal mistakes stop the run; a slot is left to the step.
    let request = |rows: Value| json!({"args": ["/k"], "docstring": null, "table": rows});
    assert!(
        refusal(
            HAS_FIELDS,
            &request(json!([["field", "value"], ["colour", "1"]]))
        )
        .contains("unknown field")
    );
    assert!(
        refusal(
            HAS_FIELDS,
            &request(json!([["field", "val"], ["version", "1"]]))
        )
        .contains("unknown table column")
    );
    assert_eq!(
        validated(
            HAS_FIELDS,
            &request(json!([["field", "value"], ["version", "<<v>>"]]))
        )["ok"],
        true
    );
    assert!(
        refusal(
            HAS_FIELDS,
            &json!({"args": ["/k"], "docstring": null, "table": null})
        )
        .contains("needs a table")
    );
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn the_server_counts_the_keys_under_a_prefix() {
    let (handle, base) = seeded("count", &[("a", "1"), ("b", "2"), ("c", "3")]);
    passes(&handle.run(COUNT, &["3", &base]));
    passes(&handle.run(COUNT, &["0", &format!("{base}zzz")]));
    not_yet(&handle.run(COUNT, &["2", &base]), "holds 3 keys");
    fatal(&handle.run(COUNT, &["many", &base]), "input", "not a count");
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn a_listing_is_exact_in_key_order_or_a_subset() {
    let (handle, base) = seeded("list", &[("b", "2"), ("a", "1"), ("c", "3")]);
    let list = |step, rows: &[&[&str]]| {
        let mut table: Vec<&[&str]> = vec![&["key", "value"]];
        table.extend_from_slice(rows);
        handle.send(step, &[&base], None, Some(&table), false)
    };
    let (a, b, c) = (format!("{base}a"), format!("{base}b"), format!("{base}c"));
    passes(&list(KEYS_BE, &[&[&a, "1"], &[&b, "2"], &[&c, "3"]]));
    // The table order is the server's key order, not the order of writing.
    not_yet(
        &list(KEYS_BE, &[&[&b, "2"], &[&a, "1"], &[&c, "3"]]),
        "key 1 under",
    );
    not_yet(
        &list(KEYS_BE, &[&[&a, "1"], &[&b, "2"]]),
        "expected 2; first extra key",
    );
    not_yet(
        &list(KEYS_BE, &[&[&a, "1"], &[&b, "2"], &[&c, "3"], &["/z", "4"]]),
        "first missing key",
    );
    not_yet(
        &list(KEYS_BE, &[&[&a, "1"], &[&b, "9"], &[&c, "3"]]),
        "has value \"2\", expected \"9\"",
    );
    passes(&list(KEYS_CONTAIN, &[&[&c, "3"], &[&a, "1"]]));
    not_yet(
        &list(KEYS_CONTAIN, &[&[&a, "1"], &["/z", "1"]]),
        "is not among",
    );
    not_yet(
        &list(KEYS_CONTAIN, &[&[&a, "2"]]),
        "has value \"1\", expected \"2\"",
    );
    // Without a `value` column only the keys are judged.
    let keys_only = |step, names: &[&str]| {
        let mut table: Vec<&[&str]> = vec![&["key"]];
        let rows: Vec<[&str; 1]> = names.iter().map(|n| [*n]).collect();
        table.extend(rows.iter().map(|r| &r[..]));
        handle.send(step, &[&base], None, Some(&table), false)
    };
    passes(&keys_only(KEYS_BE, &[&a, &b, &c]));
    // An empty table is an empty prefix.
    let none = format!("{base}none/");
    passes(&handle.send(KEYS_BE, &[&none], None, Some(&[&["key"]]), false));
    fatal(
        &handle.send(KEYS_BE, &[&base], None, Some(&[&["value"]]), false),
        "input",
        "needs a \"key\" column",
    );
    fatal(
        &handle.send(KEYS_BE, &[&base], None, Some(&[&["key", "key"]]), false),
        "input",
        "repeated",
    );
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn a_dead_server_is_not_yet_db_connection_but_an_action_stays_fatal() {
    let handle = Handle::new(&json!({"endpoints": "http://127.0.0.1:1", "timeout_secs": 2}));
    let reply = handle.run(EXISTS, &["/k"]);
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "db_connection", "{reply}");
    // The host fails an action that answers `not_yet`.
    let reply = handle.run(PUT, &["/k", "v"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_connection", "{reply}");
    let silent = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = format!("http://{}", silent.local_addr().expect("addr"));
    let handle = Handle::new(&json!({"endpoints": endpoint, "timeout_secs": 1}));
    let reply = handle.run(COUNT, &["0", "/k"]);
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["error"], "no response within 1 s");
}

#[test]
fn a_status_from_the_server_is_not_yet_db_statement() {
    let handle = Handle::live();
    // Over the server's request size limit: refused with a status.
    let key = "k".repeat(3 * 1024 * 1024);
    let reply = handle.run(EXISTS, &[&key]);
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "db_statement", "{reply}");
}

#[test]
fn a_check_leaves_its_read_to_the_failure_dump_and_scans_a_json_value() {
    let (handle, base) = seeded("evidence", &[("k", r#"{"password":"hunter2hunter2"}"#)]);
    let key = format!("{base}k");
    let reply = handle.run(HAS_VALUE, &[&key, "x"]);
    assert_eq!(reply["scan"].as_array().unwrap().len(), 1, "{reply}");
    let title = reply["diagnostics"][0]["title"].as_str().unwrap();
    assert_eq!(title, format!("etcd Get {key}"));
    let context = handle.context();
    assert_eq!(context["diagnostics"][0]["title"], title);
    handle.run(DELETE_PREFIX, &[&base]);
}

#[test]
fn an_empty_prefix_is_input_for_every_read_of_keys_literal_or_from_a_slot() {
    let handle = Handle::live();
    let keys: &[&[&str]] = &[&["key"], &["/k"]];
    // (step, index of the prefix in the arguments, the arguments around it)
    for (step, args, at) in [
        (COUNT, ["0", ""], 1),
        (KEYS_BE, ["", ""], 0),
        (KEYS_CONTAIN, ["", ""], 0),
    ] {
        let request = json!({"args": &args[..=at], "docstring": null, "table": keys});
        assert!(refusal(step, &request).contains("the prefix is empty"));
        let slot = json!({"args": if at == 1 { ["0", "<<p>>"] } else { ["<<p>>", ""] }, "docstring": null, "table": keys});
        assert_eq!(validated(step, &slot)["ok"], true);
        let reply = handle.send(step, &args[..=at], None, Some(keys), false);
        fatal(&reply, "input", "the prefix is empty");
    }
}

#[test]
fn a_contain_table_with_no_rows_checks_nothing_and_is_refused() {
    let handle = Handle::live();
    let header: &[&[&str]] = &[&["key"]];
    fatal(
        &handle.send(KEYS_CONTAIN, &["/p/"], None, Some(header), false),
        "input",
        "nothing to check",
    );
    let request = json!({"args": ["/p/"], "docstring": null, "table": header});
    assert!(refusal(KEYS_CONTAIN, &request).contains("nothing to check"));
}
