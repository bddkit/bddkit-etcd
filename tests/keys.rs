//! The key steps through the exports, the way the host dispatches them, on a
//! live server at `BDDKIT_TEST_ETCD` (or `http://127.0.0.1:2379`).
//!
//! Every test owns a prefix and tidies it, except the one that clears the
//! whole server: it takes the write side of `SERVER`, the others the read side,
//! and it runs only with `BDDKIT_TEST_ETCD_WIPE=1`.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use std::sync::RwLock;
use std::time::{Duration, Instant};

use bddkit_etcd::*;
use common::{call, cstr, request};
use serde_json::{Value, json};

static SERVER: RwLock<()> = RwLock::new(());

const PUT: u32 = 0;
const PUT_DOC: u32 = 1;
const GET: u32 = 2;
const DELETE_KEY: u32 = 3;
const DELETE_PREFIX: u32 = 4;
const DELETE_ALL: u32 = 5;

/// A handle that is dropped with it.
struct Handle(u64);

impl Handle {
    fn new(config: &Value) -> Self {
        let reply = call(bddkit_init_instance(request(config).as_ptr()));
        Self(reply["handle"].as_u64().expect("handle"))
    }

    fn live() -> Self {
        Self::new(&json!({"endpoints": common::etcd_endpoint()}))
    }

    fn dispatch(&self, step: u32, args: &[&str], docstring: Option<&str>, debug: bool) -> Value {
        let request =
            json!({"args": args, "docstring": docstring, "debug": debug, "instance": "main"});
        call(bddkit_dispatch(self.0, step, cstr(&request).as_ptr()))
    }

    fn run(&self, step: u32, args: &[&str]) -> Value {
        self.dispatch(step, args, None, false)
    }

    fn context(&self) -> Value {
        call(bddkit_failure_context(self.0))
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        call(bddkit_drop_instance(self.0));
    }
}

fn prefix(test: &str) -> String {
    format!("/bddkit-etcd/{test}/{}/", std::process::id())
}

fn passed(reply: &Value) -> &Value {
    assert_eq!(reply["status"], "passed", "{reply}");
    &reply["vars"]
}

fn number(vars: &Value, name: &str) -> i64 {
    vars[name]
        .as_str()
        .unwrap_or_else(|| panic!("{name} is not set in {vars}"))
        .parse()
        .expect("a decimal number")
}

fn tidy(handle: &Handle, prefix: &str) {
    passed(&handle.run(DELETE_PREFIX, &[prefix]));
}

#[test]
fn put_then_get_publishes_the_value_the_revisions_the_version_and_the_lease() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("roundtrip"));
    let first = number(passed(&handle.run(PUT, &[&key, "30s"])), "etcd_revision");
    let vars = passed(&handle.run(GET, &[&key])).clone();
    assert_eq!(vars["etcd_value"], "30s");
    assert_eq!(number(&vars, "etcd_create_revision"), first);
    assert_eq!(number(&vars, "etcd_mod_revision"), first);
    assert_eq!(vars["etcd_version"], "1");
    assert_eq!(vars["etcd_lease"], "0");
    assert!(number(&vars, "etcd_revision") >= first);

    let second = number(passed(&handle.run(PUT, &[&key, "60s"])), "etcd_revision");
    assert!(second > first);
    let vars = passed(&handle.run(GET, &[&key])).clone();
    assert_eq!(vars["etcd_value"], "60s");
    assert_eq!(number(&vars, "etcd_create_revision"), first);
    assert_eq!(number(&vars, "etcd_mod_revision"), second);
    assert_eq!(vars["etcd_version"], "2");
    tidy(&handle, &prefix("roundtrip"));
}

#[test]
fn a_doc_string_is_the_value_without_the_line_breaks_gherkin_keeps() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}doc", prefix("doc"));
    passed(&handle.dispatch(PUT_DOC, &[&key], Some("\nline 1\n  line 2\n"), false));
    assert_eq!(
        passed(&handle.run(GET, &[&key]))["etcd_value"],
        "line 1\n  line 2"
    );
    tidy(&handle, &prefix("doc"));
}

#[test]
fn a_missing_key_fails_the_step_as_an_assertion_and_points_to_the_absence_check() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}absent", prefix("missing"));
    let reply = handle.run(GET, &[&key]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "assertion");
    let error = reply["error"].as_str().expect("error");
    assert!(error.contains("should not exist"), "{error}");
    assert!(reply.get("vars").is_none(), "no variables on a failure");
}

#[test]
fn delete_counts_the_keys_it_removed() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let base = prefix("delete");
    for name in ["a/1", "a/2", "a/3", "b/1"] {
        passed(&handle.run(PUT, &[&format!("{base}{name}"), "v"]));
    }
    let one = handle.run(DELETE_KEY, &[&format!("{base}b/1")]);
    assert_eq!(passed(&one)["etcd_deleted"], "1");
    assert!(number(passed(&one), "etcd_revision") > 0);
    let again = handle.run(DELETE_KEY, &[&format!("{base}b/1")]);
    assert_eq!(
        passed(&again)["etcd_deleted"],
        "0",
        "a missing key is not an error"
    );
    let some = handle.run(DELETE_PREFIX, &[&format!("{base}a/")]);
    assert_eq!(passed(&some)["etcd_deleted"], "3");
}

#[test]
fn an_empty_prefix_deletes_nothing_written_literally_or_coming_from_a_variable() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("empty-prefix"));
    passed(&handle.run(PUT, &[&key, "v"]));

    let literal = call(bddkit_validate_step(
        DELETE_PREFIX,
        cstr(&json!({"args": [""], "docstring": null, "table": null, "source_dir": "/"})).as_ptr(),
    ));
    assert_eq!(literal["ok"], false);
    assert!(
        literal["error"]
            .as_str()
            .expect("error")
            .contains("I delete all etcd keys")
    );
    let slot = call(bddkit_validate_step(
        DELETE_PREFIX,
        cstr(&json!({"args": ["<<p>>"], "docstring": null, "table": null, "source_dir": "/"}))
            .as_ptr(),
    ));
    assert_eq!(slot["ok"], true);

    let reply = handle.run(DELETE_PREFIX, &[""]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "input");
    assert_eq!(passed(&handle.run(GET, &[&key]))["etcd_value"], "v");
    tidy(&handle, &prefix("empty-prefix"));
}

#[test]
fn delete_all_clears_the_server_and_counts_what_it_cleared() {
    // It clears the whole server, so it runs only where the server is the
    // test's own. Under `CI` a missing opt-in is a failure, not a skip.
    if std::env::var_os("BDDKIT_TEST_ETCD_WIPE").is_none() {
        assert!(
            std::env::var_os("CI").is_none(),
            "BDDKIT_TEST_ETCD_WIPE must be set on CI, or this test is skipped for good"
        );
        eprintln!("SKIP: set BDDKIT_TEST_ETCD_WIPE=1 to let a test clear the server");
        return;
    }
    let _server = SERVER.write().unwrap();
    let handle = Handle::live();
    let base = prefix("wipe");
    for name in ["1", "2", "3"] {
        passed(&handle.run(PUT, &[&format!("{base}{name}"), "v"]));
    }
    let reply = handle.run(DELETE_ALL, &[]);
    assert!(number(passed(&reply), "etcd_deleted") >= 3, "{reply}");
    assert_eq!(
        handle.run(GET, &[&format!("{base}1")])["class"],
        "assertion"
    );
    assert_eq!(
        passed(&handle.run(DELETE_ALL, &[]))["etcd_deleted"],
        "0",
        "nothing left"
    );
}

#[test]
fn a_closed_port_fails_as_not_reached_and_the_next_step_tries_again() {
    let handle = Handle::new(&json!({"endpoints": "http://127.0.0.1:1", "timeout_secs": 3}));
    for _ in 0..2 {
        let started = Instant::now();
        let reply = handle.run(GET, &["/k"]);
        assert_eq!(reply["status"], "fatal", "{reply}");
        assert_eq!(reply["class"], "db_connection");
        assert!(
            reply["error"]
                .as_str()
                .expect("error")
                .starts_with("the server was not reached: "),
            "{reply}"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
    }
}

#[test]
fn an_address_that_never_answers_is_named_as_no_response_within_the_timeout() {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = format!("http://{}", silent.local_addr().expect("addr"));
    let handle = Handle::new(&json!({"endpoints": endpoint, "timeout_secs": 2}));
    let started = Instant::now();
    let reply = handle.run(PUT, &["/k", "v"]);
    assert_eq!(reply["class"], "db_connection", "{reply}");
    assert_eq!(reply["error"], "no response within 2 s");
    let took = started.elapsed();
    assert!(
        took >= Duration::from_secs(2) && took < Duration::from_secs(4),
        "{took:?}"
    );
}

#[test]
fn a_request_the_server_refuses_is_a_statement_not_a_refused_password() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let reply = handle.run(PUT, &["", "v"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement");
    let error = reply["error"].as_str().expect("error");
    assert!(
        error.starts_with("the server answered with an error (InvalidArgument)"),
        "{error}"
    );
    assert!(!error.contains("credentials"), "{error}");
}

#[test]
fn the_diagnostic_names_the_instance_the_request_and_the_response_header() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("evidence"));
    let reply = handle.run(PUT, &[&key, "v"]);
    let diagnostic = &reply["diagnostics"][0];
    assert_eq!(diagnostic["kind"], "json");
    assert_eq!(diagnostic["title"], format!("etcd Put {key}"));
    let content: Value =
        serde_json::from_str(diagnostic["content"].as_str().expect("text")).expect("JSON");
    assert_eq!(content["instance"], "main");
    assert_eq!(content["request"], json!({"key": key, "value": "v"}));
    for field in ["cluster_id", "member_id", "revision", "raft_term"] {
        assert!(
            content["response_header"][field].is_number(),
            "{field} in {content}"
        );
    }
    assert!(content["duration_us"].is_number());
    assert_eq!(reply["output"], json!([]), "debug is off");
    let debug = handle.dispatch(GET, &[&key], None, true);
    assert_eq!(debug["output"].as_array().expect("output").len(), 1);
    tidy(&handle, &prefix("evidence"));
}

#[test]
fn a_value_that_parses_as_json_goes_to_scan_and_other_text_does_not() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    let key = format!("{}k", prefix("scan"));
    let json_value = r#"{"password":"hunter2hunter2"}"#;
    assert_eq!(
        handle.run(PUT, &[&key, json_value])["scan"],
        json!([{"kind": "json", "content": json_value}])
    );
    assert_eq!(
        handle.run(GET, &[&key])["scan"],
        json!([{"kind": "json", "content": json_value}])
    );
    assert_eq!(handle.run(PUT, &[&key, "30s"])["scan"], json!([]));
    tidy(&handle, &prefix("scan"));
}

#[test]
fn a_value_that_is_not_utf8_fails_the_step_so_no_earlier_value_can_stand_in_for_it() {
    let _server = SERVER.read().unwrap();
    let base = prefix("binary");
    let (text_key, binary_key) = (format!("{base}text"), format!("{base}binary"));
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let mut client = etcd_client::Client::connect([common::etcd_endpoint()], None)
            .await
            .expect("connect");
        client
            .put(binary_key.as_str(), vec![0xff, 0xfe], None)
            .await
            .expect("put");
    });
    let handle = Handle::live();
    passed(&handle.run(PUT, &[&text_key, "30s"]));
    // The host keeps a variable a reply does not name, so a step that left
    // `etcd_value` alone would let a check read the text key's value.
    let reply = handle.run(GET, &[&binary_key]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "assertion");
    let error = reply["error"].as_str().expect("error");
    assert!(error.contains("not UTF-8"), "{error}");
    assert!(error.contains("//4="), "the base64 of the value: {error}");
    assert!(reply.get("vars").is_none(), "no variables on a failure");
    let content: Value =
        serde_json::from_str(reply["diagnostics"][0]["content"].as_str().expect("text"))
            .expect("JSON");
    assert_eq!(
        content["keys"][0]["value"],
        json!({"base64": "//4=", "note": "not UTF-8"})
    );
    tidy(&handle, &base);
}

#[test]
fn the_failure_context_answers_the_last_operation_until_the_scenario_resets() {
    let _server = SERVER.read().unwrap();
    let handle = Handle::live();
    assert_eq!(handle.context()["diagnostics"], json!([]), "nothing yet");
    let key = format!("{}k", prefix("context"));
    handle.run(PUT, &[&key, "v"]);
    handle.run(GET, &[&format!("{key}-missing")]);
    let context = handle.context();
    let title = context["diagnostics"][0]["title"].as_str().expect("title");
    assert_eq!(
        title,
        format!("etcd Get {key}-missing"),
        "the LAST operation, a failed one too"
    );
    assert_eq!(call(bddkit_reset_scenario(handle.0))["ok"], true);
    assert_eq!(handle.context()["diagnostics"], json!([]));
    tidy(&handle, &prefix("context"));
}

#[test]
fn the_reset_opens_no_connection() {
    let handle = Handle::new(&json!({"endpoints": "http://10.255.255.1:2379", "timeout_secs": 30}));
    let started = Instant::now();
    assert_eq!(call(bddkit_reset_scenario(handle.0))["ok"], true);
    assert_eq!(handle.context()["diagnostics"], json!([]));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(call(bddkit_reset_scenario(u64::MAX))["ok"], false);
}
