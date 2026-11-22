//! The C exports driven in-process, the way the host drives them: JSON in,
//! JSON out, every returned string handed back to `bddkit_free_string`. Needs
//! no `bddkit` binary. The live tests need the server `docker-compose.yml`
//! starts, at `BDDKIT_TEST_ETCD` or `http://127.0.0.1:2379`.
//!
//! TLS and authentication are in `tests/secure.rs`.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use std::time::{Duration, Instant};

use bddkit_etcd::*;
use common::{call, cstr, request};
use serde_json::json;

#[test]
fn the_manifest_and_the_step_list_are_what_the_host_loads() {
    assert_eq!(bddkit_abi_version(), 4);
    let manifest = call(bddkit_manifest());
    assert_eq!(manifest["groups"], json!(["etcd"]));
    let steps = call(bddkit_list_steps());
    let steps = steps.as_array().expect("an array");
    assert_eq!(steps.len(), 17);
    assert!(steps.iter().all(|s| s["group"] == "etcd"));
    assert_eq!(
        steps.iter().filter(|s| s["kind"] == "assertion").count(),
        11
    );
}

#[test]
fn validate_config_touches_nothing_and_names_the_endpoints() {
    let started = Instant::now();
    let reply = call(bddkit_validate_config(
        request(&json!({"endpoints": "http://10.255.255.1:2379,http://127.0.0.1:1"})).as_ptr(),
    ));
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(
        reply["detail"],
        "http://10.255.255.1:2379, http://127.0.0.1:1"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "validate must not connect"
    );
}

#[test]
fn init_opens_no_connection_so_an_unreachable_server_still_gets_a_handle() {
    let started = Instant::now();
    let reply = call(bddkit_init_instance(
        request(
            &json!({"endpoints": "http://10.255.255.1:2379", "username": "u", "password": "p"}),
        )
        .as_ptr(),
    ));
    assert_eq!(reply["ok"], true, "{reply}");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "init must not connect"
    );
    let handle = reply["handle"].as_u64().expect("handle");
    assert_eq!(call(bddkit_drop_instance(handle))["ok"], true);
    assert_eq!(
        call(bddkit_drop_instance(handle))["ok"],
        false,
        "dropped once"
    );
}

#[test]
fn a_step_that_does_not_exist_is_fatal_not_a_crash() {
    let handle = call(bddkit_init_instance(
        request(&json!({"endpoints": "http://127.0.0.1:1"})).as_ptr(),
    ))["handle"]
        .as_u64()
        .expect("handle");
    let reply = call(bddkit_dispatch(
        handle,
        99,
        cstr(&json!({"args": []})).as_ptr(),
    ));
    assert_eq!(reply["error"], "unknown step");
    assert_eq!(reply["status"], "fatal");
    assert_eq!(call(bddkit_drop_instance(handle))["ok"], true);
}

#[test]
fn probe_reports_the_version_and_the_revision() {
    let reply = call(bddkit_probe_config(
        request(&json!({"endpoints": common::etcd_endpoint()})).as_ptr(),
    ));
    assert_eq!(reply["ok"], true, "{reply}");
    let detail = reply["detail"].as_str().expect("detail");
    assert!(detail.starts_with("etcd 3."), "{detail}");
    assert!(detail.contains(", revision "), "{detail}");
}

#[test]
fn probe_asks_every_endpoint_and_names_each() {
    let one = common::etcd_endpoint();
    let reply = call(bddkit_probe_config(
        request(&json!({"endpoints": format!("{one},{one}")})).as_ptr(),
    ));
    assert_eq!(reply["ok"], true, "{reply}");
    let detail = reply["detail"].as_str().expect("detail");
    assert_eq!(detail.matches("; ").count(), 1, "{detail}");
    assert_eq!(
        detail.matches(&format!("{one}: etcd 3.")).count(),
        2,
        "{detail}"
    );
}

#[test]
fn probe_of_a_closed_port_fails_fast_as_not_reached() {
    let started = Instant::now();
    let reply = call(bddkit_probe_config(
        request(&json!({"endpoints": "http://127.0.0.1:1", "timeout_secs": 3})).as_ptr(),
    ));
    assert_eq!(reply["ok"], false);
    assert!(
        reply["error"]
            .as_str()
            .expect("error")
            .starts_with("the server was not reached: ")
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "a refused connection is not a timeout"
    );
}

#[test]
fn probe_of_an_address_that_never_answers_gives_up_after_timeout_secs() {
    // The kernel completes the TCP handshake of a listener that never
    // accepts, so the connect succeeds and HTTP/2 hangs: always the timeout.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = format!("http://{}", silent.local_addr().expect("addr"));
    let started = Instant::now();
    let reply = call(bddkit_probe_config(
        request(&json!({"endpoints": endpoint, "timeout_secs": 2})).as_ptr(),
    ));
    assert_eq!(reply["ok"], false);
    assert_eq!(
        reply["error"],
        "the server was not reached: no response within 2 s"
    );
    let took = started.elapsed();
    assert!(
        took >= Duration::from_secs(2) && took < Duration::from_secs(4),
        "{took:?}"
    );
    drop(silent);
}

#[test]
fn probe_names_the_endpoint_that_failed_when_there_are_several() {
    let one = common::etcd_endpoint();
    let reply = call(bddkit_probe_config(
        request(&json!({"endpoints": format!("{one},http://127.0.0.1:1")})).as_ptr(),
    ));
    assert_eq!(reply["ok"], false);
    let error = reply["error"].as_str().expect("error");
    assert!(
        error.starts_with("http://127.0.0.1:1: the server was not reached: "),
        "{error}"
    );
}

#[test]
fn probe_of_https_against_a_plain_port_says_tls_did_not_agree() {
    let https = common::etcd_endpoint().replacen("http://", "https://", 1);
    let reply = call(bddkit_probe_config(
        request(&json!({"endpoints": https})).as_ptr(),
    ));
    assert_eq!(reply["ok"], false);
    assert!(
        reply["error"]
            .as_str()
            .expect("error")
            .starts_with("TLS did not agree: "),
        "{reply}"
    );
}
