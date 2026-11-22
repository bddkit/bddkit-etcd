//! TLS, mutual TLS and a password against real servers: the `docker
//! compose` services `etcd-secure`, `etcd-mtls` and `etcd-auth` (addresses in
//! `tests/common`). The probe is what `doctor --live` shows, so each case
//! names the failure a tester has to fix.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use bddkit_etcd::*;
use common::{
    Handle, call, cert, enable_auth, mtls_config, request, root_config, secure_config,
    secure_endpoint,
};
use serde_json::{Value, json};

const PUT: u32 = 0;
const GET: u32 = 2;

fn probe(config: &Value) -> Value {
    call(bddkit_probe_config(request(config).as_ptr()))
}

fn error(reply: &Value) -> &str {
    assert_eq!(reply["ok"], false, "{reply}");
    reply["error"].as_str().expect("error")
}

fn with(mut config: Value, key: &str, value: Value) -> Value {
    config[key] = value;
    config
}

fn without(mut config: Value, key: &str) -> Value {
    config.as_object_mut().expect("object").remove(key);
    config
}

fn assert_tls_failure(reply: &Value) {
    assert!(error(reply).starts_with("TLS did not agree: "), "{reply}");
}

#[test]
fn tls_with_the_given_ca_reaches_the_server() {
    let reply = probe(&secure_config());
    assert_eq!(reply["ok"], true, "{reply}");
    assert!(
        reply["detail"]
            .as_str()
            .expect("detail")
            .starts_with("etcd 3.")
    );
}

#[test]
fn a_ca_that_did_not_sign_the_server_is_a_tls_failure() {
    assert_tls_failure(&probe(&with(
        secure_config(),
        "tls_ca",
        json!(cert("other-ca.crt")),
    )));
}

#[test]
fn without_tls_ca_only_the_system_roots_count_and_the_test_ca_is_not_one() {
    assert_tls_failure(&probe(&without(secure_config(), "tls_ca")));
}

#[test]
fn an_ip_address_the_certificate_does_not_name_needs_tls_server_name() {
    assert_tls_failure(&probe(&without(secure_config(), "tls_server_name")));
    assert_tls_failure(&probe(&with(
        secure_config(),
        "tls_server_name",
        json!("other.test"),
    )));
}

#[test]
fn a_plain_http_address_on_a_tls_port_is_not_reached() {
    let http = secure_endpoint().replacen("https://", "http://", 1);
    let reply = probe(&json!({"endpoints": http}));
    assert!(
        error(&reply).starts_with("the server was not reached: "),
        "{reply}"
    );
}

#[test]
fn mutual_tls_needs_the_client_certificate() {
    let reply = probe(&mtls_config());
    assert_eq!(reply["ok"], true, "{reply}");
    assert_tls_failure(&probe(&without(
        without(mtls_config(), "tls_cert"),
        "tls_key",
    )));
}

#[test]
fn a_client_certificate_of_another_ca_is_refused() {
    let rogue = with(
        with(mtls_config(), "tls_cert", json!(cert("rogue-client.crt"))),
        "tls_key",
        json!(cert("rogue-client.key")),
    );
    assert_tls_failure(&probe(&rogue));
}

#[test]
fn a_right_and_a_wrong_password() {
    enable_auth();
    let reply = probe(&root_config());
    assert_eq!(reply["ok"], true, "{reply}");
    let wrong = probe(&with(root_config(), "password", json!("not-the-password")));
    assert!(
        error(&wrong).starts_with("the server refused the credentials: "),
        "{wrong}"
    );
    // No credentials at all, and a user the server does not know.
    for config in [
        without(without(root_config(), "username"), "password"),
        with(root_config(), "username", json!("nobody")),
    ] {
        let refused = probe(&config);
        assert!(
            error(&refused).starts_with("the server refused the credentials: "),
            "{refused}"
        );
    }
}

#[test]
fn a_password_sent_to_a_server_without_authentication_is_refused_as_credentials() {
    let reply = probe(&json!({
        "endpoints": common::etcd_endpoint(),
        "username": "root",
        "password": "whatever-it-is",
    }));
    assert!(
        error(&reply).starts_with("the server refused the credentials: "),
        "{reply}"
    );
}

#[test]
fn steps_over_tls_and_a_password() {
    enable_auth();
    let handle = Handle::new(&root_config());
    let key = format!("/bddkit-etcd/secure/{}/k", std::process::id());
    assert_eq!(handle.run(PUT, &[&key, "v"])["status"], "passed");
    let got = handle.run(GET, &[&key]);
    assert_eq!(got["status"], "passed", "{got}");
    assert_eq!(got["vars"]["etcd_value"], "v");
}

#[test]
fn a_refused_password_fails_the_step_as_db_statement() {
    enable_auth();
    let handle = Handle::new(&with(root_config(), "password", json!("not-the-password")));
    let reply = handle.run(PUT, &["/bddkit-etcd/secure/refused", "v"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement", "{reply}");
    assert!(
        reply["error"]
            .as_str()
            .expect("error")
            .starts_with("the server refused the credentials: "),
        "{reply}"
    );
}

#[test]
fn a_failed_handshake_fails_the_step_as_db_connection() {
    let handle = Handle::new(&with(
        secure_config(),
        "tls_ca",
        json!(cert("other-ca.crt")),
    ));
    let reply = handle.run(PUT, &["/bddkit-etcd/secure/handshake", "v"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_connection", "{reply}");
    assert!(
        reply["error"]
            .as_str()
            .expect("error")
            .starts_with("TLS did not agree: "),
        "{reply}"
    );
}
