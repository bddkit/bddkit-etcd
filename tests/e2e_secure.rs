//! The real `bddkit` binary against the secure servers: what `doctor --live`
//! says for each way of failing to get in, and that no output of a run, with
//! or without evidence, carries the password.

#![allow(
    clippy::expect_used,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use common::{
    ROOT_PASSWORD, SERVER_NAME, auth_endpoint, bddkit, cert, combined, enable_auth, project,
    project_with,
};

fn path(p: &std::path::Path) -> &str {
    p.to_str().expect("utf-8 path")
}

/// The body of one instance of the authentication server, the password as given.
fn instance(name: &str, ca: &str, password: &str) -> String {
    format!(
        "    {name}:\n      endpoints: \"{}\"\n      tls_ca: \"{}\"\n      tls_server_name: {SERVER_NAME}\n      username: root\n      password: \"{password}\"\n",
        auth_endpoint(),
        cert(ca),
    )
}

#[test]
fn doctor_live_tells_the_three_failures_apart() {
    let bin = require_bddkit!();
    enable_auth();
    let section = format!(
        "    down:\n      endpoints: \"https://127.0.0.1:1\"\n{}{}default_etcd: down\n",
        instance("tls", "other-ca.crt", ROOT_PASSWORD),
        instance("login", "ca.crt", "not-the-password"),
    );
    let out = bddkit(
        &bin,
        &[
            "doctor",
            "--config",
            path(&project("doctor", &section)),
            "--live",
        ],
    );
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    for expected in [
        "the server was not reached",
        "TLS did not agree",
        "the server refused the credentials",
    ] {
        assert_eq!(text.matches(expected).count(), 1, "{expected}\n{text}");
    }
    assert!(!text.contains("not-the-password"), "{text}");
    assert!(!text.contains(ROOT_PASSWORD), "{text}");
}

#[test]
fn doctor_live_reaches_the_server_over_tls_and_a_password() {
    let bin = require_bddkit!();
    enable_auth();
    let cfg = project("doctor-ok", &instance("main", "ca.crt", ROOT_PASSWORD));
    let out = bddkit(&bin, &["doctor", "--config", path(&cfg), "--live"]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("etcd 3."), "{text}");
    assert!(!text.contains(ROOT_PASSWORD), "{text}");
}

/// A failing run over the authentication server, every output it can write.
/// The password is also the value the step writes: nothing but the host's
/// masking, fed by the plugin's `secrets`, can keep it out of the step text
/// and the evidence. Each caller has a `name` of its own: the tests run in
/// parallel and the project directory is named after it.
fn failing_run(bin: &std::path::Path, name: &str, evidence: &[&str]) -> String {
    enable_auth();
    let feature = format!(
        "Feature: secure\n  Scenario: one\n    When I put etcd key \"/bddkit-etcd/e2e/secure/{}\" with value \"{ROOT_PASSWORD}\"\n    Then variable \"etcd_revision\" should be equal to \"0\"\n",
        std::process::id()
    );
    let cfg = project_with(name, &instance("main", "ca.crt", ROOT_PASSWORD), &feature);
    let dir = cfg.parent().expect("dir").to_path_buf();
    let (events, report) = (dir.join("events.jsonl"), dir.join("report.xml"));
    let mut args = vec![
        "run",
        "--config",
        path(&cfg),
        "--events",
        path(&events),
        "--junit",
        path(&report),
    ];
    args.extend_from_slice(evidence);
    let out = bddkit(bin, &args);
    let console = combined(&out);
    // The run did reach the server and fail on the step, or the check below
    // would pass on a run that printed nothing.
    assert_eq!(out.status.code(), Some(1), "{console}");
    assert!(
        console.contains("etcd Put /bddkit-etcd/e2e/secure/"),
        "{console}"
    );
    let files = [events, report].map(|f| std::fs::read_to_string(f).expect("written"));
    format!("{console}{}{}", files[0], files[1])
}

/// Neither the password nor anything shaped like a token. The plugin never
/// holds the token (see CLAUDE.md), so this is the test of that argument; the
/// shape is checked against a token the server issues now.
fn assert_no_secret(everything: &str) {
    assert!(!everything.contains(ROOT_PASSWORD), "{everything}");
    let shape = regex::Regex::new(r"[A-Za-z]{16}\.\d+").expect("regex");
    assert!(
        shape.is_match(&common::sample_token()),
        "the token format changed: update the shape in this test"
    );
    assert!(
        !shape.is_match(everything),
        "a token-like value: {everything}"
    );
}

#[test]
fn the_password_is_in_no_output_of_a_run() {
    let bin = require_bddkit!();
    let everything = failing_run(&bin, "secure-run", &[]);
    assert_no_secret(&everything);
}

#[test]
fn the_password_is_in_no_output_of_a_run_with_evidence() {
    let bin = require_bddkit!();
    let everything = failing_run(&bin, "secure-run-evidence", &["--evidence=all"]);
    assert_no_secret(&everything);
    assert!(everything.contains("\"response_header\""), "evidence is on");
}

/// The positive control of the two tests above: the same run, told to show
/// real values, does carry the password, so their absence is the masking's
/// doing and not an output that never had it.
#[test]
fn with_reveal_the_same_run_does_show_the_password() {
    let bin = require_bddkit!();
    let everything = failing_run(&bin, "secure-run-reveal", &["--evidence=all", "--reveal"]);
    assert!(everything.contains(ROOT_PASSWORD), "{everything}");
}
