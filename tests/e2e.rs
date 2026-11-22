//! The real `bddkit` binary and this crate's `cdylib`: what a tester sees from
//! `doctor` before and after the first request.

#![allow(
    clippy::expect_used,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use common::{bddkit, combined, etcd_endpoint, project, project_with};

fn path(p: &std::path::Path) -> &str {
    p.to_str().expect("utf-8 path")
}

#[test]
fn doctor_shows_the_instance_statically_without_connecting() {
    let bin = require_bddkit!();
    // A closed port: a static `doctor` must not care.
    let cfg = project(
        "static",
        "    main:\n      endpoints: \"http://127.0.0.1:1\"\n",
    );
    let out = bddkit(&bin, &["doctor", "--config", path(&cfg)]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("main"), "{text}");
    assert!(text.contains("http://127.0.0.1:1"), "{text}");
}

#[test]
fn doctor_live_shows_the_version_and_the_revision() {
    let bin = require_bddkit!();
    let cfg = project(
        "live",
        &format!("    main:\n      endpoints: \"{}\"\n", etcd_endpoint()),
    );
    let out = bddkit(&bin, &["doctor", "--config", path(&cfg), "--live"]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("etcd 3."), "{text}");
    assert!(text.contains("revision"), "{text}");
}

#[test]
fn doctor_live_against_a_closed_port_says_the_server_was_not_reached() {
    let bin = require_bddkit!();
    let cfg = project(
        "closed",
        "    main:\n      endpoints: \"http://127.0.0.1:1\"\n",
    );
    let out = bddkit(&bin, &["doctor", "--config", path(&cfg), "--live"]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(text.contains("the server was not reached"), "{text}");
}

#[test]
fn each_refusal_of_the_config_is_exit_2_naming_the_instance_and_the_key() {
    let bin = require_bddkit!();
    let bodies = [
        ("      colour: red\n", "unknown key \"colour\""),
        ("      endpoints: \"\"\n", "endpoints"),
        (
            "      endpoints: \"http://a:1,https://b:1\"\n",
            "mixes http and https",
        ),
        (
            "      endpoints: \"https://a:1\"\n      username: root\n",
            "\"username\" needs \"password\"",
        ),
        (
            "      endpoints: \"https://a:1\"\n      tls_cert: Cargo.toml\n",
            "\"tls_cert\" needs \"tls_key\"",
        ),
        (
            "      endpoints: \"https://a:1\"\n      tls_ca: no/such/ca.pem\n",
            "\"tls_ca\": no such file",
        ),
    ];
    for (body, expected) in bodies {
        let cfg = project("refused", &format!("    main:\n{body}"));
        let out = bddkit(&bin, &["run", "--config", path(&cfg)]);
        let text = combined(&out);
        assert_eq!(out.status.code(), Some(2), "{text}");
        assert!(text.contains("resources.etcd.main"), "{text}");
        assert!(text.contains(expected), "{expected}\n{text}");
    }
}

#[test]
fn a_run_with_a_refused_config_exits_2_and_never_prints_the_password() {
    let bin = require_bddkit!();
    let cfg = project(
        "password",
        "    main:\n      endpoints: \"http://a:1,https://b:1\"\n      username: root\n      password: hunter2hunter2\n",
    );
    let out = bddkit(&bin, &["run", "--config", path(&cfg)]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(2), "{text}");
    assert!(text.contains("mixes http and https"), "{text}");
    assert!(!text.contains("hunter2"), "{text}");
}

/// A project over the live server whose only feature is `steps`, with the key
/// space of this run in `<<prefix>>`.
fn keys_project(name: &str, steps: &str) -> std::path::PathBuf {
    let feature = format!(
        "Feature: keys\n  Scenario: one\n    Given set variable \"prefix\" to \"/bddkit-etcd/e2e/{name}/{}/\"\n{steps}",
        std::process::id()
    );
    project_with(
        name,
        &format!("    main:\n      endpoints: \"{}\"\n", etcd_endpoint()),
        &feature,
    )
}

#[test]
fn a_scenario_writes_reads_and_deletes_keys_and_checks_what_they_published() {
    let bin = require_bddkit!();
    let cfg = keys_project(
        "scenario",
        r#"    When I put etcd key "<<prefix>>timeout" with value "30s"
    And I get etcd key "<<prefix>>timeout"
    Then variable "etcd_value" should be equal to "30s"
    And variable "etcd_version" should be equal to "1"
    And variable "etcd_lease" should be equal to "0"
    When I put etcd key "<<prefix>>text" with value:
      """
      line 1
      line 2
      """
    And I get etcd key "<<prefix>>text"
    Then variable "etcd_value" should match "(?s)^line 1.line 2$"
    When I delete etcd key "<<prefix>>timeout"
    Then variable "etcd_deleted" should be equal to "1"
    When I delete etcd keys with prefix "<<prefix>>"
    Then variable "etcd_deleted" should be equal to "1"
"#,
    );
    let out = bddkit(&bin, &["run", "--config", path(&cfg)]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(0), "{text}");
}

#[test]
fn a_literal_empty_prefix_stops_the_run_before_the_first_request() {
    let bin = require_bddkit!();
    let cfg = keys_project("literal", "    When I delete etcd keys with prefix \"\"\n");
    let out = bddkit(&bin, &["run", "--config", path(&cfg)]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(2), "{text}");
    assert!(text.contains("the prefix is empty"), "{text}");
    assert!(text.contains("I delete all etcd keys"), "{text}");
}

#[test]
fn an_empty_prefix_from_a_variable_fails_the_step_and_deletes_nothing() {
    let bin = require_bddkit!();
    let cfg = keys_project(
        "variable",
        r#"    And I put etcd key "<<prefix>>kept" with value "v"
    And set variable "nothing" to ""
    When I delete etcd keys with prefix "<<nothing>>"
"#,
    );
    let out = bddkit(&bin, &["run", "--config", path(&cfg)]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(text.contains("the prefix is empty"), "{text}");
    assert!(text.contains("input"), "{text}");
}

#[test]
fn a_failing_step_of_the_host_shows_the_last_etcd_operation() {
    let bin = require_bddkit!();
    let cfg = keys_project(
        "context",
        r#"    When I put etcd key "<<prefix>>timeout" with value "30s"
    Then variable "etcd_revision" should be equal to "0"
"#,
    );
    let out = bddkit(&bin, &["run", "--config", path(&cfg)]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(
        text.contains("etcd Put /bddkit-etcd/e2e/context/"),
        "{text}"
    );
    assert!(text.contains("\"instance\": \"main\""), "{text}");
}
