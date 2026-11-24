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
fn a_transaction_with_an_unknown_flag_stops_the_run_naming_its_line() {
    let bin = require_bddkit!();
    let cfg = keys_project(
        "txn-flag",
        r#"    When I commit an etcd transaction:
      """
      create("/k") = "0"

      put /k v --nope
      """
"#,
    );
    let out = bddkit(&bin, &["run", "--config", path(&cfg)]);
    let text = combined(&out);
    assert_eq!(out.status.code(), Some(2), "{text}");
    assert!(text.contains("line 3:"), "{text}");
    assert!(text.contains("unknown flag"), "{text}");
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

/// Deletes what a failing scenario left under its prefix.
fn wipe(prefix: &str) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let mut client = etcd_client::Client::connect([etcd_endpoint()], None)
            .await
            .expect("connect");
        let options = etcd_client::DeleteOptions::new().with_prefix();
        client.delete(prefix, Some(options)).await.expect("delete");
    });
}

/// Runs the host on `cfg` with a JUnit report, and hands back the exit code,
/// the console and the report.
fn run_with_report(bin: &std::path::Path, cfg: &std::path::Path) -> (Option<i32>, String, String) {
    let report = cfg.with_file_name("report.xml");
    let out = bddkit(
        bin,
        &["run", "--config", path(cfg), "--junit", path(&report)],
    );
    let xml = std::fs::read_to_string(&report).unwrap_or_default();
    (out.status.code(), combined(&out), xml)
}

#[test]
fn a_polled_assertion_passes_once_a_key_is_written_after_it_started() {
    let bin = require_bddkit!();
    let name = "polled";
    let key = format!("/bddkit-etcd/e2e/{name}/{}/late", std::process::id());
    // The first line fails loudly if the key is there before the polling
    // starts, which would make the pass below prove nothing.
    let cfg = keys_project(
        name,
        r#"    Then etcd key "<<prefix>>late" should not exist
    And I expect the next assertion to pass within 15 seconds, checking every 200 milliseconds
    And etcd key "<<prefix>>late" should have value "arrived"
    When I delete etcd keys with prefix "<<prefix>>"
"#,
    );
    // The plugin is built on first use: do it before the writer's clock starts,
    // or the key could be there before the host reaches the step.
    common::lock_dir();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(2));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let mut client = etcd_client::Client::connect([etcd_endpoint()], None)
                .await
                .expect("connect");
            client
                .put(key.as_str(), "arrived", None)
                .await
                .expect("put");
        });
    });
    let (code, text, _) = run_with_report(&bin, &cfg);
    writer.join().expect("writer");
    assert_eq!(code, Some(0), "{text}");
}

#[test]
fn a_polled_assertion_that_runs_out_keeps_the_class_of_its_last_attempt() {
    let bin = require_bddkit!();
    // A wrong value: the server answered, the check did not hold.
    let cfg = keys_project(
        "wrong",
        r#"    When I put etcd key "<<prefix>>k" with value "30s"
    Then I expect the next assertion to pass within 2 seconds, checking every 200 milliseconds
    And etcd key "<<prefix>>k" should have value "60s"
"#,
    );
    let (code, text, xml) = run_with_report(&bin, &cfg);
    wipe(&format!("/bddkit-etcd/e2e/wrong/{}/", std::process::id()));
    assert_eq!(code, Some(1), "{text}");
    assert!(
        text.contains("has value \"30s\", expected \"60s\""),
        "{text}"
    );
    assert!(xml.contains("type=\"assertion\""), "{xml}");

    // A dead server: the same step, and the class says it was never reached.
    let feature = "Feature: dead\n  Scenario: one\n    Then I expect the next assertion to pass within 2 seconds, checking every 200 milliseconds\n    And etcd key \"/k\" should exist\n";
    let cfg = project_with(
        "dead",
        "    main:\n      endpoints: \"http://127.0.0.1:1\"\n      timeout_secs: 1\n",
        feature,
    );
    let (code, text, xml) = run_with_report(&bin, &cfg);
    assert_eq!(code, Some(1), "{text}");
    assert!(xml.contains("type=\"db_connection\""), "{xml}");
}

#[test]
fn a_literal_mistake_in_an_assertion_stops_the_run_before_the_first_request() {
    let bin = require_bddkit!();
    let cfg = keys_project(
        "mistake",
        r#"    Then etcd key "<<prefix>>k" should have:
      | field  | value |
      | colour | 1     |
"#,
    );
    let (code, text, _) = run_with_report(&bin, &cfg);
    assert_eq!(code, Some(2), "{text}");
    assert!(text.contains("unknown field \"colour\""), "{text}");
}

#[test]
fn a_watch_scenario_runs_through_the_host_and_a_failure_dump_shows_the_watch() {
    let bin = require_bddkit!();
    let cfg = keys_project(
        "watch",
        r#"    Given I watch etcd keys with prefix "<<prefix>>" as "cfg"
    And I watch etcd key "<<prefix>>a" as "late" from revision <<etcd_watch_cfg_revision>>
    When I put etcd key "<<prefix>>a" with value "1"
    And I put etcd key "<<prefix>>a" with value "2"
    Then I expect the next assertion to pass within 10 seconds, checking every 100 milliseconds
    And etcd watch "cfg" should have received events:
      | type | key         | value | prev_value |
      | PUT  | <<prefix>>a | 1     |            |
      | PUT  | <<prefix>>a | 2     | 1          |
    And I expect the next assertion to pass within 10 seconds, checking every 100 milliseconds
    And etcd watch "late" should have received 2 events
    And I cancel etcd watch "cfg"
    And I delete etcd keys with prefix "<<prefix>>"
    And variable "etcd_watch_cfg_revision" should be equal to "0"
"#,
    );
    let (code, text, _) = run_with_report(&bin, &cfg);
    wipe(&format!("/bddkit-etcd/e2e/watch/{}/", std::process::id()));
    // The last step belongs to the host, and the dump says what each watch holds.
    assert_eq!(code, Some(1), "{text}");
    assert!(text.contains("etcd watch cfg (json)"), "{text}");
    assert!(text.contains("\"state\": \"cancelled\""), "{text}");
    assert!(text.contains("\"state\": \"running\""), "{text}");
}

#[test]
fn a_literal_mistake_in_a_watch_table_stops_the_run_before_the_first_request() {
    let bin = require_bddkit!();
    let cfg = keys_project(
        "watchmistake",
        r#"    Given I watch etcd keys with prefix "<<prefix>>" as "cfg"
    Then etcd watch "cfg" should have received events:
      | type | revision |
      | PUT  | 1        |
"#,
    );
    let (code, text, _) = run_with_report(&bin, &cfg);
    assert_eq!(code, Some(2), "{text}");
    assert!(text.contains("unknown table column \"revision\""), "{text}");
}
