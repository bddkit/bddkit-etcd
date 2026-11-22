//! The real `bddkit` binary and this crate's `cdylib`: what a tester sees from
//! `doctor` before and after the first request.

#![allow(
    clippy::expect_used,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use common::{bddkit, combined, etcd_endpoint, project};

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
