//! The suites in `examples/`, run by the real `bddkit` binary: the green one
//! against two servers, the one that fails on purpose for what it prints. They
//! are the README's examples, so they must not rot.
//!
//! They need the two plain servers of `docker-compose.yml` (`etcd` on 2379 and
//! `etcd-b` on 12379): the addresses are written in `examples/*.yaml`, as a
//! reader would copy them. Without the servers they skip, except under `CI`.
//! The failover example needs no server: it needs etcd binaries and the `exec`
//! plugin, see `run_with_binaries`.

#![allow(
    clippy::expect_used,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use std::net::TcpStream;

use common::{bddkit, combined};

const PASSWORD: &str = "example-password-7f3a";

fn path(p: &std::path::Path) -> &str {
    p.to_str().expect("utf-8 path")
}

/// Whether both servers of the examples answer; a skip is a failure under CI.
fn servers_are_up() -> bool {
    let up = ["127.0.0.1:2379", "127.0.0.1:12379"]
        .iter()
        .all(|address| TcpStream::connect(address).is_ok());
    assert!(
        up || std::env::var_os("CI").is_none(),
        "the example servers are not up, and CI must not skip: docker compose up --wait etcd etcd-b"
    );
    if !up {
        eprintln!("SKIP: start `etcd` and `etcd-b` of docker-compose.yml");
    }
    up
}

/// The `resource` of every evidence event of the two files that never switch
/// instance, so only the default can have picked it.
fn resources_of_default_files(stream: &str) -> Vec<String> {
    stream
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|event| event["type"] == "evidence")
        .filter(|event| {
            let file = event["file"].as_str().unwrap_or_default();
            file.ends_with("keys.feature") || file.ends_with("assertions.feature")
        })
        .map(|event| event["resource"].as_str().unwrap_or_default().to_string())
        .collect()
}

#[test]
fn the_example_suite_passes_against_either_server() {
    let bin = require_bddkit!();
    if !servers_are_up() {
        return;
    }
    let dir = std::env::temp_dir().join(format!("bddkit-etcd-examples-ok-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    for default in ["main", "second"] {
        let events = dir.join(format!("{default}.jsonl"));
        let out = bddkit(
            &bin,
            &[
                "run",
                "--config",
                "examples/etcd.yaml",
                "--default",
                &format!("etcd={default}"),
                "--events",
                path(&events),
                "--evidence=all",
            ],
        );
        let text = combined(&out);
        assert_eq!(out.status.code(), Some(0), "--default {default}\n{text}");
        assert!(text.contains("failed: 0"), "{text}");
        // `--default` must be what chose the server, not a no-op.
        let stream = std::fs::read_to_string(&events).expect("events written");
        let resources = resources_of_default_files(&stream);
        assert!(!resources.is_empty(), "no evidence\n{stream}");
        assert!(
            resources.iter().all(|r| r == default),
            "{default}: {resources:?}"
        );
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_failing_suite_fails_each_scenario_with_its_class_and_text() {
    let bin = require_bddkit!();
    if !servers_are_up() {
        return;
    }
    let dir = std::env::temp_dir().join(format!("bddkit-etcd-examples-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let (events, report) = (dir.join("events.jsonl"), dir.join("report.xml"));
    let run = |extra: &[&str]| {
        let mut args = vec![
            "run",
            "--config",
            "examples/failing.yaml",
            "--events",
            path(&events),
            "--junit",
            path(&report),
            "--evidence=all",
        ];
        args.extend_from_slice(extra);
        let out = bddkit(&bin, &args);
        let files = [&events, &report].map(|f| std::fs::read_to_string(f).expect("written"));
        (out.status.code(), combined(&out), files)
    };

    let (code, text, [stream, junit]) = run(&[]);
    assert_eq!(code, Some(1), "{text}");
    for expected in [
        "has value \"30s\", expected \"60s\"",
        "does not exist; to check that a key is absent",
        "the server was not reached",
        "the server refused the credentials: ",
        "failed: 4 (2 assertion, 1 db_connection, 1 db_statement)",
    ] {
        assert!(text.contains(expected), "{expected}\n{text}");
    }
    for kind in ["assertion", "db_connection", "db_statement"] {
        assert!(
            stream.contains(&format!("\"kind\":\"{kind}\"")),
            "{kind}\n{stream}"
        );
    }
    assert!(stream.contains("response_header"), "evidence is on");
    assert!(text.contains("\"value\": \"***\""), "{text}");
    for (name, output) in [("console", &text), ("events", &stream), ("junit", &junit)] {
        assert!(!output.contains(PASSWORD), "{name}\n{output}");
    }

    // The control: told to show real values, the same run prints the password
    // in each output, so its absence above is the masking's doing.
    let (_, text, [stream, junit]) = run(&["--reveal"]);
    for (name, output) in [("console", &text), ("events", &stream), ("junit", &junit)] {
        assert!(output.contains(PASSWORD), "{name}\n{output}");
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// The examples that start their own etcd need binaries and not servers:
/// `etcd` and `etcdctl` (the directory of `BDDKIT_TEST_ETCD_BIN` or `PATH`) and
/// the `exec` plugin (`BDDKIT_EXEC_PLUGIN`). Runs the config, returns the exit
/// code and the console; `None` when a skip applies.
fn run_with_binaries(
    bin: &std::path::Path,
    etcd: &std::path::Path,
    config: &str,
) -> Option<(Option<i32>, String)> {
    let Some(exec) = common::exec_plugin() else {
        assert!(
            std::env::var_os("CI").is_none(),
            "BDDKIT_EXEC_PLUGIN does not name the exec plugin, and CI must not skip"
        );
        eprintln!("SKIP: set BDDKIT_EXEC_PLUGIN to libbddkit_exec.so");
        return None;
    };
    let mut dirs = vec![etcd.parent().expect("a directory").to_path_buf()];
    dirs.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let out = std::process::Command::new(bin)
        .args(["run", "--config", config, "--bddkit-dir"])
        .arg(common::lock_dir_with_exec(&exec))
        .env("PATH", std::env::join_paths(dirs).expect("a PATH"))
        .output()
        .expect("bddkit");
    Some((out.status.code(), combined(&out)))
}

#[test]
fn the_failover_example_passes_on_three_real_nodes() {
    let bin = require_bddkit!();
    let etcd = require_etcd_bin!();
    let Some((code, text)) = run_with_binaries(&bin, &etcd, "examples/cluster.yaml") else {
        return;
    };
    assert_eq!(code, Some(0), "{text}");
    assert!(text.contains("scenarios: 4, failed: 0"), "{text}");
}

/// The authentication example sets a server up with `etcdctl` and logs in as a
/// user who may only read; the passwords it uses are in no output.
#[test]
fn the_auth_example_passes_on_a_real_server() {
    let bin = require_bddkit!();
    let etcd = require_etcd_bin!();
    let Some((code, text)) = run_with_binaries(&bin, &etcd, "examples/auth.yaml") else {
        return;
    };
    assert_eq!(code, Some(0), "{text}");
    assert!(text.contains("scenarios: 2, failed: 0"), "{text}");
    for password in [
        "reader-example-9d2b",
        "root-example-5c1e",
        "wrong-example-0000",
    ] {
        assert!(!text.contains(password), "{text}");
    }
}
