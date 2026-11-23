//! A watch that is resumed after its stream broke, through the exports,
//! against a three-node cluster of real etcd processes that the tests break
//! (`common/cluster.rs`). One reply is one attempt, so a test that waits asks
//! again, as the host does under polling.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bddkit_etcd::*;
use common::cluster::Cluster;
use common::{Handle, block_on, call};
use serde_json::{Value, json};

const PUT: u32 = 0;
const WATCH_PREFIX: u32 = 31;
const RECEIVED_COUNT: u32 = 37;
const RECEIVED_EVENTS: u32 = 38;
const RESUMED: u32 = 39;
const RESUMED_TIMES: u32 = 40;

/// A handle on one node of the cluster.
fn node(cluster: &Cluster, index: usize) -> Handle {
    Handle::new(&json!({"endpoints": cluster.endpoint(index), "timeout_secs": 3}))
}

fn passed(reply: &Value) -> &Value {
    assert_eq!(reply["status"], "passed", "{reply}");
    &reply["vars"]
}

/// Asks again until the step passes, and returns the last reply.
fn eventually(handle: &Handle, step: u32, args: &[&str]) -> Value {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let reply = handle.run(step, args);
        if reply["status"] == "passed" || Instant::now() > deadline {
            return reply;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// What the failure dump says of the watch.
fn described(handle: &Handle, name: &str) -> Value {
    let context = handle.context();
    let title = format!("etcd watch {name}");
    let text = context["diagnostics"]
        .as_array()
        .and_then(|all| all.iter().find(|d| d["title"] == title.as_str()))
        .and_then(|d| d["content"].as_str())
        .unwrap_or_else(|| panic!("no diagnostic for the watch in {context}"))
        .to_string();
    serde_json::from_str(&text).expect("JSON")
}

/// Waits until the watch knows its stream is down.
fn until_down(handle: &Handle, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(40);
    while described(handle, name)["reconnecting"] != true {
        assert!(
            Instant::now() < deadline,
            "the watch never noticed: {}",
            described(handle, name)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn put(handle: &Handle, key: &str, value: &str) -> String {
    let reply = eventually(handle, PUT, &[key, value]);
    passed(&reply)["etcd_revision"]
        .as_str()
        .unwrap()
        .to_string()
}

fn compact(endpoint: String, revision: i64) {
    block_on(async {
        let client = etcd_client::Client::connect([endpoint], None)
            .await
            .expect("connect");
        client
            .kv_client()
            .compact(revision, None)
            .await
            .expect("compact");
    });
}

/// The history is the table, the watch was resumed exactly once, and what it
/// kept of the break shows in its context.
fn assert_resumed_once(watched: &Handle, table: &[&[&str]]) {
    let reply = eventually(
        watched,
        RECEIVED_COUNT,
        &["w", &(table.len() - 1).to_string()],
    );
    assert_eq!(reply["status"], "passed", "{reply}");
    let history = watched.send(RECEIVED_EVENTS, &["w"], None, Some(table), false);
    assert_eq!(history["status"], "passed", "{history}");
    let resumed = eventually(watched, RESUMED, &["w"]);
    assert_eq!(resumed["status"], "passed", "{resumed}");
    assert_eq!(watched.run(RESUMED_TIMES, &["w", "1"])["status"], "passed");
    assert_eq!(watched.run(RESUMED_TIMES, &["w", "2"])["status"], "not_yet");
    let state = described(watched, "w");
    assert_eq!(state["reconnecting"], false, "{state}");
    assert!(state["last_error"].is_string(), "{state}");
}

#[test]
fn a_watch_on_a_node_that_is_killed_resumes_once_and_misses_no_write() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let (watched, other) = (node(&cluster, 0), node(&cluster, 1));
    passed(&watched.run(WATCH_PREFIX, &["/resume/kill/", "w"]));
    let first = put(&other, "/resume/kill/a", "1");
    assert_eq!(
        eventually(&watched, RECEIVED_COUNT, &["w", "1"])["status"],
        "passed"
    );
    // It never broke: the step waits.
    assert_eq!(watched.run(RESUMED, &["w"])["status"], "not_yet");

    cluster.kill(0);
    until_down(&watched, "w");
    let second = put(&other, "/resume/kill/b", "2");
    let third = put(&other, "/resume/kill/c", "3");
    // While the node is down the history is what it was, and the wait says why.
    let waiting = watched.run(RECEIVED_COUNT, &["w", "3"]);
    assert_eq!(waiting["status"], "not_yet", "{waiting}");
    assert_eq!(waiting["class"], "db_connection", "{waiting}");
    assert!(waiting["error"].as_str().unwrap().contains("opened again"));
    cluster.restart(0);

    let table: &[&[&str]] = &[
        &["type", "key", "value", "mod_revision"],
        &["PUT", "/resume/kill/a", "1", &first],
        &["PUT", "/resume/kill/b", "2", &second],
        &["PUT", "/resume/kill/c", "3", &third],
    ];
    assert_resumed_once(&watched, table);
    // Writes after the resume arrive too, and are not sent twice.
    put(&other, "/resume/kill/d", "4");
    assert_eq!(
        eventually(&watched, RECEIVED_COUNT, &["w", "4"])["status"],
        "passed"
    );
}

#[test]
fn a_watch_on_a_node_without_a_leader_resumes_when_the_quorum_is_back() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let watched = node(&cluster, 0);
    passed(&watched.run(WATCH_PREFIX, &["/resume/quorum/", "w"]));
    put(&watched, "/resume/quorum/a", "1");
    assert_eq!(
        eventually(&watched, RECEIVED_COUNT, &["w", "1"])["status"],
        "passed"
    );

    cluster.kill(1);
    cluster.kill(2);
    until_down(&watched, "w");
    cluster.restart(1);
    cluster.restart(2);
    put(&watched, "/resume/quorum/b", "2");
    put(&watched, "/resume/quorum/c", "3");

    let reply = eventually(&watched, RECEIVED_COUNT, &["w", "3"]);
    assert_eq!(reply["status"], "passed", "{reply}");
    let resumed = eventually(&watched, RESUMED, &["w"]);
    assert_eq!(resumed["status"], "passed", "{resumed}");
    // Every write is in the history once, in order.
    let table: &[&[&str]] = &[
        &["key", "value"],
        &["/resume/quorum/a", "1"],
        &["/resume/quorum/b", "2"],
        &["/resume/quorum/c", "3"],
    ];
    let history = watched.send(RECEIVED_EVENTS, &["w"], None, Some(table), false);
    assert_eq!(history["status"], "passed", "{history}");
}

#[test]
fn a_watch_whose_gap_was_compacted_fails_naming_the_compacted_revision() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let (watched, other) = (node(&cluster, 0), node(&cluster, 1));
    passed(&watched.run(WATCH_PREFIX, &["/resume/compact/", "w"]));
    put(&other, "/resume/compact/a", "1");
    assert_eq!(
        eventually(&watched, RECEIVED_COUNT, &["w", "1"])["status"],
        "passed"
    );

    cluster.kill(0);
    until_down(&watched, "w");
    put(&other, "/resume/compact/b", "2");
    let last: i64 = put(&other, "/resume/compact/c", "3").parse().unwrap();
    compact(cluster.endpoint(1), last);
    cluster.restart(0);

    let deadline = Instant::now() + Duration::from_secs(40);
    let reply = loop {
        let reply = watched.run(RECEIVED_COUNT, &["w", "3"]);
        if reply["status"] == "fatal" || Instant::now() > deadline {
            break reply;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(reply["status"], "fatal", "{reply}");
    let error = reply["error"].as_str().unwrap();
    assert!(
        error.contains("compacted") && error.contains(&last.to_string()),
        "{reply}"
    );
    // It stays final: asking again, or about the resumes, does not change it.
    assert_eq!(watched.run(RESUMED, &["w"])["status"], "fatal");
}

#[test]
fn a_reset_or_a_drop_while_the_watch_is_reconnecting_leaves_no_task() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let watched = node(&cluster, 0);
    passed(&watched.run(WATCH_PREFIX, &["/resume/reset/", "w"]));
    cluster.kill(0);
    until_down(&watched, "w");
    let counter = watch_tasks(watched.0).expect("handle");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(call(bddkit_reset_scenario(watched.0))["ok"], true);
    assert_eq!(counter.load(Ordering::SeqCst), 0, "after the reset");

    cluster.restart(0);
    // The node takes a moment to answer again.
    passed(&eventually(
        &watched,
        WATCH_PREFIX,
        &["/resume/reset/", "w"],
    ));
    cluster.kill(0);
    until_down(&watched, "w");
    drop(watched);
    assert_eq!(counter.load(Ordering::SeqCst), 0, "after the drop");
}
