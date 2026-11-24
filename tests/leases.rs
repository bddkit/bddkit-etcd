//! The lease steps through the exports, the way the host dispatches them,
//! against a three-node cluster of real etcd processes (`common/cluster.rs`),
//! which the failover tests also break. One reply is one attempt, so a test
//! that waits asks again, as the host does under polling.

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
use common::{Handle, call};
use serde_json::{Value, json};

const GET: u32 = 2;
const EXISTS: u32 = 6;
const ABSENT: u32 = 7;
const GRANT: u32 = 41;
const KEEP: u32 = 42;
const STOP: u32 = 43;
const REVOKE: u32 = 44;
const PUT_LEASED: u32 = 45;
const ALIVE: u32 = 46;
const EXPIRED: u32 = 47;
const ALIVE_ID: u32 = 48;
const EXPIRED_ID: u32 = 49;
const HOLDS: u32 = 50;

/// A handle on every node: the client balances, and a node that dies is
/// replaced by another one.
fn all_nodes(cluster: &Cluster) -> Handle {
    let endpoints = (0..3)
        .map(|node| cluster.endpoint(node))
        .collect::<Vec<_>>()
        .join(",");
    Handle::new(&json!({"endpoints": endpoints, "timeout_secs": 3}))
}

/// A handle on one node.
fn node(cluster: &Cluster, index: usize) -> Handle {
    Handle::new(&json!({"endpoints": cluster.endpoint(index), "timeout_secs": 3}))
}

fn passed(reply: &Value) -> &Value {
    assert_eq!(reply["status"], "passed", "{reply}");
    &reply["vars"]
}

/// Grants a lease and returns its ID, in decimal.
fn grant(handle: &Handle, name: &str, seconds: &str) -> String {
    let reply = handle.run(GRANT, &[name, seconds]);
    passed(&reply)[format!("etcd_lease_{name}")]
        .as_str()
        .expect("the ID")
        .to_string()
}

/// Asks again until the step passes, and returns the last reply.
fn eventually(handle: &Handle, step: u32, args: &[&str]) -> Value {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let reply = handle.run(step, args);
        if reply["status"] == "passed" || Instant::now() > deadline {
            return reply;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn tasks(handle: &Handle) -> usize {
    lease_tasks(handle.0)
        .expect("handle")
        .load(Ordering::SeqCst)
}

/// What the failure dump says of the lease.
fn described(handle: &Handle, name: &str) -> Value {
    let context = handle.context();
    let title = format!("etcd lease {name}");
    let text = context["diagnostics"]
        .as_array()
        .and_then(|all| all.iter().find(|d| d["title"] == title.as_str()))
        .and_then(|d| d["content"].as_str())
        .unwrap_or_else(|| panic!("no diagnostic for the lease in {context}"))
        .to_string();
    serde_json::from_str(&text).expect("JSON")
}

#[test]
fn a_key_under_a_lease_is_gone_after_the_revoke_and_the_name_stays_known() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let id = grant(&handle, "session", "60");
    assert!(id.parse::<i64>().is_ok_and(|id| id > 0), "{id}");
    passed(&handle.run(PUT_LEASED, &["/leases/revoke/k", "v", "session"]));
    assert_eq!(
        handle.run(EXISTS, &["/leases/revoke/k"])["status"],
        "passed"
    );
    assert_eq!(handle.run(ALIVE, &["session"])["status"], "passed");
    assert_eq!(handle.run(EXPIRED, &["session"])["status"], "not_yet");

    passed(&handle.run(REVOKE, &["session"]));
    let reply = eventually(&handle, ABSENT, &["/leases/revoke/k"]);
    assert_eq!(reply["status"], "passed", "{reply}");
    // The name stays known after the revoke.
    assert_eq!(handle.run(EXPIRED, &["session"])["status"], "passed");
    assert_eq!(handle.run(EXPIRED_ID, &[&id])["status"], "passed");
    let again = handle.run(REVOKE, &["session"]);
    assert_eq!(again["status"], "fatal", "{again}");
    assert_eq!(again["class"], "input");
}

#[test]
fn a_lease_nobody_keeps_expires_under_polling_and_alive_on_it_is_final() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    grant(&handle, "short", "2");
    let reply = eventually(&handle, EXPIRED, &["short"]);
    assert_eq!(reply["status"], "passed", "{reply}");
    let reply = handle.run(ALIVE, &["short"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "assertion");
    assert!(
        reply["error"]
            .as_str()
            .unwrap()
            .contains("does not come back"),
        "{reply}"
    );
    assert_eq!(
        described(&handle, "short")["seen_expired"],
        true,
        "{}",
        handle.context()
    );
}

#[test]
fn a_kept_lease_outlives_twice_its_ttl_and_expires_once_the_keepalive_stops() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let id = grant(&handle, "kept", "3");
    passed(&handle.run(KEEP, &["kept"]));
    // The wait below is against the TTL asked for, so the server must have kept it.
    assert_eq!(described(&handle, "kept")["granted_ttl"], 3);
    assert_eq!(tasks(&handle), 1);
    assert_eq!(handle.run(KEEP, &["kept"])["class"], "input");
    std::thread::sleep(Duration::from_secs(7));
    assert_eq!(handle.run(ALIVE, &["kept"])["status"], "passed");
    assert_eq!(described(&handle, "kept")["kept_alive"], true);

    passed(&handle.run(STOP, &["kept"]));
    assert_eq!(tasks(&handle), 0);
    assert_eq!(handle.run(STOP, &["kept"])["class"], "input");
    let reply = eventually(&handle, EXPIRED_ID, &[&id]);
    assert_eq!(reply["status"], "passed", "{reply}");
    // A lease that is gone cannot be kept alive.
    let reply = handle.run(KEEP, &["kept"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "assertion");
}

#[test]
fn a_lease_holds_the_keys_put_under_it_and_is_seen_by_id_through_another_handle() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let other = node(&cluster, 1);
    let id = grant(&handle, "pair", "60");
    assert_eq!(handle.run(HOLDS, &["pair", "0"])["status"], "passed");
    passed(&handle.run(PUT_LEASED, &["/leases/pair/a", "1", "pair"]));
    passed(&handle.run(PUT_LEASED, &["/leases/pair/b", "2", "pair"]));
    assert_eq!(handle.run(HOLDS, &["pair", "2"])["status"], "passed");
    let reply = handle.run(HOLDS, &["pair", "3"]);
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "assertion");

    // A key put with the lease attached reports the lease it holds.
    let read = handle.run(GET, &["/leases/pair/a"]);
    assert_eq!(passed(&read)["etcd_lease"], id.as_str());

    // The other handle never granted it, and sees it by its ID.
    assert_eq!(other.run(ALIVE_ID, &[&id])["status"], "passed");
    assert_eq!(other.run(EXPIRED_ID, &[&id])["status"], "not_yet");
    let unknown = other.run(ALIVE, &["pair"]);
    assert_eq!(unknown["status"], "fatal", "{unknown}");
    assert_eq!(unknown["class"], "input");
    passed(&handle.run(REVOKE, &["pair"]));
    let reply = eventually(&other, EXPIRED_ID, &[&id]);
    assert_eq!(reply["status"], "passed", "{reply}");
    // A lease that is gone holds nothing, and says so as final.
    let reply = handle.run(HOLDS, &["pair", "0"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
}

#[test]
fn a_name_is_taken_until_the_scenario_ends_and_a_reset_revokes_the_leases_and_stops_the_tasks() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let other = node(&cluster, 2);
    let id = grant(&handle, "mine", "60");
    let again = handle.run(GRANT, &["mine", "60"]);
    assert_eq!(again["status"], "fatal", "{again}");
    assert_eq!(again["class"], "input");
    let unknown = handle.run(KEEP, &["nobody"]);
    assert_eq!(unknown["class"], "input", "{unknown}");
    passed(&handle.run(KEEP, &["mine"]));
    passed(&handle.run(PUT_LEASED, &["/leases/reset/k", "v", "mine"]));
    let running = lease_tasks(handle.0).expect("handle");
    assert_eq!(running.load(Ordering::SeqCst), 1);

    let started = Instant::now();
    assert_eq!(call(bddkit_reset_scenario(handle.0))["ok"], true);
    // At most a second each, all at once, and no connection is opened.
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(running.load(Ordering::SeqCst), 0);
    // The lease is gone at once, not after its TTL, with its key.
    assert_eq!(other.run(EXPIRED_ID, &[&id])["status"], "passed");
    assert_eq!(other.run(ABSENT, &["/leases/reset/k"])["status"], "passed");
    // The name is free again in the next scenario.
    grant(&handle, "mine", "60");
    passed(&handle.run(KEEP, &["mine"]));
    assert_eq!(running.load(Ordering::SeqCst), 1);

    drop(handle);
    assert_eq!(
        running.load(Ordering::SeqCst),
        0,
        "no task outlives the drop"
    );
}

#[test]
fn a_reset_that_finds_the_server_gone_stops_the_tasks_and_does_not_wait() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handle = node(&cluster, 0);
    grant(&handle, "stranded", "60");
    passed(&handle.run(KEEP, &["stranded"]));
    let running = lease_tasks(handle.0).expect("handle");
    cluster.kill(0);
    let started = Instant::now();
    assert_eq!(call(bddkit_reset_scenario(handle.0))["ok"], true);
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(running.load(Ordering::SeqCst), 0);
}

#[test]
fn a_keepalive_through_one_endpoint_stops_when_that_node_is_killed_and_the_lease_expires() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handle = node(&cluster, 0);
    let other = node(&cluster, 1);
    let id = grant(&handle, "doomed", "3");
    passed(&handle.run(KEEP, &["doomed"]));
    assert_eq!(other.run(ALIVE_ID, &[&id])["status"], "passed");

    cluster.kill(0);
    // The survivors expire the lease; a new leader gives every lease its whole
    // TTL again, so the wait is the TTL, the election and a margin.
    let reply = eventually(&other, EXPIRED_ID, &[&id]);
    assert_eq!(reply["status"], "passed", "{reply}");
    // The task is still trying to reach its node, and says why.
    let shown = described(&handle, "doomed");
    assert_eq!(shown["reconnecting"], true, "{shown}");
    assert!(shown["last_error"].is_string(), "{shown}");
    assert_eq!(tasks(&handle), 1);

    // The node is back: the stream opens, the server says the lease is gone,
    // and the task ends with it.
    cluster.restart(0);
    let deadline = Instant::now() + Duration::from_secs(40);
    while tasks(&handle) != 0 {
        assert!(Instant::now() < deadline, "{}", handle.context());
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(described(&handle, "doomed")["seen_expired"], true);
}

#[test]
fn a_keepalive_through_every_endpoint_keeps_the_lease_alive_while_one_node_is_killed() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let other = node(&cluster, 1);
    let id = grant(&handle, "survivor", "6");
    passed(&handle.run(KEEP, &["survivor"]));
    assert_eq!(described(&handle, "survivor")["granted_ttl"], 6);

    cluster.kill(0);
    // More than twice the TTL, through a leader change.
    std::thread::sleep(Duration::from_secs(14));
    let reply = other.run(ALIVE_ID, &[&id]);
    assert_eq!(reply["status"], "passed", "{reply}\n{}", handle.context());
    assert_eq!(described(&handle, "survivor")["seen_expired"], false);
}

/// Runs a one-scenario feature on an instance whose server is down, so that
/// everything that is judged is judged by the plugin and the host alone.
fn run_feature(bin: &std::path::Path, name: &str, steps: &[&str]) -> (Option<i32>, String) {
    let mut feature = "Feature: leases\n".to_string();
    for (i, step) in steps.iter().enumerate() {
        feature.push_str(&format!("  Scenario: s{i}\n    When {step}\n"));
    }
    let cfg = common::project_with(
        name,
        "    main:\n      endpoints: http://127.0.0.1:1\n      timeout_secs: 1\n",
        &feature,
    );
    let out = common::bddkit(bin, &["run", "--config", cfg.to_str().expect("path")]);
    (out.status.code(), common::combined(&out))
}

#[test]
fn the_host_matches_every_lease_step() {
    let bin = require_bddkit!();
    let (code, text) = run_feature(
        &bin,
        "lease-steps",
        &[
            "I grant etcd lease \"a\" with TTL 5 seconds",
            "I keep etcd lease \"a\" alive",
            "I stop keeping etcd lease \"a\" alive",
            "I revoke etcd lease \"a\"",
            "I put etcd key \"/k\" with value \"v\" under lease \"a\"",
            "etcd lease \"a\" should be alive",
            "etcd lease \"a\" should have expired",
            "etcd lease with ID 7 should be alive",
            "etcd lease with ID 7 should have expired",
            "etcd lease \"a\" should hold 2 keys",
        ],
    );
    // Exit 1 is failed scenarios: every step was found and checked up front.
    assert_eq!(code, Some(1), "{text}");
    assert!(!text.contains("no step matches"), "{text}");
}

#[test]
fn a_literal_ttl_of_zero_and_an_id_above_i64_max_are_exit_2() {
    let bin = require_bddkit!();
    for (name, step, said) in [
        (
            "lease-ttl-zero",
            "I grant etcd lease \"a\" with TTL 0 seconds",
            "at least 1 second",
        ),
        (
            "lease-id-big",
            "etcd lease with ID 9223372036854775808 should be alive",
            "not a lease ID",
        ),
    ] {
        let (code, text) = run_feature(&bin, name, &[step]);
        assert_eq!(code, Some(2), "{text}");
        assert!(text.contains(said), "{text}");
    }
}
