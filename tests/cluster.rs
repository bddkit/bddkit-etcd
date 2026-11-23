//! The cluster steps through the exports, against a three-node cluster of real
//! etcd processes that the tests break (`common/cluster.rs`). One reply is one
//! attempt, so a test that waits for a change asks again, as the host does
//! under polling.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use std::time::{Duration, Instant};

use common::Handle;
use common::cluster::Cluster;
use serde_json::{Value, json};

const PUT: u32 = 0;
const GET: u32 = 2;
const GET_SERIALIZABLE: u32 = 17;
const LEADER: u32 = 18;
const NOT_LEADER: u32 = 19;
const REACHED: u32 = 20;
const COMPUTE_HASH: u32 = 21;
const HASH_IS: u32 = 22;

/// A handle on one node of the cluster.
fn node(cluster: &Cluster, index: usize) -> Handle {
    Handle::new(&json!({"endpoints": cluster.endpoint(index), "timeout_secs": 3}))
}

fn nodes(cluster: &Cluster) -> Vec<Handle> {
    (0..3).map(|i| node(cluster, i)).collect()
}

fn passed(reply: &Value) -> bool {
    reply["status"] == "passed"
}

/// Asks again until the step passes, and returns the last reply.
fn eventually(handle: &Handle, step: u32, args: &[&str]) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let reply = handle.run(step, args);
        if passed(&reply) || Instant::now() > deadline {
            return reply;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn put(handle: &Handle, key: &str, value: &str) -> String {
    let reply = handle.run(PUT, &[key, value]);
    assert!(passed(&reply), "{reply}");
    reply["vars"]["etcd_revision"]
        .as_str()
        .expect("revision")
        .to_string()
}

fn leaders(handles: &[Handle]) -> Vec<usize> {
    (0..handles.len())
        .filter(|i| passed(&handles[*i].run(LEADER, &[])))
        .collect()
}

#[test]
fn exactly_one_node_is_the_leader_and_the_others_are_not() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handles = nodes(&cluster);
    // A re-election between two reads would flip an answer: ask again, a
    // bounded number of times, until one round agrees with itself.
    let mut last = String::new();
    for _ in 0..5 {
        let leaders = leaders(&handles);
        let mut agreed = leaders.len() == 1;
        for (index, handle) in handles.iter().enumerate() {
            let leading = leaders.contains(&index);
            let reply = handle.run(LEADER, &[]);
            let inverse = handle.run(NOT_LEADER, &[]);
            agreed &= passed(&reply) == leading && passed(&inverse) != leading;
            if !leading && !passed(&reply) {
                assert_eq!(reply["status"], "not_yet", "{reply}");
                assert_eq!(reply["class"], "assertion", "{reply}");
            }
        }
        if agreed {
            return;
        }
        last = format!("{leaders:?}");
        std::thread::sleep(Duration::from_secs(1));
    }
    panic!("no round of answers agreed with itself: {last}");
}

#[test]
fn another_node_leads_once_the_leader_is_killed() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handles = nodes(&cluster);
    let old = leaders(&handles)[0];
    cluster.kill(old);
    let survivors: Vec<usize> = (0..3).filter(|i| *i != old).collect();
    let deadline = Instant::now() + Duration::from_secs(30);
    let new = loop {
        if let Some(found) = survivors
            .iter()
            .find(|i| passed(&handles[**i].run(LEADER, &[])))
        {
            break *found;
        }
        assert!(Instant::now() < deadline, "no new leader in 30 s");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_ne!(new, old);
    // The node that is down is not reached: a failure of the server's, which
    // a polled assertion asks again.
    let down = handles[old].run(LEADER, &[]);
    assert_eq!(down["status"], "not_yet", "{down}");
    assert_eq!(down["class"], "db_connection", "{down}");
}

#[test]
fn a_node_that_was_down_reaches_the_revision_after_it_restarts() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handles = nodes(&cluster);
    let leaders = leaders(&handles);
    // A node that does not lead, so the cluster keeps its leader.
    let lagging = (0..3).find(|i| !leaders.contains(i)).expect("a follower");
    let writer = leaders[0];
    cluster.kill(lagging);
    let revision = put(&handles[writer], "/bddkit-etcd/cluster/reached", "1");
    // The cluster went on without it.
    assert!(passed(&handles[writer].run(REACHED, &[&revision])));
    cluster.restart(lagging);
    let reply = eventually(&handles[lagging], REACHED, &[&revision]);
    assert!(passed(&reply), "{reply}");
    // A revision nobody wrote yet is not reached, and may be later.
    let future = (revision.parse::<u64>().unwrap() + 1000).to_string();
    let reply = handles[lagging].run(REACHED, &[&future]);
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "assertion", "{reply}");
}

#[test]
fn the_hashes_of_all_nodes_at_one_revision_are_equal() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handles = nodes(&cluster);
    let mut revision = String::new();
    for i in 0..5 {
        revision = put(&handles[0], &format!("/bddkit-etcd/cluster/hash/{i}"), "v");
    }
    for handle in &handles {
        assert!(passed(&eventually(handle, REACHED, &[&revision])));
    }
    let computed = handles[0].run(COMPUTE_HASH, &[&revision, "h"]);
    assert!(passed(&computed), "{computed}");
    let hash = computed["vars"]["h"].as_str().expect("hash").to_string();
    assert!(hash.parse::<u32>().is_ok(), "{hash}");
    for handle in &handles {
        let reply = handle.run(HASH_IS, &[&revision, &hash]);
        assert!(passed(&reply), "{reply}");
        let again = handle.run(COMPUTE_HASH, &[&revision, "other"]);
        assert_eq!(again["vars"]["other"], hash.as_str(), "{again}");
    }
    // A different hash at a revision the node has reached is final.
    let wrong = (hash.parse::<u32>().unwrap() ^ 1).to_string();
    let reply = handles[1].run(HASH_IS, &[&revision, &wrong]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "assertion", "{reply}");
}

#[test]
fn a_hash_at_a_revision_the_node_has_not_reached_is_not_yet() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = node(&cluster, 0);
    let reply = handle.run(HASH_IS, &["99999", "1"]);
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "assertion", "{reply}");
    // An action does not wait: the server refuses and the step fails.
    let reply = handle.run(COMPUTE_HASH, &["99999", "h"]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement", "{reply}");
}

#[test]
fn a_serializable_read_answers_on_a_node_that_lost_its_quorum_and_a_linearizable_one_does_not() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handles = nodes(&cluster);
    let revision = put(&handles[0], "/bddkit-etcd/cluster/quorum", "kept");
    for handle in &handles {
        assert!(passed(&eventually(handle, REACHED, &[&revision])));
    }
    cluster.kill(1);
    cluster.kill(2);
    common::wait_without_leader(&handles[0]);
    let local = handles[0].run(GET_SERIALIZABLE, &["/bddkit-etcd/cluster/quorum"]);
    assert!(passed(&local), "{local}");
    assert_eq!(local["vars"]["etcd_value"], "kept");
    let linearizable = handles[0].run(GET, &["/bddkit-etcd/cluster/quorum"]);
    assert_eq!(linearizable["status"], "fatal", "{linearizable}");
    assert_eq!(linearizable["class"], "db_statement", "{linearizable}");
}

#[test]
fn a_node_step_on_an_instance_with_two_endpoints_is_input() {
    // Refused before any connection, so no cluster is needed.
    let handle = Handle::new(&json!({"endpoints": "http://127.0.0.1:1,http://127.0.0.1:2"}));
    for (step, args) in [
        (LEADER, vec![]),
        (NOT_LEADER, vec![]),
        (REACHED, vec!["1"]),
        (COMPUTE_HASH, vec!["1", "h"]),
        (HASH_IS, vec!["1", "2"]),
    ] {
        let reply = handle.run(step, &args);
        assert_eq!(reply["status"], "fatal", "{reply}");
        assert_eq!(reply["class"], "input", "{reply}");
        let error = reply["error"].as_str().expect("error");
        assert!(error.contains("instance \"main\""), "{error}");
    }
}

#[test]
fn the_host_matches_every_cluster_step_and_the_node_rule_reaches_the_tester() {
    let bin = require_bddkit!();
    let steps = [
        "I get etcd key \"/k\" serializable",
        "etcd node should be the leader",
        "etcd node should not be the leader",
        "etcd node should have reached revision 3",
        "I compute etcd hash at revision 3 as \"h\"",
        "etcd hash at revision 3 should be \"7\"",
    ];
    let mut feature = "Feature: cluster\n".to_string();
    for (i, step) in steps.iter().enumerate() {
        feature.push_str(&format!("  Scenario: s{i}\n    When {step}\n"));
    }
    let cfg = common::project_with(
        "cluster-steps",
        "    main:\n      endpoints: http://127.0.0.1:1,http://127.0.0.1:2\n",
        &feature,
    );
    let out = common::bddkit(&bin, &["run", "--config", cfg.to_str().expect("path")]);
    let text = common::combined(&out);
    // Exit 1 is failed scenarios: every step was found and checked up front.
    assert_eq!(out.status.code(), Some(1), "{text}");
    // Five node steps are refused by the one-endpoint rule; the serializable
    // read goes to a server that is down.
    assert_eq!(text.matches("has 2 endpoints").count(), 5, "{text}");
}

#[test]
#[cfg(target_os = "linux")]
fn a_cluster_left_by_a_failing_test_is_torn_down() {
    let bin = require_etcd_bin!();
    let pids = std::sync::Mutex::new(Vec::new());
    let outcome = std::panic::catch_unwind(|| {
        let cluster = Cluster::start(&bin);
        *pids.lock().unwrap() = cluster.pids();
        panic!("the test fails");
    });
    assert!(outcome.is_err());
    let pids = pids.into_inner().unwrap();
    assert_eq!(pids.len(), 3);
    for pid in pids {
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "etcd {pid} outlived the test"
        );
    }
}
