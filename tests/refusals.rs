//! The accept / refuse steps through the exports, against a three-node cluster
//! of real etcd processes that the tests break (`common/cluster.rs`). One reply
//! is one attempt, so a test that waits asks again, as the host does under
//! polling.

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
const REACHED: u32 = 20;
const ACCEPT_PUT: u32 = 23;
const REFUSE_PUT: u32 = 24;
const ACCEPT_READ: u32 = 25;
const ACCEPT_READ_SERIALIZABLE: u32 = 26;
const REFUSE_READ: u32 = 27;
const REFUSE_READ_SERIALIZABLE: u32 = 28;
const REFUSE_COMPACT: u32 = 29;

fn node(cluster: &Cluster, index: usize) -> Handle {
    Handle::new(&json!({"endpoints": cluster.endpoint(index), "timeout_secs": 3}))
}

fn passed(reply: &Value) -> bool {
    reply["status"] == "passed"
}

fn not_yet(reply: &Value, class: &str) {
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], class, "{reply}");
}

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

fn revision(reply: &Value) -> u64 {
    reply["vars"]["etcd_revision"]
        .as_str()
        .expect("revision")
        .parse()
        .expect("number")
}

/// Kills the other two nodes and waits until the survivor knows it has no
/// leader.
fn lose_quorum(cluster: &mut Cluster, survivor: &Handle) {
    cluster.kill(1);
    cluster.kill(2);
    common::wait_without_leader(survivor);
}

#[test]
fn a_node_that_lost_its_quorum_refuses_writes_and_linearizable_reads_and_serves_serializable_ones()
{
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handles: Vec<Handle> = (0..3).map(|i| node(&cluster, i)).collect();
    let key = "/bddkit-etcd/refusals/kept";
    let put = handles[0].run(PUT, &[key, "v"]);
    let wrote = revision(&put).to_string();
    for handle in &handles {
        assert!(passed(&eventually(handle, REACHED, &[&wrote])));
    }
    let node = &handles[0];
    lose_quorum(&mut cluster, node);
    let refused = node.run(REFUSE_PUT, &[key, "Unavailable"]);
    assert!(passed(&refused), "{refused}");
    // Not the code the server answered with.
    not_yet(
        &node.run(REFUSE_PUT, &[key, "PermissionDenied"]),
        "assertion",
    );
    // Accepting is not what happened, and a later attempt may find otherwise.
    let accept = node.run(ACCEPT_PUT, &[key, "w"]);
    not_yet(&accept, "assertion");
    assert!(
        accept["error"].as_str().unwrap().contains("Unavailable"),
        "{accept}"
    );
    assert!(passed(&node.run(REFUSE_READ, &[key, "Unavailable"])));
    not_yet(&node.run(ACCEPT_READ, &[key]), "assertion");
    // The node answers from what it has, and a key it lacks is an answer too.
    assert!(passed(&node.run(ACCEPT_READ_SERIALIZABLE, &[key])));
    assert!(passed(
        &node.run(ACCEPT_READ_SERIALIZABLE, &["/bddkit-etcd/refusals/none"])
    ));
    not_yet(
        &node.run(REFUSE_READ_SERIALIZABLE, &[key, "Unavailable"]),
        "assertion",
    );
    // The quorum is back: the write goes through, under polling.
    cluster.restart(1);
    cluster.restart(2);
    let reply = eventually(node, ACCEPT_PUT, &[key, "w"]);
    assert!(passed(&reply), "{reply}");
    let read = node.run(GET, &[key]);
    assert_eq!(read["vars"]["etcd_value"], "w", "{read}");
    assert!(passed(&eventually(node, ACCEPT_READ, &[key])));
}

#[test]
fn without_require_leader_the_same_put_is_not_an_unavailable_of_the_server() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let survivor = node(&cluster, 0);
    lose_quorum(&mut cluster, &survivor);
    let timeout = Duration::from_secs(3);
    let options = etcd_client::ConnectOptions::new()
        .with_timeout(timeout)
        .with_connect_timeout(timeout);
    let outcome = common::block_on(async {
        let client = etcd_client::Client::connect([cluster.endpoint(0)], Some(options))
            .await
            .expect("connect");
        client
            .kv_client()
            .put("/bddkit-etcd/refusals/x", "v", None)
            .await
    });
    // The node holds the request until the client gives up, so the answer is
    // not the server's `Unavailable`: it is what `refuse a put ... with
    // "Unavailable"` would not pass on.
    let by_server = match &outcome {
        Err(etcd_client::Error::GRpcStatus(status)) => {
            std::error::Error::source(status).is_none() && status.code() == tonic::Code::Unavailable
        }
        _ => false,
    };
    assert!(!by_server, "{outcome:?}");
}

#[test]
fn a_killed_node_is_not_a_refusal_with_unavailable() {
    let bin = require_etcd_bin!();
    let mut cluster = Cluster::start(&bin);
    let handle = node(&cluster, 2);
    assert!(passed(
        &handle.run(ACCEPT_READ, &["/bddkit-etcd/refusals/up"])
    ));
    cluster.kill(2);
    for (step, args) in [
        (REFUSE_PUT, vec!["/k", "Unavailable"]),
        (REFUSE_READ, vec!["/k", "Unavailable"]),
        (REFUSE_READ_SERIALIZABLE, vec!["/k", "Unavailable"]),
        (REFUSE_COMPACT, vec!["1", "Unavailable"]),
        (ACCEPT_PUT, vec!["/k", "v"]),
        (ACCEPT_READ, vec!["/k"]),
        (ACCEPT_READ_SERIALIZABLE, vec!["/k"]),
    ] {
        not_yet(&handle.run(step, &args), "db_connection");
    }
}

#[test]
fn a_compaction_to_a_future_revision_is_refused_with_out_of_range_and_one_that_works_really_compacts()
 {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = node(&cluster, 0);
    let put = handle.run(PUT, &["/bddkit-etcd/refusals/compact", "v"]);
    let current = revision(&put);
    let future = (current + 1000).to_string();
    assert!(passed(
        &handle.run(REFUSE_COMPACT, &[&future, "OutOfRange"])
    ));
    not_yet(
        &handle.run(REFUSE_COMPACT, &[&future, "Unavailable"]),
        "assertion",
    );
    // At a revision that exists the server accepts, and the message says so.
    let now = current.to_string();
    let accepted = handle.run(REFUSE_COMPACT, &[&now, "OutOfRange"]);
    not_yet(&accepted, "assertion");
    let error = accepted["error"].as_str().unwrap();
    assert!(error.contains("accepted"), "{accepted}");
    // It really compacted: the revision is now compacted, an `OutOfRange` too.
    assert!(passed(&handle.run(REFUSE_COMPACT, &[&now, "OutOfRange"])));
}

#[test]
fn an_accepted_put_moves_the_revision_and_an_unknown_key_is_an_accepted_read() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = node(&cluster, 0);
    let key = "/bddkit-etcd/refusals/accepted";
    assert!(passed(&handle.run(ACCEPT_READ, &[key])));
    let before = handle.run(PUT, &[key, "1"]);
    assert!(passed(&handle.run(ACCEPT_PUT, &[key, "2"])));
    let after = handle.run(GET, &[key]);
    assert_eq!(after["vars"]["etcd_value"], "2", "{after}");
    assert_eq!(after["vars"]["etcd_version"], "2", "{after}");
    assert!(revision(&after) > revision(&before));
    // Every attempt is evidence, as any other operation.
    let reply = handle.run(ACCEPT_READ, &[key]);
    assert_eq!(reply["diagnostics"][0]["title"], format!("etcd Get {key}"));
}

#[test]
fn an_unknown_code_in_a_literal_step_is_exit_2_and_the_host_finds_every_step() {
    let bin = require_bddkit!();
    let down = "    main:\n      endpoints: http://127.0.0.1:1\n      timeout_secs: 1\n";
    let run = |name: &str, steps: &[&str]| {
        let mut feature = "Feature: refusals\n  Scenario: s\n".to_string();
        for step in steps {
            feature.push_str(&format!("    {step}\n"));
        }
        let cfg = common::project_with(name, down, &feature);
        let out = common::bddkit(&bin, &["run", "--config", cfg.to_str().expect("path")]);
        (out.status.code(), common::combined(&out))
    };
    for step in [
        r#"When etcd should refuse a put of key "/k" with "OK""#,
        r#"When etcd should refuse a read of key "/k" serializable with "Teapot""#,
        r#"When etcd should refuse a compaction up to revision 3 with "unavailable""#,
    ] {
        let (code, text) = run("refusal-unknown-code", &[step]);
        assert_eq!(code, Some(2), "{step}: {text}");
        assert!(text.contains("is not a gRPC code to refuse with"), "{text}");
    }
    // A slot is checked by the step, which fails on the unknown name.
    let (code, text) = run(
        "refusal-slot",
        &[
            r#"Given set variable "c" to "Teapot""#,
            r#"When etcd should refuse a put of key "/k" with "<<c>>""#,
        ],
    );
    assert_eq!(code, Some(1), "{text}");
    assert!(text.contains("is not a gRPC code to refuse with"), "{text}");
    // Every expression is found, with nothing ambiguous: what fails is the
    // server being down.
    for step in [
        r#"etcd should accept a put of key "/k" with value "v""#,
        r#"etcd should refuse a put of key "/k" with "Unavailable""#,
        r#"etcd should accept a read of key "/k""#,
        r#"etcd should accept a read of key "/k" serializable"#,
        r#"etcd should refuse a read of key "/k" with "Unavailable""#,
        r#"etcd should refuse a read of key "/k" serializable with "Unavailable""#,
        r#"etcd should refuse a compaction up to revision 3 with "OutOfRange""#,
    ] {
        let (code, text) = run("refusal-found", &[&format!("When {step}")]);
        assert_eq!(code, Some(1), "{step}: {text}");
        assert!(text.contains("not reached"), "{step}: {text}");
    }
}
