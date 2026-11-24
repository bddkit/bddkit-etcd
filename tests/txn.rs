//! The transaction steps through the exports, the way the host dispatches
//! them, against a three-node cluster of real etcd processes
//! (`common/cluster.rs`). A doc string is what gherkin hands over: a line
//! break after the opening quotes and one before the closing quotes, so an
//! empty compares section is an empty first line.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use bddkit_etcd::*;
use common::cluster::Cluster;
use common::{Handle, call, cstr};
use serde_json::{Value, json};

const PUT: u32 = 0;
const EXISTS: u32 = 6;
const ABSENT: u32 = 7;
const COUNT: u32 = 14;
const GRANT: u32 = 41;
const REVOKE: u32 = 44;
const HOLDS: u32 = 50;
const COMMIT: u32 = 51;
const SUCCEEDED: u32 = 52;
const FAILED: u32 = 53;
const REFUSE: u32 = 54;

fn all_nodes(cluster: &Cluster) -> Handle {
    let endpoints = (0..3)
        .map(|node| cluster.endpoint(node))
        .collect::<Vec<_>>()
        .join(",");
    Handle::new(&json!({"endpoints": endpoints, "timeout_secs": 3}))
}

fn commit(handle: &Handle, body: &str) -> Value {
    handle.dispatch(COMMIT, &[], Some(&format!("\n{body}\n")), false)
}

fn refuse(handle: &Handle, code: &str, body: &str) -> Value {
    handle.dispatch(REFUSE, &[code], Some(&format!("\n{body}\n")), false)
}

fn passed(reply: &Value) -> &Value {
    assert_eq!(reply["status"], "passed", "{reply}");
    &reply["vars"]
}

/// The evidence of a reply, as one text.
fn evidence(reply: &Value) -> String {
    reply["diagnostics"][0]["content"]
        .as_str()
        .expect("evidence")
        .to_string()
}

fn validate(doc: &str) -> Value {
    let request = json!({"args": [], "docstring": format!("\n{doc}\n")});
    call(bddkit_validate_step(COMMIT, cstr(&request).as_ptr()))
}

#[test]
fn a_lock_is_taken_once_and_the_assertions_say_which_branch_ran() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let lock = "create(\"/txn/lock\") = \"0\"\n\nput /txn/lock alice\n\nget /txn/lock";

    let first = commit(&handle, lock);
    let vars = passed(&first);
    assert_eq!(vars["etcd_txn_succeeded"], "true");
    assert!(
        vars["etcd_revision"]
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap()
            > 0
    );
    assert_eq!(handle.run(SUCCEEDED, &[])["status"], "passed");
    let mismatch = handle.run(FAILED, &[]);
    assert_eq!(mismatch["status"], "fatal", "{mismatch}");
    assert_eq!(mismatch["class"], "assertion");

    // The second attempt does not hold: the step passes, and its failure
    // branch read the key.
    let second = commit(&handle, lock);
    assert_eq!(passed(&second)["etcd_txn_succeeded"], "false");
    let seen = evidence(&second);
    assert!(seen.contains("\"branch\": \"failure\""), "{seen}");
    assert!(
        seen.contains("/txn/lock") && seen.contains("alice"),
        "{seen}"
    );
    assert_eq!(handle.run(FAILED, &[])["status"], "passed");
    let mismatch = handle.run(SUCCEEDED, &[]);
    assert_eq!(mismatch["status"], "fatal", "{mismatch}");
    assert_eq!(mismatch["class"], "assertion");
}

#[test]
fn every_compare_target_reads_the_key() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let granted = handle.run(GRANT, &["l", "60"]);
    let id = passed(&granted)["etcd_lease_l"]
        .as_str()
        .unwrap()
        .to_string();
    let put = commit(&handle, &format!("\nput /txn/c v --lease={id}"));
    let revision = passed(&put)["etcd_revision"].as_str().unwrap().to_string();
    let holds = [
        "value(\"/txn/c\") = \"v\"".to_string(),
        "val(\"/txn/c\") != \"w\"".to_string(),
        "version(\"/txn/c\") = \"1\"".to_string(),
        "ver(\"/txn/c\") < \"2\"".to_string(),
        format!("create(\"/txn/c\") = \"{revision}\""),
        "c(\"/txn/c\") > \"0\"".to_string(),
        format!("mod(\"/txn/c\") = \"{revision}\""),
        "m(\"/txn/c\") > \"0\"".to_string(),
        format!("lease(\"/txn/c\") = \"{id}\""),
        "l(\"/txn/c\") > \"0\"".to_string(),
    ];
    for compare in &holds {
        let reply = commit(&handle, &format!("{compare}\n\nget /txn/c"));
        assert_eq!(passed(&reply)["etcd_txn_succeeded"], "true", "{compare}");
    }
    let broken = [
        "value(\"/txn/c\") = \"w\"",
        "version(\"/txn/c\") > \"1\"",
        "create(\"/txn/c\") = \"0\"",
        "mod(\"/txn/c\") < \"1\"",
        "lease(\"/txn/c\") = \"0\"",
    ];
    for compare in broken {
        let reply = commit(&handle, compare);
        assert_eq!(passed(&reply)["etcd_txn_succeeded"], "false", "{compare}");
    }
    // A key with a space and a value with quotes, both in quotes.
    let quoted = commit(
        &handle,
        "\nput \"/txn/a key\" \"say \\\"hi\\\" \\\\ bye\"\n\nget \"/txn/a key\"",
    );
    assert_eq!(passed(&quoted)["etcd_txn_succeeded"], "true");
}

#[test]
fn a_delete_with_a_prefix_in_a_branch_deletes_what_is_under_it() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    for key in ["/txn/p/a", "/txn/p/b", "/txn/p/c", "/txn/q"] {
        passed(&handle.run(PUT, &[key, "v"]));
    }
    let reply = commit(&handle, "\ndel /txn/p/ --prefix");
    assert_eq!(passed(&reply)["etcd_txn_succeeded"], "true");
    assert!(evidence(&reply).contains("\"deleted\": 3"), "{reply}");
    assert_eq!(handle.run(COUNT, &["0", "/txn/p/"])["status"], "passed");
    assert_eq!(handle.run(ABSENT, &["/txn/p/b"])["status"], "passed");
    assert_eq!(handle.run(EXISTS, &["/txn/q"])["status"], "passed");
    // A get with a range end reads the half-open range.
    let range = commit(&handle, "\nget /txn/p/ /txn/q");
    assert_eq!(passed(&range)["etcd_txn_succeeded"], "true");
}

#[test]
fn a_put_under_a_lease_attaches_the_key_to_it() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let granted = handle.run(GRANT, &["session", "60"]);
    let id = passed(&granted)["etcd_lease_session"]
        .as_str()
        .unwrap()
        .to_string();
    let reply = commit(&handle, &format!("\nput /txn/leased v --lease={id}"));
    assert_eq!(passed(&reply)["etcd_txn_succeeded"], "true");
    assert_eq!(handle.run(HOLDS, &["session", "1"])["status"], "passed");
    passed(&handle.run(REVOKE, &["session"]));
    let gone = (0..100).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        handle.run(ABSENT, &["/txn/leased"])["status"] == "passed"
    });
    assert!(gone, "the key outlived its lease");
}

#[test]
fn two_puts_of_one_key_are_refused_by_the_server_and_the_step_says_so() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let twice = "\nput /txn/dup a\nput /txn/dup b";

    // The committing step fails, with the class of a status of the server.
    let reply = commit(&handle, twice);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement");
    // That is no outcome: nothing was committed.
    assert_eq!(handle.run(SUCCEEDED, &[])["class"], "input");

    let reply = refuse(&handle, "InvalidArgument", twice);
    assert_eq!(reply["status"], "passed", "{reply}");
    let other = refuse(&handle, "PermissionDenied", twice);
    assert_eq!(other["status"], "not_yet", "{other}");
    assert_eq!(other["class"], "assertion");
    assert!(other["error"].as_str().unwrap().contains("InvalidArgument"));

    // A transaction the server accepts is not refused, and was applied.
    let accepted = refuse(&handle, "InvalidArgument", "\nput /txn/fine v");
    assert_eq!(accepted["status"], "not_yet", "{accepted}");
    assert_eq!(accepted["class"], "assertion");
    assert!(accepted["error"].as_str().unwrap().contains("accepted"));
    assert_eq!(handle.run(EXISTS, &["/txn/fine"])["status"], "passed");
}

#[test]
fn the_outcome_belongs_to_the_scenario_and_a_client_failure_is_never_a_refusal() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = all_nodes(&cluster);
    let none = handle.run(SUCCEEDED, &[]);
    assert_eq!(none["status"], "fatal", "{none}");
    assert_eq!(none["class"], "input");
    passed(&commit(&handle, "\nput /txn/s v"));
    assert_eq!(handle.run(SUCCEEDED, &[])["status"], "passed");
    let reset = call(bddkit_reset_scenario(handle.0));
    assert_eq!(reset["ok"], true, "{reset}");
    assert_eq!(handle.run(SUCCEEDED, &[])["class"], "input");

    // No server: the failure is on the client side.
    let down = Handle::new(&json!({"endpoints": "http://127.0.0.1:1", "timeout_secs": 1}));
    let reply = commit(&down, "\nput /txn/k v");
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_connection");
    let reply = refuse(&down, "Unavailable", "\nput /txn/k v");
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "db_connection");
}

#[test]
fn the_syntax_is_checked_before_the_first_request_and_again_after_interpolation() {
    // No server needed: nothing is sent.
    let literal = validate("c(\"/k\") = \"0\"\n\nput /k v\n\nget /k --keys-only");
    assert_eq!(literal["ok"], false, "{literal}");
    assert!(
        literal["error"].as_str().unwrap().starts_with("line 5:"),
        "{literal}"
    );
    assert_eq!(
        validate("c(\"/k\") = \"<<n>>\"\n\nput /k v --lease=<<id>>")["ok"],
        true
    );
    let handle = Handle::new(&json!({"endpoints": "http://127.0.0.1:1", "timeout_secs": 1}));
    let reply = commit(&handle, "c(\"/k\") = \"x\"");
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "input");
    assert!(
        reply["error"].as_str().unwrap().starts_with("line 1:"),
        "{reply}"
    );
}
