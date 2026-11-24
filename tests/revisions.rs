//! Compaction and reads at a past revision through the exports, against a
//! cluster of real etcd processes of their own (`common/cluster.rs`), so the
//! compaction touches no other test.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use common::Handle;
use common::cluster::Cluster;
use serde_json::{Value, json};

const PUT: u32 = 0;
const WATCH_KEY_FROM: u32 = 32;
const REFUSE_COMPACT: u32 = 29;
const GET_AT_REVISION: u32 = 55;
const COMPACT: u32 = 56;
const REFUSE_READ_AT: u32 = 57;

const KEY: &str = "/bddkit-etcd/revisions/k";

fn revision(reply: &Value) -> String {
    reply["vars"]["etcd_revision"]
        .as_str()
        .expect("revision")
        .to_string()
}

fn passed(reply: &Value) -> bool {
    reply["status"] == "passed"
}

/// The key written twice: the revision of the first write, the second one.
fn written_twice(handle: &Handle) -> (String, String) {
    let first = handle.run(PUT, &[KEY, "first"]);
    assert!(passed(&first), "{first}");
    let second = handle.run(PUT, &[KEY, "second"]);
    assert!(passed(&second), "{second}");
    (revision(&first), revision(&second))
}

#[test]
fn a_key_is_read_as_it_was_and_a_future_revision_is_refused() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = Handle::new(&json!({"endpoints": cluster.endpoint(0), "timeout_secs": 3}));
    let (first, second) = written_twice(&handle);

    let old = handle.run(GET_AT_REVISION, &[KEY, &first]);
    assert!(passed(&old), "{old}");
    assert_eq!(old["vars"]["etcd_value"], "first", "{old}");
    assert_eq!(old["vars"]["etcd_version"], "1", "{old}");
    // The evidence names the revision that was read.
    let content: Value =
        serde_json::from_str(old["diagnostics"][0]["content"].as_str().unwrap()).unwrap();
    assert_eq!(
        content["request"]["revision"],
        first.parse::<u64>().unwrap()
    );
    let now = handle.run(GET_AT_REVISION, &[KEY, &second]);
    assert_eq!(now["vars"]["etcd_value"], "second", "{now}");

    // A revision the server has not reached is refused, as a statement.
    let future = (second.parse::<u64>().unwrap() + 1000).to_string();
    let reply = handle.run(GET_AT_REVISION, &[KEY, &future]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement", "{reply}");
    assert!(passed(
        &handle.run(REFUSE_READ_AT, &[KEY, &future, "OutOfRange"])
    ));
    // An answer other than the wanted one is not a pass, and says so.
    let wrong = handle.run(REFUSE_READ_AT, &[KEY, &first, "OutOfRange"]);
    assert_eq!(wrong["status"], "not_yet", "{wrong}");
    assert!(
        wrong["error"].as_str().unwrap().contains("accepted"),
        "{wrong}"
    );
}

#[test]
fn a_compacted_revision_is_refused_and_so_is_compacting_it_again() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = Handle::new(&json!({"endpoints": cluster.endpoint(0), "timeout_secs": 3}));
    let (first, second) = written_twice(&handle);
    // Compacting past the first write makes its revision unreadable.
    let compacted = handle.run(COMPACT, &[&second]);
    assert!(passed(&compacted), "{compacted}");
    assert!(revision(&compacted).parse::<u64>().unwrap() >= second.parse().unwrap());
    let reply = handle.run(GET_AT_REVISION, &[KEY, &first]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement", "{reply}");
    assert!(passed(
        &handle.run(REFUSE_READ_AT, &[KEY, &first, "OutOfRange"])
    ));
    // From the compaction itself the read still works.
    assert!(passed(&handle.run(GET_AT_REVISION, &[KEY, &second])));

    // Compacting what is compacted already is refused.
    let again = handle.run(COMPACT, &[&second]);
    assert_eq!(again["class"], "db_statement", "{again}");
    assert!(passed(
        &handle.run(REFUSE_COMPACT, &[&second, "OutOfRange"])
    ));
}

#[test]
fn a_watch_from_a_revision_compacted_by_the_step_fails_the_step() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = Handle::new(&json!({"endpoints": cluster.endpoint(0), "timeout_secs": 3}));
    let (first, second) = written_twice(&handle);
    assert!(passed(&handle.run(COMPACT, &[&second])));
    let reply = handle.run(WATCH_KEY_FROM, &[KEY, "old", &first]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert!(
        reply["error"].as_str().unwrap().contains("compacted"),
        "{reply}"
    );
}

#[test]
fn revision_zero_is_refused_because_the_server_would_read_the_latest() {
    let bin = require_etcd_bin!();
    let cluster = Cluster::start(&bin);
    let handle = Handle::new(&json!({"endpoints": cluster.endpoint(0), "timeout_secs": 3}));
    written_twice(&handle);
    for (step, args) in [
        (GET_AT_REVISION, vec![KEY, "0"]),
        (REFUSE_READ_AT, vec![KEY, "0", "OutOfRange"]),
    ] {
        let reply = handle.run(step, &args);
        assert_eq!(reply["class"], "input", "{reply}");
    }
}

#[test]
fn the_host_runs_a_scenario_that_compacts_and_reads_the_past() {
    let bin = require_bddkit!();
    let etcd = require_etcd_bin!();
    let cluster = Cluster::start(&etcd);
    let section = format!(
        "    main:\n      endpoints: {}\n      timeout_secs: 3\n",
        cluster.endpoint(0)
    );
    let feature = format!(
        r#"Feature: past revisions
  Scenario: compaction
    When I put etcd key "{KEY}" with value "first"
    And set variable "old" to "<<etcd_revision>>"
    And I put etcd key "{KEY}" with value "second"
    And set variable "new" to "<<etcd_revision>>"
    And I get etcd key "{KEY}" at revision <<old>>
    Then variable "etcd_value" should be equal to "first"
    When I compact etcd up to revision <<new>>
    Then etcd should refuse a read of key "{KEY}" at revision <<old>> with "OutOfRange"
    And etcd should refuse a compaction up to revision <<new>> with "OutOfRange"
"#
    );
    let cfg = common::project_with("revisions", &section, &feature);
    let out = common::bddkit(&bin, &["run", "--config", cfg.to_str().expect("path")]);
    assert_eq!(out.status.code(), Some(0), "{}", common::combined(&out));
}
