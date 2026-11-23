//! The steps: what the host is told about each, and the one check that needs
//! no connection. The index of a step in `ALL` is its identity, the `u32` the
//! host passes back, so the array grows by appending and the list, the specs
//! and the dispatch live in this one file.

use serde_json::{Value, json};

use crate::check;
use crate::expect;
use crate::node;
use crate::ops::Request;
use crate::reply;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Put,
    PutDocString,
    Get,
    DeleteKey,
    DeletePrefix,
    DeleteAll,
    Exists,
    Absent,
    HasValue,
    HasValueDoc,
    Matches,
    ContainsJson,
    EqualsJson,
    HasFields,
    Count,
    KeysBe,
    KeysContain,
    GetSerializable,
    Leader,
    NotLeader,
    Reached,
    ComputeHash,
    HashIs,
    AcceptPut,
    RefusePut,
    AcceptRead,
    AcceptReadSerializable,
    RefuseRead,
    RefuseReadSerializable,
    RefuseCompact,
}

/// In the order the host indexes them.
const ALL: [Step; 30] = [
    Step::Put,
    Step::PutDocString,
    Step::Get,
    Step::DeleteKey,
    Step::DeletePrefix,
    Step::DeleteAll,
    Step::Exists,
    Step::Absent,
    Step::HasValue,
    Step::HasValueDoc,
    Step::Matches,
    Step::ContainsJson,
    Step::EqualsJson,
    Step::HasFields,
    Step::Count,
    Step::KeysBe,
    Step::KeysContain,
    Step::GetSerializable,
    Step::Leader,
    Step::NotLeader,
    Step::Reached,
    Step::ComputeHash,
    Step::HashIs,
    Step::AcceptPut,
    Step::RefusePut,
    Step::AcceptRead,
    Step::AcceptReadSerializable,
    Step::RefuseRead,
    Step::RefuseReadSerializable,
    Step::RefuseCompact,
];

struct Spec {
    expression: &'static str,
    description: &'static str,
    /// The variables the step sets. Always declared, even when empty: a step
    /// that declares no effect is unknown to `doctor`.
    sets_vars: &'static [&'static str],
}

const NONE: &[&str] = &[];
const REVISION: &[&str] = &["etcd_revision"];
const DELETED: &[&str] = &["etcd_revision", "etcd_deleted"];
const HASH: &[&str] = &["{name}"];
const READ: &[&str] = &[
    "etcd_value",
    "etcd_create_revision",
    "etcd_mod_revision",
    "etcd_version",
    "etcd_lease",
    "etcd_revision",
];

impl Step {
    pub fn from_index(index: u32) -> Option<Self> {
        ALL.get(usize::try_from(index).ok()?).copied()
    }

    /// An assertion only reads, and the host may ask it again.
    pub fn is_assertion(self) -> bool {
        match self {
            Self::Put
            | Self::PutDocString
            | Self::Get
            | Self::DeleteKey
            | Self::DeletePrefix
            | Self::DeleteAll
            | Self::GetSerializable
            | Self::ComputeHash => false,
            Self::Exists
            | Self::Absent
            | Self::HasValue
            | Self::HasValueDoc
            | Self::Matches
            | Self::ContainsJson
            | Self::EqualsJson
            | Self::HasFields
            | Self::Count
            | Self::KeysBe
            | Self::KeysContain
            | Self::Leader
            | Self::NotLeader
            | Self::Reached
            | Self::HashIs
            | Self::AcceptPut
            | Self::RefusePut
            | Self::AcceptRead
            | Self::AcceptReadSerializable
            | Self::RefuseRead
            | Self::RefuseReadSerializable
            | Self::RefuseCompact => true,
        }
    }

    /// The steps about one node of a cluster, see `node.rs`.
    pub fn is_node(self) -> bool {
        matches!(
            self,
            Self::Leader | Self::NotLeader | Self::Reached | Self::ComputeHash | Self::HashIs
        )
    }

    /// The steps that expect the server to accept or refuse, see `expect.rs`.
    pub fn is_expect(self) -> bool {
        matches!(
            self,
            Self::AcceptPut
                | Self::RefusePut
                | Self::AcceptRead
                | Self::AcceptReadSerializable
                | Self::RefuseRead
                | Self::RefuseReadSerializable
                | Self::RefuseCompact
        )
    }

    /// The assertions whose first argument is the one key they read.
    pub fn reads_one_key(self) -> bool {
        matches!(
            self,
            Self::Exists
                | Self::Absent
                | Self::HasValue
                | Self::HasValueDoc
                | Self::Matches
                | Self::ContainsJson
                | Self::EqualsJson
                | Self::HasFields
        )
    }

    fn spec(self) -> Spec {
        let (expression, description, sets_vars) = match self {
            Self::Put => (
                r#"I put etcd key "{key}" with value "{value}""#,
                "writes a key and stores the revision of the response in etcd_revision",
                REVISION,
            ),
            Self::PutDocString => (
                r#"I put etcd key "{key}" with value:"#,
                "writes a key whose value is the doc string below, and stores the revision in etcd_revision",
                REVISION,
            ),
            Self::Get => (
                r#"I get etcd key "{key}""#,
                "reads a key (linearizable) into etcd_value and its revisions, version and lease; fails when the key does not exist",
                READ,
            ),
            Self::DeleteKey => (
                r#"I delete etcd key "{key}""#,
                "deletes a key and stores etcd_revision and the number of keys deleted in etcd_deleted",
                DELETED,
            ),
            Self::DeletePrefix => (
                r#"I delete etcd keys with prefix "{prefix}""#,
                "deletes every key under a prefix; an empty prefix is refused, it would delete the whole server",
                DELETED,
            ),
            Self::DeleteAll => (
                "I delete all etcd keys",
                "clears the whole server, with no awareness of files running in parallel",
                DELETED,
            ),
            Self::Exists => (
                r#"etcd key "{key}" should exist"#,
                "reads the key again on every attempt; passes when it exists",
                NONE,
            ),
            Self::Absent => (
                r#"etcd key "{key}" should not exist"#,
                "reads the key again on every attempt; passes when it does not exist",
                NONE,
            ),
            Self::HasValue => (
                r#"etcd key "{key}" should have value "{value}""#,
                "passes when the value of the key is exactly this text",
                NONE,
            ),
            Self::HasValueDoc => (
                r#"etcd key "{key}" should have value:"#,
                "passes when the value of the key is exactly the doc string below",
                NONE,
            ),
            Self::Matches => (
                r#"etcd key "{key}" value should match "{regex}""#,
                "passes when the value matches the regular expression (not anchored)",
                NONE,
            ),
            Self::ContainsJson => (
                r#"etcd key "{key}" value should contain JSON:"#,
                "passes when the JSON value contains the JSON of the doc string, matchers allowed",
                NONE,
            ),
            Self::EqualsJson => (
                r#"etcd key "{key}" value should equal JSON:"#,
                "passes when the JSON value equals the JSON of the doc string, matchers allowed",
                NONE,
            ),
            Self::HasFields => (
                r#"etcd key "{key}" should have:"#,
                "passes when the key has the version, create_revision, mod_revision and lease of a field / value table",
                NONE,
            ),
            Self::Count | Self::KeysBe | Self::KeysContain => self.range_spec(),
            Self::GetSerializable
            | Self::Leader
            | Self::NotLeader
            | Self::Reached
            | Self::ComputeHash
            | Self::HashIs => self.cluster_spec(),
            Self::AcceptPut
            | Self::RefusePut
            | Self::AcceptRead
            | Self::AcceptReadSerializable
            | Self::RefuseRead
            | Self::RefuseReadSerializable
            | Self::RefuseCompact => self.expect_spec(),
        };
        Spec {
            expression,
            description,
            sets_vars,
        }
    }

    /// The assertions over a prefix, kept apart so `spec` stays short.
    fn range_spec(self) -> (&'static str, &'static str, &'static [&'static str]) {
        match self {
            Self::Count => (
                r#"there should be {n:uint} etcd keys with prefix "{prefix}""#,
                "passes when the server counts this many keys under the prefix",
                NONE,
            ),
            Self::KeysBe => (
                r#"etcd keys with prefix "{prefix}" should be:"#,
                "passes when the keys under the prefix are exactly those of a key / value table, in key order",
                NONE,
            ),
            Self::KeysContain => (
                r#"etcd keys with prefix "{prefix}" should contain:"#,
                "passes when every key of a key / value table is under the prefix",
                NONE,
            ),
            _ => unreachable!("not a prefix assertion"),
        }
    }

    /// The accept / refuse steps, kept apart so `spec` stays short.
    fn expect_spec(self) -> (&'static str, &'static str, &'static [&'static str]) {
        match self {
            Self::AcceptPut => (
                r#"etcd should accept a put of key "{key}" with value "{value}""#,
                "performs the put again on every attempt and passes once the server accepts it; an accepted put really writes, so under polling it may write more than once",
                NONE,
            ),
            Self::RefusePut => (
                r#"etcd should refuse a put of key "{key}" with "{code}""#,
                "puts an empty value again on every attempt and passes once the server refuses it with this gRPC code (a name as tonic spells it: PermissionDenied, Unavailable, ...); a failure on the client side is never a refusal, and a put the server accepts really writes",
                NONE,
            ),
            Self::AcceptRead => (
                r#"etcd should accept a read of key "{key}""#,
                "reads the key (linearizable) again on every attempt and passes once the server answers; a key that does not exist is an answer",
                NONE,
            ),
            Self::AcceptReadSerializable => (
                r#"etcd should accept a read of key "{key}" serializable"#,
                "reads the key from the node that answers, without asking the leader, and passes once it answers; a key that does not exist is an answer",
                NONE,
            ),
            Self::RefuseRead => (
                r#"etcd should refuse a read of key "{key}" with "{code}""#,
                "reads the key (linearizable) again on every attempt and passes once the server refuses it with this gRPC code (a name as tonic spells it: PermissionDenied, Unavailable, ...); a failure on the client side is never a refusal",
                NONE,
            ),
            Self::RefuseReadSerializable => (
                r#"etcd should refuse a read of key "{key}" serializable with "{code}""#,
                "reads the key without asking the leader again on every attempt and passes once the server refuses it with this gRPC code (a name as tonic spells it: PermissionDenied, Unavailable, ...); a failure on the client side is never a refusal",
                NONE,
            ),
            Self::RefuseCompact => (
                "etcd should refuse a compaction up to revision {rev:uint} with \"{code}\"",
                "compacts up to the revision again on every attempt and passes once the server refuses it with this gRPC code (a name as tonic spells it: PermissionDenied, Unavailable, ...); a compaction the server accepts really compacts, so keep such files in a @serial chain",
                NONE,
            ),
            _ => unreachable!("not an accept / refuse step"),
        }
    }

    /// The steps of the cluster, kept apart so `spec` stays short.
    fn cluster_spec(self) -> (&'static str, &'static str, &'static [&'static str]) {
        match self {
            Self::GetSerializable => (
                r#"I get etcd key "{key}" serializable"#,
                "reads a key from the node that answers, without asking the leader (it may be stale); same variables and failures as I get etcd key",
                READ,
            ),
            Self::Leader => (
                "etcd node should be the leader",
                "passes when the node of the instance, which must have exactly one endpoint, leads the cluster",
                NONE,
            ),
            Self::NotLeader => (
                "etcd node should not be the leader",
                "passes when the node of the instance, which must have exactly one endpoint, does not lead the cluster",
                NONE,
            ),
            Self::Reached => (
                "etcd node should have reached revision {rev:uint}",
                "passes once the revision of the node, which must have exactly one endpoint, is at least this",
                NONE,
            ),
            Self::ComputeHash => (
                r#"I compute etcd hash at revision {rev:uint} as "{name}""#,
                "stores the HashKV hash of the node, which must have exactly one endpoint, at the revision in the variable; a compaction changes the hash of an older revision",
                HASH,
            ),
            Self::HashIs => (
                r#"etcd hash at revision {rev:uint} should be "{hash}""#,
                "passes when the HashKV hash of the node at the revision is this decimal number; not yet while the node has not reached the revision, a different hash is final (assuming no compaction between the two hashes)",
                NONE,
            ),
            _ => unreachable!("not a cluster step"),
        }
    }
}

/// The `bddkit_list_steps` reply.
pub fn list_json() -> String {
    let steps: Vec<Value> = ALL
        .iter()
        .map(|step| {
            let spec = step.spec();
            json!({
                "expression": spec.expression,
                "group": "etcd",
                "kind": if step.is_assertion() { "assertion" } else { "action" },
                "description": spec.description,
                "sets_vars": spec.sets_vars,
            })
        })
        .collect();
    Value::Array(steps).to_string()
}

pub const EMPTY_PREFIX: &str = "the prefix is empty, and its range is the whole keyspace; to clear the server write \"I delete all etcd keys\"";

/// `bddkit_validate_step`: what can be checked before the first request. A
/// value that holds a `<<slot>>` is accepted here, and refused by the step
/// itself if it ends up empty.
pub fn validate(index: u32, request: &str) -> String {
    let Some(step) = Step::from_index(index) else {
        return reply::err("unknown step", &[]);
    };
    let request: Value = match serde_json::from_str(request) {
        Ok(request) => request,
        Err(error) => return reply::err(format!("malformed request: {error}"), &[]),
    };
    if step == Step::DeletePrefix && request["args"][0] == "" {
        return reply::err(EMPTY_PREFIX, &[]);
    }
    if step.is_expect() {
        let checked = Request::parse(&request.to_string()).and_then(|r| expect::validate(step, &r));
        if let Err(error) = checked {
            return reply::err(error, &[]);
        }
    } else if step.is_node() {
        let checked = Request::parse(&request.to_string()).and_then(|r| node::validate(step, &r));
        if let Err(error) = checked {
            return reply::err(error, &[]);
        }
    } else if step.is_assertion() {
        let checked =
            Request::parse(&request.to_string()).and_then(|parsed| check::validate(step, &parsed));
        if let Err(error) = checked {
            return reply::err(error, &[]);
        }
    }
    reply::ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_step_declares_only_etcd_variables_and_says_what_kind_it_is() {
        let steps: Vec<Value> = serde_json::from_str(&list_json()).expect("JSON");
        assert_eq!(steps.len(), ALL.len());
        for step in &steps {
            assert_eq!(step["group"], "etcd");
            let expression = step["expression"].as_str().expect("text");
            let kind = if expression.contains("should") {
                "assertion"
            } else {
                "action"
            };
            assert_eq!(step["kind"], kind, "{expression}");
            assert!(step["expression"].as_str().expect("text").contains("etcd"));
            for var in step["sets_vars"].as_array().expect("list") {
                let var = var.as_str().expect("text");
                // A variable the tester names is the value of a parameter.
                assert!(var.starts_with("etcd_") || var == "{name}", "{var}");
            }
        }
    }

    #[test]
    fn the_index_is_stable() {
        for (index, step) in ALL.iter().enumerate() {
            let found = Step::from_index(u32::try_from(index).expect("small")).expect("step");
            assert!(found == *step);
        }
        assert!(Step::from_index(30).is_none());
    }

    #[test]
    fn a_literal_empty_prefix_is_refused_and_a_slot_is_not() {
        let check = |index, args: Value| -> Value {
            serde_json::from_str(&validate(index, &json!({"args": args}).to_string()))
                .expect("JSON")
        };
        let prefix = 4;
        assert_eq!(check(prefix, json!([""]))["ok"], false);
        assert_eq!(check(prefix, json!(["<<p>>"]))["ok"], true);
        assert_eq!(check(prefix, json!(["/a/"]))["ok"], true);
        // Only the prefix step has this rule.
        assert_eq!(check(3, json!([""]))["ok"], true);
        assert_eq!(check(99, json!([]))["ok"], false);
    }
}
