//! The steps: what the host is told about each, and the one check that needs
//! no connection. The index of a step in `ALL` is its identity, the `u32` the
//! host passes back, so the array grows by appending and the list, the specs
//! and the dispatch live in this one file.

use serde_json::{Value, json};

use crate::reply;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Put,
    PutDocString,
    Get,
    DeleteKey,
    DeletePrefix,
    DeleteAll,
}

/// In the order the host indexes them.
const ALL: [Step; 6] = [
    Step::Put,
    Step::PutDocString,
    Step::Get,
    Step::DeleteKey,
    Step::DeletePrefix,
    Step::DeleteAll,
];

struct Spec {
    expression: &'static str,
    description: &'static str,
    /// The variables the step sets. Always declared, even when empty: a step
    /// that declares no effect is unknown to `doctor`.
    sets_vars: &'static [&'static str],
}

const REVISION: &[&str] = &["etcd_revision"];
const DELETED: &[&str] = &["etcd_revision", "etcd_deleted"];
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
        };
        Spec {
            expression,
            description,
            sets_vars,
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
                "kind": "action",
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
    reply::ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_step_is_an_action_that_declares_only_etcd_variables() {
        let steps: Vec<Value> = serde_json::from_str(&list_json()).expect("JSON");
        assert_eq!(steps.len(), ALL.len());
        for step in &steps {
            assert_eq!(step["group"], "etcd");
            assert_eq!(step["kind"], "action");
            assert!(step["expression"].as_str().expect("text").contains("etcd"));
            for var in step["sets_vars"].as_array().expect("list") {
                assert!(var.as_str().expect("text").starts_with("etcd_"), "{var}");
            }
        }
    }

    #[test]
    fn the_index_is_stable() {
        for (index, step) in ALL.iter().enumerate() {
            let found = Step::from_index(u32::try_from(index).expect("small")).expect("step");
            assert!(found == *step);
        }
        assert!(Step::from_index(6).is_none());
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
