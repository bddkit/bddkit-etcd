//! The steps that look at one node of a cluster: who leads, how far it has
//! applied, what its keyspace hashes to. They need an instance with exactly
//! one endpoint, because a client over several balances and would answer for
//! whichever node it picked, and they run on a client without
//! `require_leader`, which a node that lost its leader would refuse.

use std::collections::BTreeMap;

use etcd_client::Client;
use serde_json::json;

use crate::check::is_slot;
use crate::client::request_failure;
use crate::error::{Class, StepError};
use crate::evidence::Record;
use crate::ops::{Request, Vars, header};
use crate::steps::Step;

#[derive(Debug)]
pub enum Node {
    Leader,
    NotLeader,
    Reached(i64),
    ComputeHash { revision: i64, name: String },
    HashIs { revision: i64, hash: u32 },
}

/// A revision a hash can be taken at: etcd reads `HashKV(0)` as "the current
/// revision", which a step that names a revision must not mean.
fn hash_revision(text: &str) -> Result<i64, StepError> {
    match revision(text)? {
        0 => Err(StepError::input(
            "revision 0 means \"the current revision\" to etcd, and its hash changes with every write; name the revision",
        )),
        rev => Ok(rev),
    }
}

fn revision(text: &str) -> Result<i64, StepError> {
    text.parse::<u64>()
        .ok()
        .and_then(|rev| i64::try_from(rev).ok())
        .ok_or_else(|| StepError::input(format!("\"{text}\" is not a revision")))
}

/// The name of a variable: what `<<name>>` can read back.
fn variable(text: &str) -> Result<String, StepError> {
    if !text.is_empty() && text.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ok(text.to_string());
    }
    Err(StepError::input(format!(
        "\"{text}\" is not a variable name: use letters, digits and underscores"
    )))
}

fn hash(text: &str) -> Result<u32, StepError> {
    text.parse().map_err(|_| {
        StepError::input(format!(
            "\"{text}\" is not a hash: it is the decimal number \"I compute etcd hash\" stores"
        ))
    })
}

/// Everything that can be refused without a request, refused here. `endpoints`
/// is how many the instance has.
pub fn plan(step: Step, request: &Request, endpoints: usize) -> Result<Node, StepError> {
    if endpoints != 1 {
        return Err(StepError::input(format!(
            "instance \"{}\" has {endpoints} endpoints, and a node step needs exactly one: with several, the client balances and would answer for whichever node it picked",
            request.instance()
        )));
    }
    Ok(match step {
        Step::Leader => Node::Leader,
        Step::NotLeader => Node::NotLeader,
        Step::Reached => Node::Reached(revision(request.arg(0)?)?),
        Step::ComputeHash => Node::ComputeHash {
            revision: hash_revision(request.arg(0)?)?,
            name: variable(request.arg(1)?)?,
        },
        Step::HashIs => Node::HashIs {
            revision: hash_revision(request.arg(0)?)?,
            hash: hash(request.arg(1)?)?,
        },
        Step::Put
        | Step::PutDocString
        | Step::Get
        | Step::GetSerializable
        | Step::DeleteKey
        | Step::DeletePrefix
        | Step::DeleteAll
        | Step::Exists
        | Step::Absent
        | Step::HasValue
        | Step::HasValueDoc
        | Step::Matches
        | Step::ContainsJson
        | Step::EqualsJson
        | Step::HasFields
        | Step::Count
        | Step::KeysBe
        | Step::KeysContain => return Err(StepError::input("not a node step")),
    })
}

/// `bddkit_validate_step` for a node step: what a literal already says.
pub fn validate(step: Step, request: &Request) -> Result<(), String> {
    let literal = |index: usize| request.args.get(index).filter(|a| !is_slot(a));
    let check = |index: usize, parse: fn(&str) -> Result<(), StepError>| {
        literal(index).map_or(Ok(()), |text| parse(text).map_err(|e| e.message))
    };
    match step {
        Step::Reached => check(0, |t| revision(t).map(drop)),
        Step::ComputeHash => {
            check(0, |t| hash_revision(t).map(drop))?;
            check(1, |t| variable(t).map(drop))
        }
        Step::HashIs => {
            check(0, |t| hash_revision(t).map(drop))?;
            check(1, |t| hash(t).map(drop))
        }
        _ => Ok(()),
    }
}

pub fn record_for(node: &Node, instance: &str) -> Record {
    match node {
        Node::Leader | Node::NotLeader => Record::new("Status", "(node)", instance, json!({})),
        Node::Reached(rev) => Record::new(
            "Status",
            "(node)",
            instance,
            json!({"wanted_revision": rev}),
        ),
        Node::ComputeHash { revision, .. } | Node::HashIs { revision, .. } => Record::new(
            "HashKV",
            &format!("revision {revision}"),
            instance,
            json!({"revision": revision}),
        ),
    }
}

fn miss(message: String) -> StepError {
    StepError::not_yet(Class::Assertion, message)
}

fn not_reached(at: i64, wanted: i64) -> StepError {
    miss(format!(
        "the node is at revision {at}, it has not reached revision {wanted}"
    ))
}

/// The revision the node has applied, from the header of a `Status`.
async fn applied(client: &Client, tls: bool, record: &mut Record) -> Result<i64, StepError> {
    let status = client
        .maintenance_client()
        .status()
        .await
        .map_err(|e| request_failure(&e, tls))?;
    record.answered(status.header());
    Ok(header(status.header())?.revision())
}

/// `HashKV` is refused while the node has not reached the revision, and that
/// is `not_yet`; the refusal itself says nothing a class could be read from
/// (a future and a compacted revision are the same status), so the node's own
/// revision is what decides.
async fn hash_at(
    client: &Client,
    tls: bool,
    revision: i64,
    record: &mut Record,
) -> Result<u32, StepError> {
    let at = applied(client, tls, record).await?;
    if at < revision {
        return Err(not_reached(at, revision));
    }
    let response = client
        .maintenance_client()
        .hash_kv(revision)
        .await
        .map_err(|e| request_failure(&e, tls))?;
    record.answered(response.header());
    record
        .keys
        .push(json!({"compact_revision": response.compact_version()}));
    Ok(response.hash())
}

pub async fn perform(
    client: &Client,
    tls: bool,
    node: &Node,
    record: &mut Record,
) -> Result<Vars, StepError> {
    match node {
        Node::Leader | Node::NotLeader => {
            let status = client
                .maintenance_client()
                .status()
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(status.header());
            let member = header(status.header())?.member_id();
            let leader = status.leader();
            record.keys.push(json!({
                "member": format!("{member:x}"),
                "leader": format!("{leader:x}"),
            }));
            let leads = leader == member;
            match (node, leads) {
                (Node::Leader, true) | (Node::NotLeader, false) => Ok(Vars::new()),
                (Node::Leader, false) if leader == 0 => Err(miss(format!(
                    "etcd node {member:x} is not the leader: the cluster has no leader"
                ))),
                (Node::Leader, false) => Err(miss(format!(
                    "etcd node {member:x} is not the leader, {leader:x} is"
                ))),
                _ => Err(miss(format!("etcd node {member:x} is the leader"))),
            }
        }
        Node::Reached(wanted) => {
            // `Status` needs no permission on a key, and its header carries
            // the revision the node has applied.
            let at = applied(client, tls, record).await?;
            if at >= *wanted {
                return Ok(Vars::new());
            }
            Err(not_reached(at, *wanted))
        }
        Node::ComputeHash { revision, name } => {
            let response = client
                .maintenance_client()
                .hash_kv(*revision)
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(response.header());
            Ok(BTreeMap::from([(
                name.clone(),
                response.hash().to_string(),
            )]))
        }
        Node::HashIs { revision, hash } => {
            let actual = hash_at(client, tls, *revision, record).await?;
            if actual == *hash {
                return Ok(Vars::new());
            }
            // Asking again is pointless while nothing changes the keyspace up
            // to the revision. A compaction does (the hash covers what is
            // kept), so a comparison is meaningful only when none came
            // between: `compact_revision` is in the evidence.
            Err(StepError::assertion(format!(
                "the hash of etcd at revision {revision} is {actual}, expected {hash}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(args: &[&str]) -> Request {
        Request::for_test(args)
    }

    #[test]
    fn a_node_step_on_several_endpoints_is_input_and_names_the_instance() {
        let error = plan(Step::Leader, &request(&[]), 2).expect_err("refused");
        assert_eq!(error.class, Class::Input);
        assert!(error.message.contains("instance \"main\""), "{error}");
        assert!(error.message.contains("2 endpoints"), "{error}");
        assert!(plan(Step::Leader, &request(&[]), 1).is_ok());
    }

    #[test]
    fn the_arguments_are_a_revision_a_variable_name_and_a_decimal_hash() {
        assert!(matches!(
            plan(Step::Reached, &request(&["12"]), 1),
            Ok(Node::Reached(12))
        ));
        assert!(plan(Step::Reached, &request(&["-1"]), 1).is_err());
        // Zero is "the current revision" to HashKV, so a hash is never taken at it.
        assert!(plan(Step::Reached, &request(&["0"]), 1).is_ok());
        for step in [Step::ComputeHash, Step::HashIs] {
            let error = plan(step, &request(&["0", "1"]), 1).expect_err("refused");
            assert_eq!(error.class, Class::Input);
            assert!(validate(step, &request(&["0", "1"])).is_err());
        }
        assert!(plan(Step::ComputeHash, &request(&["3", "h 1"]), 1).is_err());
        assert!(plan(Step::ComputeHash, &request(&["3", ""]), 1).is_err());
        assert!(plan(Step::HashIs, &request(&["3", "0xff"]), 1).is_err());
        assert!(matches!(
            plan(Step::HashIs, &request(&["3", "4294967295"]), 1),
            Ok(Node::HashIs {
                revision: 3,
                hash: u32::MAX
            })
        ));
    }

    #[test]
    fn validation_checks_literals_and_lets_a_slot_through() {
        let bad = validate(Step::HashIs, &request(&["3", "x"]));
        assert!(bad.expect_err("refused").contains("not a hash"));
        assert!(validate(Step::HashIs, &request(&["<<rev>>", "<<h>>"])).is_ok());
        assert!(validate(Step::ComputeHash, &request(&["3", "a-b"])).is_err());
        assert!(validate(Step::Leader, &request(&[])).is_ok());
    }
}
