//! The steps that expect the server to accept or to refuse an operation:
//! `etcd should accept a put …`, `etcd should refuse a read …`. Each attempt
//! performs the operation itself, so the host's polling repeats it. Only an
//! answer of the server counts as a refusal: a failure on the client side
//! (connect, transport, timeout) is `db_connection`, so that a node that was
//! simply killed never satisfies a refusal with `Unavailable`.

use etcd_client::{Client, Error, GetOptions};
use serde_json::json;
use tonic::Code;

use crate::check::is_slot;
use crate::client::{class_of, request_failure};
use crate::error::{Class, StepError};
use crate::evidence::Record;
use crate::node::revision;
use crate::ops::{Request, Vars};
use crate::steps::Step;

#[derive(Debug, PartialEq, Eq)]
enum Call {
    /// The value is empty when the step names none: a refusal is not about it.
    Put {
        key: String,
        value: String,
    },
    Read {
        key: String,
        serializable: bool,
    },
    Compact(i64),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Wanted {
    Accept,
    Refuse(Code),
}

#[derive(Debug)]
pub struct Expect {
    call: Call,
    wanted: Wanted,
}

impl Expect {
    /// Whether the step runs on the client that does not ask for a leader,
    /// like a serializable read: the node answers it with no leader too.
    pub fn local(&self) -> bool {
        matches!(
            self.call,
            Call::Read {
                serializable: true,
                ..
            }
        )
    }

    /// The operation as a sentence: what a message says the server did with it.
    fn what(&self) -> String {
        match &self.call {
            Call::Put { key, .. } => format!("a put of etcd key \"{key}\""),
            Call::Read { key, serializable } => format!(
                "a {}read of etcd key \"{key}\"",
                if *serializable { "serializable " } else { "" }
            ),
            Call::Compact(rev) => format!("a compaction up to revision {rev}"),
        }
    }
}

/// The gRPC codes a refusal can name: every one but `OK`.
fn valid_codes() -> impl Iterator<Item = Code> {
    (1..=16).map(Code::from_i32)
}

pub(crate) fn code(name: &str) -> Result<Code, String> {
    valid_codes()
        .find(|code| format!("{code:?}") == name)
        .ok_or_else(|| {
            let names: Vec<String> = valid_codes().map(|c| format!("{c:?}")).collect();
            format!(
                "\"{name}\" is not a gRPC code to refuse with (OK is not one); the codes are {}",
                names.join(", ")
            )
        })
}

/// Everything that can be refused without a request, refused here.
pub fn plan(step: Step, request: &Request) -> Result<Expect, StepError> {
    let key = || request.arg(0).map(str::to_string);
    let refuse = |index| {
        code(request.arg(index)?)
            .map(Wanted::Refuse)
            .map_err(StepError::input)
    };
    let (call, wanted) = match step {
        Step::AcceptPut => (
            Call::Put {
                key: key()?,
                value: request.arg(1)?.to_string(),
            },
            Wanted::Accept,
        ),
        Step::RefusePut => (
            Call::Put {
                key: key()?,
                value: String::new(),
            },
            refuse(1)?,
        ),
        Step::AcceptRead | Step::AcceptReadSerializable => (
            Call::Read {
                key: key()?,
                serializable: step == Step::AcceptReadSerializable,
            },
            Wanted::Accept,
        ),
        Step::RefuseRead | Step::RefuseReadSerializable => (
            Call::Read {
                key: key()?,
                serializable: step == Step::RefuseReadSerializable,
            },
            refuse(1)?,
        ),
        Step::RefuseCompact => (Call::Compact(revision(request.arg(0)?)?), refuse(1)?),
        Step::Put
        | Step::PutDocString
        | Step::Get
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
        | Step::KeysContain
        | Step::GetSerializable
        | Step::Leader
        | Step::NotLeader
        | Step::Reached
        | Step::ComputeHash
        | Step::HashIs
        | Step::WatchKey
        | Step::WatchPrefix
        | Step::WatchKeyFrom
        | Step::WatchPrefixFrom
        | Step::WatchCancel
        | Step::WatchedPut
        | Step::WatchedDelete
        | Step::WatchedCount
        | Step::WatchedEvents
        | Step::WatchResumed
        | Step::LeaseGrant
        | Step::LeaseKeep
        | Step::LeaseStop
        | Step::LeaseRevoke
        | Step::PutLeased
        | Step::LeaseAlive
        | Step::LeaseExpired
        | Step::LeaseAliveId
        | Step::LeaseExpiredId
        | Step::LeaseHolds
        | Step::TxnCommit
        | Step::TxnSucceeded
        | Step::TxnFailed
        | Step::TxnRefuse
        | Step::Authenticate
        | Step::RefuseAuth
        | Step::WatchResumedTimes => return Err(StepError::input("not an accept / refuse step")),
    };
    Ok(Expect { call, wanted })
}

/// `bddkit_validate_step`: a code that is a literal must be a known one.
pub fn validate(step: Step, request: &Request) -> Result<(), String> {
    if matches!(
        step,
        Step::AcceptPut | Step::AcceptRead | Step::AcceptReadSerializable
    ) {
        return Ok(());
    }
    // The code is the second argument of every refusal.
    match request.args.get(1) {
        Some(name) if !is_slot(name) => code(name).map(drop),
        _ => Ok(()),
    }
}

pub fn record_for(expect: &Expect, instance: &str) -> Record {
    match &expect.call {
        Call::Put { key, value } => {
            let mut record = Record::new("Put", key, instance, json!({"key": key, "value": value}));
            record.scan_text(value);
            record
        }
        Call::Read { key, serializable } => Record::new(
            "Get",
            key,
            instance,
            json!({"key": key, "serializable": serializable}),
        ),
        Call::Compact(rev) => Record::new(
            "Compact",
            &format!("revision {rev}"),
            instance,
            json!({"revision": rev}),
        ),
    }
}

/// The verdict on one attempt. An answer that is not the wanted one is
/// `not_yet`: a node comes back, a quorum is restored, a compaction is
/// refused until the revision exists.
fn judge(expect: &Expect, outcome: Result<(), &Error>, tls: bool) -> Result<(), StepError> {
    judge_what(&expect.what(), &expect.wanted, outcome, tls)
}

/// What the server said when it refused: kept in the evidence. A failure on
/// the client side said nothing.
pub(crate) fn note_refusal(record: &mut Record, error: &Error) {
    if let Error::GRpcStatus(status) = error
        && class_of(error) == Class::DbStatement
    {
        record
            .keys
            .push(json!({"refused": format!("{:?}", status.code()), "message": status.message()}));
    }
}

/// The same verdict for any operation, named by `what`.
pub(crate) fn judge_what(
    what: &str,
    wanted: &Wanted,
    outcome: Result<(), &Error>,
    tls: bool,
) -> Result<(), StepError> {
    let miss = |message: String| StepError::not_yet(Class::Assertion, message);
    let status = match outcome {
        Ok(()) => {
            return match wanted {
                Wanted::Accept => Ok(()),
                Wanted::Refuse(want) => Err(miss(format!(
                    "the server accepted {what}, expected a refusal with {want:?}"
                ))),
            };
        }
        // Only a status of the server is a refusal.
        Err(error @ Error::GRpcStatus(status)) if class_of(error) == Class::DbStatement => status,
        Err(error) => {
            let failure = request_failure(error, tls);
            return Err(StepError::not_yet(failure.class, failure.message));
        }
    };
    let got = status.code();
    match wanted {
        Wanted::Refuse(want) if *want == got => Ok(()),
        Wanted::Refuse(want) => Err(miss(format!(
            "the server refused {what} with {got:?}, expected {want:?}: {}",
            status.message()
        ))),
        Wanted::Accept => Err(miss(format!(
            "the server refused {what} with {got:?}: {}",
            status.message()
        ))),
    }
}

pub async fn perform(
    client: &Client,
    tls: bool,
    expect: &Expect,
    record: &mut Record,
) -> Result<Vars, StepError> {
    let mut kv = client.kv_client();
    // A read of a key that is not there is an answer of the server too.
    let answer = match &expect.call {
        Call::Put { key, value } => kv
            .put(key.as_str(), value.as_str(), None)
            .await
            .map(|r| r.header().cloned()),
        Call::Read { key, serializable } => {
            let options = serializable.then(|| GetOptions::new().with_serializable());
            kv.get(key.as_str(), options)
                .await
                .map(|r| r.header().cloned())
        }
        Call::Compact(rev) => kv.compact(*rev, None).await.map(|r| r.header().cloned()),
    };
    match &answer {
        Ok(header) => record.answered(header.as_ref()),
        Err(error) => note_refusal(record, error),
    }
    judge(expect, answer.as_ref().map(drop), tls).map(|()| Vars::new())
}

#[cfg(test)]
mod tests {
    use std::io;

    use tonic::Status;

    use super::*;

    fn request(args: &[&str]) -> Request {
        Request::for_test(args)
    }

    fn expect(step: Step, args: &[&str]) -> Expect {
        plan(step, &request(args)).expect("planned")
    }

    fn from_server(code: Code) -> Error {
        Error::GRpcStatus(Status::new(code, "etcdserver: said so"))
    }

    fn client_side(code: Code) -> Error {
        let mut status = Status::new(code, "socket");
        status.set_source(std::sync::Arc::new(io::Error::other("socket")));
        Error::GRpcStatus(status)
    }

    #[test]
    fn every_code_but_ok_has_a_name_and_ok_is_not_one() {
        assert_eq!(code("Unavailable"), Ok(Code::Unavailable));
        assert_eq!(code("PermissionDenied"), Ok(Code::PermissionDenied));
        assert_eq!(valid_codes().count(), 16);
        for name in ["OK", "Ok", "unavailable", "", "Teapot"] {
            let error = code(name).expect_err("refused");
            assert!(error.contains("OutOfRange"), "{error}");
        }
    }

    #[test]
    fn a_literal_code_is_checked_before_the_run_and_a_slot_is_not() {
        let check = |step, args: &[&str]| validate(step, &request(args));
        assert!(check(Step::RefusePut, &["/k", "Nope"]).is_err());
        assert!(check(Step::RefuseRead, &["/k", "OK"]).is_err());
        assert!(check(Step::RefuseReadSerializable, &["/k", "OK"]).is_err());
        assert!(check(Step::RefuseCompact, &["5", "OK"]).is_err());
        assert!(check(Step::RefuseCompact, &["5", "OutOfRange"]).is_ok());
        assert!(check(Step::RefusePut, &["/k", "<<code>>"]).is_ok());
        assert!(check(Step::AcceptPut, &["/k", "v"]).is_ok());
        // A slot that ends up unknown is the step's to refuse.
        let error = plan(Step::RefusePut, &request(&["/k", "Nope"])).expect_err("refused");
        assert_eq!(error.class, Class::Input);
    }

    #[test]
    fn the_compaction_revision_must_be_a_revision() {
        assert!(plan(Step::RefuseCompact, &request(&["x", "OutOfRange"])).is_err());
        assert!(plan(Step::RefuseCompact, &request(&["-1", "OutOfRange"])).is_err());
    }

    #[test]
    fn only_a_serializable_read_runs_on_the_client_without_a_leader() {
        assert!(expect(Step::AcceptReadSerializable, &["/k"]).local());
        assert!(expect(Step::RefuseReadSerializable, &["/k", "Unavailable"]).local());
        assert!(!expect(Step::AcceptRead, &["/k"]).local());
        assert!(!expect(Step::AcceptPut, &["/k", "v"]).local());
        assert!(!expect(Step::RefuseCompact, &["1", "OutOfRange"]).local());
    }

    #[test]
    fn a_refusal_of_the_wanted_code_passes_and_any_other_answer_is_not_yet() {
        let refuse = expect(Step::RefusePut, &["/k", "Unavailable"]);
        assert!(judge(&refuse, Err(&from_server(Code::Unavailable)), false).is_ok());
        let other = judge(&refuse, Err(&from_server(Code::PermissionDenied)), false);
        let other = other.expect_err("wrong code");
        assert!(other.retry);
        assert_eq!(other.class, Class::Assertion);
        assert!(other.message.contains("PermissionDenied"), "{other}");
        assert!(other.message.contains("expected Unavailable"), "{other}");
        let accepted = judge(&refuse, Ok(()), false).expect_err("went through");
        assert!(accepted.retry);
        assert_eq!(accepted.class, Class::Assertion);
        assert!(accepted.message.contains("accepted"), "{accepted}");
    }

    #[test]
    fn an_accept_passes_on_an_answer_and_not_on_a_refusal() {
        let accept = expect(Step::AcceptRead, &["/k"]);
        assert!(judge(&accept, Ok(()), false).is_ok());
        let refused = judge(&accept, Err(&from_server(Code::Unavailable)), false);
        let refused = refused.expect_err("refused");
        assert!(refused.retry);
        assert_eq!(refused.class, Class::Assertion);
        assert!(refused.message.contains("etcdserver: said so"), "{refused}");
    }

    #[test]
    fn a_failure_on_the_client_side_is_never_a_refusal() {
        // The killed-node case: `Unavailable`, but made by the client.
        let refuse = expect(Step::RefusePut, &["/k", "Unavailable"]);
        let error = client_side(Code::Unavailable);
        let down = judge(&refuse, Err(&error), false).expect_err("not reached");
        assert!(down.retry);
        assert_eq!(down.class, Class::DbConnection);
        let accept = expect(Step::AcceptPut, &["/k", "v"]);
        let down = judge(&accept, Err(&error), false).expect_err("not reached");
        assert_eq!(down.class, Class::DbConnection);
        let other = Error::InvalidArgs("bad".to_string());
        let down = judge(&refuse, Err(&other), false).expect_err("not reached");
        assert_eq!(down.class, Class::DbConnection);
    }

    #[test]
    fn a_message_never_quotes_the_value() {
        let accept = expect(Step::AcceptPut, &["/k", "s3cret-value"]);
        let error = judge(&accept, Err(&from_server(Code::Unavailable)), false).expect_err("x");
        assert!(!error.message.contains("s3cret-value"), "{error}");
    }
}
