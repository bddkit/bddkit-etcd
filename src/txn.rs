//! Transactions: a scenario commits a transaction written as `etcdctl txn`
//! reads it and asserts which branch ran. The syntax is checked before the
//! first request (`parse.rs`); a compare that does not hold is not a failure,
//! the step passes and publishes `false`. The outcome of the last transaction
//! belongs to the handle and ends with the scenario, so the two outcome
//! assertions read it without a request.

use std::sync::{Mutex, MutexGuard, PoisonError};

use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn as EtcdTxn, TxnOp,
    TxnOpResponse,
};
use serde_json::json;

use crate::check::is_slot;
use crate::client::request_failure;
use crate::error::StepError;
use crate::evidence::{Record, shown};
use crate::expect::{Wanted, code, judge_what, note_refusal};
use crate::instance::Instance;
use crate::ops::{Request, Vars, doc_value, header, revision_vars};
use crate::steps::Step;

mod parse;

use parse::{Cmp, Operation, Parsed, Span, Test, parse};

/// Whether the compares of the last transaction of the scenario held.
#[derive(Default)]
pub struct Outcome(Mutex<Option<bool>>);

impl Outcome {
    /// Nothing can panic under this lock (an assignment).
    fn slot(&self) -> MutexGuard<'_, Option<bool>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn forget(&self) {
        *self.slot() = None;
    }
}

#[derive(Debug)]
pub enum Txn {
    Commit(Parsed),
    /// `should have succeeded` (true) or `should have failed` (false).
    Outcome(bool),
    Refuse(Parsed, Wanted),
}

fn parsed(request: &Request, shape: bool) -> Result<Parsed, String> {
    let doc = request
        .docstring
        .as_deref()
        .ok_or("the step needs a doc string with the transaction")?;
    parse(doc_value(doc), shape)
}

/// Everything that can be refused without a request, refused here. The doc
/// string is interpolated by now, so a slot is parsed with its value.
pub fn plan(step: Step, request: &Request) -> Result<Txn, StepError> {
    let doc = || parsed(request, false).map_err(StepError::input);
    match step {
        Step::TxnCommit => doc().map(Txn::Commit),
        Step::TxnSucceeded => Ok(Txn::Outcome(true)),
        Step::TxnFailed => Ok(Txn::Outcome(false)),
        Step::TxnRefuse => {
            let wanted = code(request.arg(0)?).map(Wanted::Refuse);
            Ok(Txn::Refuse(doc()?, wanted.map_err(StepError::input)?))
        }
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
        | Step::AcceptPut
        | Step::RefusePut
        | Step::AcceptRead
        | Step::AcceptReadSerializable
        | Step::RefuseRead
        | Step::RefuseReadSerializable
        | Step::RefuseCompact
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
        | Step::WatchResumedTimes
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
        | Step::Authenticate
        | Step::RefuseAuth => Err(StepError::input("not a transaction step")),
    }
}

/// `bddkit_validate_step`: the doc string is read as it is written, with a
/// number that holds a `<<slot>>` taken by its shape, and a literal code must
/// be a known one.
pub fn validate(step: Step, request: &Request) -> Result<(), String> {
    if matches!(step, Step::TxnSucceeded | Step::TxnFailed) {
        return Ok(());
    }
    if step == Step::TxnRefuse {
        match request.args.first() {
            Some(name) if !is_slot(name) => code(name).map(drop)?,
            _ => {}
        }
    }
    parsed(request, true).map(drop)
}

pub fn record_for(txn: &Txn, instance: &str) -> Record {
    let sizes = |p: &Parsed| {
        json!({
            "compares": p.compares.len(),
            "success": p.success.len(),
            "failure": p.failure.len(),
        })
    };
    let mut record = Record::new("Txn", "(transaction)", instance, json!({}));
    if let Txn::Commit(p) | Txn::Refuse(p, _) = txn {
        record = Record::new("Txn", "(transaction)", instance, sizes(p));
        // Both branches: the host shows the whole doc string, and a JSON
        // value with a secret in it is masked only once it is scanned.
        for op in p.success.iter().chain(&p.failure) {
            if let Operation::Put { value, .. } = op {
                record.scan_text(value);
            }
        }
        for compare in &p.compares {
            if let Test::Value(text) = &compare.test {
                record.scan_text(text);
            }
        }
    }
    record
}

fn compare(compare: &parse::Compare) -> Compare {
    let key = compare.key.as_str();
    let op = match compare.cmp {
        Cmp::Equal => CompareOp::Equal,
        Cmp::NotEqual => CompareOp::NotEqual,
        Cmp::Less => CompareOp::Less,
        Cmp::Greater => CompareOp::Greater,
    };
    match &compare.test {
        Test::Value(text) => Compare::value(key, op, text.as_str()),
        Test::Version(n) => Compare::version(key, op, *n),
        Test::Create(n) => Compare::create_revision(key, op, *n),
        Test::Mod(n) => Compare::mod_revision(key, op, *n),
        Test::Lease(n) => Compare::lease(key, op, *n),
    }
}

fn operation(operation: &Operation) -> TxnOp {
    match operation {
        Operation::Put { key, value, lease } => TxnOp::put(
            key.as_str(),
            value.as_str(),
            lease.map(|id| PutOptions::new().with_lease(id)),
        ),
        Operation::Get(span) => TxnOp::get(
            span.key(),
            match span {
                Span::Key(_) => None,
                Span::Range(_, end) => Some(GetOptions::new().with_range(end.as_str())),
                Span::Prefix(_) => Some(GetOptions::new().with_prefix()),
            },
        ),
        Operation::Del(span) => TxnOp::delete(
            span.key(),
            match span {
                Span::Key(_) => None,
                Span::Range(_, end) => Some(DeleteOptions::new().with_range(end.as_str())),
                Span::Prefix(_) => Some(DeleteOptions::new().with_prefix()),
            },
        ),
    }
}

fn transaction(parsed: &Parsed) -> EtcdTxn {
    EtcdTxn::new()
        .when(parsed.compares.iter().map(compare).collect::<Vec<_>>())
        .and_then(parsed.success.iter().map(operation).collect::<Vec<_>>())
        .or_else(parsed.failure.iter().map(operation).collect::<Vec<_>>())
}

/// What each operation of the branch that ran returned, for the evidence.
fn evidence(record: &mut Record, branch: &str, ops: &[Operation], answers: &[TxnOpResponse]) {
    record.keys.push(json!({"branch": branch}));
    for (op, answer) in ops.iter().zip(answers) {
        let line = match answer {
            TxnOpResponse::Put(r) => json!({
                "op": "put",
                "key": op.key(),
                "revision": r.header().map(etcd_client::ResponseHeader::revision),
            }),
            TxnOpResponse::Get(r) => {
                let keys: Vec<_> = r
                    .kvs()
                    .iter()
                    .map(|kv| json!({"key": shown(kv.key()), "value": shown(kv.value())}))
                    .collect();
                for kv in r.kvs() {
                    if let Ok(text) = std::str::from_utf8(kv.value()) {
                        record.scan_text(text);
                    }
                }
                json!({"op": "get", "key": op.key(), "count": r.count(), "keys": keys})
            }
            TxnOpResponse::Delete(r) => {
                json!({"op": "del", "key": op.key(), "deleted": r.deleted()})
            }
            TxnOpResponse::Txn(_) => json!({"op": "txn"}),
        };
        record.keys.push(line);
    }
}

/// `should have succeeded` / `should have failed`: reads what the last
/// transaction kept, and sends nothing.
pub fn outcome(instance: &Instance, wanted: bool) -> Result<Vars, StepError> {
    let held = |held: bool| if held { "held" } else { "did not hold" };
    match *instance.txn.slot() {
        None => Err(StepError::input(
            "no etcd transaction was committed in this scenario on this instance",
        )),
        Some(got) if got == wanted => Ok(Vars::new()),
        Some(got) => Err(StepError::assertion(format!(
            "the compares of the last etcd transaction {}, so it {}; expected it to have {}",
            held(got),
            if got { "succeeded" } else { "failed" },
            if wanted { "succeeded" } else { "failed" },
        ))),
    }
}

pub async fn perform(
    instance: &Instance,
    client: &Client,
    txn: &Txn,
    record: &mut Record,
) -> Result<Vars, StepError> {
    let tls = instance.config.tls;
    let mut kv = client.kv_client();
    match txn {
        Txn::Outcome(wanted) => outcome(instance, *wanted),
        Txn::Commit(parsed) => {
            let response = kv
                .txn(transaction(parsed))
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(response.header());
            let revision = header(response.header())?;
            let succeeded = response.succeeded();
            let (branch, ops) = if succeeded {
                ("success", &parsed.success)
            } else {
                ("failure", &parsed.failure)
            };
            evidence(record, branch, ops, response.op_responses().as_slice());
            *instance.txn.slot() = Some(succeeded);
            let mut vars = revision_vars(revision);
            vars.insert("etcd_txn_succeeded".to_string(), succeeded.to_string());
            Ok(vars)
        }
        Txn::Refuse(parsed, wanted) => {
            let answer = kv.txn(transaction(parsed)).await;
            match &answer {
                Ok(response) => record.answered(response.header()),
                Err(error) => note_refusal(record, error),
            }
            judge_what("a transaction", wanted, answer.as_ref().map(drop), tls)
                .map(|()| Vars::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Class;

    fn request(args: &[&str], doc: Option<&str>) -> Request {
        let mut request = Request::for_test(args);
        request.docstring = doc.map(ToString::to_string);
        request
    }

    #[test]
    fn a_literal_doc_string_is_checked_before_the_run_with_its_line() {
        let doc = "\nc(\"/k\") = \"0\"\n\nput /k v --nope\n";
        let error = validate(Step::TxnCommit, &request(&[], Some(doc))).expect_err("refused");
        assert!(error.starts_with("line 3:"), "{error}");
        assert!(validate(Step::TxnCommit, &request(&[], None)).is_err());
        assert!(validate(Step::TxnCommit, &request(&[], Some("\n\nput /k v\n"))).is_ok());
    }

    #[test]
    fn a_slot_passes_the_shape_and_is_parsed_again_at_dispatch() {
        let doc = "\nm(\"/k\") = \"<<rev>>\"\n";
        assert!(validate(Step::TxnCommit, &request(&[], Some(doc))).is_ok());
        let error = plan(Step::TxnCommit, &request(&[], Some(doc))).expect_err("refused");
        assert_eq!(error.class, Class::Input);
        assert!(error.message.starts_with("line 1:"), "{error}");
        assert!(
            plan(
                Step::TxnCommit,
                &request(&[], Some("\nm(\"/k\") = \"5\"\n"))
            )
            .is_ok()
        );
    }

    #[test]
    fn a_refusal_names_a_known_code_unless_it_is_a_slot() {
        let doc = Some("\n\nput /k a\nput /k b\n");
        let check = |code| validate(Step::TxnRefuse, &request(&[code], doc));
        assert!(check("InvalidArgument").is_ok());
        assert!(check("<<code>>").is_ok());
        assert!(check("OK").is_err());
        let error = plan(Step::TxnRefuse, &request(&["Nope"], doc)).expect_err("refused");
        assert_eq!(error.class, Class::Input);
    }

    #[test]
    fn a_json_value_in_either_branch_or_a_compare_is_scanned_for_secrets() {
        let json = r#""{\"password\":\"hunter2hunter2\"}""#;
        let doc = format!("\nvalue(\"/a\") = {json}\n\nput /k {json}\n\nput /k {json}\n");
        let txn = plan(Step::TxnCommit, &request(&[], Some(&doc))).expect("planned");
        assert_eq!(record_for(&txn, "main").scan().len(), 3);
    }

    #[test]
    fn an_empty_doc_string_is_a_transaction_with_nothing_in_it() {
        let Ok(Txn::Commit(parsed)) = plan(Step::TxnCommit, &request(&[], Some("\n\n"))) else {
            panic!("planned");
        };
        assert!(parsed.compares.is_empty());
    }

    #[test]
    fn the_outcome_is_read_without_a_request_and_is_input_before_any_transaction() {
        let instance = Instance::new(
            crate::config::Config::parse(&json!({"endpoints": "http://127.0.0.1:1"}))
                .expect("config"),
        );
        let none = outcome(&instance, true).expect_err("nothing yet");
        assert_eq!(none.class, Class::Input);
        assert!(!none.retry);
        *instance.txn.slot() = Some(true);
        assert!(outcome(&instance, true).is_ok());
        let wrong = outcome(&instance, false).expect_err("held");
        assert_eq!(wrong.class, Class::Assertion);
        assert!(!wrong.retry);
        instance.txn.forget();
        assert!(outcome(&instance, true).is_err());
    }
}
