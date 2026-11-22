//! One dispatch: the request in, the reply out. Each step is planned from its
//! arguments before anything is sent, then performed against the server under
//! the config's `timeout_secs`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use etcd_client::{Client, DeleteOptions, ResponseHeader};
use serde_json::{Value, json};

use crate::check::{self, Check};
use crate::client::request_failure;
use crate::error::{Class, StepError};
use crate::evidence::{Record, base64, shown};
use crate::instance::{Instance, Last};
use crate::reply;
use crate::steps::{EMPTY_PREFIX, Step};

/// What a step reads of the dispatch request.
pub struct Request {
    pub args: Vec<String>,
    pub docstring: Option<String>,
    pub table: Option<Vec<Vec<String>>>,
    debug: bool,
    instance: String,
}

impl Request {
    pub fn parse(text: &str) -> Result<Self, String> {
        let value: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        Ok(Self {
            args: value["args"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|a| a.as_str().unwrap_or_default().to_string())
                .collect(),
            docstring: value["docstring"].as_str().map(str::to_string),
            table: value["table"].as_array().map(|rows| {
                rows.iter()
                    .map(|row| {
                        row.as_array()
                            .into_iter()
                            .flatten()
                            .map(|c| c.as_str().unwrap_or_default().to_string())
                            .collect()
                    })
                    .collect()
            }),
            debug: value["debug"].as_bool().unwrap_or(false),
            instance: value["instance"].as_str().unwrap_or_default().to_string(),
        })
    }

    pub fn arg(&self, index: usize) -> Result<&str, StepError> {
        self.args
            .get(index)
            .map(String::as_str)
            .ok_or_else(|| StepError::input(format!("the step has no argument {}", index + 1)))
    }

    /// The doc string of a step that needs one.
    pub fn doc(&self) -> Result<&str, StepError> {
        self.docstring
            .as_deref()
            .ok_or_else(|| StepError::input("the step needs a doc string"))
    }
}

#[derive(Debug)]
enum Range {
    Key(String),
    Prefix(String),
    All,
}

#[derive(Debug)]
enum Action {
    Put { key: String, value: String },
    Get { key: String },
    Delete(Range),
}

/// The value a doc string means: gherkin keeps the line break after the
/// opening quotes and the one before the closing quotes, and they are not
/// part of the value.
pub fn doc_value(docstring: &str) -> &str {
    let text = docstring.strip_prefix('\n').unwrap_or(docstring);
    text.strip_suffix('\n').unwrap_or(text)
}

/// Everything that can be refused without a request, refused here.
fn plan(step: Step, request: &Request) -> Result<Action, StepError> {
    let key = || request.arg(0).map(str::to_string);
    Ok(match step {
        Step::Put => Action::Put {
            key: key()?,
            value: request.arg(1)?.to_string(),
        },
        Step::PutDocString => Action::Put {
            key: key()?,
            value: doc_value(request.doc()?).to_string(),
        },
        Step::Get => Action::Get { key: key()? },
        Step::DeleteKey => Action::Delete(Range::Key(key()?)),
        // An empty value that came through a variable: a literal one never
        // gets here, `validate` stops it.
        Step::DeletePrefix => match key()? {
            prefix if prefix.is_empty() => return Err(StepError::input(EMPTY_PREFIX)),
            prefix => Action::Delete(Range::Prefix(prefix)),
        },
        Step::DeleteAll => Action::Delete(Range::All),
        Step::Exists
        | Step::Absent
        | Step::HasValue
        | Step::HasValueDoc
        | Step::Matches
        | Step::ContainsJson
        | Step::EqualsJson
        | Step::HasFields
        | Step::Count
        | Step::KeysBe
        | Step::KeysContain => return Err(StepError::input("not an action")),
    })
}

/// What a step does: an action that changes the server, or an assertion that
/// only reads it and may be asked again.
enum Op {
    Act(Action),
    Check(Check),
}

fn plan_op(step: Step, request: &Request) -> Result<Op, StepError> {
    if step.is_assertion() {
        check::plan(step, request).map(Op::Check)
    } else {
        plan(step, request).map(Op::Act)
    }
}

fn record_for(op: &Op, instance: &str) -> Record {
    match op {
        Op::Check(check) => check::record_for(check, instance),
        Op::Act(action) => record_for_action(action, instance),
    }
}

fn record_for_action(action: &Action, instance: &str) -> Record {
    match action {
        Action::Put { key, value } => {
            let mut record = Record::new("Put", key, instance, json!({"key": key, "value": value}));
            record.scan_text(value);
            record
        }
        Action::Get { key } => Record::new("Get", key, instance, json!({"key": key})),
        Action::Delete(Range::Key(key)) => {
            Record::new("Delete", key, instance, json!({"key": key}))
        }
        Action::Delete(Range::Prefix(prefix)) => Record::new(
            "Delete",
            &format!("{prefix} (prefix)"),
            instance,
            json!({"prefix": prefix}),
        ),
        Action::Delete(Range::All) => {
            Record::new("Delete", "(all keys)", instance, json!({"all_keys": true}))
        }
    }
}

type Vars = BTreeMap<String, String>;

/// The header of an answer. A server that sends none gives no revision, and
/// the host keeps a variable a reply does not name, so a step that went on
/// would leave the previous step's `etcd_revision` to be read as this one's.
fn header(header: Option<&ResponseHeader>) -> Result<&ResponseHeader, StepError> {
    header.ok_or_else(|| {
        StepError::new(
            Class::DbStatement,
            "the server's answer carries no header, so there is no revision to report",
        )
    })
}

fn revision_vars(header: &ResponseHeader) -> Vars {
    BTreeMap::from([("etcd_revision".to_string(), header.revision().to_string())])
}

async fn perform_action(
    client: &Client,
    tls: bool,
    action: &Action,
    record: &mut Record,
) -> Result<Vars, StepError> {
    let mut kv = client.kv_client();
    match action {
        Action::Put { key, value } => {
            let response = kv
                .put(key.as_str(), value.as_str(), None)
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(response.header());
            Ok(revision_vars(header(response.header())?))
        }
        Action::Get { key } => {
            let response = kv
                .get(key.as_str(), None)
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(response.header());
            let revision = header(response.header())?;
            let Some(item) = response.kvs().first() else {
                return Err(StepError::assertion(format!(
                    "etcd key \"{key}\" does not exist; to check that a key is absent, use: etcd key \"{key}\" should not exist"
                )));
            };
            record.keys.push(json!({
                "key": shown(item.key()),
                "value": shown(item.value()),
            }));
            // A value that is not UTF-8 cannot be a variable, and the host
            // keeps a variable a reply does not name: going on would leave an
            // earlier key's `etcd_value` to be read as this one's.
            let Ok(text) = std::str::from_utf8(item.value()) else {
                return Err(StepError::assertion(format!(
                    "the value of etcd key \"{key}\" is not UTF-8, which a variable cannot hold (base64: {})",
                    base64(item.value())
                )));
            };
            record.scan_text(text);
            let mut vars = revision_vars(revision);
            vars.insert("etcd_value".to_string(), text.to_string());
            for (name, number) in [
                ("etcd_create_revision", item.create_revision()),
                ("etcd_mod_revision", item.mod_revision()),
                ("etcd_version", item.version()),
                ("etcd_lease", item.lease()),
            ] {
                vars.insert(name.to_string(), number.to_string());
            }
            Ok(vars)
        }
        Action::Delete(range) => {
            let (key, options) = match range {
                Range::Key(key) => (key.as_str(), None),
                Range::Prefix(prefix) => {
                    (prefix.as_str(), Some(DeleteOptions::new().with_prefix()))
                }
                Range::All => ("", Some(DeleteOptions::new().with_all_keys())),
            };
            let response = kv
                .delete(key, options)
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(response.header());
            let mut vars = revision_vars(header(response.header())?);
            vars.insert("etcd_deleted".to_string(), response.deleted().to_string());
            Ok(vars)
        }
    }
}

async fn perform(
    client: &Client,
    tls: bool,
    op: &Op,
    record: &mut Record,
) -> Result<Vars, StepError> {
    match op {
        Op::Act(action) => perform_action(client, tls, action, record).await,
        Op::Check(check) => check::perform(client, tls, check, record)
            .await
            .map(|()| Vars::new()),
    }
}

pub async fn run(instance: &Instance, step: Step, request: &Request) -> String {
    let op = match plan_op(step, request) {
        Ok(op) => op,
        Err(error) => return reply::failed(&error, &[], &[], &[]),
    };
    let mut record = record_for(&op, &request.instance);
    let seconds = instance.config.timeout_secs;
    let limit = Duration::from_secs(seconds);
    // Each phase has `timeout_secs` of its own. Neither names the phase it
    // was in: without a password the client connects lazily, so a client that
    // exists proves nothing about the server.
    let no_response = || {
        StepError::new(
            Class::DbConnection,
            format!("no response within {seconds} s"),
        )
    };
    let started = Instant::now();
    let result = async {
        let client = tokio::time::timeout(limit, instance.client())
            .await
            .map_err(|_| no_response())??;
        let tls = instance.config.tls;
        tokio::time::timeout(limit, perform(client, tls, &op, &mut record))
            .await
            .map_err(|_| no_response())?
    }
    .await;
    // An assertion is asked again by the host; an action never answers
    // `not_yet`.
    let result = match op {
        Op::Check(_) => result.map_err(StepError::polled),
        Op::Act(_) => result,
    };
    record.took(started.elapsed());
    let diagnostics = vec![record.diagnostic()];
    let output = if request.debug {
        vec![record.output()]
    } else {
        Vec::new()
    };
    // Always kept, evidence or not: the dump of a failing step of another
    // plugin asks for it.
    instance.remember(Last {
        diagnostics: diagnostics.clone(),
        scan: record.scan().to_vec(),
    });
    match result {
        Ok(vars) => reply::passed(&vars, &output, &diagnostics, record.scan()),
        Err(error) => reply::failed(&error, &output, &diagnostics, record.scan()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(args: &[&str], docstring: Option<&str>) -> Request {
        Request {
            args: args.iter().map(ToString::to_string).collect(),
            docstring: docstring.map(ToString::to_string),
            table: None,
            debug: false,
            instance: "main".to_string(),
        }
    }

    #[test]
    fn a_doc_string_loses_only_the_line_breaks_gherkin_keeps() {
        assert_eq!(doc_value("\nline 1\nline 2\n"), "line 1\nline 2");
        assert_eq!(doc_value("\n\n"), "");
        assert_eq!(doc_value("bare"), "bare");
        assert_eq!(doc_value("\n\ntwo blank lines\n\n"), "\ntwo blank lines\n");
    }

    #[test]
    fn an_empty_prefix_from_a_variable_is_refused_as_input_before_any_request() {
        let error = plan(Step::DeletePrefix, &request(&[""], None)).expect_err("refused");
        assert_eq!(error.class, Class::Input);
        assert_eq!(error.message, EMPTY_PREFIX);
        assert!(plan(Step::DeletePrefix, &request(&["/a/"], None)).is_ok());
        assert!(plan(Step::DeleteAll, &request(&[], None)).is_ok());
    }

    #[test]
    fn an_answer_without_a_header_is_a_failure_not_a_stale_revision() {
        let error = header(None).expect_err("refused");
        assert_eq!(error.class, Class::DbStatement);
        assert!(error.message.contains("no header"));
    }

    #[test]
    fn a_doc_string_step_without_a_doc_string_is_input() {
        let error = plan(Step::PutDocString, &request(&["/k"], None)).expect_err("refused");
        assert_eq!(error.class, Class::Input);
    }
}
