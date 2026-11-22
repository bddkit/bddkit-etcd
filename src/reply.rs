//! Every reply the plugin sends is built here, so the shape the host parses
//! has one author.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::error::StepError;

/// An `ok` envelope with the one-line `detail` the host shows in `doctor`, and
/// the values the host must keep out of everything it prints.
pub fn ok_detail(detail: &str, secrets: &[String]) -> String {
    json!({"ok": true, "detail": detail, "secrets": secrets}).to_string()
}

/// A refusal. `secrets` travels with it: the host registers them before it
/// reads `error`.
pub fn err(error: impl Into<String>, secrets: &[String]) -> String {
    json!({"ok": false, "error": error.into(), "secrets": secrets}).to_string()
}

pub fn ok() -> String {
    r#"{"ok":true}"#.to_string()
}

pub fn handle(handle: u64) -> String {
    json!({"ok": true, "handle": handle}).to_string()
}

/// Retrying cannot help.
pub fn fatal(error: &str) -> String {
    json!({"status": "fatal", "error": error}).to_string()
}

/// The step passed: its variables, its debug lines, its evidence.
pub fn passed(
    vars: &BTreeMap<String, String>,
    output: &[Value],
    diagnostics: &[Value],
    scan: &[Value],
) -> String {
    json!({
        "status": "passed",
        "vars": vars,
        "output": output,
        "diagnostics": diagnostics,
        "scan": scan,
    })
    .to_string()
}

/// The step failed, in the class its error was born with: `fatal`, or
/// `not_yet` when it is an assertion's and a fresh read may change it. No
/// variables travel on a failure.
pub fn failed(
    error: &StepError,
    output: &[Value],
    diagnostics: &[Value],
    scan: &[Value],
) -> String {
    json!({
        "status": if error.retry { "not_yet" } else { "fatal" },
        "error": error.message,
        "class": error.class.as_str(),
        "output": output,
        "diagnostics": diagnostics,
        "scan": scan,
    })
    .to_string()
}

/// The answer of `bddkit_failure_context`.
pub fn failure_context(diagnostics: &[Value], scan: &[Value]) -> String {
    json!({"diagnostics": diagnostics, "scan": scan}).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_carries_the_secrets_beside_the_error() {
        let v: Value = serde_json::from_str(&err("boom", &["pw".to_string()])).expect("JSON");
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"], "boom");
        assert_eq!(v["secrets"], json!(["pw"]));
    }
}
