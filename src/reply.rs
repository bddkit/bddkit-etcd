//! Every reply the plugin sends is built here, so the shape the host parses
//! has one author.

use serde_json::json;

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn a_refusal_carries_the_secrets_beside_the_error() {
        let v: Value = serde_json::from_str(&err("boom", &["pw".to_string()])).expect("JSON");
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"], "boom");
        assert_eq!(v["secrets"], json!(["pw"]));
    }
}
