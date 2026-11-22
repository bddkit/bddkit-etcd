//! What one operation leaves behind: the diagnostic the failure dump and the
//! event stream carry, the debug `output`, and the `scan` that lets the host
//! find the secrets inside a value. The plugin masks nothing itself.

use std::time::Duration;

use etcd_client::ResponseHeader;
use serde_json::{Value, json};

pub struct Record {
    operation: &'static str,
    target: String,
    instance: String,
    request: Value,
    header: Value,
    pub keys: Vec<Value>,
    duration: Duration,
    scan: Vec<Value>,
}

impl Record {
    pub fn new(operation: &'static str, target: &str, instance: &str, request: Value) -> Self {
        Self {
            operation,
            target: target.to_string(),
            instance: instance.to_string(),
            request,
            header: Value::Null,
            keys: Vec::new(),
            duration: Duration::ZERO,
            scan: Vec::new(),
        }
    }

    /// The header of the server's answer. Some servers send a synthetic one,
    /// which is why the instance's name is in the diagnostic too.
    pub fn answered(&mut self, header: Option<&ResponseHeader>) {
        self.header = header.map_or(Value::Null, |h| {
            json!({
                "cluster_id": h.cluster_id(),
                "member_id": h.member_id(),
                "revision": h.revision(),
                "raft_term": h.raft_term(),
            })
        });
    }

    pub fn took(&mut self, duration: Duration) {
        self.duration = duration;
    }

    /// A value that parses as a JSON object or array goes to the host's
    /// `scan`, which registers every string under a sensitive field name.
    pub fn scan_text(&mut self, text: &str) {
        if matches!(
            serde_json::from_str::<Value>(text),
            Ok(Value::Object(_) | Value::Array(_))
        ) {
            self.scan.push(json!({"kind": "json", "content": text}));
        }
    }

    fn content(&self) -> String {
        let content = json!({
            "instance": self.instance,
            "request": self.request,
            "response_header": self.header,
            "keys": self.keys,
            "duration_us": u64::try_from(self.duration.as_micros()).unwrap_or(u64::MAX),
        });
        serde_json::to_string_pretty(&content).unwrap_or_else(|_| content.to_string())
    }

    pub fn diagnostic(&self) -> Value {
        json!({
            "title": format!("etcd {} {}", self.operation, self.target),
            "kind": "json",
            "content": self.content(),
        })
    }

    pub fn output(&self) -> Value {
        json!({
            "text": format!("etcd {} {}\n{}", self.operation, self.target, self.content()),
            "syntax": null,
        })
    }

    pub fn scan(&self) -> &[Value] {
        &self.scan
    }
}

/// A key or a value as the diagnostic shows it: text, or base64 with a note
/// when it is not UTF-8.
pub fn shown(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => Value::String(text.to_string()),
        Err(_) => json!({"base64": base64(bytes), "note": "not UTF-8"}),
    }
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_rfc_4648_vectors() {
        for (plain, encoded) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(plain.as_bytes()), encoded);
        }
        assert_eq!(base64(&[0xff, 0xfe, 0xfd]), "//79");
    }

    #[test]
    fn a_value_that_is_not_utf8_is_shown_as_base64_with_a_note() {
        assert_eq!(shown(b"abc"), json!("abc"));
        assert_eq!(
            shown(&[0xff, 0xfe]),
            json!({"base64": "//4=", "note": "not UTF-8"})
        );
    }

    #[test]
    fn only_a_json_object_or_array_is_scanned() {
        let mut record = Record::new("Put", "/k", "main", json!({}));
        for text in ["30s", "30", "\"x\"", "{not json"] {
            record.scan_text(text);
        }
        assert!(record.scan().is_empty());
        record.scan_text(r#"{"password":"hunter2hunter2"}"#);
        assert_eq!(record.scan().len(), 1);
    }

    #[test]
    fn the_diagnostic_names_the_operation_the_key_and_the_instance() {
        let mut record = Record::new("Put", "/config/timeout", "node2", json!({"key": "/k"}));
        record.took(Duration::from_micros(1500));
        let diagnostic = record.diagnostic();
        assert_eq!(diagnostic["title"], "etcd Put /config/timeout");
        assert_eq!(diagnostic["kind"], "json");
        let content: Value =
            serde_json::from_str(diagnostic["content"].as_str().expect("text")).expect("JSON");
        assert_eq!(content["instance"], "node2");
        assert_eq!(content["duration_us"], 1500);
        assert!(content["response_header"].is_null());
    }
}
