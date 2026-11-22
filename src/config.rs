//! One `resources.etcd.<name>` body: what the host hands over verbatim, minus
//! its reserved `options` key. Every check reads the raw JSON, never a serde
//! derive: serde's type errors quote the offending value, and a password that
//! is a number must not reach an error message.

use std::path::PathBuf;

use serde_json::{Value, json};
use tonic::codegen::http::Uri;

/// A key the body accepts: the one list both the manifest and the unknown-key
/// check read, so a key added to one is not missed in the other.
struct Field {
    name: &'static str,
    required: bool,
    kind: &'static str,
    description: &'static str,
    example: &'static str,
}

const FIELDS: &[Field] = &[
    Field {
        name: "endpoints",
        required: true,
        kind: "string",
        description: "one address or several separated by commas, http:// or https:// (not both); the scheme selects TLS",
        example: "http://127.0.0.1:2379",
    },
    Field {
        name: "username",
        required: false,
        kind: "string",
        description: "user to authenticate as; goes with password",
        example: "root",
    },
    Field {
        name: "password",
        required: false,
        kind: "string",
        description: "password of the user; goes with username",
        example: "${ETCD_ROOT_PASSWORD}",
    },
    Field {
        name: "tls_ca",
        required: false,
        kind: "string",
        description: "CA certificate of the server; the system roots when omitted. A relative path is resolved from the directory `bddkit run` starts in",
        example: "/etc/etcd/ca.crt",
    },
    Field {
        name: "tls_cert",
        required: false,
        kind: "string",
        description: "client certificate for mutual TLS; goes with tls_key",
        example: "/etc/etcd/client.crt",
    },
    Field {
        name: "tls_key",
        required: false,
        kind: "string",
        description: "private key of the client certificate; goes with tls_cert",
        example: "/etc/etcd/client.key",
    },
    Field {
        name: "tls_server_name",
        required: false,
        kind: "string",
        description: "name to verify the server certificate against, when the endpoint is an IP address",
        example: "etcd.internal",
    },
    Field {
        name: "timeout_secs",
        required: false,
        kind: "number",
        description: "limit of one request and of connecting, in seconds; default 5",
        example: "5",
    },
    Field {
        name: "watch_buffer_events",
        required: false,
        kind: "number",
        description: "events one watch keeps before it fails; default 10000",
        example: "10000",
    },
    Field {
        name: "options",
        required: false,
        kind: "nonscalar",
        description: "polling.timeout_secs / polling.interval_ms for eventual assertions against this instance",
        example: "",
    },
];

const DEFAULT_TIMEOUT_SECS: u64 = 5;
const DEFAULT_WATCH_BUFFER_EVENTS: u64 = 10_000;

/// What `bddkit resource fields` and `resource add` know about the keys.
pub fn fields_json() -> Value {
    let fields: Vec<Value> = FIELDS
        .iter()
        .map(|f| {
            let mut field = json!({
                "name": f.name,
                "required": f.required,
                "type": f.kind,
                "description": f.description,
            });
            if !f.example.is_empty() {
                field["example"] = json!(f.example);
            }
            field
        })
        .collect();
    Value::Array(fields)
}

#[derive(Debug)]
pub struct Config {
    /// Every address as written, scheme included.
    pub endpoints: Vec<String>,
    pub tls: bool,
    pub credentials: Option<(String, String)>,
    pub tls_ca: Option<PathBuf>,
    pub client_identity: Option<(PathBuf, PathBuf)>,
    pub tls_server_name: Option<String>,
    pub timeout_secs: u64,
    pub watch_buffer_events: u64,
}

impl Config {
    /// Shape only, no network. Every message names the key and quotes no value
    /// but a file path.
    pub fn parse(config: &Value) -> Result<Self, String> {
        let body = config
            .as_object()
            .ok_or("the resource body must be a mapping")?;
        if let Some(key) = body.keys().find(|k| FIELDS.iter().all(|f| f.name != *k)) {
            return Err(format!("unknown key \"{key}\""));
        }
        let (endpoints, tls) = endpoints(config)?;
        // etcd-client refuses TLS options on an http address; dropping them
        // here would hide a scheme typo that turns TLS off.
        if !tls
            && let Some(key) = ["tls_ca", "tls_cert", "tls_key", "tls_server_name"]
                .into_iter()
                .find(|key| !config[*key].is_null())
        {
            return Err(format!("\"{key}\" needs https:// endpoints"));
        }
        let username = string(config, "username")?;
        let password = string(config, "password")?;
        let credentials = pair(username, password, "username", "password")?;
        let tls_ca = file(config, "tls_ca")?;
        let client_identity = pair(
            file(config, "tls_cert")?,
            file(config, "tls_key")?,
            "tls_cert",
            "tls_key",
        )?;
        Ok(Self {
            endpoints,
            tls,
            credentials,
            tls_ca,
            client_identity,
            tls_server_name: string(config, "tls_server_name")?,
            timeout_secs: positive(config, "timeout_secs", DEFAULT_TIMEOUT_SECS)?,
            watch_buffer_events: positive(
                config,
                "watch_buffer_events",
                DEFAULT_WATCH_BUFFER_EVENTS,
            )?,
        })
    }
}

/// The values of the body the host must never print: asked of the raw JSON, so
/// a refusal of any other key still carries them.
pub fn secrets(config: &Value) -> Vec<String> {
    match &config["password"] {
        Value::String(password) if !password.is_empty() => vec![password.clone()],
        // Refused as a non-string, and still kept out of what the host prints.
        Value::Number(number) => vec![number.to_string()],
        _ => Vec::new(),
    }
}

fn endpoints(config: &Value) -> Result<(Vec<String>, bool), String> {
    let text = string(config, "endpoints")?.ok_or("\"endpoints\" is required")?;
    let mut list = Vec::new();
    let (mut http, mut https) = (false, false);
    for (index, item) in text.split(',').enumerate() {
        let item = item.trim();
        let number = index + 1;
        if item.is_empty() {
            return Err(format!("\"endpoints\" item {number} is empty"));
        }
        // Case-sensitive on purpose: etcd-client matches the prefix as written,
        // and anything else it would build as `http://HTTP://…`.
        let bad = || format!("\"endpoints\" item {number} needs an http:// or https:// address");
        if item.starts_with("https://") {
            https = true;
        } else if item.starts_with("http://") {
            http = true;
        } else {
            return Err(bad());
        }
        if !item
            .parse::<Uri>()
            .is_ok_and(|uri| uri.authority().is_some())
        {
            return Err(bad());
        }
        list.push(item.to_string());
    }
    if http && https {
        return Err("\"endpoints\" mixes http and https addresses".to_string());
    }
    Ok((list, https))
}

/// A non-empty string, or `None` when the key is absent.
fn string(config: &Value, key: &str) -> Result<Option<String>, String> {
    match &config[key] {
        Value::Null => Ok(None),
        Value::String(s) if s.trim().is_empty() => Err(format!("\"{key}\" must not be empty")),
        Value::String(s) => Ok(Some(s.clone())),
        _ => Err(format!("\"{key}\" must be a string")),
    }
}

/// A path to a file that exists, resolved from the working directory.
fn file(config: &Value, key: &str) -> Result<Option<PathBuf>, String> {
    let Some(path) = string(config, key)? else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    if path.is_file() {
        Ok(Some(path))
    } else {
        Err(format!(
            "\"{key}\": no such file \"{}\" (a relative path is resolved from the directory `bddkit run` starts in)",
            path.display()
        ))
    }
}

fn pair<T>(
    first: Option<T>,
    second: Option<T>,
    first_key: &str,
    second_key: &str,
) -> Result<Option<(T, T)>, String> {
    match (first, second) {
        (Some(a), Some(b)) => Ok(Some((a, b))),
        (None, None) => Ok(None),
        (Some(_), None) => Err(format!("\"{first_key}\" needs \"{second_key}\"")),
        (None, Some(_)) => Err(format!("\"{second_key}\" needs \"{first_key}\"")),
    }
}

fn positive(config: &Value, key: &str, default: u64) -> Result<u64, String> {
    match &config[key] {
        Value::Null => Ok(default),
        value => value
            .as_u64()
            .filter(|n| *n > 0)
            .ok_or_else(|| format!("\"{key}\" must be a positive whole number")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(config: &Value) -> String {
        Config::parse(config).expect_err("must be refused")
    }

    #[test]
    fn one_endpoint_with_defaults() {
        let c = Config::parse(&json!({"endpoints": "http://127.0.0.1:2379"})).expect("ok");
        assert_eq!(c.endpoints, ["http://127.0.0.1:2379"]);
        assert!(!c.tls);
        assert_eq!(c.timeout_secs, 5);
        assert_eq!(c.watch_buffer_events, 10_000);
        assert!(c.credentials.is_none());
    }

    #[test]
    fn several_endpoints_are_split_on_commas_and_trimmed() {
        let c = Config::parse(&json!({"endpoints": "https://a:2379, https://b:2379"})).expect("ok");
        assert_eq!(c.endpoints, ["https://a:2379", "https://b:2379"]);
        assert!(c.tls);
    }

    #[test]
    fn endpoints_are_required_and_not_empty() {
        assert!(refused(&json!({})).contains("\"endpoints\" is required"));
        assert!(refused(&json!({"endpoints": ""})).contains("must not be empty"));
        assert!(
            refused(&json!({"endpoints": "http://a:1,,http://b:1"})).contains("item 2 is empty")
        );
        assert!(refused(&json!({"endpoints": "http://a:1,"})).contains("item 2 is empty"));
    }

    #[test]
    fn an_endpoint_without_a_scheme_is_refused_by_position() {
        let error = refused(&json!({"endpoints": "http://a:1,secret-host:2379"}));
        assert!(
            error.contains("item 2 needs an http:// or https:// address"),
            "{error}"
        );
        assert!(!error.contains("secret-host"), "{error}");
        assert!(refused(&json!({"endpoints": "ftp://a:1"})).contains("item 1"));
        assert!(refused(&json!({"endpoints": "http://"})).contains("item 1"));
    }

    #[test]
    fn a_scheme_in_capitals_or_a_malformed_address_is_refused() {
        for bad in ["HTTP://a:1", "Https://a:1", "http://a:1 x", "http://"] {
            let error = refused(&json!({"endpoints": bad}));
            assert!(
                error.contains("item 1 needs an http:// or https:// address"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn tls_keys_need_https_endpoints() {
        for key in ["tls_ca", "tls_cert", "tls_key", "tls_server_name"] {
            let error = refused(&json!({"endpoints": "http://a:1", key: "Cargo.toml"}));
            assert_eq!(error, format!("\"{key}\" needs https:// endpoints"));
        }
    }

    #[test]
    fn http_mixed_with_https_is_refused() {
        let error = refused(&json!({"endpoints": "http://a:1,https://b:1"}));
        assert!(error.contains("mixes http and https"), "{error}");
    }

    #[test]
    fn an_unknown_key_is_refused_and_options_is_tolerated() {
        let error = refused(&json!({"endpoints": "http://a:1", "colour": "red"}));
        assert_eq!(error, "unknown key \"colour\"");
        assert!(Config::parse(&json!({"endpoints": "http://a:1", "options": {}})).is_ok());
    }

    #[test]
    fn a_body_that_is_not_a_mapping_is_refused() {
        assert!(refused(&json!("http://a:1")).contains("mapping"));
    }

    #[test]
    fn credentials_and_the_client_identity_come_only_in_pairs() {
        let base = |extra: Value| {
            let mut body = json!({"endpoints": "https://a:1"});
            body.as_object_mut()
                .expect("object")
                .extend(extra.as_object().expect("object").clone());
            body
        };
        assert!(
            refused(&base(json!({"username": "root"}))).contains("\"username\" needs \"password\"")
        );
        assert!(
            refused(&base(json!({"password": "x"}))).contains("\"password\" needs \"username\"")
        );
        assert!(
            refused(&base(json!({"tls_key": "Cargo.toml"})))
                .contains("\"tls_key\" needs \"tls_cert\"")
        );
        assert!(
            refused(&base(json!({"tls_cert": "Cargo.toml"})))
                .contains("\"tls_cert\" needs \"tls_key\"")
        );
        let c = Config::parse(&base(json!({"username": "root", "password": "x"}))).expect("pair");
        assert_eq!(c.credentials, Some(("root".to_string(), "x".to_string())));
    }

    #[test]
    fn a_certificate_file_that_does_not_exist_is_refused_naming_the_key() {
        for key in ["tls_ca", "tls_cert", "tls_key"] {
            let mut body = json!({"endpoints": "https://a:1", key: "no/such/file.pem"});
            if key == "tls_cert" {
                body["tls_key"] = json!("Cargo.toml");
            }
            if key == "tls_key" {
                body["tls_cert"] = json!("Cargo.toml");
            }
            let error = refused(&body);
            assert!(
                error.starts_with(&format!("\"{key}\": no such file")),
                "{error}"
            );
        }
    }

    #[test]
    fn an_existing_certificate_file_is_accepted() {
        let c = Config::parse(&json!({"endpoints": "https://a:1", "tls_ca": "Cargo.toml"}))
            .expect("ok");
        assert_eq!(c.tls_ca, Some(PathBuf::from("Cargo.toml")));
    }

    #[test]
    fn numbers_must_be_positive_whole_numbers() {
        for bad in [json!(0), json!(-1), json!(1.5), json!("5"), json!(true)] {
            let error = refused(&json!({"endpoints": "http://a:1", "timeout_secs": bad}));
            assert_eq!(error, "\"timeout_secs\" must be a positive whole number");
        }
        let c = Config::parse(
            &json!({"endpoints": "http://a:1", "timeout_secs": 9, "watch_buffer_events": 3}),
        )
        .expect("ok");
        assert_eq!((c.timeout_secs, c.watch_buffer_events), (9, 3));
    }

    #[test]
    fn a_refusal_never_quotes_a_value() {
        // serde would say "invalid type: integer `123456789`".
        let error =
            refused(&json!({"endpoints": "http://a:1", "username": "u", "password": 123_456_789}));
        assert!(!error.contains("123456789"), "{error}");
        assert_eq!(error, "\"password\" must be a string");
        let error = refused(&json!({"endpoints": "http://a:1", "password": "hunter2hunter2"}));
        assert!(!error.contains("hunter2"), "{error}");
    }

    #[test]
    fn the_password_is_a_secret_whatever_else_is_wrong() {
        let body = json!({"endpoints": "", "username": "u", "password": "hunter2hunter2"});
        assert_eq!(secrets(&body), ["hunter2hunter2"]);
        assert!(secrets(&json!({"endpoints": "http://a:1"})).is_empty());
        assert_eq!(secrets(&json!({"password": 123_456_789})), ["123456789"]);
    }

    #[test]
    fn every_key_the_body_accepts_is_described_once() {
        let fields = fields_json();
        let names: Vec<&str> = fields
            .as_array()
            .expect("array")
            .iter()
            .map(|f| f["name"].as_str().expect("name"))
            .collect();
        assert_eq!(
            names,
            [
                "endpoints",
                "username",
                "password",
                "tls_ca",
                "tls_cert",
                "tls_key",
                "tls_server_name",
                "timeout_secs",
                "watch_buffer_events",
                "options"
            ]
        );
        assert_eq!(fields[0]["required"], true);
        // `example` is always a string for the host.
        assert!(
            fields
                .as_array()
                .expect("array")
                .iter()
                .all(|f| f.get("example").is_none_or(Value::is_string))
        );
    }
}
