//! What every end-to-end test shares: the host, the plugin built for it and
//! the lock directory that ties the two. `BDDKIT_BIN` names a `bddkit` host.
//! Without it the tests skip, except under `CI`, where a missing host is a
//! failure: a skipped suite must never look green there.

#![allow(
    dead_code,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "each test binary uses a part of this module; `allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

pub mod cluster;

use std::ffi::{CStr, CString, c_char};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;

use bddkit_etcd::*;
use serde_json::{Value, json};

/// The server the live tests reach: the one `docker-compose.yml` starts.
pub fn etcd_endpoint() -> String {
    std::env::var("BDDKIT_TEST_ETCD").unwrap_or_else(|_| "http://127.0.0.1:2379".to_string())
}

pub fn bddkit_bin() -> Option<PathBuf> {
    std::env::var("BDDKIT_BIN")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_file())
}

/// Returns from the test, with a note, when there is no host to run it on.
#[macro_export]
macro_rules! require_bddkit {
    () => {
        match $crate::common::bddkit_bin() {
            Some(bin) => bin,
            None if std::env::var_os("CI").is_some() => {
                panic!("BDDKIT_BIN does not name a bddkit binary, and CI must not skip")
            }
            None => {
                eprintln!("SKIP: set BDDKIT_BIN to a bddkit binary");
                return;
            }
        }
    };
}

pub fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// Builds the plugin once and writes a lock file for it into a directory
/// handed to `--bddkit-dir`.
pub fn lock_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let out = Command::new(env!("CARGO"))
            .arg("build")
            .current_dir(root())
            .output()
            .expect("cargo build");
        assert!(
            out.status.success(),
            "plugin build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let library = root().join("target/debug").join(format!(
            "{}bddkit_etcd{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        ));
        let dir = std::env::temp_dir().join(format!("bddkit-etcd-e2e-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(
            dir.join("plugins.yaml"),
            format!("plugin:\n  - name: etcd\n    path: {}\n", library.display()),
        )
        .expect("lock");
        dir
    })
}

pub fn bddkit(bin: &Path, args: &[&str]) -> Output {
    Command::new(bin)
        .args(args)
        .arg("--bddkit-dir")
        .arg(lock_dir())
        .output()
        .expect("bddkit")
}

/// A config declaring `resources.etcd` as `section`, over a feature that
/// needs no resource, so every outcome comes from the instances.
pub fn project(name: &str, section: &str) -> PathBuf {
    project_with(
        name,
        section,
        "Feature: only\n  Scenario: one\n    Given set variable \"x\" to \"1\"\n",
    )
}

/// The same, over a feature of the test's own.
pub fn project_with(name: &str, section: &str, feature: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("bddkit-etcd-{name}-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("features")).expect("mkdir");
    std::fs::write(dir.join("features/only.feature"), feature).expect("feature");
    let config = dir.join("cfg.yaml");
    std::fs::write(
        &config,
        format!(
            "paths: [{}]\nresources:\n  etcd:\n{section}",
            dir.join("features")
                .display()
                .to_string()
                .replace('\\', "/")
        ),
    )
    .expect("config");
    config
}

/// Standard output and standard error as one string.
pub fn combined(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A reply of an export, parsed and handed back to `bddkit_free_string`.
pub fn call(reply: *mut c_char) -> Value {
    assert!(!reply.is_null(), "an export returned NULL");
    // SAFETY: not null (asserted above), and every reply of this library comes
    // from `CString::into_raw`, so it is NUL-terminated and lives until it is
    // freed below.
    let text = unsafe { CStr::from_ptr(reply) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: an export returned it, and this is the one place it is freed.
    unsafe { bddkit_etcd::bddkit_free_string(reply) };
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

pub fn cstr(v: &Value) -> CString {
    CString::new(v.to_string()).expect("no NUL")
}

pub fn request(config: &Value) -> CString {
    cstr(&json!({"group": "etcd", "instance": "main", "config": config, "options": {}}))
}

/// A handle that is dropped with it.
pub struct Handle(pub u64);

impl Handle {
    pub fn new(config: &Value) -> Self {
        let reply = call(bddkit_init_instance(request(config).as_ptr()));
        Self(reply["handle"].as_u64().expect("handle"))
    }

    pub fn live() -> Self {
        Self::new(&json!({"endpoints": etcd_endpoint()}))
    }

    pub fn dispatch(
        &self,
        step: u32,
        args: &[&str],
        docstring: Option<&str>,
        debug: bool,
    ) -> Value {
        self.send(step, args, docstring, None, debug)
    }

    pub fn send(
        &self,
        step: u32,
        args: &[&str],
        docstring: Option<&str>,
        table: Option<&[&[&str]]>,
        debug: bool,
    ) -> Value {
        let request = json!({"args": args, "docstring": docstring, "table": table, "debug": debug, "instance": "main"});
        call(bddkit_dispatch(self.0, step, cstr(&request).as_ptr()))
    }

    pub fn run(&self, step: u32, args: &[&str]) -> Value {
        self.dispatch(step, args, None, false)
    }

    pub fn context(&self) -> Value {
        call(bddkit_failure_context(self.0))
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        call(bddkit_drop_instance(self.0));
    }
}

// ---- the secure servers of docker-compose.yml ----

/// TLS only.
pub fn secure_endpoint() -> String {
    std::env::var("BDDKIT_TEST_ETCD_SECURE").unwrap_or_else(|_| "https://127.0.0.1:22379".into())
}

/// TLS, and authentication once `enable_auth` has run.
pub fn auth_endpoint() -> String {
    std::env::var("BDDKIT_TEST_ETCD_AUTH").unwrap_or_else(|_| "https://127.0.0.1:24379".into())
}

/// TLS, and a client certificate signed by the test CA is required.
pub fn mtls_endpoint() -> String {
    std::env::var("BDDKIT_TEST_ETCD_MTLS").unwrap_or_else(|_| "https://127.0.0.1:23379".into())
}

/// The name on the server certificate; the address is an IP it does not cover.
pub const SERVER_NAME: &str = "etcd.test";
pub const ROOT_PASSWORD: &str = "root-test-password-7f3a";

/// A file of `tests/certs`, as the absolute path a config names.
pub fn cert(name: &str) -> String {
    root().join("tests/certs").join(name).display().to_string()
}

/// The secure server, reached the way a tester configures it: the CA and the
/// name of the certificate.
pub fn secure_config() -> Value {
    json!({
        "endpoints": secure_endpoint(),
        "tls_ca": cert("ca.crt"),
        "tls_server_name": SERVER_NAME,
    })
}

pub fn root_config() -> Value {
    let mut config = secure_config();
    config["endpoints"] = json!(auth_endpoint());
    config["username"] = json!("root");
    config["password"] = json!(ROOT_PASSWORD);
    config
}

pub fn mtls_config() -> Value {
    json!({
        "endpoints": mtls_endpoint(),
        "tls_ca": cert("ca.crt"),
        "tls_server_name": SERVER_NAME,
        "tls_cert": cert("client.crt"),
        "tls_key": cert("client.key"),
    })
}

/// A client of the authentication server of the tests' own, not the plugin's: it sets
/// the server up, and it breaks it.
async fn admin(user: Option<(&str, &str)>) -> etcd_client::Client {
    let tls = etcd_client::TlsOptions::new()
        .ca_certificate(etcd_client::Certificate::from_pem(
            std::fs::read(cert("ca.crt")).expect("ca.crt"),
        ))
        .domain_name(SERVER_NAME);
    let mut options = etcd_client::ConnectOptions::new().with_tls(tls);
    if let Some((name, password)) = user {
        options = options.with_user(name, password);
    }
    etcd_client::Client::connect([auth_endpoint()], Some(options))
        .await
        .expect("connect to the secure etcd (is it up? docker compose up --wait etcd-auth)")
}

pub fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(future)
}

/// Creates the root user and switches authentication on, once per test
/// binary. A server that already has it on (an earlier run) is left alone.
pub fn enable_auth() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        block_on(async {
            let mut auth = admin(None).await.auth_client();
            // With authentication on, even a listing is refused.
            if auth.role_list().await.is_err() {
                return;
            }
            // Either may be left over from an interrupted run.
            let _ = auth.role_add("root").await;
            let _ = auth.user_add("root", ROOT_PASSWORD, None).await;
            auth.user_grant_role("root", "root").await.expect("grant");
            auth.auth_enable().await.expect("auth enable");
        });
    });
}

/// Every token the authentication server has issued stops being valid: authentication goes
/// off and on again, which is what a member restart does to the simple tokens.
pub fn invalidate_tokens() {
    block_on(async {
        admin(Some(("root", ROOT_PASSWORD)))
            .await
            .auth_client()
            .auth_disable()
            .await
            .expect("auth disable");
        admin(None)
            .await
            .auth_client()
            .auth_enable()
            .await
            .expect("auth enable");
    });
}

/// A token as the server issues them, to know what one looks like.
pub fn sample_token() -> String {
    block_on(async {
        admin(None)
            .await
            .auth_client()
            .authenticate("root".into(), ROOT_PASSWORD.into())
            .await
            .expect("authenticate")
            .token()
            .to_string()
    })
}
