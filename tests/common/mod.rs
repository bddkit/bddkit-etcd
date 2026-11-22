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

use std::ffi::{CStr, CString, c_char};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;

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
