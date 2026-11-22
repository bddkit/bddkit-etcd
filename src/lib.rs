//! bddkit plugin serving the `etcd` resource group: any etcd v3 compatible
//! server. Written against docs/plugin-authoring.md; it must never need the
//! host's source.

mod client;
pub mod config;
mod reply;
mod runtime;

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use config::Config;
use serde_json::Value;

/// The plugin ABI this library is built for.
const ABI: u32 = 4;

/// Outside `guard`, and safe there only because it provably cannot panic:
/// `CString::new` returns a `Result` and the fallback is a C string literal.
/// Anything added here must keep that property.
fn out(s: String) -> *mut c_char {
    CString::new(s)
        .unwrap_or_else(|_| c"{\"ok\":false,\"error\":\"NUL in reply\"}".to_owned())
        .into_raw()
}

/// Inside `guard` at every call site.
fn input(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: `p` is not null. The host builds a request from a `CString` and
    // keeps it valid for the duration of the call, which encloses this one;
    // it is copied before returning.
    unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

/// A panic must never unwind across the FFI boundary. Every export is
/// guarded, including the trivial ones, so "every export is guarded" is an
/// invariant a reader checks in one pass.
fn guard(envelope_kind: &str, body: impl FnOnce() -> String) -> *mut c_char {
    out(guarded(envelope_kind, body))
}

fn guarded(envelope_kind: &str, body: impl FnOnce() -> String) -> String {
    catch_unwind(AssertUnwindSafe(body)).unwrap_or_else(|_| match envelope_kind {
        "dispatch" => r#"{"status":"fatal","error":"the plugin panicked"}"#.to_string(),
        _ => r#"{"ok":false,"error":"the plugin panicked"}"#.to_string(),
    })
}

pub fn manifest_json() -> String {
    serde_json::json!({
        "name": "etcd",
        "version": env!("CARGO_PKG_VERSION"),
        "groups": ["etcd"],
        // The plugin keeps per-scenario state from the next phase on, and the
        // host allows a reset under parallel runs only in this mode.
        "concurrency": "per_worker",
        // No `implicit_instance`: a server has no default address.
        "fields": { "etcd": config::fields_json() },
    })
    .to_string()
}

#[unsafe(no_mangle)]
pub extern "C" fn bddkit_abi_version() -> u32 {
    ABI
}

#[unsafe(no_mangle)]
pub extern "C" fn bddkit_manifest() -> *mut c_char {
    guard("envelope", manifest_json)
}

/// No steps yet: the index of a step is its identity, so the array grows by
/// appending.
#[unsafe(no_mangle)]
pub extern "C" fn bddkit_list_steps() -> *mut c_char {
    guard("envelope", || "[]".to_string())
}

/// # Safety
/// `s` must be a pointer this library returned and has not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bddkit_free_string(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: by this function's contract `s` came from `out`'s
        // `CString::into_raw` in this library and has not been freed yet.
        drop(unsafe { CString::from_raw(s) });
    }
}

/// The instance body of a `validate_config`, `probe_config` or
/// `init_instance` request.
fn request_config(request: &str) -> Result<Value, String> {
    let value: Value = serde_json::from_str(request).map_err(|e| e.to_string())?;
    Ok(value["config"].clone())
}

/// Eager, at startup, for every declared instance, touching nothing.
fn validate(request: &str) -> String {
    let body = match request_config(request) {
        Ok(body) => body,
        Err(error) => return reply::err(error, &[]),
    };
    let secrets = config::secrets(&body);
    match Config::parse(&body) {
        Ok(config) => reply::ok_detail(&config.endpoints.join(", "), &secrets),
        Err(error) => reply::err(error, &secrets),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn bddkit_validate_config(request: *const c_char) -> *mut c_char {
    guard("envelope", move || validate(&input(request)))
}

/// `doctor --live`: `Status` on every endpoint.
#[unsafe(no_mangle)]
pub extern "C" fn bddkit_probe_config(request: *const c_char) -> *mut c_char {
    guard("envelope", move || {
        let body = match request_config(&input(request)) {
            Ok(body) => body,
            Err(error) => return reply::err(error, &[]),
        };
        let secrets = config::secrets(&body);
        let config = match Config::parse(&body) {
            Ok(config) => config,
            Err(error) => return reply::err(error, &secrets),
        };
        match runtime::block_on(client::probe(&config)) {
            Ok(detail) => reply::ok_detail(&detail, &secrets),
            Err(error) => reply::err(error, &secrets),
        }
    })
}

/// What a handle holds. The client joins it at the first dispatch.
struct Instance {
    #[expect(dead_code, reason = "read by the steps of the next issue")]
    config: Config,
}

/// A handle is an index into this table, never a pointer.
static INSTANCES: LazyLock<Mutex<HashMap<u64, Arc<Instance>>>> = LazyLock::new(Mutex::default);
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

/// Nothing can panic under this lock (an insert, a remove), and the table
/// holds no half-updated state if it ever did.
fn instances() -> MutexGuard<'static, HashMap<u64, Arc<Instance>>> {
    INSTANCES.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Opens no connection: with a password the client fetches a token at
/// connect, and a failed init is `fatal` class `plugin`, which would end
/// polling at once. The client is built at the first dispatch.
fn init(request: &str) -> String {
    let body = match request_config(request) {
        Ok(body) => body,
        Err(error) => return reply::err(error, &[]),
    };
    let secrets = config::secrets(&body);
    match Config::parse(&body) {
        Ok(config) => {
            let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
            instances().insert(handle, Arc::new(Instance { config }));
            reply::handle(handle)
        }
        Err(error) => reply::err(error, &secrets),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn bddkit_init_instance(request: *const c_char) -> *mut c_char {
    guard("envelope", move || init(&input(request)))
}

#[unsafe(no_mangle)]
pub extern "C" fn bddkit_dispatch(
    handle: u64,
    _step_index: u32,
    _request: *const c_char,
) -> *mut c_char {
    guard("dispatch", move || {
        if instances().contains_key(&handle) {
            reply::fatal("unknown step")
        } else {
            reply::fatal("unknown handle")
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn bddkit_drop_instance(handle: u64) -> *mut c_char {
    guard("envelope", move || match instances().remove(&handle) {
        Some(_) => reply::ok(),
        None => reply::err("unknown handle", &[]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bddkit_plugin_json_matches_the_library_and_the_manifest() {
        let declared: Value =
            serde_json::from_str(include_str!("../bddkit-plugin.json")).expect("valid JSON");
        assert_eq!(declared["abi"], bddkit_abi_version());
        let manifest: Value = serde_json::from_str(&manifest_json()).expect("JSON");
        assert_eq!(declared["groups"], manifest["groups"]);
    }

    #[test]
    fn the_manifest_claims_etcd_per_worker_with_no_implicit_instance() {
        let v: Value = serde_json::from_str(&manifest_json()).expect("JSON");
        assert_eq!(v["name"], "etcd");
        assert_eq!(v["concurrency"], "per_worker");
        assert!(v.get("handle_scope").is_none(), "one handle per instance");
        assert!(v.get("implicit_instance").is_none());
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert!(v["fields"]["etcd"].is_array());
    }

    #[test]
    fn a_panic_in_an_export_answers_an_envelope() {
        let dispatch: Value =
            serde_json::from_str(&guarded("dispatch", || panic!("boom"))).expect("JSON");
        assert_eq!(dispatch["status"], "fatal");
        let other: Value =
            serde_json::from_str(&guarded("envelope", || panic!("boom"))).expect("JSON");
        assert_eq!(other["ok"], false);
    }

    #[test]
    fn validate_names_the_endpoints_and_hides_the_password_even_in_a_refusal() {
        let ok: Value = serde_json::from_str(&validate(
            &json!({"config": {"endpoints": "http://a:1,http://b:1", "username": "u", "password": "hunter2hunter2"}})
                .to_string(),
        ))
        .expect("JSON");
        assert_eq!(ok["ok"], true);
        assert_eq!(ok["detail"], "http://a:1, http://b:1");
        assert_eq!(ok["secrets"], json!(["hunter2hunter2"]));

        let refused: Value = serde_json::from_str(&validate(
            &json!({"config": {"endpoints": "", "password": "hunter2hunter2"}}).to_string(),
        ))
        .expect("JSON");
        assert_eq!(refused["ok"], false);
        assert_eq!(refused["secrets"], json!(["hunter2hunter2"]));
        assert!(
            !refused["error"]
                .as_str()
                .expect("error")
                .contains("hunter2")
        );
    }

    #[test]
    fn init_opens_nothing_and_hands_out_distinct_handles_that_drop_once() {
        let request = json!({"config": {"endpoints": "http://127.0.0.1:1"}}).to_string();
        let first: Value = serde_json::from_str(&init(&request)).expect("JSON");
        let second: Value = serde_json::from_str(&init(&request)).expect("JSON");
        assert_ne!(first["handle"], second["handle"]);
        let handle = first["handle"].as_u64().expect("handle");
        assert!(instances().remove(&handle).is_some());
        assert!(instances().remove(&handle).is_none());
    }
}
