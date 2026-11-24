//! `I authenticate to etcd as …` and `etcd should refuse authentication …`
//! against the authentication server (`etcd-auth`): a login lasts until the
//! scenario ends, a refused one changes nothing, and the password is in no
//! output. The root user is the configured one; `bddkit-reader` may only read
//! under `/bddkit-etcd/auth/`.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bddkit_etcd::*;
use common::{
    Handle, ROOT_PASSWORD, SERVER_NAME, auth_endpoint, block_on, call, cert, enable_auth,
    root_config,
};
use serde_json::{Value, json};

const PUT: u32 = 0;
const GET: u32 = 2;
const ACCEPT_READ_SERIALIZABLE: u32 = 26;
const REFUSE_PUT: u32 = 24;
const REFUSE_READ_SERIALIZABLE: u32 = 28;
const WATCH_KEY: u32 = 30;
const WATCHED_PUT: u32 = 35;
const AUTHENTICATE: u32 = 55;
const REFUSE_AUTH: u32 = 56;

const READER: &str = "bddkit-reader";
const READER_PASSWORD: &str = "reader-pw-9d2b41";
const WRONG_PASSWORD: &str = "wrong-pw-c07e55";

/// The prefix the reader may read.
fn key(name: &str) -> String {
    format!("/bddkit-etcd/auth/{}/{name}", std::process::id())
}

/// Authentication on, root as the configured user, and a reader that may read
/// under one prefix and write nowhere. Once per test binary.
fn reader() {
    static DONE: OnceLock<()> = OnceLock::new();
    enable_auth();
    DONE.get_or_init(|| {
        block_on(async {
            let tls = etcd_client::TlsOptions::new()
                .ca_certificate(etcd_client::Certificate::from_pem(
                    std::fs::read(cert("ca.crt")).expect("ca.crt"),
                ))
                .domain_name(SERVER_NAME);
            let options = etcd_client::ConnectOptions::new()
                .with_tls(tls)
                .with_user("root", ROOT_PASSWORD);
            let client = etcd_client::Client::connect([auth_endpoint()], Some(options))
                .await
                .expect("connect as root");
            let mut auth = client.auth_client();
            // Left over from an earlier run: already there is fine.
            let _ = auth.role_add("bddkit-reader-role").await;
            let _ = auth.user_add(READER, READER_PASSWORD, None).await;
            auth.role_grant_permission(
                "bddkit-reader-role",
                etcd_client::Permission::read("/bddkit-etcd/auth/").with_prefix(),
            )
            .await
            .expect("grant read");
            auth.user_grant_role(READER, "bddkit-reader-role")
                .await
                .expect("grant role");
        });
    });
}

fn root() -> Handle {
    reader();
    Handle::new(&root_config())
}

fn passed(reply: &Value) {
    assert_eq!(reply["status"], "passed", "{reply}");
}

fn eventually(handle: &Handle, step: u32, args: &[&str]) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let reply = handle.run(step, args);
        if reply["status"] == "passed" || Instant::now() > deadline {
            return reply;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn reset(handle: &Handle) {
    assert_eq!(call(bddkit_reset_scenario(handle.0))["ok"], true);
}

#[test]
fn a_login_lasts_until_the_scenario_ends() {
    let handle = root();
    let (readable, other) = (key("readable"), "/bddkit-etcd/other/k");
    passed(&handle.run(PUT, &[&readable, "v"]));

    passed(&handle.run(AUTHENTICATE, &[READER, READER_PASSWORD]));
    // The reader reads, and is refused a write, on both clients: the one that
    // asks the leader and the one that does not.
    passed(&handle.run(GET, &[&readable]));
    passed(&handle.run(REFUSE_PUT, &[&readable, "PermissionDenied"]));
    passed(&handle.run(ACCEPT_READ_SERIALIZABLE, &[&readable]));
    passed(&handle.run(REFUSE_READ_SERIALIZABLE, &[other, "PermissionDenied"]));

    reset(&handle);
    // The next scenario is the configured user again.
    passed(&handle.run(PUT, &[&readable, "again"]));
    passed(&handle.run(PUT, &[other, "root-writes-anywhere"]));
}

#[test]
fn a_refused_password_fails_the_step_and_changes_nothing() {
    let handle = root();
    let readable = key("refused");
    passed(&handle.run(AUTHENTICATE, &[READER, READER_PASSWORD]));

    let reply = handle.run(AUTHENTICATE, &[READER, WRONG_PASSWORD]);
    assert_eq!(reply["status"], "fatal", "{reply}");
    assert_eq!(reply["class"], "db_statement", "{reply}");
    // Still the reader: a write is refused.
    passed(&handle.run(REFUSE_PUT, &[&readable, "PermissionDenied"]));
    reset(&handle);

    let reply = handle.run(AUTHENTICATE, &["nobody-at-all", WRONG_PASSWORD]);
    assert_eq!(reply["class"], "db_statement", "{reply}");
    // The configured user is untouched.
    passed(&handle.run(PUT, &[&readable, "v"]));
}

#[test]
fn a_refused_login_is_expected_with_the_code_the_server_answers() {
    let handle = root();
    passed(&handle.run(REFUSE_AUTH, &[READER, WRONG_PASSWORD, "InvalidArgument"]));
    passed(&handle.run(
        REFUSE_AUTH,
        &["nobody-at-all", WRONG_PASSWORD, "InvalidArgument"],
    ));
    // It logs nobody in: root still writes.
    passed(&handle.run(PUT, &[&key("after-refusal"), "v"]));

    let wrong_code = handle.run(REFUSE_AUTH, &[READER, WRONG_PASSWORD, "Unauthenticated"]);
    assert_eq!(wrong_code["status"], "not_yet", "{wrong_code}");
    assert_eq!(wrong_code["class"], "assertion", "{wrong_code}");

    let accepted = handle.run(REFUSE_AUTH, &[READER, READER_PASSWORD, "InvalidArgument"]);
    assert_eq!(accepted["status"], "not_yet", "{accepted}");
    assert_eq!(accepted["class"], "assertion", "{accepted}");
    assert!(
        accepted["error"]
            .as_str()
            .expect("error")
            .contains("accepted"),
        "{accepted}"
    );
    // A login the server accepted is not a login of the instance: root still
    // writes where the reader may not.
    passed(&handle.run(PUT, &["/bddkit-etcd/other/accepted", "v"]));
}

#[test]
fn a_login_the_client_could_not_even_try_is_never_a_refusal() {
    let handle = Handle::new(&json!({
        "endpoints": "https://127.0.0.1:1",
        "tls_ca": cert("ca.crt"),
        "timeout_secs": 2,
    }));
    let reply = handle.run(REFUSE_AUTH, &[READER, WRONG_PASSWORD, "InvalidArgument"]);
    assert_eq!(reply["status"], "not_yet", "{reply}");
    assert_eq!(reply["class"], "db_connection", "{reply}");
}

#[test]
fn a_watch_opened_before_the_login_keeps_receiving_events() {
    let handle = root();
    // Outside what the reader may read: a watch moved onto the reader's
    // client would be cancelled for permission and never see the event.
    let watched = format!("/bddkit-etcd/other/{}/watched", std::process::id());
    let writer = root();
    passed(&handle.run(WATCH_KEY, &[&watched, "w"]));
    passed(&handle.run(AUTHENTICATE, &[READER, READER_PASSWORD]));
    passed(&writer.run(PUT, &[&watched, "from-root"]));
    let reply = eventually(&handle, WATCHED_PUT, &["w", &watched, "from-root"]);
    passed(&reply);
}

#[test]
fn the_password_is_in_no_reply_error_or_diagnostic() {
    let handle = root();
    let readable = key("secret");
    let mut everything = String::new();
    for (args, step) in [
        (vec![READER, READER_PASSWORD], AUTHENTICATE),
        (vec![READER, WRONG_PASSWORD], AUTHENTICATE),
        (vec![READER, WRONG_PASSWORD, "InvalidArgument"], REFUSE_AUTH),
        (
            vec![READER, READER_PASSWORD, "InvalidArgument"],
            REFUSE_AUTH,
        ),
        (vec![READER, WRONG_PASSWORD, "Unauthenticated"], REFUSE_AUTH),
    ] {
        for debug in [false, true] {
            let reply = handle.dispatch(step, &args, None, debug);
            everything.push_str(&reply.to_string());
        }
    }
    passed(&handle.run(REFUSE_PUT, &[&readable, "PermissionDenied"]));
    // A failing step after the login: the failure context of the handle.
    everything.push_str(&handle.context().to_string());
    for password in [READER_PASSWORD, WRONG_PASSWORD] {
        assert!(!everything.contains(password), "{everything}");
    }
    assert!(everything.contains(READER), "the evidence names the user");
    // A login with no code, and a refusal with an unknown one, validate
    // before the run without quoting the password.
    let request = json!({"args": [READER, WRONG_PASSWORD, "Nope"]});
    let reply = call(bddkit_validate_step(
        REFUSE_AUTH,
        common::cstr(&request).as_ptr(),
    ));
    assert_eq!(reply["ok"], false, "{reply}");
    assert!(!reply.to_string().contains(WRONG_PASSWORD), "{reply}");
}

/// The real host: a password written in a step is `***` in everything it
/// prints, and the failing step of a polled refusal shows no password either.
#[test]
fn a_password_written_in_a_step_is_masked_by_the_host() {
    let bin = require_bddkit!();
    reader();
    let feature = format!(
        "Feature: login\n  Scenario: one\n    When I authenticate to etcd as \"{READER}\" with password \"{READER_PASSWORD}\"\n    Then etcd should refuse a put of key \"/bddkit-etcd/auth/e2e/k\" with \"PermissionDenied\"\n    And etcd should refuse authentication as \"{READER}\" with password \"{WRONG_PASSWORD}\" with \"InvalidArgument\"\n    And etcd should refuse authentication as \"{READER}\" with password \"{READER_PASSWORD}\" with \"InvalidArgument\"\n",
    );
    let section = format!(
        "    main:\n      endpoints: \"{}\"\n      tls_ca: \"{}\"\n      tls_server_name: {SERVER_NAME}\n      username: root\n      password: \"{ROOT_PASSWORD}\"\n      options:\n        polling:\n          timeout_secs: 1\n          interval_ms: 200\n",
        auth_endpoint(),
        cert("ca.crt"),
    );
    let cfg = common::project_with("login", &section, &feature);
    let events = cfg.with_file_name("events.jsonl");
    let out = common::bddkit(
        &bin,
        &[
            "run",
            "--config",
            cfg.to_str().expect("path"),
            "--events",
            events.to_str().expect("path"),
            "--evidence=all",
        ],
    );
    let text = format!(
        "{}{}",
        common::combined(&out),
        std::fs::read_to_string(&events).expect("events")
    );
    // The last step fails, and the ones before it passed.
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(text.contains("the server accepted a login as"), "{text}");
    assert!(text.contains("with password \"***\" with"), "{text}");
    for password in [READER_PASSWORD, WRONG_PASSWORD, ROOT_PASSWORD] {
        assert!(!text.contains(password), "{text}");
    }
}
