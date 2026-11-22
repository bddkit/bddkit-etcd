//! The client outlives its token. Its own binary: the server's authentication
//! goes off and on again here, which would fail any test running beside it.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "`allow-*-in-tests` covers only `#[test]` functions, not the helpers here"
)]

mod common;

use common::{Handle, enable_auth, invalidate_tokens, root_config};

const PUT: u32 = 0;
const GET: u32 = 2;

#[test]
fn a_token_the_server_dropped_is_fetched_again_by_the_next_step() {
    enable_auth();
    let handle = Handle::new(&root_config());
    let key = format!("/bddkit-etcd/refresh/{}/k", std::process::id());
    assert_eq!(handle.run(PUT, &[&key, "before"])["status"], "passed");

    invalidate_tokens();

    // The same handle, the same client: no step of ours asked for a new token.
    let reply = handle.run(GET, &[&key]);
    assert_eq!(reply["status"], "passed", "{reply}");
    assert_eq!(reply["vars"]["etcd_value"], "before");
    assert_eq!(handle.run(PUT, &[&key, "after"])["status"], "passed");
}
