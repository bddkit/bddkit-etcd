# CLAUDE.md — bddkit-etcd

Agent orientation for this plugin repository.

`bddkit-etcd` is a plugin for `bddkit`, a Gherkin-based acceptance-testing tool in Rust: it gives scenarios steps against any server that speaks the etcd v3 gRPC API. It is a `cdylib` loaded by the host at run time; resource group: `etcd`; plugin ABI 4 (`bddkit-plugin.json`). `README.md` is the tester-facing description.

## Where the contract lives

The authoring contract is `docs/plugin-authoring.md` in the `bddkit/bddkit` repository (https://github.com/bddkit/bddkit/blob/main/docs/plugin-authoring.md). If your memory has no entry saying where a local checkout of the host is, do not guess a path: ask the operator for a folder or a URL and save the answer as a `reference` memory. A change to the ABI is made in the host first and then followed here.

## Rules only the plugin can keep (host cannot check them)

- **Every `extern "C"` export runs inside `catch_unwind`, argument decoding included** (`guard` in `lib.rs`). Unwinding across the FFI boundary is undefined behaviour. A panic becomes `{"status":"fatal","error":"the plugin panicked"}` for dispatch and `{"ok":false,"error":"…"}` for the rest.
- **Only JSON strings cross the boundary** — never a Rust type, never a borrowed slice.
- **Never print to stdout.** The host composes a file's output atomically; evidence goes back as `diagnostics` in the reply.
- **`<<null>>` arrives as two real NUL bytes inside a `String`**, so `CString::new(arg)` fails on it. Handle it before building a C string.
- **`init_instance` opens no connection.** With a password the client fetches a token at connect, and a failed init is `fatal` class `plugin`, which ends polling at once. The client is built at the first dispatch.
- **No config error may quote a value.** `config.rs` reads the raw JSON and writes messages that name the key; a serde derive would quote a numeric password. `password` goes to `secrets` on every reply, refusals included.
- **The plugin owns one process-wide tokio runtime** (`runtime.rs`); a cdylib is never unloaded, so nothing may live on a runtime of its own.
- **Failure classes are set where the error is born, by the error's type and never by its text** (`client::class_of`: a `tonic::Status` with a `source()` is `db_connection`, one without is `db_statement`). There is no `From<String> for StepError`, on purpose. A connect failure keeps `explain`'s wording (a refused password is a status too); a failed request says what the server answered, because `InvalidArgument` on an empty key is not a credentials refusal.
- **A step that cannot publish a variable fails, it never leaves it alone**: the host keeps a variable a reply does not name, so an unset `etcd_value` or `etcd_revision` would let a check read an earlier step's value. A non-UTF-8 value on `get`, and a reply with no header, are `fatal`; the base64 of the value is in the message and the diagnostic.
- **The client refreshes its token** (`with_auto_token_refresh`): it lives as long as the feature file, past the server's token TTL and a member restart. Covered by `tests/token_refresh.rs`, which switches the authentication of `etcd-auth` off and on to drop every token (a member restart does the same to the simple tokens).
- **The token is never in a reply's `secrets`, and cannot be**: `etcd-client` fetches it and keeps it private, so the plugin never holds it. It travels only in gRPC metadata, which no diagnostic copies. Do not "fix" this by authenticating a second time to get a value to hide: that is a different token. The password goes to `secrets`.
- **The plugin is written against `docs/plugin-authoring.md` and must never need the host's source.**

## Layout

`src/lib.rs` exports, manifest, guard, handle table · `config.rs` instance config and its one list of keys · `client.rs` connect options, the live probe, failure classes and their wording · `steps.rs` the step list, specs, dispatch index and `bddkit_validate_step` · `check.rs` the assertions over keys: plan, one read per attempt, the table comparisons · `ops.rs` one dispatch: plan, perform under `timeout_secs`, reply · `instance.rs` what a handle holds (lazy client, last operation) · `evidence.rs` the diagnostic, debug `output`, `scan`, base64 · `error.rs` `Class` and `StepError` · `runtime.rs` the tokio runtime · `reply.rs` reply shapes · `tests/exports.rs` config and probe exports against a real etcd · `tests/keys.rs` the key actions and `tests/checks.rs` the key assertions, through the exports · `tests/e2e.rs` the real `bddkit` binary · `tests/secure.rs`, `tests/token_refresh.rs` and `tests/e2e_secure.rs` TLS, mutual TLS and a password against `etcd-secure`, `etcd-mtls` and `etcd-auth` · `examples/` the green suite over two servers (`etcd.yaml`, `features/`) and `failing.yaml`, which fails on purpose · `tests/examples.rs` runs both through the real `bddkit` and checks the console, the event stream, the reports and the masking · `tests/certs/` the test-only certificates and `generate.sh` · `docker-compose.yml` a plain etcd, `etcd-b` (a second plain server on port 12379, for `examples/`), `etcd-secure` (TLS), `etcd-mtls` (a client certificate required) and `etcd-auth` (TLS, authentication switched on by the tests); `ETCD_VERSION` picks the release.

## Dev workflow

- Edition 2024, `rust-version` 1.97 (CI pins the toolchain). Dependency versions are not bumped without being asked; `etcd-client` is pinned exactly.
- Building needs `protoc` and the well-known types: `sudo apt-get install protobuf-compiler libprotobuf-dev`. `etcd-client`'s build script compiles `.proto` files.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must be clean.
- Code health limits, enforced in CI: a function has at most 100 lines as Clippy counts them, blocks nest at most 5 levels, and a Rust, `.feature` or YAML file (workflows aside) has at most 1,000 lines. The policy is `[lints.clippy]` in `Cargo.toml` and `clippy.toml`; the reasoning is bddkit/bddkit#200. An exception is `#[expect(..., reason = "...")]`, with a link to a tracking issue when it is a function over the limit.
- `cargo test --locked` runs everything. The live tests need an etcd: `docker compose up --wait etcd etcd-b` (`ETCD_VERSION=v3.5.36` for the other minor), or set `BDDKIT_TEST_ETCD` to another address; the secure tests need `docker compose up --wait etcd-secure etcd-mtls etcd-auth` (`BDDKIT_TEST_ETCD_SECURE`, `_MTLS` and `_AUTH` name other addresses). The certificates in `tests/certs` are test-only and committed; `tests/certs/generate.sh` makes new ones. One test clears the whole server (`I delete all etcd keys`): it runs only with `BDDKIT_TEST_ETCD_WIPE=1`, which CI sets; never set it against an etcd you care about. `tests/e2e.rs` also needs `BDDKIT_BIN`, a `bddkit` binary; without it the tests skip, except under `CI`.
- Everything in the repository is English: code, comments, error messages, documentation. Conversational replies follow the operator's language.
- Markdown: never soft-wrap prose; one paragraph or list item is one physical line.
- `CHANGELOG.md` (where present) is generated by release-plz from commit messages — never hand-edit it. Use Conventional Commits.
