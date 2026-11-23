# bddkit-etcd

A bddkit plugin that runs steps against any etcd v3 compatible server (keys and revisions today; watches, leases and transactions are planned) as a resource group.

It speaks the etcd v3 gRPC API, so it serves a team checking what its service wrote to etcd and a team building an etcd-compatible server alike. Tested against etcd 3.5 and 3.6.

## Install

```sh
bddkit plugin install bddkit/bddkit-etcd
```

Written against plugin ABI 4, so it needs a bddkit that loads that generation.

## Config

```yaml
resources:
  etcd:
    main:
      endpoints: http://127.0.0.1:2379
    secure:
      endpoints: https://etcd-1.internal:2379,https://etcd-2.internal:2379
      tls_ca: certs/ca.crt
      tls_cert: certs/client.crt
      tls_key: certs/client.key
      username: root
      password: ${ETCD_ROOT_PASSWORD}
default_etcd: main
```

| Key | Meaning |
|---|---|
| `endpoints` | required; one address or several separated by commas, `http://` or `https://` and not both. The scheme selects TLS |
| `username`, `password` | authenticate as this user; each needs the other. The password is masked in every output |
| `tls_ca` | CA certificate of the server; the system roots when omitted |
| `tls_cert`, `tls_key` | client certificate and key for mutual TLS; each needs the other |
| `tls_server_name` | the name to verify the server certificate against, when the endpoint is an IP address |
| `timeout_secs` | limit of connecting and of one request, default 5 |
| `watch_buffer_events` | events one watch keeps before it fails, default 10000; for the planned watch steps |
| `options` | `polling.timeout_secs` and `polling.interval_ms` for eventual assertions against this instance |

A certificate path is read from the working directory of `bddkit run`, not from the directory of the config: the paths above work when `bddkit run` starts in the project root.

No connection is opened at startup. The client is built at the first step that uses the instance, so a server that is down fails that step with class `db_connection` and not the whole run. `bddkit doctor --live` connects and prints the version and revision of every instance.

Any scenario reaches any instance: `I use "secure" etcd` switches the instance for the rest of the scenario, and the next scenario starts on the default again. The default is `default_etcd`, `--default etcd=<name>` or `BDDKIT_DEFAULT_ETCD`, so one suite runs against two servers unchanged:

```sh
bddkit run --config examples/etcd.yaml
bddkit run --config examples/etcd.yaml --default etcd=second
```

## Steps

`bddkit steps list etcd` prints them, in a project whose config loads the plugin. A value of several lines is a doc string.

| Step | Effect |
|---|---|
| `I put etcd key "<key>" with value "<value>"` / `with value:` | writes a key |
| `I get etcd key "<key>"` | reads a key; fails when it does not exist |
| `I delete etcd key "<key>"` | deletes a key |
| `I delete etcd keys with prefix "<prefix>"` | deletes every key under the prefix; an empty prefix is refused |
| `I delete all etcd keys` | clears the whole server, see Running in parallel |
| `etcd key "<key>" should exist` / `should not exist` | the key is there / is not |
| `etcd key "<key>" should have value "<value>"` / `value:` | the value is exactly this text |
| `etcd key "<key>" value should match "<regex>"` | the value matches the regular expression, not anchored |
| `etcd key "<key>" value should contain JSON:` / `should equal JSON:` | the JSON value contains / equals the doc string; matchers such as `@variableType` work |
| `etcd key "<key>" should have:` | a `field` / `value` table over `version`, `create_revision`, `mod_revision` and `lease` |
| `there should be <n> etcd keys with prefix "<prefix>"` | the server counts this many keys |
| `etcd keys with prefix "<prefix>" should be:` / `should contain:` | a `key` / `value` table equals the keys under the prefix, rows in key order / is contained in them, in any order |

Every assertion reads the server again on each attempt, so `I expect the next assertion to pass within 5 seconds` waits for a key that someone else writes.

Variables a step sets, readable as `<<...>>`:

- `etcd_revision` after every action: the revision of the server in the response.
- `etcd_deleted` after a delete: the number of keys deleted.
- `etcd_value`, `etcd_create_revision`, `etcd_mod_revision`, `etcd_version` and `etcd_lease` after `I get`.

A step that cannot publish one of its variables fails instead of leaving the old value: a value that is not UTF-8 fails `I get`, and so does a response with no header.

Lease IDs are decimal, in `etcd_lease` and in the `lease` field of a table, while `etcdctl` prints them in hex. `0` means the key has no lease.

`I am in debug mode` prints each operation. Under `bddkit run --events <file> --evidence` every operation is an evidence event with its request, its response header and the instance it ran on.

## Failures

The class is set where the error is born and never read back from its text. It shows in the console summary, in the `type` of a JUnit report and in the `kind` of the event stream:

| Class | Meaning |
|---|---|
| `db_connection` | the server was not reached: connect, transport, handshake, timeout |
| `db_statement` | the server answered with a status, a refused password included |
| `assertion` | the check did not hold, or `I get` found no key |
| `input` | the step cannot run as written, such as a prefix that a variable left empty |

A polled assertion that runs out keeps the class of its last attempt, so a server that stayed down is `db_connection` and not `assertion`.

The password never reaches an output: the console, the event stream, the evidence and the reports show `***` for it, wherever the text of a step or a request carries it. `bddkit run --reveal` shows real values. A token is never printed either: the client fetches it and keeps it private.

## Running in parallel

Feature files run at the same time against one server, and isolation is by key uniqueness, never by rollback.

- The revision belongs to the whole server, so another file moves it too. Compare with the `<<etcd_revision>>` of your own write, never with "it grew by one" (`examples/features/keys.feature`).
- A prefix shared by two files races. Build every key under `/<suite>/<<run_id>>/...`, so each run owns its keys.
- Clean up with exactly that prefix: `I delete etcd keys with prefix "/<suite>/<<run_id>>/"`.
- `I delete all etcd keys` clears the keys of other files too, and of other runs. Use it only in a `@serial(<name>)` chain, or on a server that is yours alone.

## Examples

`examples/` holds the suites this README quotes. CI runs them on etcd 3.5 and 3.6, so they do not rot. They need two plain servers, which `docker compose up --wait etcd etcd-b` starts:

```sh
bddkit run --config examples/etcd.yaml
bddkit run --config examples/etcd.yaml --default etcd=second
bddkit run --config examples/failing.yaml
```

`examples/failing.yaml` fails four scenarios on purpose (a wrong value, a missing key, a server that is down, refused credentials) and exits 1; `tests/examples.rs` checks what it prints.
