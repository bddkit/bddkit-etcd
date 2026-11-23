# bddkit-etcd

A bddkit plugin that runs steps against any etcd v3 compatible server (keys, the nodes of a cluster and refusals today; watches, leases and transactions are planned) as a resource group.

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

The steps of a cluster and of refusals are in their own sections below: [A node of a cluster](#a-node-of-a-cluster) and [Refusals](#refusals).

Variables a step sets, readable as `<<...>>`:

- `etcd_revision` after every action: the revision of the server in the response.
- `etcd_deleted` after a delete: the number of keys deleted.
- `etcd_value`, `etcd_create_revision`, `etcd_mod_revision`, `etcd_version` and `etcd_lease` after `I get`.

A step that cannot publish one of its variables fails instead of leaving the old value: a value that is not UTF-8 fails `I get`, and so does a response with no header.

Lease IDs are decimal, in `etcd_lease` and in the `lease` field of a table, while `etcdctl` prints them in hex. `0` means the key has no lease.

`I am in debug mode` prints each operation. Under `bddkit run --events <file> --evidence` every operation is an evidence event with its request, its response header and the instance it ran on.

## A node of a cluster

These steps look at one node, so they need an instance with exactly one endpoint: a client over several endpoints balances and would answer for whichever node it picked, so a node step on such an instance fails with class `input`. Declare one instance per node, as `examples/cluster.yaml` does, and switch with `I use "<name>" etcd`.

| Step | Effect |
|---|---|
| `etcd node should be the leader` / `should not be the leader` | the node leads the cluster / does not |
| `etcd node should have reached revision <rev>` | the revision the node has applied is at least this |
| `I compute etcd hash at revision <rev> as "<name>"` | stores the `HashKV` hash of the node at the revision in the variable |
| `etcd hash at revision <rev> should be "<hash>"` | the `HashKV` hash of the node at the revision is this decimal number |

The one-endpoint rule is for these five. `I get etcd key "<key>" serializable` reads from the node that answers, without asking the leader, so the answer may be stale, and sets the variables of `I get`; it works on any instance. All six run on a client that does not require a leader, so a node that lost its quorum still answers: `etcd node should be the leader` fails with "the cluster has no leader" and a serializable read is served. Behind them:

- `etcd node should have reached revision` reads the node's `Status`, which needs no permission on any key, so it works for a user that can read nothing.
- Revision 0 is refused in the hash steps (`input`): etcd reads `HashKV(0)` as "the current revision", and that hash changes with every write. Name the revision a write returned in `<<etcd_revision>>`.
- A hash comparison is meaningful only if no compaction came between the two hashes: a compaction changes the hash of an older revision. A hash that differs at a revision the node has reached is final and fails at once; a node that has not reached the revision yet is `not_yet`, so a polled assertion waits for it. The evidence of `etcd hash at revision … should be` carries the `compact_revision` of the hash it read.
- Under password authentication `HashKV` needs the `root` role. Connecting also runs `Authenticate`, which goes through raft, so the plugin's client cannot be built on a node that has no leader when the instance has a password: such an instance cannot build its client on a leaderless node.
- Some etcd-compatible servers send a synthetic response header, so on them `should not be the leader` says nothing about a leader elsewhere.

## Refusals

`etcd should accept …` and `etcd should refuse … with "<code>"` perform the operation themselves on every attempt, so a polled assertion waits for the moment a server starts or stops refusing, such as a quorum lost or regained. A refusal is only what the server itself answers, by its gRPC code in the spelling of `tonic` (`Unavailable`, `PermissionDenied`, ...): a connection that failed or a timeout on the client's side is not a refusal. It fails with class `db_connection`, or is asked again under polling.

| Step | Effect |
|---|---|
| `etcd should accept a put of key "<key>" with value "<value>"` | the put succeeds |
| `etcd should refuse a put of key "<key>" with "<code>"` | the put is refused with this code |
| `etcd should accept a read of key "<key>"` / `… serializable` | the read is answered; a key that does not exist is an answer |
| `etcd should refuse a read of key "<key>" with "<code>"` / `… serializable with "<code>"` | the read is refused with this code |
| `etcd should refuse a compaction up to revision <rev> with "<code>"` | the compaction is refused with this code |

What an attempt leaves behind:

- A put under polling may write more than once, and an attempt that timed out may still have been applied.
- `etcd should refuse a put …` writes an empty value when the server accepts it: the step then fails, and the key is changed.
- `etcd should refuse a compaction …` really compacts when the server accepts it, and a compaction cannot be undone. Files with such steps belong in a `@serial(<name>)` chain.
- A failover file breaks the cluster it runs on, and a `@serial` chain orders only the files that carry its tag, so every file that uses that cluster belongs in the same `@serial(<name>)` chain.

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

`examples/` holds the suites this README quotes. CI runs them on etcd 3.5 and 3.6, so they do not rot. Most need two plain servers, which `docker compose up --wait etcd etcd-b` starts:

```sh
bddkit run --config examples/etcd.yaml
bddkit run --config examples/etcd.yaml --default etcd=second
bddkit run --config examples/failing.yaml
```

`examples/failing.yaml` fails four scenarios on purpose (a wrong value, a missing key, a server that is down, refused credentials) and exits 1; `tests/examples.rs` checks what it prints.

`examples/cluster.yaml` runs a failover on three nodes that the [exec plugin](https://github.com/bddkit/bddkit-exec) starts and kills, and needs no server:

```sh
bddkit plugin install exec
bddkit run --config examples/cluster.yaml
```

It needs the `etcd` and `etcdctl` binaries of an etcd release on `PATH`, and the ports 25001 to 25003 and 25011 to 25013 free. `examples/cluster/failover.feature` is tagged `@serial(etcd-cluster)` and has two scenarios, each on a cluster of its own:

- The leader is chosen with `etcdctl move-leader`, never by timing, and killed. The survivors elect another leader, which a polled assertion waits for, a write is accepted again, and the key written before the failover is still there.
- Two of the three nodes are killed. Writes are refused with `Unavailable`, and a serializable read of a key written earlier is still served.

A passing scenario ends by deleting the data directories of its nodes; a failed one leaves them in the run's workspace. A killed node gets no time to hand its leadership over (`stop_grace_secs: 0` in the config), so what the scenarios see is a node that is gone. CI runs the example on etcd 3.5 and 3.6, ten times in a row in each job.
