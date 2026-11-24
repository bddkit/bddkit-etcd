# bddkit-etcd

A bddkit plugin that runs steps against any etcd v3 compatible server (keys, the nodes of a cluster, refusals, watches, leases and transactions) as a resource group.

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
| `watch_buffer_events` | events one watch keeps before it fails, default 10000; it caps the count, not the size, see [Watches](#watches) |
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

The steps of a cluster, of refusals, of watches, of leases and of transactions are in their own sections below: [A node of a cluster](#a-node-of-a-cluster), [Refusals](#refusals), [Watches](#watches), [Leases](#leases) and [Transactions](#transactions).

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

## Watches

A watch is opened by name and keeps every event it receives until the scenario ends. It belongs to the instance that was selected when it was opened, so the assertions on it need that instance selected, and the next scenario starts with no watch. `I watch …` returns once the server has confirmed the watch, so a write in the next step is in the stream (a watch `from revision` also waits up to 500 ms for the server to turn a compacted revision down), and the assertions only read what the watch has collected, never the server, so a polled assertion waits for an event.

| Step | Effect |
|---|---|
| `I watch etcd key "<key>" as "<name>"` / `I watch etcd keys with prefix "<prefix>" as "<name>"` | opens a watch on a key / on every key under a prefix; sets `etcd_watch_<name>_revision` to the revision of the confirmation |
| `… as "<name>" from revision <rev>` | the same, and first replays the writes from this revision on; fails the step when the server has compacted it, or the first assertion on the watch when the server turns it down later than 500 ms |
| `I cancel etcd watch "<name>"` | stops the watch; what it received stays readable until the scenario ends |
| `etcd watch "<name>" should have received a PUT of "<key>" with value "<value>"` / `a DELETE of "<key>"` | any event of the history is this one |
| `etcd watch "<name>" should have received <n> events` | the history holds exactly this many events: not yet while it holds fewer, a failure once it holds more |
| `etcd watch "<name>" should have received events:` | a table that equals the whole history, in order. The columns, in any order: `type` (`PUT` or `DELETE`), `key`, `value`, `prev_value`, `mod_revision`, `version`. The history may still be a beginning of the rows (not yet), and differs from them for good once it does not match |
| `etcd watch "<name>" should have been resumed` / `… should have been resumed <n> times` | the stream broke and a new one was confirmed, at least once / exactly this many times, a failure once there are more |

How to write a watch scenario that does not flake:

- The assertions never consume events: a retried attempt sees the same history plus the new events. Isolation comes from a unique key or prefix (`/<suite>/<<run_id>>/…`), as in [Running in parallel](#running-in-parallel).
- An empty key or prefix is refused (`input`), a literal one before the first request.
- `prev_value` is the value before the event, and is empty when the key had none, as for the first PUT of a key. An empty `prev_value` therefore does not tell a missing previous value from an empty one.
- "No event arrived" is a race on a live stream: the event may be on its way. Write a marker key into the watched prefix after the quiet period, and assert the exact list with the marker last. Events come in revision order, so everything written before the marker is already in the history when the marker is (`examples/features/watch.feature`).
- A put retried under polling (`etcd should accept a put …`) may write more than once, so assert such keys with `a PUT of …` and not with an exact list.
- A cancelled watch keeps its history and its name: the name stays taken until the scenario ends, and an assertion that misses on a cancelled watch is final, not a wait.
- `watch_buffer_events` caps the number of events a watch keeps, not their size, so a few huge values can still use a lot of memory.

What breaks a stream, and what the plugin does about it:

- A break is any stream error or close, and a cancel that is not for compaction or permission. A watch goes through `require_leader`: a node that lost the quorum ends the stream, and the plugin opens it again, with a backoff from 100 ms to 1 s, until the scenario ends. A watch that cannot be opened again is a watch that has not received more yet, so a polled assertion keeps waiting and fails with the class of the break, `db_connection` when the server stayed down.
- The new stream starts at the revision after the last event, so no write of the gap is lost. A watch that opened `from revision` and has received nothing resumes from that revision, and one that has received nothing and had no revision resumes from the revision of its confirmation plus one.
- `should have been resumed` is the proof that the break happened: without it a scenario can pass on a stream that never broke. A resume counts when the server confirmed the new stream.
- A terminal state ends the watch for good, and the next assertion on it fails with the reason: the server cancelled it because its revision was compacted or because the user may not read the key, or the watch holds `watch_buffer_events` events and another one arrived, or an event arrived out of order or twice (class `assertion`). Nothing is dropped silently.
- A watch that has been quiet for longer than the server's compaction interval can fail on resume: its resume point, the revision after its last event, may be compacted by the time the stream is opened again, and the watch then ends for good. Keep the server's compaction retention longer than the quiet time plus the outage you test.
- A gRPC `PermissionDenied` status of the stream (as opposed to a cancel) is retried forever, so a polled assertion runs out with class `db_statement`.
- `from revision 0` is refused, and a watch name is made of letters, digits and underscores, so that `etcd_watch_<name>_revision` is a variable.

## Leases

A lease is granted by name and keeps the name until the scenario ends. It belongs to the instance that was selected when it was granted, so the steps by name need that instance selected, and the next scenario starts with no lease: the reset revokes the scenario's leases on the way out, best effort (at most a second for each, errors ignored), so a lease whose server is already gone is simply left to expire. A name is used once per scenario.

| Step | Effect |
|---|---|
| `I grant etcd lease "<name>" with TTL <n> seconds` | grants a lease and sets `etcd_lease_<name>` to its ID; a TTL of 0 is refused |
| `I put etcd key "<key>" with value "<value>" under lease "<name>"` | writes a key attached to the lease; the key goes when the lease does |
| `I keep etcd lease "<name>" alive` | sends a keepalive every third of the granted TTL until the scenario ends, you stop it or you revoke the lease |
| `I stop keeping etcd lease "<name>" alive` | stops the keepalive; the lease then lives its TTL out |
| `I revoke etcd lease "<name>"` | revokes the lease and deletes the keys attached to it; the name stays known, so `should have expired` holds afterwards |
| `etcd lease "<name>" should be alive` / `should have expired` | the server still has the lease / no longer has it |
| `etcd lease with ID <id> should be alive` / `should have expired` | the same for a lease of any instance or scenario, by its decimal ID |
| `etcd lease "<name>" should hold <n> keys` | exactly this many keys are attached to the lease |

What the assertions mean:

- A lease is gone when the server answers a TTL below zero, or `NotFound` (some servers answer that for a lease they do not have; etcd answers a TTL of -1). It is not gone at a TTL of 0: etcd counts whole seconds down and truncates, so 0 means under a second left, and a keepalive still saves the lease. `should have expired` is not yet while the lease is alive, so a polled assertion waits for the expiry.
- `should be alive` asks the server on every attempt. A lease that is gone never comes back, so once it has been seen gone `should be alive` fails for good, with class `assertion`, and does not wait.
- A keepalive or a revoke of a name that you already revoked is `input`, and so is `I stop keeping` on a lease nobody keeps. `I revoke` on a lease that expired on its own is answered `NotFound` by the server and fails with class `db_statement`. A keepalive of a lease that the server has already dropped is an `assertion`, final like `should be alive`.
- A server raises a TTL below its minimum (about two seconds on a default etcd), so a short TTL may live longer than written. Wait for an expiry with a polled assertion, never for a number of seconds.
- A keepalive reconnects silently, with the backoff of a watch resume (100 ms to 1 s), and ends when the server answers that the lease is gone. Through an instance with one endpoint it cannot move: while that node is down the keepalive keeps trying to reopen the stream, and the lease expires if the node stays away longer than the TTL (the keepalive ends once the node is back and the server says the lease is gone); through an instance with all the endpoints it moves to another node, and a lease asked by ID through any node is still alive (`examples/cluster/lease.feature`).
- Lease IDs are decimal here, in `etcd_lease_<name>` and everywhere an ID is read, while `etcdctl` prints and reads them in hex. Convert before you compare with its output.

## Transactions

`I commit an etcd transaction:` takes a doc string in the format of `etcdctl txn`: compares, an empty line, the operations to run when they hold, an empty line, the operations to run when they do not. The compares and the failure branch are optional. One line break at the start of the doc string is removed, so a transaction with no compares starts with an empty first line, as in `examples/features/transaction.feature`.

```gherkin
When I commit an etcd transaction:
  """
  create("/locks/job") = "0"

  put "/locks/job" "alice"

  get "/locks/job"
  """
Then the etcd transaction should have succeeded
```

| Step | Effect |
|---|---|
| `I commit an etcd transaction:` | commits the doc string; sets `etcd_txn_succeeded` (`true` when the compares held) and `etcd_revision` |
| `the etcd transaction should have succeeded` / `should have failed` | the compares of the last transaction of the scenario on this instance held / did not hold; reads what the commit kept and sends no request |
| `etcd should refuse a transaction with "<code>":` | commits the doc string on every attempt and passes once the server refuses it with this gRPC code |

The part of `etcdctl txn` the plugin takes:

- Compares `value("<key>")`, `version`, `create`, `mod` and `lease`, each with `=`, `!=`, `<` or `>` and a value in double quotes; the short names `val`, `ver`, `c`, `m` and `l` work. A compare that is not true sends the failure branch and is not a failure of the step: assert it with `should have failed`, or read `etcd_txn_succeeded`.
- Operations `put <key> <value>` (with `--lease=<id>`), `get <key>` and `del <key>`, which take a `<range_end>` after the key or `--prefix`. A token with a space, a quote or an empty text is in double quotes, with `\"` and `\\` inside. Nothing else of `etcdctl` is read: no interactive mode, no `--from-key`, no `--rev`.
- What a `get` returned is in the evidence of the step, not in a variable.
- A mismatch of `should have succeeded` with the branch that ran is a final `assertion`, and a transaction the server refused fails the commit itself (`db_statement`).

What to know before writing one:

- A `<<slot>>` belongs inside double quotes. The doc string is read after the variables are put in, so a value becomes text of the transaction, and a value that holds a double quote, a backslash or a line break breaks the compare it was put in (the error names the line, never the value). A literal is checked before the first request, a slot by shape and again once it has a value.
- Lease IDs are decimal here too, `--lease=` included, while `etcdctl txn` reads them in hex: `put /k v --lease=<<etcd_lease_session>>` works, and an ID copied from `etcdctl` output usually fails to parse; a hex ID made only of digits is read as another lease.
- `del "" --prefix` is refused as an empty prefix (`input`), as in `I delete etcd keys with prefix`; `get "" --prefix` reads every key and is allowed.
- `etcd should refuse a transaction …` really applies a transaction the server accepts, as `etcd should refuse a put …` does. Under polling an accepted transaction is applied again on every attempt, so use it without a wait or with idempotent operations.

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

`examples/features/lease.feature` grants leases, puts keys under them, revokes one and lets another expire after `I stop keeping … alive`. `examples/features/transaction.feature` is the lock recipe on a transaction (`create("<key>") = "0"`: the first commit takes the lock, the second one finds it taken), a value from a variable in quotes and a compare that does not hold.

`examples/features/watch.feature` is the watch example: the whole history of a prefix with `<<etcd_revision>>` in the table, the marker recipe, a replay from a revision and a cancel.

`examples/failing.yaml` fails four scenarios on purpose (a wrong value, a missing key, a server that is down, refused credentials) and exits 1; `tests/examples.rs` checks what it prints.

`examples/cluster.yaml` runs a failover on three nodes that the [exec plugin](https://github.com/bddkit/bddkit-exec) starts and kills, and needs no server:

```sh
bddkit plugin install exec
bddkit run --config examples/cluster.yaml
```

It needs the `etcd` and `etcdctl` binaries of an etcd release on `PATH`, and the ports 25001 to 25003 and 25011 to 25013 free. `examples/cluster/failover.feature` is tagged `@serial(etcd-cluster)` and has two scenarios, each on a cluster of its own, and shares its chain with two files that follow:

- The leader is chosen with `etcdctl move-leader`, never by timing, and killed. The survivors elect another leader, which a polled assertion waits for, a write is accepted again, and the key written before the failover is still there.
- Two of the three nodes are killed. Writes are refused with `Unavailable`, and a serializable read of a key written earlier is still served.

`examples/cluster/lease.feature` has one scenario: a lease kept alive through an instance with all three endpoints (`all` in `examples/cluster.yaml`), the leader moved to node 1 and killed, twice the TTL waited out, and the lease still alive when asked by its ID through node 2.

`examples/cluster/watch.feature` has one scenario: a watch on node 1 alone, node 1 killed on purpose (a watch on a follower that stays in the quorum is not broken by a change of leader), a key written through node 2 while the watch has no stream, node 1 restarted under a new process name (a name is used once per scenario). Under polling the watch has received the key and `should have been resumed 1 times` passes.

A passing scenario ends by deleting the data directories of its nodes; a failed one leaves them in the run's workspace. A killed node gets no time to hand its leadership over (`stop_grace_secs: 0` in the config), so what the scenarios see is a node that is gone. CI runs the example on etcd 3.5 and 3.6, ten times in a row in each job.
