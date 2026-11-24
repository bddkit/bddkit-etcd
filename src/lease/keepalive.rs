//! The leases of one instance and the task that keeps one alive. The table is
//! scenario state: a name lives until `reset` (which revokes the leases) or
//! `stop_all` (the end of the instance, which only stops the tasks).

use std::collections::{BTreeMap, HashMap};
use std::error::Error as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper, LeaseTimeToLiveResponse};
use serde_json::{Value, json};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::request_failure;
use crate::error::StepError;
use crate::ops::no_response;
use crate::watch::{FIRST_WAIT, LONGEST_WAIT};

/// How long `reset` waits for the server to revoke one lease. The server
/// expires a lease that is not revoked anyway, so the wait is a courtesy.
const REVOKE_WAIT: Duration = Duration::from_secs(1);

/// Whether a task keeps a lease alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keep {
    No,
    Flowing,
    /// The keepalive stream is down and being opened again.
    Reconnecting,
}

/// What the steps and the failure dump know about one lease.
#[derive(Debug)]
pub struct LeaseState {
    pub id: i64,
    pub granted_ttl: i64,
    pub keep: Keep,
    /// The last break of the keepalive stream, kept after it flowed again.
    pub last_error: Option<String>,
    pub revoked: bool,
    /// The server said the lease is gone.
    pub expired: bool,
}

impl LeaseState {
    pub fn new(id: i64, granted_ttl: i64) -> Self {
        Self {
            id,
            granted_ttl,
            keep: Keep::No,
            last_error: None,
            revoked: false,
            expired: false,
        }
    }

    /// The stream answered: a lease that is kept alive again flows, and one
    /// that no task keeps stays as it is.
    fn flowing(&mut self) {
        if self.keep == Keep::Reconnecting {
            self.keep = Keep::Flowing;
        }
    }

    pub fn kept(&self) -> bool {
        self.keep != Keep::No
    }

    fn broke(&mut self, why: String) {
        self.keep = Keep::Reconnecting;
        self.last_error = Some(why);
    }

    fn describe(&self) -> Value {
        json!({
            "id": self.id,
            "granted_ttl": self.granted_ttl,
            "kept_alive": self.keep == Keep::Flowing,
            "reconnecting": self.keep == Keep::Reconnecting,
            "last_error": self.last_error,
            "revoked": self.revoked,
            "seen_expired": self.expired,
        })
    }
}

/// Nothing can panic under these locks (an assignment, a clone).
pub(super) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The TTL of a `LeaseTimeToLive` answer that says the lease is gone: etcd
/// counts whole seconds down, so 0 is a lease with under a second left.
pub(super) fn ttl_gone(ttl: i64) -> bool {
    ttl < 0
}

/// Whether an answer to `LeaseTimeToLive` says the lease is gone: etcd answers
/// with a TTL of -1, other servers refuse with `NotFound`. A TTL of 0 is a
/// lease with under a second left, which a keepalive still saves. A refusal
/// that never left the client says nothing.
pub(super) fn gone(answer: &Result<LeaseTimeToLiveResponse, etcd_client::Error>) -> bool {
    match answer {
        Ok(response) => ttl_gone(response.ttl()),
        Err(etcd_client::Error::GRpcStatus(status)) => {
            status.code() == tonic::Code::NotFound && status.source().is_none()
        }
        Err(_) => false,
    }
}

/// Asks the server whether the lease is gone; a failed question is "no".
pub(super) async fn is_gone(client: &Client, id: i64, seconds: u64) -> bool {
    let asked = tokio::time::timeout(
        Duration::from_secs(seconds),
        client.lease_client().time_to_live(id, None),
    )
    .await;
    asked.is_ok_and(|answer| gone(&answer))
}

/// A keepalive every third of the TTL the server granted: two may be lost
/// before the lease is.
pub(super) fn interval(granted_ttl: i64) -> Duration {
    let millis = u64::try_from(granted_ttl).unwrap_or(1).max(1) * 1000 / 3;
    Duration::from_millis(millis.max(1))
}

/// What the task needs to open the stream again.
pub(super) struct Keeper {
    pub client: Client,
    pub id: i64,
    pub interval: Duration,
    /// The time one request may take.
    pub seconds: u64,
    pub tls: bool,
}

type Opened = (LeaseKeeper, LeaseKeepAliveStream);

enum Over {
    Expired,
    Broke(String),
}

/// Counts a running task, and when the task ends for any reason (an abort
/// included) says that nothing keeps the lease any more.
struct Alive {
    running: Arc<AtomicUsize>,
    state: Arc<Mutex<LeaseState>>,
}

impl Drop for Alive {
    fn drop(&mut self) {
        let mut state = locked(&self.state);
        state.keep = Keep::No;
        drop(state);
        self.running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Sends a keepalive and reads its answer, until the lease is gone or the
/// stream breaks.
async fn beats(opened: &mut Opened, keeper: &Keeper, state: &Mutex<LeaseState>) -> Over {
    let (sender, stream) = opened;
    let limit = Duration::from_secs(keeper.seconds);
    loop {
        tokio::time::sleep(keeper.interval).await;
        if let Err(error) = sender.keep_alive().await {
            return Over::Broke(format!(
                "the keepalive could not be sent: {}",
                request_failure(&error, keeper.tls).message
            ));
        }
        match tokio::time::timeout(limit, stream.message()).await {
            Ok(Ok(Some(answer))) if answer.ttl() <= 0 => return Over::Expired,
            Ok(Ok(Some(_))) => {
                locked(state).flowing();
            }
            Ok(Ok(None)) => {
                return Over::Broke("the server closed the keepalive stream".to_string());
            }
            Ok(Err(error)) => {
                return Over::Broke(format!(
                    "the keepalive stream broke: {}",
                    request_failure(&error, keeper.tls).message
                ));
            }
            Err(_) => return Over::Broke(no_response(keeper.seconds).message),
        }
    }
}

/// Opens the stream again, as often as it takes, with the backoff of a watch
/// resume. `None` when the server says the lease is gone; never returns while
/// the server stays away, the task is aborted instead.
async fn reopen(keeper: &Keeper, state: &Mutex<LeaseState>) -> Option<Opened> {
    let mut wait = FIRST_WAIT;
    loop {
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(LONGEST_WAIT);
        let attempt = tokio::time::timeout(
            Duration::from_secs(keeper.seconds),
            keeper.client.lease_client().keep_alive(keeper.id),
        )
        .await;
        let why = match attempt {
            Ok(Ok(opened)) => {
                locked(state).flowing();
                return Some(opened);
            }
            Ok(Err(error)) => request_failure(&error, keeper.tls).message,
            Err(_) => no_response(keeper.seconds).message,
        };
        // The refusal of a lease that is gone is told apart from one of a
        // server that is down by asking, never by the text of the refusal.
        if is_gone(&keeper.client, keeper.id, keeper.seconds).await {
            return None;
        }
        locked(state).broke(format!("the keepalive could not be opened again: {why}"));
    }
}

async fn follow(first: Opened, state: Arc<Mutex<LeaseState>>, keeper: Keeper, _alive: Alive) {
    let mut open = Some(first);
    loop {
        let mut opened = match open.take() {
            Some(opened) => opened,
            None => match reopen(&keeper, &state).await {
                Some(opened) => opened,
                None => break,
            },
        };
        match beats(&mut opened, &keeper, &state).await {
            Over::Expired => break,
            Over::Broke(why) => locked(&state).broke(why),
        }
    }
    locked(&state).expired = true;
}

struct Entry {
    state: Arc<Mutex<LeaseState>>,
    task: Option<JoinHandle<()>>,
}

/// Dropping a `JoinHandle` detaches the task, so an entry that is dropped
/// without a stop still ends it.
impl Drop for Entry {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn stop(task: JoinHandle<()>) {
    task.abort();
    // An aborted task answers at once; the wait is for its drop to have run.
    let _ = task.await;
}

#[derive(Default)]
pub struct Leases {
    map: Mutex<HashMap<String, Entry>>,
    running: Arc<AtomicUsize>,
}

impl Leases {
    fn map(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        locked(&self.map)
    }

    /// How many keepalive tasks are alive, for the tests.
    pub fn running(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.running)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.map().contains_key(name)
    }

    pub fn add(&self, name: &str, state: LeaseState) {
        self.map().insert(
            name.to_string(),
            Entry {
                state: Arc::new(Mutex::new(state)),
                task: None,
            },
        );
    }

    pub fn state(&self, name: &str) -> Result<Arc<Mutex<LeaseState>>, StepError> {
        self.map()
            .get(name)
            .map(|entry| Arc::clone(&entry.state))
            .ok_or_else(|| {
                StepError::input(format!(
                    "this instance has no lease \"{name}\"; grant it with \"I grant etcd lease \\\"{name}\\\" with TTL ... seconds\" in the same scenario, on the same instance"
                ))
            })
    }

    /// Starts the task that keeps the lease alive, on a stream that is open.
    pub(super) fn start(&self, name: &str, keeper: Keeper, opened: Opened) {
        let mut map = self.map();
        let Some(entry) = map.get_mut(name) else {
            return;
        };
        locked(&entry.state).keep = Keep::Flowing;
        self.running.fetch_add(1, Ordering::SeqCst);
        let alive = Alive {
            running: Arc::clone(&self.running),
            state: Arc::clone(&entry.state),
        };
        entry.task = Some(tokio::spawn(follow(
            opened,
            Arc::clone(&entry.state),
            keeper,
            alive,
        )));
    }

    /// Ends the keepalive of a lease, and waits until the task is gone.
    pub async fn stop_keeping(&self, name: &str) {
        let task = self.map().get_mut(name).and_then(|entry| entry.task.take());
        if let Some(task) = task {
            stop(task).await;
        }
    }

    /// The end of the instance: no task outlives it. The leases are left to
    /// expire.
    pub async fn stop_all(&self) {
        let entries: Vec<Entry> = self.map().drain().map(|(_, entry)| entry).collect();
        for mut entry in entries {
            if let Some(task) = entry.task.take() {
                stop(task).await;
            }
        }
    }

    /// The scenario boundary. Every keepalive is stopped without waiting on
    /// the network, then every lease that is still alive is revoked, all at
    /// once, each for at most a second, ignoring every error. `client` is the
    /// one the steps already built: a reset opens no connection, and without
    /// a client nothing was granted.
    pub async fn reset(&self, client: Option<&Client>) {
        let entries: Vec<Entry> = self.map().drain().map(|(_, entry)| entry).collect();
        let mut ids = Vec::new();
        for mut entry in entries {
            if let Some(task) = entry.task.take() {
                stop(task).await;
            }
            let state = locked(&entry.state);
            if !state.revoked && !state.expired {
                ids.push(state.id);
            }
        }
        let Some(client) = client else {
            return;
        };
        let mut revoking = JoinSet::new();
        for id in ids {
            let mut leases = client.lease_client();
            revoking.spawn(async move {
                let _ = tokio::time::timeout(REVOKE_WAIT, leases.revoke(id)).await;
            });
        }
        while revoking.join_next().await.is_some() {}
    }

    /// What the failure dump adds: the state of every lease.
    pub fn context(&self) -> Vec<Value> {
        let map = self.map();
        let sorted: BTreeMap<_, _> = map.iter().collect();
        sorted
            .into_iter()
            .map(|(name, entry)| {
                let mut content = locked(&entry.state).describe();
                content["name"] = json!(name);
                json!({
                    "title": format!("etcd lease {name}"),
                    "kind": "json",
                    "content": serde_json::to_string_pretty(&content).unwrap_or_else(|_| content.to_string()),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_keepalive_goes_out_every_third_of_the_granted_ttl_and_never_in_a_busy_loop() {
        assert_eq!(interval(3), Duration::from_secs(1));
        assert_eq!(interval(1), Duration::from_millis(333));
        assert_eq!(interval(2), Duration::from_millis(666));
        assert_eq!(interval(60), Duration::from_secs(20));
        // A server that grants nothing, or a negative number, still waits.
        assert_eq!(interval(0), Duration::from_millis(333));
        assert_eq!(interval(-5), Duration::from_millis(333));
    }

    #[test]
    fn a_ttl_of_zero_is_a_lease_with_under_a_second_left_and_only_below_zero_is_gone() {
        assert!(!ttl_gone(5));
        assert!(!ttl_gone(0));
        assert!(ttl_gone(-1));
    }

    #[test]
    fn a_refusal_that_never_left_the_client_does_not_say_the_lease_is_gone() {
        let from_server = Err(etcd_client::Error::GRpcStatus(tonic::Status::new(
            tonic::Code::NotFound,
            "etcdserver: requested lease not found",
        )));
        assert!(gone(&from_server));
        let mut status = tonic::Status::new(tonic::Code::NotFound, "not found");
        status.set_source(Arc::new(std::fmt::Error));
        assert!(!gone(&Err(etcd_client::Error::GRpcStatus(status))));
        let unavailable = Err(etcd_client::Error::GRpcStatus(tonic::Status::new(
            tonic::Code::Unavailable,
            "no leader",
        )));
        assert!(!gone(&unavailable));
    }

    #[test]
    fn the_dump_says_kept_or_reconnecting_never_both_and_keeps_the_last_error() {
        let mut state = LeaseState::new(7, 5);
        assert_eq!(state.describe()["kept_alive"], false);
        state.keep = Keep::Flowing;
        assert_eq!(state.describe()["kept_alive"], true);
        state.broke("socket closed".to_string());
        let shown = state.describe();
        assert_eq!(shown["kept_alive"], false);
        assert_eq!(shown["reconnecting"], true);
        assert_eq!(shown["last_error"], "socket closed");
        state.flowing();
        assert_eq!(shown["kept_alive"], false);
        assert_eq!(state.describe()["kept_alive"], true);
        assert_eq!(state.describe()["last_error"], "socket closed");
    }

    #[test]
    fn the_context_lists_every_lease_by_name_in_order() {
        let leases = Leases::default();
        leases.add("b", LeaseState::new(2, 5));
        leases.add("a", LeaseState::new(1, 5));
        let titles: Vec<_> = leases
            .context()
            .iter()
            .map(|d| d["title"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(titles, ["etcd lease a", "etcd lease b"]);
    }
}
