//! The task that reads a watch, the table of the watches of one instance, and
//! the performing of the watch steps.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use etcd_client::{WatchOptions, WatchStream};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::resume::{FIRST_WAIT, Resume, reopen};
use super::{Create, Ending, Next, REPLAY_GRACE, State, Watch, cancel_reason};
use crate::client::request_failure;
use crate::error::{Class, StepError};
use crate::evidence::Record;
use crate::instance::Instance;
use crate::ops::{Vars, header, no_response};

// ---- the task and the table of watches ----

/// Counts a running task, and when the task ends for any reason (an abort
/// included) leaves a reason behind if it left none: a task that died
/// without saying why must not read as a watch that is merely quiet.
struct Alive {
    running: Arc<AtomicUsize>,
    state: Arc<Mutex<State>>,
}

impl Alive {
    fn new(running: &Arc<AtomicUsize>, state: &Arc<Mutex<State>>) -> Self {
        running.fetch_add(1, Ordering::SeqCst);
        Self {
            running: Arc::clone(running),
            state: Arc::clone(state),
        }
    }
}

impl Drop for Alive {
    fn drop(&mut self) {
        let mut state = locked(&self.state);
        if !state.stopped() {
            state.fail(Class::Assertion, "the watch task stopped without a reason");
        }
        drop(state);
        self.running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Nothing can panic under these locks (a push, an assignment).
pub(super) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Reads the stream into the state. A stream that breaks is opened again
/// until the watch ends for good (or the task is aborted); `stream` is `None`
/// when it is already over.
async fn follow(
    mut stream: Option<WatchStream>,
    state: Arc<Mutex<State>>,
    resume: Resume,
    _alive: Alive,
) {
    let mut wait = FIRST_WAIT;
    loop {
        let (open, message) = match stream.take() {
            Some(mut open) => {
                let message = open.message().await;
                (open, message)
            }
            None => reopen(&resume, &state, &mut wait).await,
        };
        let mut state = locked(&state);
        match state.apply(message, resume.tls) {
            Next::Read => {
                stream = Some(open);
                // A stream that flows again starts the next break from the
                // short wait.
                if state.broken.is_none() {
                    wait = FIRST_WAIT;
                }
            }
            Next::Reopen => {}
            Next::Stop => return,
        }
    }
}

struct Entry {
    state: Arc<Mutex<State>>,
    /// Taken by a cancellation.
    task: Option<JoinHandle<()>>,
}

/// Dropping a `JoinHandle` detaches the task, so an entry that is dropped
/// without `stop` (a watch opened while its instance was being dropped) still
/// ends it.
impl Drop for Entry {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[derive(Default)]
pub struct Watches {
    map: Mutex<HashMap<String, Entry>>,
    running: Arc<AtomicUsize>,
}

fn unknown(name: &str) -> StepError {
    StepError::input(format!(
        "this instance has no watch \"{name}\"; open it with \"I watch etcd key ... as \\\"{name}\\\"\" in the same scenario, on the same instance"
    ))
}

async fn stop(task: JoinHandle<()>) {
    task.abort();
    // An aborted task answers at once; the wait is for its drop to have run.
    let _ = task.await;
}

impl Watches {
    fn map(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        locked(&self.map)
    }

    /// How many watch tasks are alive, for the tests.
    pub fn running(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.running)
    }

    pub(super) fn state(&self, name: &str) -> Result<Arc<Mutex<State>>, StepError> {
        self.map()
            .get(name)
            .map(|entry| Arc::clone(&entry.state))
            .ok_or_else(|| unknown(name))
    }

    async fn cancel(&self, name: &str) -> Result<(), StepError> {
        let (state, task) = {
            let mut map = self.map();
            let entry = map.get_mut(name).ok_or_else(|| unknown(name))?;
            (Arc::clone(&entry.state), entry.task.take())
        };
        let Some(task) = task else {
            return Err(StepError::input(format!(
                "watch \"{name}\" is already cancelled"
            )));
        };
        {
            let mut state = locked(&state);
            if state.ending.is_none() {
                state.ending = Some(Ending::Cancelled);
            }
        }
        stop(task).await;
        Ok(())
    }

    /// The scenario boundary and the end of the instance: no task outlives it.
    pub async fn stop_all(&self) {
        let entries: Vec<Entry> = self.map().drain().map(|(_, entry)| entry).collect();
        for task in entries
            .into_iter()
            .filter_map(|mut entry| entry.task.take())
        {
            stop(task).await;
        }
    }

    /// What the failure dump adds: the state of every watch.
    pub fn context(&self) -> (Vec<Value>, Vec<Value>) {
        let map = self.map();
        let sorted: BTreeMap<_, _> = map.iter().collect();
        let (mut diagnostics, mut scan) = (Vec::new(), Vec::new());
        for (name, entry) in sorted {
            let state = locked(&entry.state);
            let content = state.describe();
            diagnostics.push(json!({
                "title": format!("etcd watch {name}"),
                "kind": "json",
                "content": serde_json::to_string_pretty(&content).unwrap_or_else(|_| content.to_string()),
            }));
            scan.extend(state.scan());
        }
        (diagnostics, scan)
    }
}

// ---- performing ----

pub async fn perform(
    instance: &Instance,
    watch: &Watch,
    record: &mut Record,
) -> Result<Vars, StepError> {
    match watch {
        Watch::Create(create) => open(instance, create, record).await,
        Watch::Cancel(name) => {
            instance.watches.cancel(name).await?;
            Ok(Vars::new())
        }
        Watch::Check { name, expect } => {
            let state = instance.watches.state(name)?;
            let state = locked(&state);
            state.judge(name, expect, record).map(|()| Vars::new())
        }
    }
}

/// Sends the request, waits for the server to confirm it, and only then
/// starts the task: a write in the next step is in the stream.
async fn open(
    instance: &Instance,
    create: &Create,
    record: &mut Record,
) -> Result<Vars, StepError> {
    if instance.watches.map().contains_key(&create.name) {
        return Err(StepError::input(format!(
            "this instance already has a watch \"{}\"; a name is taken until the scenario ends, even by a cancelled watch",
            create.name
        )));
    }
    let seconds = instance.config.timeout_secs;
    let limit = Duration::from_secs(seconds);
    let tls = instance.config.tls;
    let client = tokio::time::timeout(limit, instance.client(true))
        .await
        .map_err(|_| no_response(seconds))??;
    let mut options = WatchOptions::new().with_prev_key();
    if create.prefix {
        options = options.with_prefix();
    }
    if let Some(from) = create.from {
        options = options.with_start_revision(from);
    }
    let mut watcher = client.watch_client();
    let opened = async {
        let mut stream = watcher
            .watch(create.key.as_str(), Some(options))
            .await
            .map_err(|e| request_failure(&e, tls))?;
        let first = stream
            .message()
            .await
            .map_err(|e| request_failure(&e, tls))?;
        Ok::<_, StepError>((stream, first))
    };
    let (mut stream, first) = tokio::time::timeout(limit, opened)
        .await
        .map_err(|_| no_response(seconds))??;
    let Some(first) = first else {
        return Err(StepError::new(
            Class::DbConnection,
            "the server closed the watch stream before it confirmed the watch",
        ));
    };
    record.answered(first.header());
    if first.canceled() {
        return Err(StepError::new(
            Class::DbStatement,
            format!("the server cancelled the watch: {}", cancel_reason(&first)),
        ));
    }
    if !first.created() {
        return Err(StepError::new(
            Class::DbStatement,
            "the server's first answer on the watch is not a confirmation",
        ));
    }
    let revision = header(first.header())?.revision();
    let target = format!(
        "{} \"{}\"",
        if create.prefix { "prefix" } else { "key" },
        create.key
    );
    let mut state = State::new(
        target,
        usize::try_from(instance.config.watch_buffer_events).unwrap_or(usize::MAX),
        revision,
        create.from,
    );
    let mut next = Next::Read;
    if create.from.is_some()
        && let Ok(message) = tokio::time::timeout(REPLAY_GRACE, stream.message()).await
    {
        next = state.apply(message, tls);
    }
    if let Some(failure) = state.failure() {
        return Err(StepError::new(failure.class, failure.message.clone()));
    }
    let state = Arc::new(Mutex::new(state));
    let alive = Alive::new(&instance.watches.running, &state);
    let resume = Resume {
        client: client.clone(),
        key: create.key.clone(),
        prefix: create.prefix,
        seconds,
        tls,
    };
    let stream = (next != Next::Reopen).then_some(stream);
    let task = tokio::spawn(follow(stream, Arc::clone(&state), resume, alive));
    instance.watches.map().insert(
        create.name.clone(),
        Entry {
            state,
            task: Some(task),
        },
    );
    Ok(Vars::from([(
        format!("etcd_watch_{}_revision", create.name),
        revision.to_string(),
    )]))
}
