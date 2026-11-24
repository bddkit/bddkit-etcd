//! Opening a watch again after its stream broke. The task owns this loop, so
//! the backoff sleeps and the attempts end with the task: the scenario's end
//! aborts them.

use std::sync::Mutex;
use std::time::Duration;

use etcd_client::{Client, WatchOptions, WatchResponse, WatchStream};

use super::State;
use super::run::locked;
use crate::client::request_failure;
use crate::ops::no_response;

/// The wait before the first attempt, doubled after each failure up to
/// `LONGEST_WAIT`.
pub(crate) const FIRST_WAIT: Duration = Duration::from_millis(100);
pub(crate) const LONGEST_WAIT: Duration = Duration::from_secs(1);

/// What opening the same watch again takes.
pub(super) struct Resume {
    pub client: Client,
    pub key: String,
    pub prefix: bool,
    /// The time one attempt may take: a restarting node may take the
    /// connection and never answer.
    pub seconds: u64,
    pub tls: bool,
}

/// Opens the watch again, as often as it takes, and returns the new stream
/// with its first message: what the server answers decides whether the watch
/// resumed (`created`), ended for good or must be opened once more. Never
/// returns when the server stays away; the task is aborted instead.
pub(super) async fn reopen(
    resume: &Resume,
    state: &Mutex<State>,
    wait: &mut Duration,
) -> (
    WatchStream,
    Result<Option<WatchResponse>, etcd_client::Error>,
) {
    loop {
        tokio::time::sleep(*wait).await;
        *wait = (*wait * 2).min(LONGEST_WAIT);
        let start = locked(state).resume_from();
        let mut options = WatchOptions::new()
            .with_prev_key()
            .with_start_revision(start);
        if resume.prefix {
            options = options.with_prefix();
        }
        let attempt = async {
            let mut stream = resume
                .client
                .watch_client()
                .watch(resume.key.as_str(), Some(options))
                .await
                .map_err(|e| request_failure(&e, resume.tls))?;
            let first = stream.message().await;
            Ok::<_, crate::error::StepError>((stream, first))
        };
        let limit = Duration::from_secs(resume.seconds);
        let failure = match tokio::time::timeout(limit, attempt).await {
            Ok(Ok(opened)) => return opened,
            Ok(Err(failure)) => failure,
            Err(_) => no_response(resume.seconds),
        };
        locked(state).broke(
            failure.class,
            format!("the watch could not be opened again: {}", failure.message),
        );
    }
}
