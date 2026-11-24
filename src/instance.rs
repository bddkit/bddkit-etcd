//! What a handle holds: the config, the client built at the first dispatch,
//! and the last operation of the scenario.

use std::sync::{Mutex, MutexGuard, PoisonError};

use etcd_client::Client;
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::client;
use crate::config::Config;
use crate::error::StepError;
use crate::lease::Leases;
use crate::watch::Watches;

/// The last operation as the failure dump shows it.
#[derive(Clone, Default)]
pub struct Last {
    pub diagnostics: Vec<Value>,
    pub scan: Vec<Value>,
}

pub struct Instance {
    pub config: Config,
    /// Built at the first dispatch, never at init. A failed connect is not
    /// kept: the next step tries again.
    client: OnceCell<Client>,
    /// The same, without `require_leader`: for what a node that lost its
    /// leader must still answer.
    local: OnceCell<Client>,
    last: Mutex<Option<Last>>,
    /// The named watches of the scenario.
    pub watches: Watches,
    /// The named leases of the scenario.
    pub leases: Leases,
}

impl Instance {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            client: OnceCell::new(),
            local: OnceCell::new(),
            last: Mutex::new(None),
            watches: Watches::default(),
            leases: Leases::default(),
        }
    }

    pub async fn client(&self, require_leader: bool) -> Result<&Client, StepError> {
        let cell = if require_leader {
            &self.client
        } else {
            &self.local
        };
        cell.get_or_try_init(|| async {
            let options =
                client::step_options(&self.config, require_leader).map_err(StepError::input)?;
            Client::connect(&self.config.endpoints, Some(options))
                .await
                .map_err(|e| client::connect_failure(&e, self.config.tls))
        })
        .await
    }

    /// Nothing can panic under this lock (an assignment, a clone).
    fn last_slot(&self) -> MutexGuard<'_, Option<Last>> {
        self.last.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn remember(&self, last: Last) {
        *self.last_slot() = Some(last);
    }

    pub fn last(&self) -> Option<Last> {
        self.last_slot().clone()
    }

    /// The scenario boundary: forgets the last operation, ends every watch and
    /// keepalive, revokes the leases of the scenario, and opens no connection
    /// (a lease can only have been granted on the client that is already
    /// there). The client stays, it is not scenario state.
    pub async fn reset(&self) {
        *self.last_slot() = None;
        self.watches.stop_all().await;
        self.leases.reset(self.client.get()).await;
    }
}
