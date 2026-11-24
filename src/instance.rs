//! What a handle holds: the config, the clients of its login (the config's,
//! or the one `I authenticate` chose for the scenario) built at the first
//! dispatch, and the last operation of the scenario.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use etcd_client::{Client, ConnectOptions};
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::client;
use crate::config::Config;
use crate::error::StepError;
use crate::lease::Leases;
use crate::txn::Outcome;
use crate::watch::Watches;

/// The last operation as the failure dump shows it.
#[derive(Clone, Default)]
pub struct Last {
    pub diagnostics: Vec<Value>,
    pub scan: Vec<Value>,
}

/// The clients of one login. Built at the first dispatch, never at init; a
/// failed connect is not kept: the next step tries again.
#[derive(Default)]
struct Session {
    /// Who `I authenticate` logged in as; none is the config's own user.
    login: Option<(String, String)>,
    client: OnceCell<Client>,
    /// The same, without `require_leader`: for what a node that lost its
    /// leader must still answer.
    local: OnceCell<Client>,
}

pub struct Instance {
    pub config: Config,
    /// The config's own login. It is not scenario state: its clients stay.
    base: Arc<Session>,
    /// What steps use now: `base`, or the login of `I authenticate` until the
    /// scenario ends.
    current: Mutex<Arc<Session>>,
    last: Mutex<Option<Last>>,
    /// The named watches of the scenario.
    pub watches: Watches,
    /// The named leases of the scenario.
    pub leases: Leases,
    /// Whether the compares of the last transaction of the scenario held.
    pub txn: Outcome,
}

impl Instance {
    pub fn new(config: Config) -> Self {
        let base = Arc::new(Session::default());
        Self {
            config,
            current: Mutex::new(Arc::clone(&base)),
            base,
            last: Mutex::new(None),
            watches: Watches::default(),
            leases: Leases::default(),
            txn: Outcome::default(),
        }
    }

    fn session(&self) -> Arc<Session> {
        Arc::clone(&self.current.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// An owned client, so that a login in the middle of a scenario never
    /// pulls one out from under a step.
    pub async fn client(&self, require_leader: bool) -> Result<Client, StepError> {
        let session = self.session();
        let cell = if require_leader {
            &session.client
        } else {
            &session.local
        };
        cell.get_or_try_init(|| async {
            let options = self.options(session.login.as_ref(), require_leader)?;
            Client::connect(&self.config.endpoints, Some(options))
                .await
                .map_err(|e| client::connect_failure(&e, self.config.tls))
        })
        .await
        .cloned()
    }

    /// The connect options of a step's client: the config's, as `login` when
    /// there is one.
    pub fn options(
        &self,
        login: Option<&(String, String)>,
        require_leader: bool,
    ) -> Result<ConnectOptions, StepError> {
        let options =
            client::step_options(&self.config, require_leader).map_err(StepError::input)?;
        Ok(match login {
            Some((user, password)) => options.with_user(user, password),
            None => options,
        })
    }

    /// From now until the scenario ends every step goes through this login;
    /// `client` is the one that just connected with it.
    pub fn log_in(&self, login: (String, String), client: Client) {
        let session = Session {
            login: Some(login),
            client: OnceCell::new_with(Some(client)),
            local: OnceCell::new(),
        };
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Arc::new(session);
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
    /// there). The config's clients stay, they are not scenario state; a login
    /// of `I authenticate` is dropped. The leases are revoked through the
    /// config's client when it is built: the configured user is the one that
    /// may revoke whatever the scenario left (root, in the setups this is
    /// written for), while a login may be a read-only user. Limit: a
    /// configured user with fewer rights than the login cannot revoke the
    /// leases the login granted, and their keys stay until the TTL.
    pub async fn reset(&self) {
        *self.last_slot() = None;
        self.txn.forget();
        self.watches.stop_all().await;
        let session = self.session();
        self.leases
            .reset(self.base.client.get().or(session.client.get()))
            .await;
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&self.base);
    }
}
