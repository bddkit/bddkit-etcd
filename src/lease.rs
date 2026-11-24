//! Leases: a scenario grants a named lease, keeps it alive or lets it go,
//! attaches keys to it and asserts that it is alive, that it expired and how
//! many keys it holds. A name belongs to the instance that granted it and
//! lives until the end of the scenario; the IDs are decimal everywhere.
//!
//! Alive and expired both come from `LeaseTimeToLive`: expired is a TTL below
//! zero (etcd answers -1 for a lease it no longer has) or a `NotFound`
//! refusal (servers that answer so), alive is anything else. A TTL of zero is
//! alive: etcd counts whole seconds down, so it means "under a second left",
//! and a keepalive at that moment still saves the lease. A lease that is gone
//! never comes back, so `should be alive` on it is final.

use etcd_client::{Client, LeaseTimeToLiveOptions, PutOptions};
use serde_json::json;

use crate::check::{is_slot, miss};
use crate::client::request_failure;
use crate::error::StepError;
use crate::evidence::Record;
use crate::instance::Instance;
use crate::node::variable;
use crate::ops::{Request, Vars, header, revision_vars};
use crate::steps::Step;

mod keepalive;

pub use keepalive::Leases;
use keepalive::{Keeper, LeaseState, gone, interval, is_gone, locked};

/// What an assertion reads: a lease of the scenario by name, or any lease by
/// the ID the server gave it.
#[derive(Debug)]
pub enum Target {
    Name(String),
    Id(i64),
}

#[derive(Debug)]
pub enum Lease {
    Grant {
        name: String,
        ttl: i64,
    },
    Keep(String),
    Stop(String),
    Revoke(String),
    Put {
        key: String,
        value: String,
        name: String,
    },
    Alive(Target),
    Expired(Target),
    Holds {
        name: String,
        keys: u64,
    },
}

/// A TTL a lease can be granted with: the server reads 0 as "pick one".
fn ttl(text: &str) -> Result<i64, StepError> {
    match text.parse::<u64>().ok().and_then(|n| i64::try_from(n).ok()) {
        Some(0) => Err(StepError::input(
            "a lease needs a TTL of at least 1 second; 0 would let the server pick one",
        )),
        Some(seconds) => Ok(seconds),
        None => Err(StepError::input(format!(
            "\"{text}\" is not a TTL in seconds"
        ))),
    }
}

/// A lease ID: the decimal number `I grant` stores, as the server holds it
/// (signed 64 bits). The hexadecimal `etcdctl` prints is not accepted.
fn lease_id(text: &str) -> Result<i64, StepError> {
    text.parse::<u64>()
        .ok()
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| {
            StepError::input(format!(
                "\"{text}\" is not a lease ID: it is the decimal number \"I grant etcd lease\" stores, at most {}",
                i64::MAX
            ))
        })
}

fn keys(text: &str) -> Result<u64, StepError> {
    text.parse()
        .map_err(|_| StepError::input(format!("\"{text}\" is not a count")))
}

/// Everything that can be refused without a request, refused here.
pub fn plan(step: Step, request: &Request) -> Result<Lease, StepError> {
    let name = |index: usize| variable(request.arg(index)?);
    Ok(match step {
        Step::LeaseGrant => Lease::Grant {
            name: name(0)?,
            ttl: ttl(request.arg(1)?)?,
        },
        Step::LeaseKeep => Lease::Keep(name(0)?),
        Step::LeaseStop => Lease::Stop(name(0)?),
        Step::LeaseRevoke => Lease::Revoke(name(0)?),
        Step::PutLeased => Lease::Put {
            key: request.arg(0)?.to_string(),
            value: request.arg(1)?.to_string(),
            name: name(2)?,
        },
        Step::LeaseAlive => Lease::Alive(Target::Name(name(0)?)),
        Step::LeaseExpired => Lease::Expired(Target::Name(name(0)?)),
        Step::LeaseAliveId => Lease::Alive(Target::Id(lease_id(request.arg(0)?)?)),
        Step::LeaseExpiredId => Lease::Expired(Target::Id(lease_id(request.arg(0)?)?)),
        Step::LeaseHolds => Lease::Holds {
            name: name(0)?,
            keys: keys(request.arg(1)?)?,
        },
        Step::Put
        | Step::PutDocString
        | Step::Get
        | Step::DeleteKey
        | Step::DeletePrefix
        | Step::DeleteAll
        | Step::Exists
        | Step::Absent
        | Step::HasValue
        | Step::HasValueDoc
        | Step::Matches
        | Step::ContainsJson
        | Step::EqualsJson
        | Step::HasFields
        | Step::Count
        | Step::KeysBe
        | Step::KeysContain
        | Step::GetSerializable
        | Step::Leader
        | Step::NotLeader
        | Step::Reached
        | Step::ComputeHash
        | Step::HashIs
        | Step::AcceptPut
        | Step::RefusePut
        | Step::AcceptRead
        | Step::AcceptReadSerializable
        | Step::RefuseRead
        | Step::RefuseReadSerializable
        | Step::RefuseCompact
        | Step::WatchKey
        | Step::WatchPrefix
        | Step::WatchKeyFrom
        | Step::WatchPrefixFrom
        | Step::WatchCancel
        | Step::WatchedPut
        | Step::WatchedDelete
        | Step::WatchedCount
        | Step::WatchedEvents
        | Step::WatchResumed
        | Step::TxnCommit
        | Step::TxnSucceeded
        | Step::TxnFailed
        | Step::TxnRefuse
        | Step::WatchResumedTimes => return Err(StepError::input("not a lease step")),
    })
}

/// `bddkit_validate_step`: what a literal already says. A value that holds a
/// `<<slot>>` is left to the step.
pub fn validate(step: Step, request: &Request) -> Result<(), String> {
    let literal = |index: usize| request.args.get(index).filter(|a| !is_slot(a));
    let check = |index: usize, parse: fn(&str) -> Result<(), StepError>| {
        literal(index).map_or(Ok(()), |text| parse(text).map_err(|e| e.message))
    };
    let name = |text: &str| variable(text).map(drop);
    match step {
        Step::LeaseGrant => {
            check(0, name)?;
            check(1, |t| ttl(t).map(drop))
        }
        Step::LeaseKeep
        | Step::LeaseStop
        | Step::LeaseRevoke
        | Step::LeaseAlive
        | Step::LeaseExpired => check(0, name),
        Step::PutLeased => check(2, name),
        Step::LeaseAliveId | Step::LeaseExpiredId => check(0, |t| lease_id(t).map(drop)),
        Step::LeaseHolds => {
            check(0, name)?;
            check(1, |t| keys(t).map(drop))
        }
        _ => Ok(()),
    }
}

pub fn record_for(lease: &Lease, instance: &str) -> Record {
    match lease {
        Lease::Grant { name, ttl } => Record::new(
            "LeaseGrant",
            name,
            instance,
            json!({"name": name, "ttl": ttl}),
        ),
        Lease::Keep(name) => Record::new("LeaseKeepAlive", name, instance, json!({"name": name})),
        Lease::Stop(name) => Record::new("LeaseStopKeeping", name, instance, json!({"name": name})),
        Lease::Revoke(name) => Record::new("LeaseRevoke", name, instance, json!({"name": name})),
        Lease::Put { key, value, name } => {
            let mut record = Record::new(
                "Put",
                key,
                instance,
                json!({"key": key, "value": value, "lease_name": name}),
            );
            record.scan_text(value);
            record
        }
        Lease::Alive(target) | Lease::Expired(target) => {
            Record::new("LeaseTimeToLive", &target.shown(), instance, json!({}))
        }
        Lease::Holds { name, .. } => Record::new(
            "LeaseTimeToLive",
            &format!("\"{name}\""),
            instance,
            json!({"keys": true}),
        ),
    }
}

impl Target {
    fn shown(&self) -> String {
        match self {
            Self::Name(name) => format!("\"{name}\""),
            Self::Id(id) => format!("ID {id}"),
        }
    }
}

/// What `LeaseTimeToLive` said.
enum Seen {
    Alive { ttl: i64, keys: usize },
    Gone,
}

async fn seen(
    client: &Client,
    tls: bool,
    id: i64,
    with_keys: bool,
    record: &mut Record,
) -> Result<Seen, StepError> {
    let options = with_keys
        .then(LeaseTimeToLiveOptions::new)
        .map(LeaseTimeToLiveOptions::with_keys);
    let answer = client.lease_client().time_to_live(id, options).await;
    let gone = gone(&answer);
    let response = match answer {
        Ok(response) => response,
        Err(_) if gone => return Ok(Seen::Gone),
        Err(error) => return Err(request_failure(&error, tls)),
    };
    // Evidence even when the lease is gone: that is what a tester reads.
    record.answered(response.header());
    record.keys.push(json!({
        "id": id,
        "ttl": response.ttl(),
        "granted_ttl": response.granted_ttl(),
        "keys": response.keys().len(),
    }));
    if gone {
        return Ok(Seen::Gone);
    }
    Ok(Seen::Alive {
        ttl: response.ttl(),
        keys: response.keys().len(),
    })
}

pub async fn perform(
    instance: &Instance,
    client: &Client,
    lease: &Lease,
    record: &mut Record,
) -> Result<Vars, StepError> {
    let tls = instance.config.tls;
    match lease {
        Lease::Grant { name, ttl } => grant(instance, client, name, *ttl, record).await,
        Lease::Keep(name) => keep(instance, client, name).await,
        Lease::Stop(name) => {
            let state = instance.leases.state(name)?;
            let (kept, expired) = {
                let state = locked(&state);
                (state.kept(), state.expired)
            };
            if !kept {
                let why = if expired {
                    "has expired, and nothing keeps it alive any more"
                } else {
                    "is not kept alive by this scenario"
                };
                return Err(StepError::input(format!("etcd lease \"{name}\" {why}")));
            }
            instance.leases.stop_keeping(name).await;
            Ok(Vars::new())
        }
        Lease::Revoke(name) => {
            let state = instance.leases.state(name)?;
            let id = {
                let state = locked(&state);
                if state.revoked {
                    return Err(StepError::input(format!(
                        "etcd lease \"{name}\" is already revoked"
                    )));
                }
                state.id
            };
            let response = client
                .lease_client()
                .revoke(id)
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(response.header());
            locked(&state).revoked = true;
            instance.leases.stop_keeping(name).await;
            Ok(Vars::new())
        }
        Lease::Put { key, value, name } => {
            let state = instance.leases.state(name)?;
            let id = locked(&state).id;
            let response = client
                .kv_client()
                .put(
                    key.as_str(),
                    value.as_str(),
                    Some(PutOptions::new().with_lease(id)),
                )
                .await
                .map_err(|e| request_failure(&e, tls))?;
            record.answered(response.header());
            Ok(revision_vars(header(response.header())?))
        }
        Lease::Alive(target) | Lease::Expired(target) => {
            judge(instance, client, lease, target, record).await
        }
        Lease::Holds { name, .. } => {
            judge(instance, client, lease, &Target::Name(name.clone()), record).await
        }
    }
}

async fn grant(
    instance: &Instance,
    client: &Client,
    name: &str,
    seconds: i64,
    record: &mut Record,
) -> Result<Vars, StepError> {
    if instance.leases.contains(name) {
        return Err(StepError::input(format!(
            "this instance already has a lease \"{name}\"; a name is taken until the scenario ends, even by a revoked lease"
        )));
    }
    let response = client
        .lease_client()
        .grant(seconds, None)
        .await
        .map_err(|e| request_failure(&e, instance.config.tls))?;
    record.answered(response.header());
    let id = response.id();
    record
        .keys
        .push(json!({"id": id, "granted_ttl": response.ttl()}));
    instance
        .leases
        .add(name, LeaseState::new(id, response.ttl()));
    Ok(Vars::from([(format!("etcd_lease_{name}"), id.to_string())]))
}

/// Opens the stream and waits for the server's first answer, then starts the
/// task: a lease that is already gone is told here, not by a task that ends.
async fn keep(instance: &Instance, client: &Client, name: &str) -> Result<Vars, StepError> {
    let state = instance.leases.state(name)?;
    let (id, granted) = {
        let state = locked(&state);
        if state.revoked {
            return Err(StepError::input(format!(
                "etcd lease \"{name}\" is revoked, nothing can keep it alive"
            )));
        }
        if state.kept() {
            return Err(StepError::input(format!(
                "etcd lease \"{name}\" is already kept alive"
            )));
        }
        (state.id, state.granted_ttl)
    };
    let seconds = instance.config.timeout_secs;
    let tls = instance.config.tls;
    let opened = match client.lease_client().keep_alive(id).await {
        Ok(opened) => opened,
        Err(error) => {
            // The client reports a lease that is gone as an error of its own
            // kind, which a failed send shares, so the server is asked.
            if is_gone(client, id, seconds).await {
                locked(&state).expired = true;
                return Err(StepError::assertion(format!(
                    "etcd lease \"{name}\" has expired, nothing can keep it alive"
                )));
            }
            return Err(request_failure(&error, tls));
        }
    };
    let keeper = Keeper {
        client: client.clone(),
        id,
        interval: interval(granted),
        seconds,
        tls,
    };
    instance.leases.start(name, keeper, opened);
    Ok(Vars::new())
}

async fn judge(
    instance: &Instance,
    client: &Client,
    lease: &Lease,
    target: &Target,
    record: &mut Record,
) -> Result<Vars, StepError> {
    let (id, state) = match target {
        Target::Name(name) => {
            let state = instance.leases.state(name)?;
            let id = locked(&state).id;
            (id, Some(state))
        }
        Target::Id(id) => (*id, None),
    };
    let tls = instance.config.tls;
    let shown = target.shown();
    let found = seen(
        client,
        tls,
        id,
        matches!(lease, Lease::Holds { .. }),
        record,
    )
    .await?;
    if matches!(found, Seen::Gone)
        && let Some(state) = &state
    {
        locked(state).expired = true;
    }
    verdict(lease, &shown, &found).map(|()| Vars::new())
}

/// What the answer means for the step.
fn verdict(lease: &Lease, shown: &str, found: &Seen) -> Result<(), StepError> {
    match (lease, found) {
        (Lease::Alive(_), Seen::Alive { .. }) | (Lease::Expired(_), Seen::Gone) => Ok(()),
        (Lease::Alive(_), Seen::Gone) => Err(StepError::assertion(format!(
            "etcd lease {shown} has expired, and a lease does not come back"
        ))),
        (Lease::Expired(_), Seen::Alive { ttl, .. }) => {
            let left = if *ttl == 0 {
                "under a second".to_string()
            } else {
                format!("{ttl} s")
            };
            Err(miss(format!("etcd lease {shown} is alive, {left} left")))
        }
        (Lease::Holds { keys, .. }, Seen::Alive { keys: held, .. }) => {
            if u64::try_from(*held).is_ok_and(|held| held == *keys) {
                Ok(())
            } else {
                Err(miss(format!(
                    "etcd lease {shown} holds {held} keys, expected {keys}"
                )))
            }
        }
        (Lease::Holds { .. }, Seen::Gone) => Err(StepError::assertion(format!(
            "etcd lease {shown} has expired, and its keys went with it"
        ))),
        (
            Lease::Grant { .. }
            | Lease::Keep(_)
            | Lease::Stop(_)
            | Lease::Revoke(_)
            | Lease::Put { .. },
            _,
        ) => Err(StepError::input("not a lease assertion")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Class;

    fn request(args: &[&str]) -> Request {
        Request::for_test(args)
    }

    #[test]
    fn a_ttl_of_zero_or_a_word_is_input_and_a_literal_one_is_refused_before_any_request() {
        for bad in ["0", "-1", "ten", "99999999999999999999"] {
            let error = plan(Step::LeaseGrant, &request(&["a", bad])).expect_err("refused");
            assert_eq!(error.class, Class::Input, "{bad}");
            assert!(validate(Step::LeaseGrant, &request(&["a", bad])).is_err());
        }
        assert!(matches!(
            plan(Step::LeaseGrant, &request(&["a", "5"])),
            Ok(Lease::Grant { ttl: 5, .. })
        ));
        assert!(validate(Step::LeaseGrant, &request(&["a", "<<ttl>>"])).is_ok());
    }

    #[test]
    fn an_id_is_decimal_and_at_most_i64_max() {
        let max = i64::MAX.to_string();
        assert!(matches!(
            plan(Step::LeaseAliveId, &request(&[&max])),
            Ok(Lease::Alive(Target::Id(i64::MAX)))
        ));
        for bad in ["9223372036854775808", "0x1f", "7587862b", "-3", ""] {
            let error = plan(Step::LeaseExpiredId, &request(&[bad])).expect_err("refused");
            assert_eq!(error.class, Class::Input, "{bad}");
            assert!(validate(Step::LeaseExpiredId, &request(&[bad])).is_err());
        }
        assert!(validate(Step::LeaseAliveId, &request(&["<<id>>"])).is_ok());
    }

    #[test]
    fn a_name_is_a_variable_name_because_it_becomes_one() {
        for step in [
            Step::LeaseKeep,
            Step::LeaseStop,
            Step::LeaseRevoke,
            Step::LeaseAlive,
            Step::LeaseExpired,
        ] {
            assert!(plan(step, &request(&["a b"])).is_err());
            assert!(validate(step, &request(&["a-b"])).is_err());
            assert!(plan(step, &request(&["session_1"])).is_ok());
        }
        assert!(plan(Step::PutLeased, &request(&["/k", "v", ""])).is_err());
        assert!(validate(Step::PutLeased, &request(&["/k", "v", "a b"])).is_err());
        assert!(plan(Step::LeaseHolds, &request(&["a", "two"])).is_err());
    }

    #[test]
    fn an_alive_check_on_a_gone_lease_is_final_and_an_expired_check_on_a_live_one_is_not() {
        let alive = Seen::Alive { ttl: 3, keys: 2 };
        let by_id = |lease: &Lease, found: &Seen| verdict(lease, "ID 1", found);
        let (is_alive, expired) = (Lease::Alive(Target::Id(1)), Lease::Expired(Target::Id(1)));
        assert!(by_id(&is_alive, &alive).is_ok());
        assert!(by_id(&expired, &Seen::Gone).is_ok());
        let error = by_id(&is_alive, &Seen::Gone).expect_err("final");
        assert!(!error.retry);
        assert_eq!(error.class, Class::Assertion);
        let error = by_id(&expired, &alive).expect_err("later");
        assert!(error.retry);
        assert_eq!(error.class, Class::Assertion);
    }

    #[test]
    fn a_key_count_is_not_yet_while_it_differs_and_final_on_a_gone_lease() {
        let holds = |keys| Lease::Holds {
            name: "a".to_string(),
            keys,
        };
        let alive = Seen::Alive { ttl: 3, keys: 2 };
        assert!(verdict(&holds(2), "\"a\"", &alive).is_ok());
        let error = verdict(&holds(3), "\"a\"", &alive).expect_err("later");
        assert!(error.retry);
        assert!(
            error.message.contains("holds 2 keys, expected 3"),
            "{error}"
        );
        let error = verdict(&holds(0), "\"a\"", &Seen::Gone).expect_err("final");
        assert!(!error.retry);
    }
}
