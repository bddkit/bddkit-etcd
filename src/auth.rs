//! Logging in as another user, and expecting a login to be refused. A login is
//! a client of its own, built with the credentials and connected at once, so
//! the server's answer to the password is the answer of the step. The password
//! is never in a message, a record or a `Debug` text: only the user is.

use std::fmt;

use etcd_client::Client;
use serde_json::json;

use crate::check::is_slot;
use crate::client::connect_failure;
use crate::error::StepError;
use crate::evidence::Record;
use crate::expect::{Wanted, code, judge_what, note_refusal};
use crate::instance::Instance;
use crate::ops::{Request, Vars};
use crate::steps::Step;

pub struct Auth {
    user: String,
    password: String,
    /// `None` logs in; a code expects the login to be refused with it.
    refuse: Option<Wanted>,
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth")
            .field("user", &self.user)
            .field("refuse", &self.refuse)
            .finish_non_exhaustive()
    }
}

pub fn plan(step: Step, request: &Request) -> Result<Auth, StepError> {
    let refuse = match step {
        Step::Authenticate => None,
        Step::RefuseAuth => Some(Wanted::Refuse(
            code(request.arg(2)?).map_err(StepError::input)?,
        )),
        _ => return Err(StepError::input("not an authentication step")),
    };
    Ok(Auth {
        user: request.arg(0)?.to_string(),
        password: request.arg(1)?.to_string(),
        refuse,
    })
}

/// `bddkit_validate_step`: a code that is a literal must be a known one. It is
/// the third argument, after the password, which no message may quote.
pub fn validate(step: Step, request: &Request) -> Result<(), String> {
    match request.args.get(2) {
        Some(name) if step == Step::RefuseAuth && !is_slot(name) => code(name).map(drop),
        _ => Ok(()),
    }
}

pub fn record_for(auth: &Auth, instance: &str) -> Record {
    Record::new("Login", &auth.user, instance, json!({"user": auth.user}))
}

pub async fn perform(
    instance: &Instance,
    auth: &Auth,
    record: &mut Record,
) -> Result<Vars, StepError> {
    let login = (auth.user.clone(), auth.password.clone());
    // With `require_leader`, as the refusal steps of `expect.rs`: a cluster
    // with no leader answers at once instead of holding the login.
    let options = instance.options(Some(&login), true)?;
    let connected = Client::connect(&instance.config.endpoints, Some(options)).await;
    let tls = instance.config.tls;
    let Some(wanted) = &auth.refuse else {
        let client = connected.map_err(|e| connect_failure(&e, tls))?;
        instance.log_in(login, client);
        return Ok(Vars::new());
    };
    if let Err(error) = &connected {
        note_refusal(record, error);
    }
    let what = format!("a login as \"{}\"", auth.user);
    judge_what(&what, wanted, connected.as_ref().map(drop), tls).map(|()| Vars::new())
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;
    use crate::error::Class;

    fn request(args: &[&str]) -> Request {
        Request::for_test(args)
    }

    #[test]
    fn a_literal_code_is_checked_before_the_run_and_a_slot_is_not() {
        let check = |args: &[&str]| validate(Step::RefuseAuth, &request(args));
        assert!(check(&["u", "pw", "Nope"]).is_err());
        assert!(check(&["u", "pw", "OK"]).is_err());
        assert!(check(&["u", "pw", "InvalidArgument"]).is_ok());
        assert!(check(&["u", "pw", "<<code>>"]).is_ok());
        // The login has no code, and its password is never read as one.
        assert!(validate(Step::Authenticate, &request(&["u", "Nope"])).is_ok());
    }

    #[test]
    fn a_bad_code_names_the_code_and_never_the_password() {
        let error = validate(Step::RefuseAuth, &request(&["u", "hunter2-pw", "Nope"]))
            .expect_err("refused");
        assert!(
            error.contains("Nope") && !error.contains("hunter2-pw"),
            "{error}"
        );
        let error =
            plan(Step::RefuseAuth, &request(&["u", "hunter2-pw", "Nope"])).expect_err("refused");
        assert_eq!(error.class, Class::Input);
        assert!(!error.message.contains("hunter2-pw"), "{error}");
    }

    #[test]
    fn the_plan_keeps_the_password_out_of_its_debug_text_and_its_evidence() {
        let auth = plan(
            Step::RefuseAuth,
            &request(&["bob", "hunter2-pw", "InvalidArgument"]),
        )
        .expect("planned");
        assert_eq!(auth.refuse, Some(Wanted::Refuse(Code::InvalidArgument)));
        assert!(!format!("{auth:?}").contains("hunter2-pw"));
        let record = record_for(&auth, "main");
        let text = record.diagnostic().to_string();
        assert!(
            text.contains("bob") && !text.contains("hunter2-pw"),
            "{text}"
        );
        assert!(!record.output().to_string().contains("hunter2-pw"));
    }

    #[test]
    fn a_login_has_no_code_to_refuse_with() {
        let auth = plan(Step::Authenticate, &request(&["bob", "pw"])).expect("planned");
        assert_eq!(auth.refuse, None);
        assert!(plan(Step::Authenticate, &request(&["bob"])).is_err());
    }
}
