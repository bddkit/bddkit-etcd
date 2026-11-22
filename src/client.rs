//! Talking to the server: the connect options a config means and the live
//! probe behind `doctor --live`.

use std::error::Error as _;
use std::io::ErrorKind;
use std::time::Duration;

use etcd_client::{Certificate, Client, ConnectOptions, Identity, TlsOptions};

use crate::config::Config;
use crate::error::{Class, StepError};

/// What the config means as connect options: both timeouts are
/// `timeout_secs`, TLS is on for `https`, and the roots are the system's
/// unless `tls_ca` names one.
pub fn connect_options(config: &Config) -> Result<ConnectOptions, String> {
    let timeout = Duration::from_secs(config.timeout_secs);
    let mut options = ConnectOptions::new()
        .with_timeout(timeout)
        .with_connect_timeout(timeout);
    if let Some((user, password)) = &config.credentials {
        options = options.with_user(user, password);
    }
    if config.tls {
        options = options.with_tls(tls_options(config)?);
    }
    Ok(options)
}

fn tls_options(config: &Config) -> Result<TlsOptions, String> {
    let mut tls = TlsOptions::new();
    match &config.tls_ca {
        Some(path) => tls = tls.ca_certificate(Certificate::from_pem(read(path)?)),
        // The TLS feature alone gives an empty root store.
        None => tls = tls.with_enabled_roots(),
    }
    if let Some((cert, key)) = &config.client_identity {
        tls = tls.identity(Identity::from_pem(read(cert)?, read(key)?));
    }
    if let Some(name) = &config.tls_server_name {
        tls = tls.domain_name(name);
    }
    Ok(tls)
}

fn read(path: &std::path::Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("cannot read \"{}\": {e}", path.display()))
}

/// The client's own timeouts get a second of slack: with the same duration
/// they raced the timer that wraps the call, and when the client's fired first
/// the message said "Timeout expired" instead of "no response within N s".
fn with_slack(options: ConnectOptions, config: &Config) -> ConnectOptions {
    let slack = Duration::from_secs(config.timeout_secs.saturating_add(1));
    options.with_timeout(slack).with_connect_timeout(slack)
}

/// What every step's client is built with. The token is refreshed when the
/// server calls it invalid (an idle file outlives it, a member restart drops
/// it), because the client lives as long as the feature file. `require_leader` is on every
/// request: without it a cluster that lost its leader holds the request until
/// the server's own timeout, and the client's fires first as `Cancelled`
/// instead of `Unavailable`. The probe leaves it off, because `Status` is
/// answered by a node with no leader too.
pub fn step_options(config: &Config) -> Result<ConnectOptions, String> {
    Ok(with_slack(connect_options(config)?, config)
        .with_auto_token_refresh(true)
        .with_require_leader(true))
}

/// `Status` on every endpoint, each reached with a client of its own: one
/// client over several addresses balances, and would answer for whichever it
/// picked. `detail` is the version and the revision of each.
pub async fn probe(config: &Config) -> Result<String, String> {
    let timeout = Duration::from_secs(config.timeout_secs);
    let options = with_slack(connect_options(config)?, config);
    let mut lines = Vec::new();
    for endpoint in &config.endpoints {
        let attempt = async {
            let client = Client::connect([endpoint.as_str()], Some(options.clone())).await?;
            client.maintenance_client().status().await
        };
        let failure = match tokio::time::timeout(timeout, attempt).await {
            Ok(Ok(status)) => {
                let revision = status
                    .header()
                    .map_or("unknown".to_string(), |h| h.revision().to_string());
                lines.push(format!("etcd {}, revision {revision}", status.version()));
                continue;
            }
            Ok(Err(error)) => explain(&error, config.tls),
            Err(_) => not_reached(&format!("no response within {} s", config.timeout_secs)),
        };
        // With several endpoints the tester must know which one failed.
        return Err(if config.endpoints.len() > 1 {
            format!("{endpoint}: {failure}")
        } else {
            failure
        });
    }
    Ok(match lines.as_slice() {
        [one] => one.clone(),
        _ => config
            .endpoints
            .iter()
            .zip(&lines)
            .map(|(endpoint, line)| format!("{endpoint}: {line}"))
            .collect::<Vec<_>>()
            .join("; "),
    })
}

pub fn not_reached(why: &str) -> String {
    format!("the server was not reached: {why}")
}

/// The first `io::Error` in the chain of causes: what the socket said.
fn io_cause<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a std::io::Error> {
    let mut cause = Some(error);
    while let Some(current) = cause {
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            return Some(io);
        }
        cause = current.source();
    }
    None
}

/// Which of three things went wrong, told apart by the error's type and never
/// by its text: the server was not reached, TLS did not agree, or the server
/// refused the credentials. A status with a source never left the client
/// (connect, transport, handshake); one without came from the server.
pub fn explain(error: &etcd_client::Error, tls: bool) -> String {
    let etcd_client::Error::GRpcStatus(status) = error else {
        return not_reached(&error.to_string());
    };
    if let Some(source) = status.source() {
        return match io_cause(source) {
            // A handshake that ends early, or a certificate refused, reaches
            // the caller as one of these once the socket itself connected.
            Some(io)
                if tls
                    && matches!(io.kind(), ErrorKind::UnexpectedEof | ErrorKind::InvalidData) =>
            {
                format!("TLS did not agree: {io}")
            }
            Some(io) => not_reached(&io.to_string()),
            // A server that refuses the handshake (a missing client
            // certificate, say) may close the connection before its alert is
            // read, and the client reports a cancelled request, not an I/O error.
            None if tls && status.code() == tonic::Code::Cancelled => format!(
                "TLS did not agree: the server closed the connection ({})",
                status.message()
            ),
            None => not_reached(status.message()),
        };
    }
    match status.code() {
        // etcd answers a refused password with `InvalidArgument`, and
        // credentials sent to a server without authentication with
        // `FailedPrecondition`.
        tonic::Code::InvalidArgument
        | tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied
        | tonic::Code::FailedPrecondition => {
            format!("the server refused the credentials: {}", status.message())
        }
        _ => server_error(status),
    }
}

/// What the server answered, for a status that came from it.
fn server_error(status: &tonic::Status) -> String {
    format!(
        "the server answered with an error ({:?}): {}",
        status.code(),
        status.message()
    )
}

/// Whether the server answered. A status with a `source()` never left the
/// client (connect, transport, timeout); one without came from the server, and
/// so does every error that is not a status the server could have sent.
pub fn class_of(error: &etcd_client::Error) -> Class {
    match error {
        etcd_client::Error::GRpcStatus(status) if status.source().is_none() => Class::DbStatement,
        _ => Class::DbConnection,
    }
}

/// A failure while the client is being built: a refused password arrives here
/// as a status of the server's, and `explain` says what it means.
pub fn connect_failure(error: &etcd_client::Error, tls: bool) -> StepError {
    StepError::new(class_of(error), explain(error, tls))
}

/// A failure of one request. Unlike a connect, a status here is the server's
/// answer to the request and says nothing about credentials: an empty key is
/// `InvalidArgument` too.
pub fn request_failure(error: &etcd_client::Error, tls: bool) -> StepError {
    match (error, class_of(error)) {
        (etcd_client::Error::GRpcStatus(status), Class::DbStatement) => {
            StepError::new(Class::DbStatement, server_error(status))
        }
        _ => connect_failure(error, tls),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, ErrorKind};

    use etcd_client::Error;
    use tonic::{Code, Status};

    use super::{class_of, explain, request_failure};
    use crate::error::Class;

    fn transport(kind: ErrorKind) -> Error {
        Error::GRpcStatus(Status::from_error(Box::new(io::Error::new(
            kind,
            "socket said so",
        ))))
    }

    #[test]
    fn a_socket_that_never_connected_is_not_reached() {
        for kind in [ErrorKind::ConnectionRefused, ErrorKind::TimedOut] {
            let text = explain(&transport(kind), true);
            assert!(text.starts_with("the server was not reached: "), "{text}");
        }
    }

    #[test]
    fn a_handshake_that_fails_is_tls_only_on_an_https_endpoint() {
        for kind in [ErrorKind::UnexpectedEof, ErrorKind::InvalidData] {
            assert!(explain(&transport(kind), true).starts_with("TLS did not agree: "));
            assert!(explain(&transport(kind), false).starts_with("the server was not reached: "));
        }
    }

    #[test]
    fn a_connection_closed_during_the_handshake_is_tls_only_on_an_https_endpoint() {
        let closed = || {
            let mut status = Status::new(Code::Cancelled, "operation was canceled");
            status.set_source(std::sync::Arc::new(std::fmt::Error));
            Error::GRpcStatus(status)
        };
        // The source is not an `io::Error`, as hyper's own error is not.
        let text = explain(&closed(), true);
        assert!(text.starts_with("TLS did not agree: "), "{text}");
        assert!(explain(&closed(), false).starts_with("the server was not reached: "),);
    }

    #[test]
    fn a_status_the_server_sent_is_credentials_or_an_error_by_its_code() {
        for code in [
            Code::InvalidArgument,
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::FailedPrecondition,
        ] {
            let text = explain(
                &Error::GRpcStatus(Status::new(code, "etcdserver: nope")),
                false,
            );
            assert_eq!(text, "the server refused the credentials: etcdserver: nope");
        }
        let text = explain(
            &Error::GRpcStatus(Status::new(Code::Unavailable, "no leader")),
            false,
        );
        assert_eq!(
            text,
            "the server answered with an error (Unavailable): no leader"
        );
    }

    #[test]
    fn an_error_that_is_not_a_status_never_reached_the_server() {
        let text = explain(&Error::InvalidArgs("bad".to_string()), false);
        assert!(text.starts_with("the server was not reached: "));
    }

    #[test]
    fn the_class_is_the_source_of_the_status_and_never_its_text() {
        let from_server = Error::GRpcStatus(Status::new(Code::Unavailable, "socket closed"));
        assert_eq!(class_of(&from_server), Class::DbStatement);
        let client_side = transport(ErrorKind::ConnectionRefused);
        assert_eq!(class_of(&client_side), Class::DbConnection);
        // A status that says "refused" in words but carries a source.
        let worded = Error::GRpcStatus(Status::from_error(Box::new(io::Error::other(
            "the server answered with an error",
        ))));
        assert_eq!(class_of(&worded), Class::DbConnection);
        assert_eq!(
            class_of(&Error::InvalidArgs("bad".to_string())),
            Class::DbConnection
        );
    }

    #[test]
    fn a_refused_request_is_not_called_a_refused_password() {
        let error = request_failure(
            &Error::GRpcStatus(Status::new(
                Code::InvalidArgument,
                "etcdserver: key is not provided",
            )),
            false,
        );
        assert_eq!(error.class, Class::DbStatement);
        assert_eq!(
            error.message,
            "the server answered with an error (InvalidArgument): etcdserver: key is not provided"
        );
        let down = request_failure(&transport(ErrorKind::ConnectionRefused), false);
        assert_eq!(down.class, Class::DbConnection);
        assert!(down.message.starts_with("the server was not reached: "));
    }
}
