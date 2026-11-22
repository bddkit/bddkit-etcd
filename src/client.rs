//! Talking to the server: the connect options a config means and the live
//! probe behind `doctor --live`.

use std::error::Error as _;
use std::io::ErrorKind;
use std::time::Duration;

use etcd_client::{Certificate, Client, ConnectOptions, Identity, TlsOptions};

use crate::config::Config;

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

/// `Status` on every endpoint, each reached with a client of its own: one
/// client over several addresses balances, and would answer for whichever it
/// picked. `detail` is the version and the revision of each.
pub async fn probe(config: &Config) -> Result<String, String> {
    let options = connect_options(config)?;
    let timeout = Duration::from_secs(config.timeout_secs);
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

fn not_reached(why: &str) -> String {
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
        code => format!(
            "the server answered with an error ({code:?}): {}",
            status.message()
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, ErrorKind};

    use etcd_client::Error;
    use tonic::{Code, Status};

    use super::explain;

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
}
