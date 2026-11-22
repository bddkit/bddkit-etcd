//! Why a step failed: its class and the text a person reads. The class is set
//! where the error is born and is never parsed back out of the message, and
//! there is deliberately no `From<String>`: a `?` on a plain string must not
//! compile, because a silent default class is how the scheme would rot.

use std::fmt;

/// The failure classes an etcd step can end in, named as the host's `class`
/// field spells them. Every `match` on it is exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// The server was not reached: connect, transport, handshake, timeout.
    DbConnection,
    /// The server answered with a status.
    DbStatement,
    /// A check did not hold.
    Assertion,
    /// The step cannot run as written.
    Input,
}

impl Class {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DbConnection => "db_connection",
            Self::DbStatement => "db_statement",
            Self::Assertion => "assertion",
            Self::Input => "input",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepError {
    pub class: Class,
    pub message: String,
    /// The reply is `not_yet`, not `fatal`: a fresh read may still change it.
    /// Only an assertion's error may carry it, the host fails an action that
    /// answers `not_yet`.
    pub retry: bool,
}

impl StepError {
    pub fn new(class: Class, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
            retry: false,
        }
    }

    /// An assertion that may hold on a later attempt.
    pub fn not_yet(class: Class, message: impl Into<String>) -> Self {
        Self {
            retry: true,
            ..Self::new(class, message)
        }
    }

    /// What an assertion makes of a failure to reach or to be answered by the
    /// server: another attempt may find it up. The other classes keep what
    /// they were born with.
    #[must_use]
    pub fn polled(mut self) -> Self {
        self.retry |= matches!(self.class, Class::DbConnection | Class::DbStatement);
        self
    }

    pub fn input(message: impl Into<String>) -> Self {
        Self::new(Class::Input, message)
    }

    pub fn assertion(message: impl Into<String>) -> Self {
        Self::new(Class::Assertion, message)
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_failure_of_the_server_becomes_retryable_when_polled() {
        for class in [Class::DbConnection, Class::DbStatement] {
            assert!(StepError::new(class, "x").polled().retry);
        }
        for class in [Class::Assertion, Class::Input] {
            assert!(!StepError::new(class, "x").polled().retry);
        }
        assert!(StepError::not_yet(Class::Assertion, "x").polled().retry);
    }

    #[test]
    fn a_class_is_spelled_as_the_host_expects() {
        for (class, name) in [
            (Class::DbConnection, "db_connection"),
            (Class::DbStatement, "db_statement"),
            (Class::Assertion, "assertion"),
            (Class::Input, "input"),
        ] {
            assert_eq!(class.as_str(), name);
        }
    }
}
