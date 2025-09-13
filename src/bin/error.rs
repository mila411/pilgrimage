//! CLI Error types and handling
//!
//! This module defines error types specific to CLI operations.

use std::fmt;

pub type CliResult<T> = Result<T, CliError>;

#[derive(Debug)]
#[allow(dead_code)]
pub enum CliError {
    BrokerError {
        kind: BrokerErrorKind,
        message: String,
    },
    InvalidCommand(String),
    UnknownCommand(String),
    NoCommand,
    AuthenticationError(String),
    ConfigurationError(String),
    NetworkError(String),
    IoError(String),
    ParseError {
        field: String,
        message: String,
    },
    SchemaError {
        kind: SchemaErrorKind,
        message: String,
    },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum BrokerErrorKind {
    StartupFailure,
    ShutdownFailure,
    ConnectionFailure,
    ConnectionFailed,
    OperationFailed,
    Timeout,
    TopicNotFound,
    PartitionError,
    Unknown,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum SchemaErrorKind {
    ValidationError,
    ValidationFailed,
    CompatibilityError,
    IncompatibleChange,
    RegistrationError,
    RegistryError,
    NotFound,
    ParseError,
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::BrokerError { kind, message } => {
                write!(f, "Broker error ({:?}): {}", kind, message)
            }
            CliError::InvalidCommand(cmd) => {
                write!(f, "Invalid command '{}'. Use --help for usage information", cmd)
            }
            CliError::UnknownCommand(cmd) => {
                write!(f, "Unknown command '{}'. Use --help to see available commands", cmd)
            }
            CliError::NoCommand => {
                write!(f, "No command provided. Use --help to see available commands")
            }
            CliError::AuthenticationError(msg) => {
                write!(f, "Authentication error: {}", msg)
            }
            CliError::ConfigurationError(msg) => {
                write!(f, "Configuration error: {}", msg)
            }
            CliError::NetworkError(msg) => {
                write!(f, "Network error: {}", msg)
            }
            CliError::IoError(msg) => {
                write!(f, "IO error: {}", msg)
            }
            CliError::ParseError { field, message } => {
                if field.is_empty() {
                    write!(f, "Parse error: {}", message)
                } else {
                    write!(f, "Parse error ({}): {}", field, message)
                }
            }
            CliError::SchemaError { kind, message } => {
                write!(f, "Schema error ({:?}): {}", kind, message)
            }
        }
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        CliError::IoError(err.to_string())
    }
}

impl From<Box<dyn std::error::Error>> for CliError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        CliError::NetworkError(err.to_string())
    }
}
