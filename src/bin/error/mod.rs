use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum CliError {
    NoCommand,
    UnknownCommand(String),
    InvalidCommand(String),
    BrokerError {
        kind: BrokerErrorKind,
        message: String,
    },
    ParseError {
        field: String,
        message: String,
    },
    IoError(std::io::Error),
    SchemaError {
        kind: SchemaErrorKind,
        message: String,
    },
}

#[derive(Debug)]
pub enum BrokerErrorKind {
    ConnectionFailed,
    OperationFailed,
    #[allow(dead_code)]
    // Reserved for future use: differentiate broker-not-found cases
    NotFound,
    Timeout,
    TopicNotFound,
    PartitionError,
    Unknown,
}

#[derive(Debug)]
pub enum SchemaErrorKind {
    ValidationFailed,
    IncompatibleChange,
    RegistryError,
    NotFound,
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::NoCommand => write!(f, "No command specified"),
            CliError::UnknownCommand(cmd) => write!(f, "Unknown command: {}", cmd),
            CliError::InvalidCommand(cmd) => write!(f, "Invalid {} command", cmd),
            CliError::BrokerError { kind, message } => {
                let kind_str = match kind {
                    BrokerErrorKind::ConnectionFailed => "Connection error",
                    BrokerErrorKind::OperationFailed => "Operation error",
                    BrokerErrorKind::NotFound => "Broker not found",
                    BrokerErrorKind::Timeout => "Timeout",
                    BrokerErrorKind::TopicNotFound => "Topic not found",
                    BrokerErrorKind::PartitionError => "Partition error",
                    BrokerErrorKind::Unknown => "Unknown error",
                };
                write!(f, "{}: {}", kind_str, message)
            },
            CliError::ParseError { field, message } => {
                write!(f, "Parse error for {}: {}", field, message)
            },
            CliError::IoError(e) => write!(f, "IO error: {}", e),
            CliError::SchemaError { kind, message } => {
                let kind_str = match kind {
                    SchemaErrorKind::ValidationFailed => "Schema validation error",
                    SchemaErrorKind::IncompatibleChange => "Compatibility error",
                    SchemaErrorKind::RegistryError => "Registry error",
                    SchemaErrorKind::NotFound => "Schema not found",
                };
                write!(f, "{}: {}", kind_str, message)
            },
        }
    }
}

impl Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        CliError::IoError(err)
    }
}

impl From<Box<dyn Error>> for CliError {
    fn from(err: Box<dyn Error>) -> Self {
        CliError::BrokerError {
            kind: BrokerErrorKind::Unknown,
            message: err.to_string(),
        }
    }
}

pub type CliResult<T> = Result<T, CliError>;
