use thiserror::Error;

/// Network-related errors
#[derive(Error, Debug, Clone)]
pub enum NetworkError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Network timeout")]
    Timeout,

    #[error("Connection timeout")]
    ConnectionTimeout,

    #[error("Handshake timeout")]
    HandshakeTimeout,

    #[error("Read timeout")]
    ReadTimeout,

    #[error("Write timeout")]
    WriteTimeout,

    #[error("Socket configuration error: {0}")]
    SocketConfigError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("Node unreachable: {0}")]
    NodeUnreachable(String),

    #[error("Invalid message format")]
    InvalidMessageFormat,

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Authentication failed")]
    AuthenticationFailed,

    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    #[error("Network partition detected")]
    NetworkPartition,

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("Authorization error: {0}")]
    AuthorizationError(String),

    #[error("Audit error: {0}")]
    AuditError(String),

    #[error("Security error: {0}")]
    SecurityError(String),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("IO error: {0}")]
    IoError(String),

    #[error("JSON error: {0}")]
    JsonError(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Replication error: {0}")]
    ReplicationError(String),

    #[error("Consensus error: {0}")]
    ConsensusError(String),

    #[error("Not leader: operation requires leader node")]
    NotLeader,

    #[error("Invalid range: {0}")]
    InvalidRange(String),
}

pub type NetworkResult<T> = Result<T, NetworkError>;

impl From<std::io::Error> for NetworkError {
    fn from(error: std::io::Error) -> Self {
        NetworkError::IoError(error.to_string())
    }
}

impl From<serde_json::Error> for NetworkError {
    fn from(error: serde_json::Error) -> Self {
        NetworkError::JsonError(error.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for NetworkError {
    fn from(error: tokio::time::error::Elapsed) -> Self {
        NetworkError::TimeoutError(error.to_string())
    }
}

impl From<bincode::Error> for NetworkError {
    fn from(error: bincode::Error) -> Self {
        NetworkError::SerializationError(error.to_string())
    }
}

impl NetworkError {
    /// Check if the error is recoverable (can retry)
    pub fn is_recoverable(&self) -> bool {
        match self {
            NetworkError::IoError(_) => true,
            NetworkError::ConnectionError(_) => true,
            NetworkError::ConnectionFailed(_) => true,
            NetworkError::Timeout => true,
            NetworkError::ConnectionTimeout => true,
            NetworkError::HandshakeTimeout => true,
            NetworkError::ReadTimeout => true,
            NetworkError::WriteTimeout => true,
            NetworkError::SocketConfigError(_) => false,
            NetworkError::TimeoutError(_) => true,
            NetworkError::ResourceExhausted(_) => true,
            NetworkError::NodeUnreachable(_) => true,
            NetworkError::NetworkPartition => true,
            NetworkError::ProtocolError(_) => false,
            NetworkError::SerializationError(_) => false,
            NetworkError::JsonError(_) => false,
            NetworkError::InvalidMessageFormat => false,
            NetworkError::AuthenticationFailed => false,
            NetworkError::AuthenticationError(_) => false,
            NetworkError::TlsError(_) => false,
            NetworkError::AuthorizationError(_) => false,
            NetworkError::AuditError(_) => false,
            NetworkError::SecurityError(_) => false,
            NetworkError::ConfigurationError(_) => false,
            NetworkError::InternalError(_) => false,
            NetworkError::Unknown(_) => false,
            NetworkError::NotFound(_) => false,
            NetworkError::Serialization(_) => false,
            NetworkError::Io(_) => true,
            NetworkError::ReplicationError(_) => false,
            NetworkError::ConsensusError(_) => false,
            NetworkError::NotLeader => true,
            NetworkError::InvalidRange(_) => false,
        }
    }

    /// Check if the error is temporary (will likely resolve by itself)
    pub fn is_temporary(&self) -> bool {
        match self {
            NetworkError::Timeout => true,
            NetworkError::TimeoutError(_) => true,
            NetworkError::ResourceExhausted(_) => true,
            NetworkError::NetworkPartition => true,
            NetworkError::InvalidRange(_) => false,
            _ => false,
        }
    }

    /// Get the error category for metrics and logging
    pub fn category(&self) -> &'static str {
        match self {
            NetworkError::IoError(_) => "io",
            NetworkError::ConnectionError(_) => "connection",
            NetworkError::ConnectionFailed(_) => "connection",
            NetworkError::ProtocolError(_) => "protocol",
            NetworkError::SerializationError(_) => "serialization",
            NetworkError::JsonError(_) => "serialization",
            NetworkError::InvalidMessageFormat => "protocol",
            NetworkError::AuthenticationFailed => "authentication",
            NetworkError::AuthenticationError(_) => "authentication",
            NetworkError::Timeout => "timeout",
            NetworkError::ConnectionTimeout => "timeout",
            NetworkError::HandshakeTimeout => "timeout",
            NetworkError::ReadTimeout => "timeout",
            NetworkError::WriteTimeout => "timeout",
            NetworkError::SocketConfigError(_) => "socket",
            NetworkError::TimeoutError(_) => "timeout",
            NetworkError::TlsError(_) => "tls",
            NetworkError::AuthorizationError(_) => "authorization",
            NetworkError::AuditError(_) => "audit",
            NetworkError::SecurityError(_) => "security",
            NetworkError::ResourceExhausted(_) => "resource",
            NetworkError::ConfigurationError(_) => "configuration",
            NetworkError::NodeUnreachable(_) => "connection",
            NetworkError::NetworkPartition => "network",
            NetworkError::InternalError(_) => "internal",
            NetworkError::Unknown(_) => "unknown",
            NetworkError::NotFound(_) => "not_found",
            NetworkError::Serialization(_) => "serialization",
            NetworkError::Io(_) => "io",
            NetworkError::ReplicationError(_) => "replication",
            NetworkError::ConsensusError(_) => "consensus",
            NetworkError::NotLeader => "consensus",
            NetworkError::InvalidRange(_) => "validation",
        }
    }
}
