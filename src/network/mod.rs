//! Network communication module for distributed broker communication.
//!
//! This module provides network communication capabilities for distributed
//! messaging between brokers, including heartbeats, data replication,
//! and consensus messages.

pub mod async_safe;
pub mod connection;
pub mod discovery;
pub mod enhanced_config;
pub mod enhanced_connection;
pub mod enhanced_protocol;
pub mod error;
pub mod flow_control;
pub mod observability;
pub mod production;
pub mod protocol;
pub mod protocol_v2;
pub mod reconnection;
pub mod reliability;
pub mod secure_protocol;
pub mod security;
pub mod tls;
pub mod transport;

pub use async_safe::{
    AsyncSafeConnectionManager, AsyncSafeMetrics, AsyncSafeResourcePool, ConcurrencyError,
};
pub use connection::ConnectionManager;
pub use discovery::NodeDiscovery;
pub use enhanced_config::{EnhancedNetworkConfig, RetryPolicy};
pub use enhanced_connection::{ConnectionInfo, ConnectionStats, EnhancedConnectionManager};
pub use enhanced_protocol::{
    ConnectionState, EnhancedCodec, EnhancedMessage, EnhancedMessageType, EnhancedPayload,
    HandshakeManager,
};
pub use error::{NetworkError, NetworkResult};
pub use flow_control::{BackpressureDetector, ConnectionFlowControl, FlowControlConfig};
pub use observability::{HealthChecker, NetworkDiagnostics, NetworkMetrics};
pub use production::{ProductionNetworkBuilder, ProductionNetworkConfig, ProductionNetworkLayer};
pub use protocol::{DistributedMessage, MessagePayload, MessageType};
pub use protocol_v2::{
    BinaryProtocolCodec, DistributedMessageV2, MessagePayloadV2, MessageTypeV2, ProtocolNegotiator,
};
pub use reconnection::{
    ReconnectionConfig, ReconnectionError, ReconnectionManager, ReconnectionStats,
};
pub use secure_protocol::{
    MessageType as SecureMessageType, ProtocolError, ProtocolVersion, SecureFrame, SecureProtocol,
};
pub use security::{SecurityConfig, SecurityManager, SecurityToken};
pub use tls::{TlsConfig, TlsManager};
pub use transport::NetworkTransport;
