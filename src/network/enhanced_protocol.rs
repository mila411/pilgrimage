//! Enhanced Protocol with Length-Delimited Framing
//!
//! Production-ready protocol implementation with proper framing,
//! handshake, and version negotiation

use crate::network::error::{NetworkError, NetworkResult};
use crate::network::enhanced_config::EnhancedNetworkConfig;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use uuid::Uuid;

/// Protocol version for compatibility checking
pub const PROTOCOL_VERSION: u16 = 1;

/// Enhanced protocol message with proper framing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedMessage {
    /// Message unique identifier
    pub id: Uuid,
    /// Protocol version
    pub version: u16,
    /// Source node identifier
    pub source_node: String,
    /// Target node identifier (None for broadcast)
    pub target_node: Option<String>,
    /// Message type
    pub message_type: EnhancedMessageType,
    /// Message payload
    pub payload: EnhancedPayload,
    /// Message timestamp (microseconds since UNIX epoch)
    pub timestamp: u64,
    /// Message metadata
    pub metadata: HashMap<String, String>,
}

/// Enhanced message types with handshake support
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EnhancedMessageType {
    /// Connection handshake
    Handshake,
    /// Handshake acknowledgment
    HandshakeAck,
    /// Regular data message
    Data,
    /// Heartbeat/keep-alive
    Heartbeat,
    /// Health check
    HealthCheck,
    /// Node discovery
    Discovery,
    /// Cluster management
    ClusterControl,
    /// Error response
    Error,
}

/// Enhanced message payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EnhancedPayload {
    /// Handshake payload with node capabilities
    Handshake {
        node_id: String,
        capabilities: Vec<String>,
        supported_versions: Vec<u16>,
        cluster_id: Option<String>,
    },
    /// Handshake acknowledgment
    HandshakeAck {
        accepted: bool,
        negotiated_version: u16,
        server_capabilities: Vec<String>,
    },
    /// Raw binary data (using Vec<u8> for serde compatibility)
    Data(Vec<u8>),
    /// JSON data
    Json(serde_json::Value),
    /// Heartbeat with node status
    Heartbeat {
        node_status: String,
        load_average: f64,
        memory_usage: f64,
    },
    /// Health check response
    HealthCheck {
        healthy: bool,
        uptime_seconds: u64,
        active_connections: u32,
    },
    /// Node discovery information
    Discovery {
        node_info: NodeDiscoveryInfo,
    },
    /// Cluster control command
    ClusterControl {
        command: String,
        parameters: HashMap<String, String>,
    },
    /// Error information
    Error {
        error_code: u32,
        error_message: String,
        retry_after: Option<u64>,
    },
}

/// Node discovery information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDiscoveryInfo {
    pub node_id: String,
    pub address: String,
    pub port: u16,
    pub node_type: String,
    pub cluster_id: Option<String>,
    pub capabilities: Vec<String>,
    pub metadata: HashMap<String, String>,
}

/// Enhanced codec with length-delimited framing
#[derive(Clone)]
pub struct EnhancedCodec {
    inner: LengthDelimitedCodec,
    config: EnhancedNetworkConfig,
}

impl EnhancedCodec {
    /// Create new enhanced codec with configuration
    pub fn new(config: EnhancedNetworkConfig) -> Self {
        let inner = LengthDelimitedCodec::builder()
            .length_field_offset(0) // Length field at the beginning
            .length_field_length(4) // 4 bytes for length (u32)
            .length_adjustment(0)   // No adjustment needed
            .num_skip(0)           // Don't skip length field
            .max_frame_length(config.max_frame_size) // Prevent DoS attacks
            .new_codec();

        Self { inner, config }
    }

    /// Create handshake message
    pub fn create_handshake(
        node_id: String,
        capabilities: Vec<String>,
        cluster_id: Option<String>,
    ) -> EnhancedMessage {
        EnhancedMessage {
            id: Uuid::new_v4(),
            version: PROTOCOL_VERSION,
            source_node: node_id.clone(),
            target_node: None,
            message_type: EnhancedMessageType::Handshake,
            payload: EnhancedPayload::Handshake {
                node_id,
                capabilities,
                supported_versions: vec![PROTOCOL_VERSION],
                cluster_id,
            },
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            metadata: HashMap::new(),
        }
    }

    /// Create handshake acknowledgment
    pub fn create_handshake_ack(
        source_node: String,
        target_node: String,
        accepted: bool,
        server_capabilities: Vec<String>,
    ) -> EnhancedMessage {
        EnhancedMessage {
            id: Uuid::new_v4(),
            version: PROTOCOL_VERSION,
            source_node,
            target_node: Some(target_node),
            message_type: EnhancedMessageType::HandshakeAck,
            payload: EnhancedPayload::HandshakeAck {
                accepted,
                negotiated_version: PROTOCOL_VERSION,
                server_capabilities,
            },
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            metadata: HashMap::new(),
        }
    }
}

impl Decoder for EnhancedCodec {
    type Item = EnhancedMessage;
    type Error = NetworkError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Use length-delimited codec to get complete frame
        match self.inner.decode(src)? {
            Some(frame) => {
                // Check frame size against configuration
                if frame.len() > self.config.max_frame_size {
                    return Err(NetworkError::ProtocolError(
                        format!("Frame too large: {} bytes (max: {})",
                               frame.len(), self.config.max_frame_size)
                    ));
                }

                // Deserialize the message
                let message: EnhancedMessage = bincode::deserialize(&frame)
                    .map_err(|e| NetworkError::SerializationError(format!("Decode error: {}", e)))?;

                // Validate protocol version
                if message.version != PROTOCOL_VERSION {
                    return Err(NetworkError::ProtocolError(
                        format!("Unsupported protocol version: {} (expected: {})",
                               message.version, PROTOCOL_VERSION)
                    ));
                }

                Ok(Some(message))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<EnhancedMessage> for EnhancedCodec {
    type Error = NetworkError;

    fn encode(&mut self, item: EnhancedMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize the message
        let serialized = bincode::serialize(&item)
            .map_err(|e| NetworkError::SerializationError(format!("Encode error: {}", e)))?;

        // Check message size
        if serialized.len() > self.config.max_frame_size {
            return Err(NetworkError::ProtocolError(
                format!("Message too large: {} bytes (max: {})",
                       serialized.len(), self.config.max_frame_size)
            ));
        }

        // Use length-delimited codec to frame the message
        let bytes = Bytes::from(serialized);
        self.inner.encode(bytes, dst)
            .map_err(|e| NetworkError::IoError(format!("Frame encoding error: {}", e)))?;

        Ok(())
    }
}

/// Connection state machine for handshake management
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    /// Initial state - no connection
    Disconnected,
    /// TCP connection established, waiting for handshake
    Connected,
    /// Handshake sent, waiting for acknowledgment
    HandshakeSent,
    /// Handshake received, sending acknowledgment
    HandshakeReceived,
    /// Handshake completed successfully
    Established,
    /// Connection failed or rejected
    Failed(String),
}

/// Connection handshake manager
pub struct HandshakeManager {
    state: ConnectionState,
    node_id: String,
    capabilities: Vec<String>,
    cluster_id: Option<String>,
    peer_capabilities: Option<Vec<String>>,
    negotiated_version: Option<u16>,
}

impl HandshakeManager {
    /// Create new handshake manager
    pub fn new(node_id: String, capabilities: Vec<String>, cluster_id: Option<String>) -> Self {
        Self {
            state: ConnectionState::Disconnected,
            node_id,
            capabilities,
            cluster_id,
            peer_capabilities: None,
            negotiated_version: None,
        }
    }

    /// Get current connection state
    pub fn state(&self) -> &ConnectionState {
        &self.state
    }

    /// Check if handshake is completed
    pub fn is_established(&self) -> bool {
        matches!(self.state, ConnectionState::Established)
    }

    /// Initiate handshake
    pub fn initiate_handshake(&mut self) -> NetworkResult<EnhancedMessage> {
        if !matches!(self.state, ConnectionState::Connected) {
            return Err(NetworkError::ProtocolError(
                "Cannot initiate handshake in current state".to_string()
            ));
        }

        self.state = ConnectionState::HandshakeSent;

        Ok(EnhancedCodec::create_handshake(
            self.node_id.clone(),
            self.capabilities.clone(),
            self.cluster_id.clone(),
        ))
    }

    /// Handle incoming handshake message
    pub fn handle_handshake(&mut self, message: &EnhancedMessage) -> NetworkResult<Option<EnhancedMessage>> {
        match &message.payload {
            EnhancedPayload::Handshake { node_id, capabilities, supported_versions, cluster_id } => {
                // Validate cluster ID if specified
                if let (Some(our_cluster), Some(peer_cluster)) = (&self.cluster_id, cluster_id) {
                    if our_cluster != peer_cluster {
                        self.state = ConnectionState::Failed("Cluster ID mismatch".to_string());
                        return Ok(Some(EnhancedCodec::create_handshake_ack(
                            self.node_id.clone(),
                            node_id.clone(),
                            false,
                            vec![],
                        )));
                    }
                }

                // Check version compatibility
                if !supported_versions.contains(&PROTOCOL_VERSION) {
                    self.state = ConnectionState::Failed("Protocol version incompatible".to_string());
                    return Ok(Some(EnhancedCodec::create_handshake_ack(
                        self.node_id.clone(),
                        node_id.clone(),
                        false,
                        vec![],
                    )));
                }

                // Accept handshake
                self.peer_capabilities = Some(capabilities.clone());
                self.negotiated_version = Some(PROTOCOL_VERSION);
                self.state = ConnectionState::HandshakeReceived;

                Ok(Some(EnhancedCodec::create_handshake_ack(
                    self.node_id.clone(),
                    node_id.clone(),
                    true,
                    self.capabilities.clone(),
                )))
            }
            EnhancedPayload::HandshakeAck { accepted, negotiated_version, server_capabilities } => {
                if !accepted {
                    self.state = ConnectionState::Failed("Handshake rejected".to_string());
                    return Err(NetworkError::ConnectionError("Handshake rejected by peer".to_string()));
                }

                self.peer_capabilities = Some(server_capabilities.clone());
                self.negotiated_version = Some(*negotiated_version);
                self.state = ConnectionState::Established;

                Ok(None) // Handshake complete
            }
            _ => Err(NetworkError::ProtocolError(
                "Expected handshake message".to_string()
            ))
        }
    }

    /// Reset handshake state
    pub fn reset(&mut self) {
        self.state = ConnectionState::Disconnected;
        self.peer_capabilities = None;
        self.negotiated_version = None;
    }

    /// Mark connection as established (TCP connected)
    pub fn mark_connected(&mut self) {
        self.state = ConnectionState::Connected;
    }
}
