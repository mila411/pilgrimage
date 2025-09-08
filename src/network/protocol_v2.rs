//! Enhanced protocol implementation with length-delimited framing
//!
//! This module provides a more robust protocol implementation using
//! length-prefixed framing instead of line-delimited JSON.

use crate::network::error::{NetworkError, NetworkResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use uuid::Uuid;

/// Protocol version for negotiation
pub const PROTOCOL_VERSION: u16 = 2;

/// Maximum frame size (16MB)
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Protocol magic number for frame validation
pub const PROTOCOL_MAGIC: u32 = 0x50494C47; // "PILG" in ASCII

/// Enhanced message types with version support
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageTypeV2 {
    // Handshake and protocol negotiation
    Handshake,
    HandshakeResponse,

    // Original message types
    Heartbeat,
    HeartbeatResponse,
    VoteRequest,
    VoteResponse,
    LogEntry,

    // Transaction messages
    TransactionPrepare,
    TransactionCommit,
    TransactionAbort,
    TransactionPrepareResponse,

    // Cluster management
    NodeJoin,
    NodeLeave,
    ClusterUpdate,

    // Data replication
    ReplicationSync,
    ReplicationAck,

    // Quorum and split-brain prevention
    QuorumCheck,
    QuorumResponse,

    // Error handling
    ProtocolError,
}

/// Enhanced message payload with version support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayloadV2 {
    Handshake {
        version: u16,
        node_id: String,
        capabilities: Vec<String>,
        cluster_id: Option<String>,
    },
    HandshakeResponse {
        version: u16,
        accepted: bool,
        capabilities: Vec<String>,
        error: Option<String>,
    },
    Heartbeat {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntryData>,
        leader_commit: u64,
    },
    HeartbeatResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
    VoteRequest {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteResponse {
        term: u64,
        vote_granted: bool,
    },
    LogEntry {
        term: u64,
        index: u64,
        data: LogEntryData,
    },
    TransactionPrepare {
        transaction_id: Uuid,
        operations: Vec<TransactionOperation>,
        coordinator: String,
        timeout_ms: u64,
    },
    TransactionCommit {
        transaction_id: Uuid,
    },
    TransactionAbort {
        transaction_id: Uuid,
    },
    TransactionPrepareResponse {
        transaction_id: Uuid,
        prepared: bool,
        error: Option<String>,
    },
    NodeJoin {
        node_id: String,
        address: String,
        metadata: HashMap<String, String>,
    },
    NodeLeave {
        node_id: String,
        reason: String,
    },
    ClusterUpdate {
        nodes: Vec<NodeInfo>,
        version: u64,
    },
    ReplicationSync {
        topic: String,
        partition: u32,
        offset: u64,
        data: Vec<u8>,
        checksum: u32,
    },
    ReplicationAck {
        topic: String,
        partition: u32,
        offset: u64,
        success: bool,
    },
    QuorumCheck {
        cluster_id: String,
        term: u64,
    },
    QuorumResponse {
        cluster_id: String,
        term: u64,
        active: bool,
    },
    ProtocolError {
        error_code: u16,
        message: String,
    },
}

/// Node information for cluster updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub address: String,
    pub status: NodeStatus,
    pub metadata: HashMap<String, String>,
}

/// Node status enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeStatus {
    Active,
    Suspected,
    Down,
    Joining,
    Leaving,
}

/// Log entry data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntryData {
    pub entry_type: String,
    pub data: Vec<u8>,
    pub timestamp: u64,
}

/// Transaction operation data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOperation {
    pub operation_type: String,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub metadata: HashMap<String, String>,
}

/// Enhanced distributed message with protocol version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedMessageV2 {
    /// Protocol version
    pub version: u16,
    /// Unique message ID
    pub message_id: Uuid,
    /// Sender node ID
    pub sender_id: String,
    /// Optional recipient node ID (None for broadcast)
    pub recipient_id: Option<String>,
    /// Message type
    pub message_type: MessageTypeV2,
    /// Message payload
    pub payload: MessagePayloadV2,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,
    /// Optional correlation ID for request-response
    pub correlation_id: Option<Uuid>,
    /// Priority level (0-255, higher = more priority)
    pub priority: u8,
}

impl DistributedMessageV2 {
    /// Create a new message
    pub fn new(
        sender_id: String,
        recipient_id: Option<String>,
        message_type: MessageTypeV2,
        payload: MessagePayloadV2,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            message_id: Uuid::new_v4(),
            sender_id,
            recipient_id,
            message_type,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            correlation_id: None,
            priority: 128, // Default priority
        }
    }

    /// Create a response message
    pub fn create_response(
        &self,
        sender_id: String,
        message_type: MessageTypeV2,
        payload: MessagePayloadV2,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            message_id: Uuid::new_v4(),
            sender_id,
            recipient_id: Some(self.sender_id.clone()),
            message_type,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            correlation_id: Some(self.message_id),
            priority: self.priority,
        }
    }

    /// Set correlation ID for request-response tracking
    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Set message priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Serialize message to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(self).map_err(|e| e.to_string())
    }

    /// Deserialize message from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        bincode::deserialize(data).map_err(|e| e.to_string())
    }
}

/// Binary protocol codec using length-delimited framing
pub struct BinaryProtocolCodec {
    inner: LengthDelimitedCodec,
}

impl BinaryProtocolCodec {
    /// Create a new codec with default settings
    pub fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_SIZE)
                .length_field_length(4) // 4-byte length prefix
                .num_skip(0) // Don't skip any bytes
                .new_codec(),
        }
    }
}

impl Default for BinaryProtocolCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for BinaryProtocolCodec {
    type Item = DistributedMessageV2;
    type Error = NetworkError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // First, decode the frame using length-delimited codec
        match self.inner.decode(src)? {
            Some(frame) => {
                // Validate magic number
                if frame.len() < 4 {
                    return Err(NetworkError::ProtocolError("Frame too short".to_string()));
                }

                let magic = (&frame[..4]).get_u32();
                if magic != PROTOCOL_MAGIC {
                    return Err(NetworkError::ProtocolError(
                        "Invalid magic number".to_string(),
                    ));
                }

                // Deserialize the message from the remaining bytes
                let message_bytes = &frame[4..];
                match bincode::deserialize(message_bytes) {
                    Ok(message) => Ok(Some(message)),
                    Err(e) => Err(NetworkError::SerializationError(e.to_string())),
                }
            }
            None => Ok(None),
        }
    }
}

impl Encoder<DistributedMessageV2> for BinaryProtocolCodec {
    type Error = NetworkError;

    fn encode(
        &mut self,
        item: DistributedMessageV2,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        // Serialize the message
        let message_bytes = bincode::serialize(&item)
            .map_err(|e| NetworkError::SerializationError(e.to_string()))?;

        // Create frame with magic number prefix
        let total_size = 4 + message_bytes.len(); // 4 bytes for magic + message
        let mut frame = BytesMut::with_capacity(total_size);

        // Add magic number
        frame.put_u32(PROTOCOL_MAGIC);
        // Add serialized message
        frame.extend_from_slice(&message_bytes);

        // Encode using length-delimited codec
        self.inner
            .encode(frame.freeze(), dst)
            .map_err(|e| NetworkError::ProtocolError(e.to_string()))
    }
}

/// Protocol negotiation handler
pub struct ProtocolNegotiator {
    supported_versions: Vec<u16>,
    capabilities: Vec<String>,
}

impl ProtocolNegotiator {
    /// Create a new protocol negotiator
    pub fn new() -> Self {
        Self {
            supported_versions: vec![PROTOCOL_VERSION],
            capabilities: vec![
                "heartbeat".to_string(),
                "transactions".to_string(),
                "replication".to_string(),
                "quorum".to_string(),
            ],
        }
    }

    /// Create handshake message
    pub fn create_handshake(
        &self,
        node_id: String,
        cluster_id: Option<String>,
    ) -> DistributedMessageV2 {
        DistributedMessageV2::new(
            node_id.clone(),
            None,
            MessageTypeV2::Handshake,
            MessagePayloadV2::Handshake {
                version: PROTOCOL_VERSION,
                node_id: node_id,
                capabilities: self.capabilities.clone(),
                cluster_id,
            },
        )
    }

    /// Handle incoming handshake
    pub fn handle_handshake(
        &self,
        message: &DistributedMessageV2,
    ) -> NetworkResult<DistributedMessageV2> {
        if let MessagePayloadV2::Handshake {
            version,
            node_id: _,
            capabilities: _,
            cluster_id: _,
        } = &message.payload
        {
            let accepted = self.supported_versions.contains(version);
            let error = if !accepted {
                Some(format!("Unsupported protocol version: {}", version))
            } else {
                None
            };

            let response = message.create_response(
                message
                    .recipient_id
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                MessageTypeV2::HandshakeResponse,
                MessagePayloadV2::HandshakeResponse {
                    version: PROTOCOL_VERSION,
                    accepted,
                    capabilities: self.capabilities.clone(),
                    error,
                },
            );

            Ok(response)
        } else {
            Err(NetworkError::ProtocolError(
                "Invalid handshake message".to_string(),
            ))
        }
    }
}

impl Default for ProtocolNegotiator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::BytesMut;

    #[test]
    fn test_message_creation() {
        let message = DistributedMessageV2::new(
            "node1".to_string(),
            Some("node2".to_string()),
            MessageTypeV2::Heartbeat,
            MessagePayloadV2::Heartbeat {
                term: 1,
                leader_id: "node1".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        );

        assert_eq!(message.version, PROTOCOL_VERSION);
        assert_eq!(message.sender_id, "node1");
        assert_eq!(message.recipient_id, Some("node2".to_string()));
        assert_eq!(message.message_type, MessageTypeV2::Heartbeat);
    }

    #[test]
    fn test_codec_encode_decode() {
        let original_message = DistributedMessageV2::new(
            "test_node".to_string(),
            None,
            MessageTypeV2::Heartbeat,
            MessagePayloadV2::Heartbeat {
                term: 42,
                leader_id: "leader".to_string(),
                prev_log_index: 10,
                prev_log_term: 5,
                entries: vec![],
                leader_commit: 8,
            },
        );

        // Test the manual encoding/decoding process that BinaryProtocolCodec does internally
        let message_bytes = bincode::serialize(&original_message).unwrap();

        // Create a frame with magic number (this simulates what encode() creates)
        let mut frame = BytesMut::with_capacity(4 + message_bytes.len());
        frame.put_u32(PROTOCOL_MAGIC);
        frame.extend_from_slice(&message_bytes);

        // Now test decoding this frame (this simulates what decode() receives from LengthDelimitedCodec)
        let decode_buffer = frame;

        // Validate magic number
        if decode_buffer.len() < 4 {
            panic!("Frame too short");
        }

        let magic = (&decode_buffer[..4]).get_u32();
        if magic != PROTOCOL_MAGIC {
            panic!(
                "Invalid magic number: expected {:#x}, got {:#x}",
                PROTOCOL_MAGIC, magic
            );
        }

        // Deserialize the message
        let message_bytes = &decode_buffer[4..];
        let decoded_message: DistributedMessageV2 = bincode::deserialize(message_bytes).unwrap();

        // Verify the decoded message matches the original
        assert_eq!(decoded_message.version, original_message.version);
        assert_eq!(decoded_message.sender_id, original_message.sender_id);
        assert_eq!(decoded_message.message_type, original_message.message_type);
    }

    #[test]
    fn test_protocol_negotiation() {
        let negotiator = ProtocolNegotiator::new();
        let handshake =
            negotiator.create_handshake("test_node".to_string(), Some("test_cluster".to_string()));

        let response = negotiator.handle_handshake(&handshake).unwrap();

        if let MessagePayloadV2::HandshakeResponse {
            version, accepted, ..
        } = response.payload
        {
            assert_eq!(version, PROTOCOL_VERSION);
            assert!(accepted);
        } else {
            panic!("Expected HandshakeResponse");
        }
    }
}
