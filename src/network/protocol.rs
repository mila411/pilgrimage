use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Import transaction operation from distributed storage
use crate::broker::distributed_storage::TransactionOperation;

/// Distributed messaging protocol for broker communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedMessage {
    pub id: Uuid,
    pub sender_id: String,
    pub receiver_id: Option<String>, // None for broadcast
    pub timestamp: DateTime<Utc>,
    pub message_type: MessageType,
    pub payload: MessagePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// Leader election messages
    VoteRequest,
    VoteResponse,

    /// Heartbeat messages
    Heartbeat,
    HeartbeatResponse,

    /// Data replication messages
    ReplicationRequest,
    ReplicationResponse,

    /// Consensus messages
    PreparePhase,
    PromisePhase,
    AcceptPhase,
    AcceptedPhase,

    /// Cluster management
    NodeJoin,
    NodeLeave,

    /// Split-brain prevention
    QuorumCheck,
    QuorumResponse,

    /// Distributed transactions (ACID)
    TransactionPrepare,
    TransactionCommit,
    TransactionAbort,
    TransactionPrepareResponse,

    /// Storage operations
    ReadRequest,
    ReadResponse,

    /// Backup and recovery
    BackupRequest,
    BackupResponse,
    RestoreRequest,
    RestoreResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayload {
    VoteRequest {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteResponse {
        term: u64,
        vote_granted: bool,
        voter_id: String,
    },
    Heartbeat {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    HeartbeatResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
    ReplicationRequest {
        partition_id: String,
        data: Vec<u8>,
        checksum: String,
        sequence_number: u64,
    },
    ReplicationResponse {
        partition_id: String,
        success: bool,
        error_message: Option<String>,
    },
    PreparePhase {
        proposal_number: u64,
        value: Option<Vec<u8>>,
    },
    PromisePhase {
        proposal_number: u64,
        accepted_proposal: Option<u64>,
        accepted_value: Option<Vec<u8>>,
    },
    AcceptPhase {
        proposal_number: u64,
        value: Vec<u8>,
    },
    AcceptedPhase {
        proposal_number: u64,
    },
    NodeJoin {
        node_id: String,
        address: String,
        capabilities: Vec<String>,
    },
    NodeLeave {
        node_id: String,
        reason: String,
    },
    QuorumCheck {
        cluster_id: String,
        term: u64,
    },
    QuorumResponse {
        cluster_id: String,
        term: u64,
        nodes_seen: Vec<String>,
    },
    TransactionPrepare {
        transaction_id: Uuid,
        operations: Vec<TransactionOperation>,
        coordinator: String,
    },
    TransactionCommit {
        transaction_id: Uuid,
    },
    TransactionAbort {
        transaction_id: Uuid,
    },
    TransactionPrepareResponse {
        transaction_id: Uuid,
        can_commit: bool,
        error_message: Option<String>,
    },
    ReadRequest {
        topic: String,
        partition: usize,
    },
    ReadResponse {
        topic: String,
        partition: usize,
        messages: Vec<u8>, // Serialized messages
        success: bool,
        error_message: Option<String>,
    },
    BackupRequest {
        backup_type: String,
        target_path: Option<String>,
    },
    BackupResponse {
        success: bool,
        backup_path: Option<String>,
        backup_size: u64,
        error_message: Option<String>,
    },
    RestoreRequest {
        backup_id: Uuid,
        restore_options: Vec<u8>, // Serialized RecoveryOptions
    },
    RestoreResponse {
        success: bool,
        restore_progress: f64,
        error_message: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
    pub checksum: String,
}

impl DistributedMessage {
    pub fn new(
        sender_id: String,
        receiver_id: Option<String>,
        message_type: MessageType,
        payload: MessagePayload,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            sender_id,
            receiver_id,
            timestamp: Utc::now(),
            message_type,
            payload,
        }
    }

    pub fn is_broadcast(&self) -> bool {
        self.receiver_id.is_none()
    }

    pub fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::distributed_storage::TransactionOperation;

    #[test]
    fn test_distributed_message_creation() {
        let payload = MessagePayload::NodeJoin {
            node_id: "node1".to_string(),
            address: "127.0.0.1:8080".to_string(),
            capabilities: vec!["leader".to_string(), "storage".to_string()],
        };

        let message = DistributedMessage::new(
            "sender1".to_string(),
            Some("receiver1".to_string()),
            MessageType::NodeJoin,
            payload,
        );

        assert_eq!(message.sender_id, "sender1");
        assert_eq!(message.receiver_id, Some("receiver1".to_string()));
        assert_eq!(message.message_type, MessageType::NodeJoin);
        assert!(!message.id.to_string().is_empty());
    }

    #[test]
    fn test_distributed_message_broadcast() {
        let payload = MessagePayload::NodeLeave {
            node_id: "node1".to_string(),
            reason: "shutdown".to_string(),
        };

        let message = DistributedMessage::new(
            "sender1".to_string(),
            None, // Broadcast
            MessageType::NodeLeave,
            payload,
        );

        assert_eq!(message.sender_id, "sender1");
        assert!(message.receiver_id.is_none());
        assert_eq!(message.message_type, MessageType::NodeLeave);
    }

    #[test]
    fn test_vote_request_payload() {
        let payload = MessagePayload::VoteRequest {
            term: 5,
            candidate_id: "candidate_1".to_string(),
            last_log_index: 10,
            last_log_term: 3,
        };

        let message = DistributedMessage::new(
            "candidate_1".to_string(),
            None,
            MessageType::VoteRequest,
            payload,
        );

        if let MessagePayload::VoteRequest {
            term, candidate_id, ..
        } = &message.payload
        {
            assert_eq!(*term, 5);
            assert_eq!(candidate_id, "candidate_1");
        } else {
            panic!("Expected VoteRequest payload");
        }
    }

    #[test]
    fn test_vote_response_payload() {
        let payload = MessagePayload::VoteResponse {
            term: 5,
            vote_granted: true,
            voter_id: "voter_1".to_string(),
        };

        let message = DistributedMessage::new(
            "voter_1".to_string(),
            Some("candidate_1".to_string()),
            MessageType::VoteResponse,
            payload,
        );

        if let MessagePayload::VoteResponse {
            term,
            vote_granted,
            voter_id,
        } = &message.payload
        {
            assert_eq!(*term, 5);
            assert!(*vote_granted);
            assert_eq!(voter_id, "voter_1");
        } else {
            panic!("Expected VoteResponse payload");
        }
    }

    #[test]
    fn test_heartbeat_payload() {
        let entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                data: b"test_command_1".to_vec(),
                checksum: "checksum1".to_string(),
            },
            LogEntry {
                term: 1,
                index: 2,
                data: b"test_command_2".to_vec(),
                checksum: "checksum2".to_string(),
            },
        ];

        let payload = MessagePayload::Heartbeat {
            term: 3,
            leader_id: "leader_1".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: entries.clone(),
            leader_commit: 2,
        };

        let message = DistributedMessage::new(
            "leader_1".to_string(),
            None,
            MessageType::Heartbeat,
            payload,
        );

        if let MessagePayload::Heartbeat {
            term,
            leader_id,
            entries: msg_entries,
            ..
        } = &message.payload
        {
            assert_eq!(*term, 3);
            assert_eq!(leader_id, "leader_1");
            assert_eq!(msg_entries.len(), 2);
            assert_eq!(msg_entries[0].data, b"test_command_1");
        } else {
            panic!("Expected Heartbeat payload");
        }
    }

    #[test]
    fn test_replication_request_payload() {
        let test_data = b"test replication data";
        let payload = MessagePayload::ReplicationRequest {
            partition_id: "partition_1".to_string(),
            data: test_data.to_vec(),
            checksum: "abc123".to_string(),
            sequence_number: 42,
        };

        let message = DistributedMessage::new(
            "leader".to_string(),
            Some("follower".to_string()),
            MessageType::ReplicationRequest,
            payload,
        );

        if let MessagePayload::ReplicationRequest {
            partition_id,
            data,
            checksum,
            sequence_number,
        } = &message.payload
        {
            assert_eq!(partition_id, "partition_1");
            assert_eq!(data, test_data);
            assert_eq!(checksum, "abc123");
            assert_eq!(*sequence_number, 42);
        } else {
            panic!("Expected ReplicationRequest payload");
        }
    }

    #[test]
    fn test_replication_response_payload() {
        let payload = MessagePayload::ReplicationResponse {
            partition_id: "partition_1".to_string(),
            success: false,
            error_message: Some("Checksum mismatch".to_string()),
        };

        let message = DistributedMessage::new(
            "follower".to_string(),
            Some("leader".to_string()),
            MessageType::ReplicationResponse,
            payload,
        );

        if let MessagePayload::ReplicationResponse {
            partition_id,
            success,
            error_message,
        } = &message.payload
        {
            assert_eq!(partition_id, "partition_1");
            assert!(!success);
            assert_eq!(error_message.as_ref().unwrap(), "Checksum mismatch");
        } else {
            panic!("Expected ReplicationResponse payload");
        }
    }

    #[test]
    fn test_transaction_payloads() {
        let message = crate::message::message::Message::new("test message".to_string());

        let operation = TransactionOperation::WriteMessage {
            message,
            topic: "test_topic".to_string(),
            partition: 0,
        };

        let transaction_id = Uuid::new_v4();
        let prepare_payload = MessagePayload::TransactionPrepare {
            transaction_id,
            operations: vec![operation.clone()],
            coordinator: "coordinator_node".to_string(),
        };

        let message = DistributedMessage::new(
            "coordinator".to_string(),
            Some("participant".to_string()),
            MessageType::TransactionPrepare,
            prepare_payload,
        );

        if let MessagePayload::TransactionPrepare {
            transaction_id: tx_id,
            operations,
            coordinator,
        } = &message.payload
        {
            assert_eq!(*tx_id, transaction_id);
            assert_eq!(operations.len(), 1);
            assert_eq!(coordinator, "coordinator_node");
        } else {
            panic!("Expected TransactionPrepare payload");
        }
    }

    #[test]
    fn test_consensus_payloads() {
        let test_value = b"consensus_value";

        // Test PreparePhase
        let prepare_payload = MessagePayload::PreparePhase {
            proposal_number: 10,
            value: Some(test_value.to_vec()),
        };

        let message = DistributedMessage::new(
            "proposer".to_string(),
            None,
            MessageType::PreparePhase,
            prepare_payload,
        );

        if let MessagePayload::PreparePhase {
            proposal_number,
            value,
        } = &message.payload
        {
            assert_eq!(*proposal_number, 10);
            assert_eq!(value.as_ref().unwrap(), test_value);
        } else {
            panic!("Expected PreparePhase payload");
        }

        // Test PromisePhase
        let promise_payload = MessagePayload::PromisePhase {
            proposal_number: 10,
            accepted_proposal: Some(8),
            accepted_value: Some(b"previous_value".to_vec()),
        };

        let promise_message = DistributedMessage::new(
            "acceptor".to_string(),
            Some("proposer".to_string()),
            MessageType::PromisePhase,
            promise_payload,
        );

        if let MessagePayload::PromisePhase {
            proposal_number,
            accepted_proposal,
            accepted_value,
        } = &promise_message.payload
        {
            assert_eq!(*proposal_number, 10);
            assert_eq!(*accepted_proposal, Some(8));
            assert_eq!(accepted_value.as_ref().unwrap(), b"previous_value");
        } else {
            panic!("Expected PromisePhase payload");
        }
    }

    #[test]
    fn test_backup_restore_payloads() {
        // Test BackupRequest
        let backup_payload = MessagePayload::BackupRequest {
            backup_type: "full".to_string(),
            target_path: Some("/backups/full_backup".to_string()),
        };

        let backup_message = DistributedMessage::new(
            "admin".to_string(),
            Some("storage_node".to_string()),
            MessageType::BackupRequest,
            backup_payload,
        );

        if let MessagePayload::BackupRequest {
            backup_type,
            target_path,
        } = &backup_message.payload
        {
            assert_eq!(backup_type, "full");
            assert_eq!(target_path.as_ref().unwrap(), "/backups/full_backup");
        } else {
            panic!("Expected BackupRequest payload");
        }

        // Test RestoreRequest
        let restore_id = Uuid::new_v4();
        let restore_payload = MessagePayload::RestoreRequest {
            backup_id: restore_id,
            restore_options: b"restore_config".to_vec(),
        };

        let restore_message = DistributedMessage::new(
            "admin".to_string(),
            Some("storage_node".to_string()),
            MessageType::RestoreRequest,
            restore_payload,
        );

        if let MessagePayload::RestoreRequest {
            backup_id,
            restore_options,
        } = &restore_message.payload
        {
            assert_eq!(*backup_id, restore_id);
            assert_eq!(restore_options, b"restore_config");
        } else {
            panic!("Expected RestoreRequest payload");
        }
    }

    #[test]
    fn test_message_serialization() {
        let payload = MessagePayload::VoteRequest {
            term: 5,
            candidate_id: "candidate_1".to_string(),
            last_log_index: 10,
            last_log_term: 3,
        };

        let message = DistributedMessage::new(
            "sender".to_string(),
            Some("receiver".to_string()),
            MessageType::VoteRequest,
            payload,
        );

        // Test serialization
        let serialized = message.serialize().unwrap();
        assert!(!serialized.is_empty());

        // Test deserialization
        let deserialized = DistributedMessage::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.sender_id, message.sender_id);
        assert_eq!(deserialized.receiver_id, message.receiver_id);
        assert_eq!(deserialized.message_type, message.message_type);
        assert_eq!(deserialized.id, message.id);
    }

    #[test]
    fn test_message_serialization_with_complex_payload() {
        let entries = vec![LogEntry {
            term: 1,
            index: 1,
            data: b"complex_command".to_vec(),
            checksum: "complex_checksum".to_string(),
        }];

        let payload = MessagePayload::Heartbeat {
            term: 10,
            leader_id: "leader_complex".to_string(),
            prev_log_index: 5,
            prev_log_term: 2,
            entries,
            leader_commit: 8,
        };

        let message = DistributedMessage::new(
            "leader_complex".to_string(),
            None,
            MessageType::Heartbeat,
            payload,
        );

        let serialized = message.serialize().unwrap();
        let deserialized = DistributedMessage::deserialize(&serialized).unwrap();

        if let MessagePayload::Heartbeat {
            term,
            leader_id,
            entries,
            ..
        } = &deserialized.payload
        {
            assert_eq!(*term, 10);
            assert_eq!(leader_id, "leader_complex");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].data, b"complex_command");
        } else {
            panic!("Expected Heartbeat payload after deserialization");
        }
    }

    #[test]
    fn test_quorum_check_payload() {
        let payload = MessagePayload::QuorumCheck {
            cluster_id: "cluster_1".to_string(),
            term: 5,
        };

        let message = DistributedMessage::new(
            "leader".to_string(),
            None,
            MessageType::QuorumCheck,
            payload,
        );

        if let MessagePayload::QuorumCheck { cluster_id, term } = &message.payload {
            assert_eq!(cluster_id, "cluster_1");
            assert_eq!(*term, 5);
        } else {
            panic!("Expected QuorumCheck payload");
        }

        // Test QuorumResponse
        let response_payload = MessagePayload::QuorumResponse {
            cluster_id: "cluster_1".to_string(),
            term: 5,
            nodes_seen: vec!["node1".to_string(), "node2".to_string()],
        };

        let response_message = DistributedMessage::new(
            "follower".to_string(),
            Some("leader".to_string()),
            MessageType::QuorumResponse,
            response_payload,
        );

        if let MessagePayload::QuorumResponse {
            cluster_id,
            term,
            nodes_seen,
        } = &response_message.payload
        {
            assert_eq!(cluster_id, "cluster_1");
            assert_eq!(*term, 5);
            assert_eq!(nodes_seen.len(), 2);
            assert!(nodes_seen.contains(&"node1".to_string()));
        } else {
            panic!("Expected QuorumResponse payload");
        }
    }

    #[test]
    fn test_message_type_equality() {
        assert_eq!(MessageType::VoteRequest, MessageType::VoteRequest);
        assert_ne!(MessageType::VoteRequest, MessageType::VoteResponse);
        assert_eq!(MessageType::Heartbeat, MessageType::Heartbeat);
    }

    #[test]
    fn test_message_with_large_data() {
        let large_data = vec![0u8; 10000]; // 10KB of data
        let payload = MessagePayload::ReplicationRequest {
            partition_id: "large_partition".to_string(),
            data: large_data.clone(),
            checksum: "large_checksum".to_string(),
            sequence_number: 999,
        };

        let message = DistributedMessage::new(
            "sender".to_string(),
            Some("receiver".to_string()),
            MessageType::ReplicationRequest,
            payload,
        );

        let serialized = message.serialize().unwrap();
        let deserialized = DistributedMessage::deserialize(&serialized).unwrap();

        if let MessagePayload::ReplicationRequest { data, .. } = &deserialized.payload {
            assert_eq!(data.len(), 10000);
            assert_eq!(*data, large_data);
        } else {
            panic!("Expected ReplicationRequest payload");
        }
    }

    #[test]
    fn test_message_timestamp() {
        let payload = MessagePayload::NodeJoin {
            node_id: "test_node".to_string(),
            address: "127.0.0.1:8080".to_string(),
            capabilities: vec!["storage".to_string()],
        };

        let message =
            DistributedMessage::new("sender".to_string(), None, MessageType::NodeJoin, payload);

        let now = Utc::now();
        let diff = now.signed_duration_since(message.timestamp);

        // Message timestamp should be very recent (within 1 second)
        assert!(diff.num_seconds() < 1);
    }

    #[test]
    fn test_invalid_deserialization() {
        let invalid_data = b"not valid json";
        let result = DistributedMessage::deserialize(invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_request_response() {
        let read_request = MessagePayload::ReadRequest {
            topic: "test_topic".to_string(),
            partition: 3,
        };

        let message = DistributedMessage::new(
            "client".to_string(),
            Some("broker".to_string()),
            MessageType::ReadRequest,
            read_request,
        );

        if let MessagePayload::ReadRequest { topic, partition } = &message.payload {
            assert_eq!(topic, "test_topic");
            assert_eq!(*partition, 3);
        } else {
            panic!("Expected ReadRequest payload");
        }

        // Test ReadResponse
        let response_payload = MessagePayload::ReadResponse {
            topic: "test_topic".to_string(),
            partition: 3,
            messages: b"serialized_messages".to_vec(),
            success: true,
            error_message: None,
        };

        let response_message = DistributedMessage::new(
            "broker".to_string(),
            Some("client".to_string()),
            MessageType::ReadResponse,
            response_payload,
        );

        if let MessagePayload::ReadResponse {
            topic,
            partition,
            messages,
            success,
            error_message,
        } = &response_message.payload
        {
            assert_eq!(topic, "test_topic");
            assert_eq!(*partition, 3);
            assert_eq!(messages, b"serialized_messages");
            assert!(*success);
            assert!(error_message.is_none());
        } else {
            panic!("Expected ReadResponse payload");
        }
    }

    #[test]
    fn test_log_entry_creation() {
        let entry = LogEntry {
            term: 5,
            index: 100,
            data: b"test_log_data".to_vec(),
            checksum: "entry_checksum".to_string(),
        };

        assert_eq!(entry.term, 5);
        assert_eq!(entry.index, 100);
        assert_eq!(entry.data, b"test_log_data");
        assert_eq!(entry.checksum, "entry_checksum");
    }

    #[test]
    fn test_message_debug_format() {
        let payload = MessagePayload::QuorumCheck {
            cluster_id: "debug_cluster".to_string(),
            term: 1,
        };

        let message = DistributedMessage::new(
            "debug_sender".to_string(),
            Some("debug_receiver".to_string()),
            MessageType::QuorumCheck,
            payload,
        );

        let debug_str = format!("{:?}", message);
        assert!(debug_str.contains("DistributedMessage"));
        assert!(debug_str.contains("debug_sender"));
        assert!(debug_str.contains("QuorumCheck"));
    }

    #[test]
    fn test_message_clone() {
        let payload = MessagePayload::NodeLeave {
            node_id: "leaving_node".to_string(),
            reason: "maintenance".to_string(),
        };

        let original = DistributedMessage::new(
            "clone_sender".to_string(),
            Some("clone_receiver".to_string()),
            MessageType::NodeLeave,
            payload,
        );

        let cloned = original.clone();

        assert_eq!(original.id, cloned.id);
        assert_eq!(original.sender_id, cloned.sender_id);
        assert_eq!(original.receiver_id, cloned.receiver_id);
        assert_eq!(original.message_type, cloned.message_type);
        assert_eq!(original.timestamp, cloned.timestamp);
    }

    #[test]
    fn test_transaction_commit_abort() {
        let transaction_id = Uuid::new_v4();

        // Test TransactionCommit
        let commit_payload = MessagePayload::TransactionCommit { transaction_id };

        let commit_message = DistributedMessage::new(
            "coordinator".to_string(),
            None,
            MessageType::TransactionCommit,
            commit_payload,
        );

        if let MessagePayload::TransactionCommit {
            transaction_id: tx_id,
        } = &commit_message.payload
        {
            assert_eq!(*tx_id, transaction_id);
        } else {
            panic!("Expected TransactionCommit payload");
        }

        // Test TransactionAbort
        let abort_payload = MessagePayload::TransactionAbort { transaction_id };

        let abort_message = DistributedMessage::new(
            "coordinator".to_string(),
            None,
            MessageType::TransactionAbort,
            abort_payload,
        );

        if let MessagePayload::TransactionAbort {
            transaction_id: tx_id,
        } = &abort_message.payload
        {
            assert_eq!(*tx_id, transaction_id);
        } else {
            panic!("Expected TransactionAbort payload");
        }
    }

    #[test]
    fn test_heartbeat_response() {
        let payload = MessagePayload::HeartbeatResponse {
            term: 10,
            success: true,
            match_index: 25,
        };

        let message = DistributedMessage::new(
            "follower".to_string(),
            Some("leader".to_string()),
            MessageType::HeartbeatResponse,
            payload,
        );

        if let MessagePayload::HeartbeatResponse {
            term,
            success,
            match_index,
        } = &message.payload
        {
            assert_eq!(*term, 10);
            assert!(*success);
            assert_eq!(*match_index, 25);
        } else {
            panic!("Expected HeartbeatResponse payload");
        }
    }

    #[test]
    fn test_accept_phase_payloads() {
        let test_value = b"accept_value";

        // Test AcceptPhase
        let accept_payload = MessagePayload::AcceptPhase {
            proposal_number: 15,
            value: test_value.to_vec(),
        };

        let accept_message = DistributedMessage::new(
            "proposer".to_string(),
            None,
            MessageType::AcceptPhase,
            accept_payload,
        );

        if let MessagePayload::AcceptPhase {
            proposal_number,
            value,
        } = &accept_message.payload
        {
            assert_eq!(*proposal_number, 15);
            assert_eq!(value, test_value);
        } else {
            panic!("Expected AcceptPhase payload");
        }

        // Test AcceptedPhase
        let accepted_payload = MessagePayload::AcceptedPhase {
            proposal_number: 15,
        };

        let accepted_message = DistributedMessage::new(
            "acceptor".to_string(),
            Some("proposer".to_string()),
            MessageType::AcceptedPhase,
            accepted_payload,
        );

        if let MessagePayload::AcceptedPhase { proposal_number } = &accepted_message.payload {
            assert_eq!(*proposal_number, 15);
        } else {
            panic!("Expected AcceptedPhase payload");
        }
    }
}
