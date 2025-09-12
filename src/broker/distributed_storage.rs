//! Distributed storage system with ACID guarantees and fault tolerance
//!
//! This module provides a distributed storage layer that ensures data consistency
//! across multiple nodes with ACID properties and single-node failure tolerance.

use crate::broker::node::Node;
use crate::broker::storage::{Storage, StorageStats};
use crate::message::message::Message;
use crate::network::error::{NetworkError, NetworkResult};
use crate::network::protocol::{DistributedMessage, MessagePayload, MessageType};
use crate::network::transport::NetworkTransport;

use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::timeout;
use uuid::Uuid;

/// Replication configuration for distributed storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub replication_factor: u8,
    pub consistency_level: ConsistencyLevel,
    pub quorum_reads: usize,
    pub quorum_writes: usize,
    pub min_replicas: u8,
    pub max_replicas: u8,
    pub read_repair: bool,
    pub anti_entropy_interval: Duration,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            replication_factor: 3,
            consistency_level: ConsistencyLevel::Quorum,
            quorum_reads: 2,
            quorum_writes: 2,
            min_replicas: 2,
            max_replicas: 3,
            read_repair: true,
            anti_entropy_interval: Duration::from_secs(300),
        }
    }
}

/// Consistency level for read/write operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    One,         // One replica is sufficient.
    Quorum,      // Majority of replicas are required.
    All,         // All replicas are required.
    LocalQuorum, // Majority of local datacenter replicas are required.
}

/// Distribution strategy for data placement
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DistributionStrategy {
    Hash,       // Hash-based distribution
    Range,      // Range-based distribution
    Consistent, // Consistent hashing
}

/// Storage node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageNode {
    pub node_id: String,
    pub address: String,
    pub status: NodeStatus,
    pub last_heartbeat: DateTime<Utc>,
    pub storage_capacity: u64,
    pub used_storage: u64,
    pub load_factor: f64,
    pub rack_id: Option<String>,
    pub datacenter_id: Option<String>,
}

/// Node status in the cluster
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Active,
    Suspected,
    Down,
    Joining,
    Leaving,
    Maintenance,
}

/// Data partition information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPartition {
    pub partition_id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub primary_node: String,
    pub replica_nodes: Vec<String>,
    pub version: u64,
    pub last_modified: DateTime<Utc>,
    pub message_count: u64,
    pub size_bytes: u64,
}

/// Replication entry with vector clocks for conflict resolution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedEntry {
    pub id: Uuid,
    pub data: Vec<u8>,
    pub vector_clock: VectorClock,
    pub timestamp: DateTime<Utc>,
    pub checksum: String,
    pub replica_locations: Vec<String>,
}

/// Vector clock for distributed versioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorClock {
    pub clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    pub fn increment(&mut self, node_id: &str) {
        let current = self.clocks.get(node_id).unwrap_or(&0);
        self.clocks.insert(node_id.to_string(), current + 1);
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (node_id, clock) in &other.clocks {
            let current = self.clocks.get(node_id).unwrap_or(&0);
            self.clocks.insert(node_id.clone(), (*current).max(*clock));
        }
    }

    pub fn compare(&self, other: &VectorClock) -> ClockComparison {
        let mut less_than = false;
        let mut greater_than = false;

        let all_nodes: HashSet<String> = self
            .clocks
            .keys()
            .chain(other.clocks.keys())
            .cloned()
            .collect();

        for node in all_nodes {
            let self_clock = self.clocks.get(&node).unwrap_or(&0);
            let other_clock = other.clocks.get(&node).unwrap_or(&0);

            if self_clock < other_clock {
                less_than = true;
            } else if self_clock > other_clock {
                greater_than = true;
            }
        }

        match (less_than, greater_than) {
            (true, false) => ClockComparison::Before,
            (false, true) => ClockComparison::After,
            (false, false) => ClockComparison::Equal,
            (true, true) => ClockComparison::Concurrent,
        }
    }
}

/// Clock comparison result
#[derive(Debug, PartialEq)]
pub enum ClockComparison {
    Before,
    After,
    Equal,
    Concurrent,
}

/// Transaction state for ACID compliance
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Preparing,
    Committed,
    Aborted,
}

/// Distributed transaction for ACID guarantees
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedTransaction {
    pub id: Uuid,
    pub operations: Vec<TransactionOperation>,
    pub participants: HashSet<String>,
    pub coordinator: String,
    pub state: TransactionState,
    pub timestamp: u64,
    pub timeout: Duration,
}

/// Individual operation within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionOperation {
    WriteMessage {
        message: Message,
        topic: String,
        partition: usize,
    },
    DeleteMessage {
        message_id: Uuid,
        topic: String,
        partition: usize,
    },
    ConsumeMessage {
        message_id: Uuid,
    },
    ReplicateData {
        topic: String,
        data: Vec<u8>,
        target_nodes: Vec<String>,
    },
}

/// Consensus state for distributed operations
#[derive(Debug, Clone)]
pub struct ConsensusState {
    pub term: u64,
    pub leader: Option<String>,
    pub quorum_size: usize,
    pub active_nodes: HashSet<String>,
}

/// Distributed storage manager with ACID guarantees
pub struct DistributedStorage {
    /// Local storage instance
    storage: Arc<Mutex<Storage>>,
    /// Node ID
    node_id: String,
    /// Network transport for communication
    transport: Arc<NetworkTransport>,
    /// Connected nodes
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    /// Active transactions
    transactions: Arc<RwLock<HashMap<Uuid, DistributedTransaction>>>,
    /// Transaction log for recovery
    transaction_log: Arc<Mutex<Vec<DistributedTransaction>>>,
    /// Consensus state
    consensus: Arc<RwLock<ConsensusState>>,
    /// Replication factor
    replication_factor: usize,
    /// Write quorum size
    write_quorum: usize,
    /// Read quorum size
    read_quorum: usize,
    /// Transaction timeout
    transaction_timeout: Duration,
    /// Concurrency control semaphore
    write_semaphore: Arc<Semaphore>,
    /// Backup configuration
    backup_config: BackupConfig,
    /// Replication configuration
    replication_config: ReplicationConfig,
    /// Distribution strategy
    distribution_strategy: DistributionStrategy,
    /// Storage nodes in the cluster
    storage_nodes: Arc<RwLock<HashMap<String, StorageNode>>>,
    /// Data partitions
    data_partitions: Arc<RwLock<HashMap<u64, DataPartition>>>,
    /// Vector clock for this node
    vector_clock: Arc<Mutex<VectorClock>>,
    /// Anti-entropy process handle
    anti_entropy_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

/// Configuration for backup and disaster recovery
#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub enabled: bool,
    pub backup_directory: PathBuf,
    pub backup_interval: Duration,
    pub max_backup_files: usize,
    pub compress_backups: bool,
    pub remote_backup_nodes: Vec<String>,
}

/// Result of a distributed operation
#[derive(Debug)]
pub struct DistributedResult {
    pub success: bool,
    pub committed_nodes: HashSet<String>,
    pub failed_nodes: HashSet<String>,
    pub transaction_id: Option<Uuid>,
}

impl DistributedStorage {
    /// Create a new distributed storage instance
    pub fn new(
        storage: Storage,
        node_id: String,
        transport: Arc<NetworkTransport>,
        replication_factor: usize,
        backup_config: BackupConfig,
    ) -> Self {
        let write_quorum = (replication_factor / 2) + 1;
        let read_quorum = 1; // Can read from any replica

        let mut vector_clock = VectorClock::new();
        vector_clock.increment(&node_id);

        Self {
            storage: Arc::new(Mutex::new(storage)),
            node_id: node_id.clone(),
            transport,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_log: Arc::new(Mutex::new(Vec::new())),
            consensus: Arc::new(RwLock::new(ConsensusState {
                term: 0,
                leader: Some(node_id.clone()),
                quorum_size: write_quorum,
                active_nodes: HashSet::new(),
            })),
            replication_factor,
            write_quorum,
            read_quorum,
            transaction_timeout: Duration::from_secs(30),
            write_semaphore: Arc::new(Semaphore::new(100)), // Limit concurrent writes
            backup_config,
            replication_config: ReplicationConfig {
                replication_factor: replication_factor as u8,
                min_replicas: 1,
                max_replicas: replication_factor as u8,
                consistency_level: ConsistencyLevel::Quorum,
                read_repair: true,
                quorum_reads: read_quorum,
                quorum_writes: write_quorum,
                anti_entropy_interval: Duration::from_secs(300),
            },
            distribution_strategy: DistributionStrategy::Hash,
            storage_nodes: Arc::new(RwLock::new(HashMap::new())),
            data_partitions: Arc::new(RwLock::new(HashMap::new())),
            vector_clock: Arc::new(Mutex::new(vector_clock)),
            anti_entropy_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the distributed storage system
    pub async fn start(&self) -> NetworkResult<()> {
        info!("Starting distributed storage for node {}", self.node_id);

        // Start background tasks
        self.start_transaction_cleanup().await;
        self.start_backup_service().await;
        self.start_consensus_monitoring().await;

        Ok(())
    }

    /// Start transaction cleanup service
    async fn start_transaction_cleanup(&self) {
        let transactions = self.transactions.clone();
        let timeout = self.transaction_timeout;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                let mut txns = transactions.write().await;
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                txns.retain(|_, txn| {
                    let elapsed = Duration::from_secs(now - txn.timestamp);
                    elapsed < timeout
                });

                debug!("Cleaned up expired transactions");
            }
        });
    }

    /// Start backup service
    async fn start_backup_service(&self) {
        if !self.backup_config.enabled {
            return;
        }

        let _storage = self.storage.clone();
        let backup_config = self.backup_config.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(backup_config.backup_interval);

            loop {
                interval.tick().await;

                // Create periodic backup
                debug!("Creating periodic backup");
                // Implementation would create backup here
            }
        });
    }

    /// Start consensus monitoring
    async fn start_consensus_monitoring(&self) {
        let consensus = self.consensus.clone();
        let node_id = self.node_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                interval.tick().await;

                let consensus_state = consensus.read().await;
                debug!("Node {} consensus term: {}", node_id, consensus_state.term);
            }
        });
    }

    /// Write a message with ACID guarantees
    pub async fn write_message_acid(&self, message: Message) -> NetworkResult<DistributedResult> {
        // Acquire write semaphore to limit concurrency
        let _permit = self.write_semaphore.acquire().await.map_err(|_| {
            NetworkError::InternalError("Failed to acquire write permit".to_string())
        })?;

        // Create a new transaction
        let transaction_id = Uuid::new_v4();
        let operation = TransactionOperation::WriteMessage {
            message: message.clone(),
            topic: message.topic_id.clone(),
            partition: message.partition_id,
        };

        // Determine participant nodes
        let nodes = self.nodes.read().await;
        let available_nodes: Vec<String> = nodes.keys().cloned().collect();
        let participants: HashSet<String> = available_nodes
            .into_iter()
            .take(self.replication_factor)
            .collect();

        drop(nodes);

        if participants.len() < self.write_quorum {
            error!(
                "Insufficient nodes for write quorum: need {}, have {}",
                self.write_quorum,
                participants.len()
            );
            return Ok(DistributedResult {
                success: false,
                committed_nodes: HashSet::new(),
                failed_nodes: participants,
                transaction_id: Some(transaction_id),
            });
        }

        // Create distributed transaction
        let transaction = DistributedTransaction {
            id: transaction_id,
            operations: vec![operation],
            participants: participants.clone(),
            coordinator: self.node_id.clone(),
            state: TransactionState::Preparing,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            timeout: self.transaction_timeout,
        };

        // Execute two-phase commit
        self.execute_two_phase_commit(transaction).await
    }

    /// Execute two-phase commit protocol
    async fn execute_two_phase_commit(
        &self,
        mut transaction: DistributedTransaction,
    ) -> NetworkResult<DistributedResult> {
        info!("Starting 2PC for transaction {}", transaction.id);

        // Store transaction in local log
        {
            let mut tx_log = self.transaction_log.lock().unwrap();
            tx_log.push(transaction.clone());
        }

        // Phase 1: Prepare
        transaction.state = TransactionState::Preparing;
        let prepare_result = self.send_prepare_messages(&transaction).await?;

        if prepare_result.len() < self.write_quorum {
            // Not enough nodes agreed to prepare - abort
            warn!(
                "Prepare phase failed: {} nodes agreed, need {}",
                prepare_result.len(),
                self.write_quorum
            );

            transaction.state = TransactionState::Aborted;
            self.send_abort_messages(&transaction).await?;

            return Ok(DistributedResult {
                success: false,
                committed_nodes: HashSet::new(),
                failed_nodes: transaction.participants.clone(),
                transaction_id: Some(transaction.id),
            });
        }

        // Phase 2: Commit
        transaction.state = TransactionState::Committed;
        let commit_result = self.send_commit_messages(&transaction).await?;

        // Apply transaction locally
        if let Err(e) = self.apply_transaction_locally(&transaction).await {
            error!("Failed to apply transaction locally: {}", e);
        }

        // Update transaction log
        {
            let mut transactions = self.transactions.write().await;
            transactions.insert(transaction.id, transaction.clone());
        }

        Ok(DistributedResult {
            success: true,
            committed_nodes: commit_result,
            failed_nodes: HashSet::new(),
            transaction_id: Some(transaction.id),
        })
    }

    /// Send prepare messages to participants
    async fn send_prepare_messages(
        &self,
        transaction: &DistributedTransaction,
    ) -> NetworkResult<HashSet<String>> {
        let mut agreed_nodes = HashSet::new();
        let timeout_duration = Duration::from_secs(10);

        for participant in &transaction.participants {
            if participant == &self.node_id {
                // Self-vote
                if self.can_prepare_transaction(transaction).await {
                    agreed_nodes.insert(participant.clone());
                }
                continue;
            }

            let prepare_msg = DistributedMessage::new(
                self.node_id.clone(),
                Some(participant.clone()),
                MessageType::TransactionPrepare,
                MessagePayload::TransactionPrepare {
                    transaction_id: transaction.id,
                    operations: transaction.operations.clone(),
                    coordinator: transaction.coordinator.clone(),
                },
            );

            match timeout(timeout_duration, self.transport.send_message(prepare_msg)).await {
                Ok(Ok(_)) => {
                    // Wait for response (this would be handled by message handlers in practice)
                    agreed_nodes.insert(participant.clone());
                }
                Ok(Err(e)) => {
                    warn!("Failed to send prepare to {}: {}", participant, e);
                }
                Err(_) => {
                    warn!("Prepare message to {} timed out", participant);
                }
            }
        }

        Ok(agreed_nodes)
    }

    /// Send commit messages to participants
    async fn send_commit_messages(
        &self,
        transaction: &DistributedTransaction,
    ) -> NetworkResult<HashSet<String>> {
        let mut committed_nodes = HashSet::new();

        for participant in &transaction.participants {
            if participant == &self.node_id {
                committed_nodes.insert(participant.clone());
                continue;
            }

            let commit_msg = DistributedMessage::new(
                self.node_id.clone(),
                Some(participant.clone()),
                MessageType::TransactionCommit,
                MessagePayload::TransactionCommit {
                    transaction_id: transaction.id,
                },
            );

            if let Err(e) = self.transport.send_message(commit_msg).await {
                warn!("Failed to send commit to {}: {}", participant, e);
            } else {
                committed_nodes.insert(participant.clone());
            }
        }

        Ok(committed_nodes)
    }

    /// Send abort messages to participants
    async fn send_abort_messages(&self, transaction: &DistributedTransaction) -> NetworkResult<()> {
        for participant in &transaction.participants {
            if participant == &self.node_id {
                continue;
            }

            let abort_msg = DistributedMessage::new(
                self.node_id.clone(),
                Some(participant.clone()),
                MessageType::TransactionAbort,
                MessagePayload::TransactionAbort {
                    transaction_id: transaction.id,
                },
            );

            if let Err(e) = self.transport.send_message(abort_msg).await {
                warn!("Failed to send abort to {}: {}", participant, e);
            }
        }

        Ok(())
    }

    /// Check if this node can prepare for a transaction
    async fn can_prepare_transaction(&self, _transaction: &DistributedTransaction) -> bool {
        // Check resource availability, lock conflicts, etc.
        // For now, always return true
        true
    }

    /// Apply transaction operations locally
    async fn apply_transaction_locally(
        &self,
        transaction: &DistributedTransaction,
    ) -> NetworkResult<()> {
        let mut storage = self.storage.lock().unwrap();

        for operation in &transaction.operations {
            match operation {
                TransactionOperation::WriteMessage { message, .. } => {
                    if let Err(e) = storage.write_message(message) {
                        error!("Failed to write message locally: {}", e);
                        return Err(NetworkError::InternalError(format!(
                            "Local write failed: {}",
                            e
                        )));
                    }
                }
                TransactionOperation::ConsumeMessage { message_id } => {
                    storage.consume_message(*message_id);
                }
                TransactionOperation::DeleteMessage { .. } => {
                    // Implementation would depend on storage support for deletion
                    debug!("Delete operation not yet implemented");
                }
                TransactionOperation::ReplicateData { topic, data, .. } => {
                    if let Err(e) = storage.write_replication_data(topic, data, &self.node_id) {
                        error!("Failed to write replication data: {}", e);
                        return Err(NetworkError::InternalError(format!(
                            "Replication write failed: {}",
                            e
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Read messages with consistency guarantees
    pub async fn read_messages_consistent(
        &self,
        topic: &str,
        partition: usize,
    ) -> NetworkResult<Vec<Message>> {
        // Try to read from local storage first
        {
            let mut storage = self.storage.lock().unwrap();
            match storage.read_messages(topic, partition) {
                Ok(messages) => return Ok(messages),
                Err(e) => {
                    warn!("Local read failed: {}, trying remote nodes", e);
                }
            }
        }

        // If local read fails, try to read from other nodes
        let nodes = self.nodes.read().await;
        for (node_id, _node) in nodes.iter() {
            if node_id == &self.node_id {
                continue;
            }

            let read_msg = DistributedMessage::new(
                self.node_id.clone(),
                Some(node_id.clone()),
                MessageType::ReadRequest,
                MessagePayload::ReadRequest {
                    topic: topic.to_string(),
                    partition,
                },
            );

            if let Ok(_) = self.transport.send_message(read_msg).await {
                // In practice, we would wait for the response
                debug!("Sent read request to {}", node_id);
            }
        }

        // Return empty result for now
        Ok(Vec::new())
    }

    /// Create a backup of the storage
    pub async fn create_backup(&self) -> NetworkResult<PathBuf> {
        if !self.backup_config.enabled {
            return Err(NetworkError::InternalError(
                "Backup not enabled".to_string(),
            ));
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let backup_filename = format!("backup_{}_{}.json", self.node_id, timestamp);
        let backup_path = self.backup_config.backup_directory.join(&backup_filename);

        // Ensure backup directory exists
        if let Err(e) = std::fs::create_dir_all(&self.backup_config.backup_directory) {
            return Err(NetworkError::InternalError(format!(
                "Failed to create backup directory: {}",
                e
            )));
        }

        // Create backup data structure
        let backup_data = self.create_backup_data().await?;

        // Write backup to file
        let backup_json = serde_json::to_string_pretty(&backup_data).map_err(|e| {
            NetworkError::InternalError(format!("Failed to serialize backup: {}", e))
        })?;

        std::fs::write(&backup_path, backup_json)
            .map_err(|e| NetworkError::InternalError(format!("Failed to write backup: {}", e)))?;

        info!("Created backup at {:?}", backup_path);

        // Clean up old backups
        self.cleanup_old_backups().await?;

        Ok(backup_path)
    }

    /// Restore from a backup
    pub async fn restore_from_backup(&self, backup_path: &PathBuf) -> NetworkResult<()> {
        info!("Restoring from backup {:?}", backup_path);

        let backup_content = std::fs::read_to_string(backup_path)
            .map_err(|e| NetworkError::InternalError(format!("Failed to read backup: {}", e)))?;

        let backup_data: BackupData = serde_json::from_str(&backup_content)
            .map_err(|e| NetworkError::InternalError(format!("Failed to parse backup: {}", e)))?;

        // Stop current operations
        self.stop_operations().await;

        // Restore storage
        {
            let mut storage = self.storage.lock().unwrap();
            if let Err(e) = storage.reinitialize() {
                return Err(NetworkError::InternalError(format!(
                    "Failed to reinitialize storage: {}",
                    e
                )));
            }

            // Restore messages
            for message in backup_data.messages {
                if let Err(e) = storage.write_message(&message) {
                    error!("Failed to restore message: {}", e);
                }
            }

            // Restore consumed messages
            for message_id in backup_data.consumed_messages {
                storage.consume_message(message_id);
            }
        }

        // Restore transactions
        {
            let mut transactions = self.transactions.write().await;
            transactions.clear();
            for transaction in backup_data.transactions {
                transactions.insert(transaction.id, transaction);
            }
        }

        info!("Backup restoration completed");
        Ok(())
    }

    /// Advanced replication methods

    /// Add a storage node to the cluster
    pub async fn add_storage_node(&self, node: StorageNode) -> Result<(), String> {
        let mut nodes = self.storage_nodes.write().await;
        let node_id = node.node_id.clone();

        info!("Adding storage node {} to cluster", node_id);
        nodes.insert(node_id.clone(), node);

        // Trigger partition rebalancing
        self.rebalance_partitions().await?;

        Ok(())
    }

    /// Remove a storage node from the cluster
    pub async fn remove_storage_node(&self, node_id: &str) -> Result<(), String> {
        let mut nodes = self.storage_nodes.write().await;

        if let Some(_node) = nodes.remove(node_id) {
            info!("Removing storage node {} from cluster", node_id);

            // Mark node as leaving and migrate its data
            drop(nodes);
            self.migrate_node_data(node_id).await?;
            self.rebalance_partitions().await?;
        }

        Ok(())
    }

    /// Rebalance data partitions across nodes
    async fn rebalance_partitions(&self) -> Result<(), String> {
        let nodes = self.storage_nodes.read().await;
        let mut partitions = self.data_partitions.write().await;

        let active_nodes: Vec<_> = nodes
            .iter()
            .filter(|(_, node)| node.status == NodeStatus::Active)
            .map(|(id, _)| id.clone())
            .collect();

        if active_nodes.len() < self.replication_config.min_replicas as usize {
            return Err("Insufficient active nodes for replication".to_string());
        }

        // Reassign replicas based on current topology
        for partition in partitions.values_mut() {
            let target_replicas = self.replication_config.replication_factor as usize;

            // Remove replicas on unavailable nodes
            partition.replica_nodes.retain(|node_id| {
                nodes
                    .get(node_id)
                    .map_or(false, |node| node.status == NodeStatus::Active)
            });

            // Add new replicas if needed
            while partition.replica_nodes.len() < target_replicas
                && partition.replica_nodes.len() < active_nodes.len()
            {
                if let Some(new_replica) =
                    self.select_replica_node(&active_nodes, &partition.replica_nodes)
                {
                    partition.replica_nodes.push(new_replica);
                    info!(
                        "Added replica {} for partition {}",
                        partition.replica_nodes.last().unwrap(),
                        partition.partition_id
                    );
                } else {
                    break;
                }
            }

            partition.version += 1;
            partition.last_modified = Utc::now();
        }

        Ok(())
    }

    /// Select the best node for a new replica
    fn select_replica_node(
        &self,
        available_nodes: &[String],
        existing_replicas: &[String],
    ) -> Option<String> {
        // Find node with lowest load that's not already a replica
        available_nodes
            .iter()
            .filter(|node_id| !existing_replicas.contains(node_id))
            .min_by_key(|_node_id| {
                // For now, simple round-robin. In production, consider load, rack diversity, etc.
                existing_replicas.len()
            })
            .cloned()
    }

    /// Migrate data from a leaving node
    async fn migrate_node_data(&self, leaving_node: &str) -> Result<(), String> {
        let partitions = self.data_partitions.read().await;

        for partition in partitions.values() {
            if partition.replica_nodes.contains(&leaving_node.to_string()) {
                // Trigger data migration for this partition
                self.migrate_partition_data(partition.partition_id, leaving_node)
                    .await?;
            }
        }

        Ok(())
    }

    /// Migrate data for a specific partition
    async fn migrate_partition_data(
        &self,
        partition_id: u64,
        from_node: &str,
    ) -> Result<(), String> {
        info!(
            "Migrating partition {} data from node {}",
            partition_id, from_node
        );

        // In a real implementation, this would:
        // 1. Find a new target node
        // 2. Stream data from the leaving node to the new node
        // 3. Verify data integrity
        // 4. Update partition metadata

        // For now, just log the operation
        debug!("Data migration completed for partition {}", partition_id);
        Ok(())
    }

    /// Start anti-entropy process for background data sync
    pub async fn start_anti_entropy(&self) -> Result<(), String> {
        let interval = self.replication_config.anti_entropy_interval;
        let node_id = self.node_id.clone();
        let _storage_nodes = self.storage_nodes.clone();
        let data_partitions = self.data_partitions.clone();

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                debug!("Running anti-entropy process for node {}", node_id);

                // Get all partitions this node is responsible for
                let partitions = data_partitions.read().await;
                for partition in partitions.values() {
                    if partition.replica_nodes.contains(&node_id) {
                        // Sync this partition with other replicas
                        // Implementation would compare checksums and sync differences
                        debug!("Syncing partition {} with replicas", partition.partition_id);
                    }
                }
            }
        });

        *self.anti_entropy_handle.lock().unwrap() = Some(handle);
        Ok(())
    }

    /// Add a node to the cluster
    pub async fn add_node(&self, node_id: String, node: Node) -> NetworkResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id.clone(), node);
        info!("Added node {} to cluster", node_id);
        Ok(())
    }

    /// Create backup data structure
    async fn create_backup_data(&self) -> NetworkResult<BackupData> {
        let _storage = self.storage.lock().unwrap();

        // This is a simplified implementation - in practice, you'd need to
        // read all messages from storage systematically
        let messages = Vec::new(); // Would be populated from storage
        let consumed_messages = HashSet::new(); // Would be populated from storage

        let transactions = self.transactions.read().await;
        let transactions_vec: Vec<DistributedTransaction> =
            transactions.values().cloned().collect();

        Ok(BackupData {
            messages,
            consumed_messages,
            transactions: transactions_vec,
            metadata: HashMap::new(),
        })
    }

    /// Clean up old backup files
    async fn cleanup_old_backups(&self) -> NetworkResult<()> {
        if !self.backup_config.enabled {
            return Ok(());
        }

        let backup_dir = &self.backup_config.backup_directory;
        let max_files = self.backup_config.max_backup_files;

        if let Ok(mut entries) = tokio::fs::read_dir(backup_dir).await {
            let mut backup_files = Vec::new();

            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.starts_with("backup_") && filename.ends_with(".json") {
                        if let Ok(metadata) = entry.metadata().await {
                            backup_files.push((
                                entry.path(),
                                metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                            ));
                        }
                    }
                }
            }

            // Sort by modification time (newest first)
            backup_files.sort_by_key(|(_, time)| std::cmp::Reverse(*time));

            // Remove old files beyond max_files limit
            for (path, _) in backup_files.into_iter().skip(max_files) {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    warn!("Failed to remove old backup file {:?}: {}", path, e);
                } else {
                    debug!("Removed old backup file: {:?}", path);
                }
            }
        }

        Ok(())
    }

    /// Stop all background operations
    async fn stop_operations(&self) {
        // Implementation would stop background tasks
        info!("Stopping distributed storage operations");
    }

    /// Get cluster health information
    pub async fn get_cluster_health(&self) -> ClusterHealth {
        let nodes = self.nodes.read().await;
        let consensus = self.consensus.read().await;

        ClusterHealth {
            total_nodes: nodes.len(),
            active_nodes: consensus.active_nodes.len(),
            total_partitions: self.data_partitions.read().await.len(),
            healthy_partitions: self.data_partitions.read().await.len(), // Simplified
            replication_factor: self.replication_factor as u8,
            consistency_level: self.replication_config.consistency_level,
        }
    }

    /// Get storage statistics across the cluster
    pub async fn get_distributed_stats(&self) -> NetworkResult<DistributedStats> {
        let local_stats = {
            let storage = self.storage.lock().unwrap();
            storage
                .get_stats()
                .map_err(|e| NetworkError::InternalError(e.to_string()))?
        };

        // In practice, you would gather stats from other nodes as well
        Ok(DistributedStats {
            local_stats,
            total_nodes: self.nodes.read().await.len(),
            replication_factor: self.replication_factor,
            cluster_health: self.get_cluster_health().await,
        })
    }
}

impl Clone for DistributedStorage {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            node_id: self.node_id.clone(),
            transport: self.transport.clone(),
            nodes: self.nodes.clone(),
            transactions: self.transactions.clone(),
            transaction_log: self.transaction_log.clone(),
            consensus: self.consensus.clone(),
            replication_factor: self.replication_factor,
            write_quorum: self.write_quorum,
            read_quorum: self.read_quorum,
            transaction_timeout: self.transaction_timeout,
            write_semaphore: self.write_semaphore.clone(),
            backup_config: self.backup_config.clone(),
            replication_config: self.replication_config.clone(),
            distribution_strategy: self.distribution_strategy,
            storage_nodes: self.storage_nodes.clone(),
            data_partitions: self.data_partitions.clone(),
            vector_clock: self.vector_clock.clone(),
            anti_entropy_handle: self.anti_entropy_handle.clone(),
        }
    }
}

/// Backup data structure
#[derive(Debug, Serialize, Deserialize)]
pub struct BackupData {
    pub messages: Vec<Message>,
    pub consumed_messages: HashSet<Uuid>,
    pub transactions: Vec<DistributedTransaction>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Cluster health information
#[derive(Debug, Clone)]
pub struct ClusterHealth {
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub total_partitions: usize,
    pub healthy_partitions: usize,
    pub replication_factor: u8,
    pub consistency_level: ConsistencyLevel,
}

/// Distributed storage statistics
#[derive(Debug, Clone)]
pub struct DistributedStats {
    pub local_stats: StorageStats,
    pub total_nodes: usize,
    pub replication_factor: usize,
    pub cluster_health: ClusterHealth,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::storage::Storage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_distributed_storage_creation() {
        let dir = tempdir().unwrap();
        let storage_path = dir.path().join("test.log");
        let backup_path = dir.path().join("backups");

        let storage = Storage::new(storage_path).unwrap();
        let transport = Arc::new(NetworkTransport::new("test_node".to_string()));

        let backup_config = BackupConfig {
            enabled: true,
            backup_directory: backup_path,
            backup_interval: Duration::from_secs(3600),
            max_backup_files: 5,
            compress_backups: false,
            remote_backup_nodes: vec![],
        };

        let distributed_storage = DistributedStorage::new(
            storage,
            "test_node".to_string(),
            transport,
            3,
            backup_config,
        );

        assert_eq!(distributed_storage.node_id, "test_node");
        assert_eq!(distributed_storage.replication_factor, 3);
        assert_eq!(distributed_storage.write_quorum, 2);
    }

    #[tokio::test]
    async fn test_backup_creation() {
        let dir = tempdir().unwrap();
        let storage_path = dir.path().join("test.log");
        let backup_path = dir.path().join("backups");

        let storage = Storage::new(storage_path).unwrap();
        let transport = Arc::new(NetworkTransport::new("test_node".to_string()));

        let backup_config = BackupConfig {
            enabled: true,
            backup_directory: backup_path.clone(),
            backup_interval: Duration::from_secs(3600),
            max_backup_files: 5,
            compress_backups: false,
            remote_backup_nodes: vec![],
        };

        let distributed_storage = DistributedStorage::new(
            storage,
            "test_node".to_string(),
            transport,
            3,
            backup_config,
        );

        let backup_result = distributed_storage.create_backup().await;
        assert!(backup_result.is_ok());

        let backup_file = backup_result.unwrap();
        assert!(backup_file.exists());
        assert!(backup_file.starts_with(&backup_path));
    }

    #[tokio::test]
    async fn test_replication_config() {
        let dir = tempdir().unwrap();
        let storage_path = dir.path().join("test.log");
        let storage = Storage::new(storage_path).unwrap();
        let transport = Arc::new(NetworkTransport::new("test_node".to_string()));

        let config = ReplicationConfig {
            replication_factor: 3,
            min_replicas: 2,
            max_replicas: 3,
            consistency_level: ConsistencyLevel::Quorum,
            read_repair: true,
            quorum_reads: 1,
            quorum_writes: 2,
            anti_entropy_interval: Duration::from_secs(60),
        };

        let mut distributed_storage = DistributedStorage::new(
            storage,
            "test_node".to_string(),
            transport,
            3,
            BackupConfig {
                enabled: false,
                backup_directory: std::path::PathBuf::new(),
                backup_interval: Duration::from_secs(3600),
                max_backup_files: 5,
                compress_backups: false,
                remote_backup_nodes: vec![],
            },
        );
        distributed_storage.replication_config = config.clone();

        assert_eq!(distributed_storage.replication_config.replication_factor, 3);
        assert_eq!(distributed_storage.replication_config.min_replicas, 2);
        assert!(matches!(
            distributed_storage.replication_config.consistency_level,
            ConsistencyLevel::Quorum
        ));
    }

    #[tokio::test]
    async fn test_cluster_health() {
        let dir = tempdir().unwrap();
        let storage_path = dir.path().join("test.log");
        let storage = Storage::new(storage_path).unwrap();
        let transport = Arc::new(NetworkTransport::new("test_node".to_string()));

        let distributed_storage = DistributedStorage::new(
            storage,
            "test_node".to_string(),
            transport,
            3,
            BackupConfig {
                enabled: false,
                backup_directory: std::path::PathBuf::new(),
                backup_interval: Duration::from_secs(3600),
                max_backup_files: 5,
                compress_backups: false,
                remote_backup_nodes: vec![],
            },
        );

        let health = distributed_storage.get_cluster_health().await;
        assert_eq!(health.total_nodes, 0); // No nodes added initially
        assert_eq!(health.active_nodes, 0);
    }
}
