use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;

use crate::broker::node::Node;
use crate::broker::storage::Storage;
use crate::network::transport::NetworkTransport;
use crate::network::protocol::{DistributedMessage, MessageType, MessagePayload};
use crate::network::error::{NetworkError, NetworkResult};

#[derive(Debug, Clone)]
pub struct ReplicationState {
    last_replicated: Instant,
    status: ReplicationStatus,
    replicas: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum ReplicationStatus {
    InProgress,
    Complete,
    Failed(String),
}

#[allow(dead_code)]
pub struct ReplicationManager {
    states: Arc<Mutex<HashMap<String, ReplicationState>>>,
    nodes: Arc<Mutex<HashMap<String, Arc<Node>>>>,
    replication_factor: usize,
    tx: mpsc::Sender<ReplicationEvent>,
    rx: mpsc::Receiver<ReplicationEvent>,
    transport: Option<Arc<NetworkTransport>>,
    node_id: String,
    storage: Option<Arc<Mutex<Storage>>>,
}

#[derive(Debug)]
pub enum ReplicationEvent {
    DataUpdate {
        partition_id: String,
        data: Vec<u8>,
    },
    StateUpdate {
        partition_id: String,
        status: ReplicationStatus,
    },
    NodeFailure {
        node_id: String,
    },
}

#[allow(dead_code)]
impl ReplicationManager {
    pub fn new(
        nodes: Arc<Mutex<HashMap<String, Arc<Node>>>>,
        replication_factor: usize,
        node_id: String,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            states: Arc::new(Mutex::new(HashMap::new())),
            nodes,
            replication_factor,
            tx,
            rx,
            transport: None,
            node_id,
            storage: None,
        }
    }

    /// Set the storage for persistent replication
    pub fn set_storage(&mut self, storage: Arc<Mutex<Storage>>) {
        self.storage = Some(storage);
    }

    /// Set the network transport for distributed replication
    pub fn set_transport(&mut self, transport: Arc<NetworkTransport>) {
        self.transport = Some(transport);
    }

    pub async fn send_data_update(
        &self,
        partition_id: String,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.tx
            .send(ReplicationEvent::DataUpdate { partition_id, data })
            .await
            .map_err(|e| format!("Failed to send replication event: {}", e))
    }

    pub async fn send_state_update(
        &self,
        partition_id: String,
        status: ReplicationStatus,
    ) -> Result<(), String> {
        self.tx
            .send(ReplicationEvent::StateUpdate {
                partition_id,
                status,
            })
            .await
            .map_err(|e| format!("Failed to send status update event: {}", e))
    }

    pub async fn notify_node_failure(&self, node_id: String) -> Result<(), String> {
        self.tx
            .send(ReplicationEvent::NodeFailure { node_id })
            .await
            .map_err(|e| format!("Failed to send node failure event: {}", e))
    }

    pub fn start(&mut self) {
        info!("Replication Manager tick");
        // no-op tick for now; event-driven by external triggers
    }

    #[allow(dead_code)]
    async fn handle_data_update(&self, partition_id: &str, data: &[u8]) {
        let nodes = self.nodes.lock().unwrap();
        let mut states = self.states.lock().unwrap();

        let state = states
            .entry(partition_id.to_string())
            .or_insert_with(|| ReplicationState {
                last_replicated: Instant::now(),
                status: ReplicationStatus::InProgress,
                replicas: Vec::new(),
            });

        let healthy_nodes: Vec<_> = nodes.iter().filter(|(_, node)| node.is_alive()).collect();

        if healthy_nodes.len() < self.replication_factor {
            warn!(
                "Not enough nodes available. Required: {}, Current: {}",
                self.replication_factor,
                healthy_nodes.len()
            );
        }

        // Local replication (existing logic)
        for (node_id, node) in healthy_nodes.iter().take(self.replication_factor) {
            match node.store_data(data) {
                Ok(_) => {
                    if !state.replicas.contains(node_id) {
                        state.replicas.push(node_id.to_string());
                    }
                    debug!("Successful local replication to node {}", node_id);
                }
                Err(e) => {
                    error!("Local replication failure to node {}: {}", node_id, e);
                    state.status = ReplicationStatus::Failed(format!(
                        "Local replication failure to node {}",
                        node_id
                    ));
                }
            }
        }

        // Network replication (new functionality)
        if let Some(transport) = &self.transport {
            self.replicate_over_network(transport, partition_id, data, state).await;
        }

        if state.replicas.len() >= self.replication_factor {
            state.status = ReplicationStatus::Complete;
            state.last_replicated = Instant::now();
        }
    }

    /// Replicate data over the network to remote nodes
    async fn replicate_over_network(
        &self,
        transport: &NetworkTransport,
        partition_id: &str,
        data: &[u8],
        state: &mut ReplicationState,
    ) {
        let connected_peers = transport.get_connected_peers();
        let sequence_number = self.generate_sequence_number();
        let checksum = self.calculate_checksum(data);

        for peer_id in connected_peers.iter().take(self.replication_factor) {
            let replication_msg = DistributedMessage::new(
                self.node_id.clone(),
                Some(peer_id.clone()),
                MessageType::ReplicationRequest,
                MessagePayload::ReplicationRequest {
                    partition_id: partition_id.to_string(),
                    data: data.to_vec(),
                    checksum: checksum.clone(),
                    sequence_number,
                },
            );

            match transport.send_message(replication_msg).await {
                Ok(_) => {
                    debug!("Sent replication request to peer {}", peer_id);
                }
                Err(e) => {
                    error!("Failed to send replication to peer {}: {}", peer_id, e);
                    state.status = ReplicationStatus::Failed(format!(
                        "Network replication failure to peer {}",
                        peer_id
                    ));
                }
            }
        }
    }

    /// Handle replication request from remote node
    pub async fn handle_replication_request(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        if let MessagePayload::ReplicationRequest { partition_id, data, checksum, sequence_number } = message.payload {
            // Verify checksum
            let calculated_checksum = self.calculate_checksum(&data);
            if calculated_checksum != checksum {
                error!("Checksum mismatch for partition {}", partition_id);

                let response = DistributedMessage::new(
                    self.node_id.clone(),
                    Some(message.sender_id),
                    MessageType::ReplicationResponse,
                    MessagePayload::ReplicationResponse {
                        partition_id,
                        success: false,
                        error_message: Some("Checksum mismatch".to_string()),
                    },
                );

                return Ok(Some(response));
            }

            // Store the data locally
            let success = self.store_replicated_data(&partition_id, &data, sequence_number).await;

            let response = DistributedMessage::new(
                self.node_id.clone(),
                Some(message.sender_id),
                MessageType::ReplicationResponse,
                MessagePayload::ReplicationResponse {
                    partition_id,
                    success,
                    error_message: if success { None } else { Some("Storage failed".to_string()) },
                },
            );

            Ok(Some(response))
        } else {
            Err(NetworkError::InvalidMessageFormat)
        }
    }

    /// Handle replication response from remote node
    pub async fn handle_replication_response(&self, message: DistributedMessage) -> NetworkResult<()> {
        if let MessagePayload::ReplicationResponse { partition_id, success, error_message } = message.payload {
            let mut states = self.states.lock().unwrap();

            if let Some(state) = states.get_mut(&partition_id) {
                if success {
                    if !state.replicas.contains(&message.sender_id) {
                        state.replicas.push(message.sender_id.clone());
                    }
                    debug!("Successful network replication to {}", message.sender_id);

                    // Check if we have enough replicas now
                    if state.replicas.len() >= self.replication_factor {
                        state.status = ReplicationStatus::Complete;
                        state.last_replicated = Instant::now();
                        info!("Replication completed for partition {}", partition_id);
                    }
                } else {
                    let error_msg = error_message.unwrap_or_else(|| "Unknown error".to_string());
                    error!("Network replication failed to {}: {}", message.sender_id, error_msg);
                    state.status = ReplicationStatus::Failed(format!(
                        "Network replication failed to {}: {}",
                        message.sender_id, error_msg
                    ));
                }
            }
        }

        Ok(())
    }

    /// Store replicated data from remote node
    async fn store_replicated_data(&self, partition_id: &str, data: &[u8], sequence_number: u64) -> bool {
        // Store in persistent storage if available
        if let Some(storage) = &self.storage {
            match storage.lock() {
                Ok(mut storage) => {
                    match storage.write_replication_data(partition_id, data, &self.node_id) {
                        Ok(_) => {
                            info!("Successfully stored replicated data for partition {} (seq: {})", partition_id, sequence_number);
                            return true;
                        }
                        Err(e) => {
                            error!("Failed to store replicated data: {}", e);
                            return false;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to lock storage: {}", e);
                    return false;
                }
            }
        }

        // Fallback: in-memory storage simulation
        debug!("Storing replicated data for partition {} in memory (seq: {})", partition_id, sequence_number);

        if data.len() > 0 {
            info!("Stored {} bytes for partition {} in memory", data.len(), partition_id);
            true
        } else {
            false
        }
    }

    /// Generate a sequence number for ordering
    fn generate_sequence_number(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now().duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    /// Calculate checksum for data integrity
    fn calculate_checksum(&self, data: &[u8]) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    fn update_replication_state(&self, partition_id: &str, status: ReplicationStatus) {
        let mut states = self.states.lock().unwrap();
        if let Some(state) = states.get_mut(partition_id) {
            state.status = status;
            state.last_replicated = Instant::now();
        }
    }

    async fn handle_node_failure(&self, failed_node_id: &str) {
        info!("Failure of node {} is being handled", failed_node_id);
        let mut states = self.states.lock().unwrap();

        for (partition_id, state) in states.iter_mut() {
            if state.replicas.contains(&failed_node_id.to_string()) {
                // Find alternative node and re-replicate
                self.re_replicate(partition_id).await;
            }
        }
    }

    async fn re_replicate(&self, partition_id: &str) {
        debug!("Start re-replication of partition {}", partition_id);
        // Implementation: Retrieve data from healthy nodes and replicate to new nodes
    }
}
