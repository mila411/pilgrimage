use crate::network::protocol::{DistributedMessage, MessageType, MessagePayload};
use crate::network::transport::{NetworkTransport, MessageHandler};
use crate::network::discovery::{NodeDiscovery, NodeStatus};
use crate::network::error::{NetworkError, NetworkResult};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::interval;
use log::{debug, error, info, warn};

/// Split-brain prevention mechanism using quorum-based approach
pub struct SplitBrainPrevention {
    node_id: String,
    cluster_id: String,
    transport: Arc<NetworkTransport>,
    discovery: Arc<NodeDiscovery>,

    // Quorum state
    current_term: Arc<Mutex<u64>>,
    quorum_size: usize,
    last_quorum_check: Arc<Mutex<Instant>>,
    active_nodes: Arc<Mutex<HashSet<String>>>,

    // Network partition detection
    partition_suspects: Arc<Mutex<HashMap<String, Instant>>>,
    partition_threshold: Duration,
    quorum_check_interval: Duration,

    // Cluster state
    is_in_majority: Arc<Mutex<bool>>,
    isolated_nodes: Arc<Mutex<HashSet<String>>>,
}

impl SplitBrainPrevention {
    pub fn new(
        node_id: String,
        cluster_id: String,
        transport: Arc<NetworkTransport>,
        discovery: Arc<NodeDiscovery>,
        expected_cluster_size: usize,
    ) -> Self {
        let quorum_size = (expected_cluster_size / 2) + 1;

        let prevention = Self {
            node_id,
            cluster_id,
            transport,
            discovery,
            current_term: Arc::new(Mutex::new(0)),
            quorum_size,
            last_quorum_check: Arc::new(Mutex::new(Instant::now())),
            active_nodes: Arc::new(Mutex::new(HashSet::new())),
            partition_suspects: Arc::new(Mutex::new(HashMap::new())),
            partition_threshold: Duration::from_secs(10),
            quorum_check_interval: Duration::from_secs(5),
            is_in_majority: Arc::new(Mutex::new(false)),
            isolated_nodes: Arc::new(Mutex::new(HashSet::new())),
        };

        // Register message handlers
        prevention.register_handlers();

        prevention
    }

    /// Start split-brain prevention
    pub async fn start(&self) -> NetworkResult<()> {
        info!("Starting split-brain prevention for cluster {}", self.cluster_id);

        // Start periodic quorum checks
        self.start_quorum_monitoring().await;

        // Start partition detection
        self.start_partition_detection().await;

        // Start cluster health monitoring
        self.start_cluster_health_monitoring().await;

        Ok(())
    }

    /// Register message handlers
    fn register_handlers(&self) {
        let quorum_handler = QuorumHandler::new(self.clone());

        self.transport.register_handler(MessageType::QuorumCheck, quorum_handler.clone());
        self.transport.register_handler(MessageType::QuorumResponse, quorum_handler);
    }

    /// Start periodic quorum monitoring
    async fn start_quorum_monitoring(&self) {
        let prevention = self.clone();

        tokio::spawn(async move {
            let mut interval = interval(prevention.quorum_check_interval);

            loop {
                interval.tick().await;

                if let Err(e) = prevention.perform_quorum_check().await {
                    error!("Quorum check failed: {}", e);
                }
            }
        });
    }

    /// Start partition detection
    async fn start_partition_detection(&self) {
        let prevention = self.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                prevention.detect_network_partitions().await;
            }
        });
    }

    /// Start cluster health monitoring
    async fn start_cluster_health_monitoring(&self) {
        let prevention = self.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));

            loop {
                interval.tick().await;

                prevention.update_cluster_health().await;
            }
        });
    }

    /// Perform a quorum check
    async fn perform_quorum_check(&self) -> NetworkResult<bool> {
        debug!("Performing quorum check for cluster {}", self.cluster_id);

        let current_term = { *self.current_term.lock().unwrap() };
        let active_nodes = self.discovery.get_active_nodes();

        // Update last quorum check time
        {
            let mut last_check = self.last_quorum_check.lock().unwrap();
            *last_check = Instant::now();
        }

        // Send quorum check to all known nodes
        for node in &active_nodes {
            let quorum_check = DistributedMessage::new(
                self.node_id.clone(),
                Some(node.id.clone()),
                MessageType::QuorumCheck,
                MessagePayload::QuorumCheck {
                    cluster_id: self.cluster_id.clone(),
                    term: current_term,
                },
            );

            if let Err(e) = self.transport.send_message(quorum_check).await {
                warn!("Failed to send quorum check to {}: {}", node.id, e);
                self.suspect_node(&node.id).await;
            }
        }

        // Check if we have quorum
        let has_quorum = self.check_quorum_status().await;

        {
            let mut is_majority = self.is_in_majority.lock().unwrap();
            if *is_majority != has_quorum {
                if has_quorum {
                    info!("Node {} joined majority partition", self.node_id);
                } else {
                    warn!("Node {} is in minority partition - entering read-only mode", self.node_id);
                }
                *is_majority = has_quorum;
            }
        }

        Ok(has_quorum)
    }

    /// Check current quorum status
    async fn check_quorum_status(&self) -> bool {
        // Get active nodes from discovery service instead of local cache
        let discovered_nodes = self.discovery.get_active_nodes();
        let total_active = discovered_nodes.len() + 1; // +1 for self

        info!("Quorum check for {}: {} discovered nodes + self = {}, need {}",
              self.node_id, discovered_nodes.len(), total_active, self.quorum_size);

        for node in &discovered_nodes {
            info!("  Active node: {} at {}", node.id, node.address);
        }

        // Update our local active nodes cache
        {
            let mut active_nodes = self.active_nodes.lock().unwrap();
            active_nodes.clear();
            for node in &discovered_nodes {
                active_nodes.insert(node.id.clone());
            }
        }

        let has_quorum = total_active >= self.quorum_size;
        info!("Quorum status for {}: {}", self.node_id, has_quorum);

        has_quorum
    }

    /// Detect network partitions
    async fn detect_network_partitions(&self) {
        let now = Instant::now();
        let mut suspects_to_remove = Vec::new();
        let mut nodes_to_isolate = Vec::new();

        // Check suspect timeouts
        {
            let mut suspects = self.partition_suspects.lock().unwrap();
            for (node_id, suspect_time) in suspects.iter() {
                if now.duration_since(*suspect_time) > self.partition_threshold {
                    nodes_to_isolate.push(node_id.clone());
                    suspects_to_remove.push(node_id.clone());
                }
            }

            for node_id in &suspects_to_remove {
                suspects.remove(node_id);
            }
        }

        // Isolate nodes that have been suspected for too long
        if !nodes_to_isolate.is_empty() {
            let mut isolated = self.isolated_nodes.lock().unwrap();
            for node_id in nodes_to_isolate {
                warn!("Isolating node {} due to suspected network partition", node_id);
                isolated.insert(node_id.clone());

                // Remove from active nodes
                let mut active = self.active_nodes.lock().unwrap();
                active.remove(&node_id);

                // Update discovery
                self.discovery.update_node_status(&node_id, NodeStatus::Suspected);
            }
        }
    }

    /// Suspect a node of network issues
    async fn suspect_node(&self, node_id: &str) {
        let mut suspects = self.partition_suspects.lock().unwrap();
        suspects.insert(node_id.to_string(), Instant::now());
        warn!("Suspecting node {} of network issues", node_id);
    }

    /// Clear suspicion for a node
    async fn clear_suspicion(&self, node_id: &str) {
        let mut suspects = self.partition_suspects.lock().unwrap();
        if suspects.remove(node_id).is_some() {
            debug!("Cleared suspicion for node {}", node_id);
        }

        // Re-add to active nodes if not isolated
        let isolated = self.isolated_nodes.lock().unwrap();
        if !isolated.contains(node_id) {
            let mut active = self.active_nodes.lock().unwrap();
            active.insert(node_id.to_string());
        }
    }

    /// Update cluster health based on discovery info
    async fn update_cluster_health(&self) {
        let discovered_nodes = self.discovery.get_active_nodes();
        let mut active_nodes = self.active_nodes.lock().unwrap();

        // Update active nodes list
        active_nodes.clear();
        for node in discovered_nodes {
            if matches!(node.status, NodeStatus::Active) {
                active_nodes.insert(node.id);
            }
        }

        // Check for recovered nodes
        let isolated = self.isolated_nodes.lock().unwrap();
        let suspects = self.partition_suspects.lock().unwrap();

        for node_id in active_nodes.iter() {
            if isolated.contains(node_id) || suspects.contains_key(node_id) {
                // Node might be recovering, clear suspicions gradually
                debug!("Node {} showing signs of recovery", node_id);
            }
        }
    }

    /// Handle quorum check request
    pub async fn handle_quorum_check(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        if let MessagePayload::QuorumCheck { cluster_id, term } = message.payload {
            if cluster_id != self.cluster_id {
                return Err(NetworkError::InvalidMessageFormat);
            }

            // Update term if necessary
            {
                let mut current_term = self.current_term.lock().unwrap();
                if term > *current_term {
                    *current_term = term;
                }
            }

            // Clear suspicion for the sender
            self.clear_suspicion(&message.sender_id).await;

            // Collect list of nodes we can see
            let active_nodes = { self.active_nodes.lock().unwrap().clone() };
            let nodes_seen: Vec<String> = active_nodes.into_iter().collect();

            let response = DistributedMessage::new(
                self.node_id.clone(),
                Some(message.sender_id),
                MessageType::QuorumResponse,
                MessagePayload::QuorumResponse {
                    cluster_id: self.cluster_id.clone(),
                    term: *self.current_term.lock().unwrap(),
                    nodes_seen,
                },
            );

            Ok(Some(response))
        } else {
            Err(NetworkError::InvalidMessageFormat)
        }
    }

    /// Handle quorum response
    pub async fn handle_quorum_response(&self, message: DistributedMessage) -> NetworkResult<()> {
        if let MessagePayload::QuorumResponse { cluster_id, term, nodes_seen } = message.payload {
            if cluster_id != self.cluster_id {
                return Ok(());
            }

            // Update term if necessary
            {
                let mut current_term = self.current_term.lock().unwrap();
                if term > *current_term {
                    *current_term = term;
                }
            }

            // Clear suspicion for the sender
            self.clear_suspicion(&message.sender_id).await;

            // Update our view of the cluster
            {
                let mut active_nodes = self.active_nodes.lock().unwrap();
                for node_id in nodes_seen {
                    if node_id != self.node_id {
                        active_nodes.insert(node_id);
                    }
                }
            }

            debug!("Received quorum response from {}", message.sender_id);
        }

        Ok(())
    }

    /// Check if this node can safely act as leader
    pub fn can_act_as_leader(&self) -> bool {
        *self.is_in_majority.lock().unwrap()
    }

    /// Check if this node is in the majority partition
    pub fn is_in_majority_partition(&self) -> bool {
        *self.is_in_majority.lock().unwrap()
    }

    /// Get the current quorum size requirement
    pub fn get_quorum_size(&self) -> usize {
        self.quorum_size
    }

    /// Get the number of currently active nodes
    pub fn get_active_node_count(&self) -> usize {
        self.active_nodes.lock().unwrap().len() + 1 // +1 for self
    }

    /// Force a quorum check (for testing or manual intervention)
    pub async fn force_quorum_check(&self) -> NetworkResult<bool> {
        self.perform_quorum_check().await
    }

    /// Get cluster health status
    pub fn get_cluster_health(&self) -> ClusterHealth {
        let active_count = self.get_active_node_count();
        let has_quorum = active_count >= self.quorum_size;
        let isolated_count = self.isolated_nodes.lock().unwrap().len();
        let suspected_count = self.partition_suspects.lock().unwrap().len();

        ClusterHealth {
            active_nodes: active_count,
            required_quorum: self.quorum_size,
            has_quorum,
            isolated_nodes: isolated_count,
            suspected_nodes: suspected_count,
            is_in_majority: *self.is_in_majority.lock().unwrap(),
        }
    }
}

impl Clone for SplitBrainPrevention {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            cluster_id: self.cluster_id.clone(),
            transport: self.transport.clone(),
            discovery: self.discovery.clone(),
            current_term: self.current_term.clone(),
            quorum_size: self.quorum_size,
            last_quorum_check: self.last_quorum_check.clone(),
            active_nodes: self.active_nodes.clone(),
            partition_suspects: self.partition_suspects.clone(),
            partition_threshold: self.partition_threshold,
            quorum_check_interval: self.quorum_check_interval,
            is_in_majority: self.is_in_majority.clone(),
            isolated_nodes: self.isolated_nodes.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterHealth {
    pub active_nodes: usize,
    pub required_quorum: usize,
    pub has_quorum: bool,
    pub isolated_nodes: usize,
    pub suspected_nodes: usize,
    pub is_in_majority: bool,
}

// Message Handler

pub struct QuorumHandler {
    prevention: SplitBrainPrevention,
}

impl QuorumHandler {
    pub fn new(prevention: SplitBrainPrevention) -> Self {
        Self { prevention }
    }
}

impl Clone for QuorumHandler {
    fn clone(&self) -> Self {
        Self {
            prevention: self.prevention.clone(),
        }
    }
}

impl MessageHandler for QuorumHandler {
    fn handle(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        // Instead of using block_on, we'll spawn the async work and return immediately
        match message.message_type {
            MessageType::QuorumCheck => {
                let prevention = self.prevention.clone();
                tokio::spawn(async move {
                    if let Err(e) = prevention.handle_quorum_check(message).await {
                        error!("Failed to handle quorum check: {}", e);
                    }
                });
                Ok(None)
            }
            MessageType::QuorumResponse => {
                let prevention = self.prevention.clone();
                tokio::spawn(async move {
                    if let Err(e) = prevention.handle_quorum_response(message).await {
                        error!("Failed to handle quorum response: {}", e);
                    }
                });
                Ok(None)
            }
            _ => Err(NetworkError::InvalidMessageFormat),
        }
    }
}
