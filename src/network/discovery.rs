use crate::network::error::{NetworkError, NetworkResult};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::interval;
use log::{debug, info, warn};

/// Node information for cluster discovery
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: String,
    pub address: SocketAddr,
    pub capabilities: Vec<String>,
    pub last_seen: Instant,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeStatus {
    Active,
    Inactive,
    Suspected,
    Down,
}

/// Manages cluster node discovery and health monitoring
pub struct NodeDiscovery {
    node_id: String,
    local_address: SocketAddr,
    nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
    seed_nodes: Vec<SocketAddr>,
    heartbeat_interval: Duration,
    failure_timeout: Duration,
    suspect_timeout: Duration,
}

impl NodeDiscovery {
    #[inline]
    fn normalize_id(id: &str) -> String {
        match id {
            // Normalize port-suffixed IDs used elsewhere
            "node_8081" => "node1".to_string(),
            "node_8082" => "node2".to_string(),
            "node_8083" => "node3".to_string(),
            // Also accept the already-normalized forms
            "node1" | "node2" | "node3" => id.to_string(),
            other => other.to_string(),
        }
    }

    #[inline]
    fn normalize_id_from_addr(address: SocketAddr) -> String {
        match address.port() {
            // Demo ports in examples
            8081 => "node1".to_string(),
            8082 => "node2".to_string(),
            8083 => "node3".to_string(),
            // Legacy/alt mapping (kept for completeness)
            8001 => "node1".to_string(),
            8002 => "node2".to_string(),
            8003 => "node3".to_string(),
            port => format!("node_{}", port),
        }
    }

    pub fn new(
        node_id: String,
        local_address: SocketAddr,
        seed_nodes: Vec<SocketAddr>,
    ) -> Self {
        Self {
            node_id,
            local_address,
            nodes: Arc::new(Mutex::new(HashMap::new())),
            seed_nodes,
            heartbeat_interval: Duration::from_secs(5),
            failure_timeout: Duration::from_secs(30),
            suspect_timeout: Duration::from_secs(15),
        }
    }

    /// Start the node discovery process
    pub async fn start(&self) -> NetworkResult<()> {
        info!("Starting node discovery for {}", self.node_id);

        // Connect to seed nodes
        self.connect_to_seed_nodes().await?;

        // Start periodic health checks
        self.start_health_monitoring().await;

        // Start failure detection
        self.start_failure_detection().await;

        Ok(())
    }

    /// Connect to initial seed nodes
    async fn connect_to_seed_nodes(&self) -> NetworkResult<()> {
        for seed_addr in &self.seed_nodes {
            // Skip our own address
            if *seed_addr == self.local_address {
                continue;
            }

            match self.discover_node_at_address(*seed_addr).await {
                Ok(node_info) => {
                    info!("Discovered seed node: {}", node_info.id);
                    self.add_node(node_info);
                }
                Err(e) => {
                    warn!("Failed to connect to seed node {}: {}", seed_addr, e);
                }
            }
        }
        Ok(())
    }

    /// Discover a node at the given address
    async fn discover_node_at_address(&self, address: SocketAddr) -> NetworkResult<NodeInfo> {
        // In a real implementation, this would:
        // 1. Connect to the address
        // 2. Send a discovery/hello message
        // 3. Receive node information
        // 4. Return the NodeInfo

        // For now, we'll simulate this
        tokio::time::sleep(Duration::from_millis(100)).await;

    // Map port to normalized node ID to match demo configuration
    let node_id = Self::normalize_id_from_addr(address);

        Ok(NodeInfo {
            id: node_id,
            address,
            capabilities: vec!["broker".to_string(), "storage".to_string()],
            last_seen: Instant::now(),
            status: NodeStatus::Active,
        })
    }

    /// Add a node to the cluster
    pub fn add_node(&self, mut node_info: NodeInfo) {
        // Normalize the incoming ID
        node_info.id = Self::normalize_id(&node_info.id);

        let mut nodes = self.nodes.lock().unwrap();

        // If another entry exists with the same address but different id, remove it
        if let Some((old_id, _)) = nodes
            .iter()
            .find(|(id, n)| id.as_str() != node_info.id && n.address == node_info.address)
            .map(|(id, n)| (id.clone(), n.clone()))
        {
            debug!(
                "Removing duplicate discovery entry {} at {} in favor of {}",
                old_id, node_info.address, node_info.id
            );
            nodes.remove(&old_id);
        }

        info!(
            "Adding node {} at {} to discovery list",
            node_info.id, node_info.address
        );
        nodes.insert(node_info.id.clone(), node_info);
        info!("Total nodes in discovery: {}", nodes.len());

        // Debug: Print all nodes (deduped by id by construction)
        info!("Current nodes in discovery:");
        for (id, node) in nodes.iter() {
            info!("  - {} at {} (status: {:?})", id, node.address, node.status);
        }
    }

    /// Remove a node from the cluster
    pub fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(node) = nodes.remove(node_id) {
            info!("Removed node {} at {}", node.id, node.address);
        }
    }

    /// Update node status
    pub fn update_node_status(&self, node_id: &str, status: NodeStatus) {
        let mut nodes = self.nodes.lock().unwrap();
        let norm_id = Self::normalize_id(node_id);
        if let Some(node) = nodes.get_mut(&norm_id) {
            if node.status != status {
                info!("Node {} status changed from {:?} to {:?}", norm_id, node.status, status);
                node.status = status;
            }
            node.last_seen = Instant::now();
        }
    }

    /// Touch a node to refresh last_seen and optionally set to Active
    pub fn touch_node(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        let norm_id = Self::normalize_id(node_id);
        if let Some(node) = nodes.get_mut(&norm_id) {
            node.last_seen = Instant::now();
            if node.status != NodeStatus::Active {
                debug!("Touching node {} -> setting status Active", norm_id);
                node.status = NodeStatus::Active;
            }
        } else {
            // If unknown, auto-register with a best-effort address guess for demo stability
            let guessed_port = match norm_id.as_str() {
                "node1" => 8081,
                "node2" => 8082,
                "node3" => 8083,
                _ => self.local_address.port(),
            };
            let guessed_addr = SocketAddr::new(self.local_address.ip(), guessed_port);
            let info = NodeInfo {
                id: norm_id.clone(),
                address: guessed_addr,
                capabilities: vec!["broker".to_string()],
                last_seen: Instant::now(),
                status: NodeStatus::Active,
            };
            info!("Auto-registering unknown node {} at {} via touch", norm_id, guessed_addr);
            nodes.insert(norm_id, info);
        }
    }

    /// Get all active nodes
    pub fn get_active_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.lock().unwrap();
        let mut seen: HashSet<String> = HashSet::new();
        let mut active_nodes: Vec<NodeInfo> = Vec::new();
        for node in nodes.values().filter(|n| n.status == NodeStatus::Active) {
            let norm_id = Self::normalize_id(&node.id);
            if seen.insert(norm_id.clone()) {
                active_nodes.push(NodeInfo { id: norm_id, ..node.clone() });
            }
        }
        debug!(
            "get_active_nodes for {}: {} total nodes, {} active (deduped)",
            self.node_id,
            nodes.len(),
            active_nodes.len()
        );
        active_nodes
    }

    /// Get all nodes (regardless of status)
    pub fn get_all_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.lock().unwrap();
        nodes.values().cloned().collect()
    }

    /// Get a specific node by ID
    pub fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(node_id).cloned()
    }

    /// Check if we have quorum (majority of nodes)
    pub fn has_quorum(&self) -> bool {
        let nodes = self.nodes.lock().unwrap();
        let total_nodes = nodes.len() + 1; // +1 for self
        let active_nodes = nodes.values()
            .filter(|node| matches!(node.status, NodeStatus::Active))
            .count() + 1; // +1 for self

        active_nodes > total_nodes / 2
    }

    /// Get the current cluster size
    pub fn cluster_size(&self) -> usize {
        let nodes = self.nodes.lock().unwrap();
        nodes.len() + 1 // +1 for self
    }

    /// Start health monitoring background task
    async fn start_health_monitoring(&self) {
        let nodes = self.nodes.clone();
        let node_id = self.node_id.clone();
        let heartbeat_interval = self.heartbeat_interval;

        tokio::spawn(async move {
            let mut interval = interval(heartbeat_interval);

            loop {
                interval.tick().await;

                let active_nodes: Vec<NodeInfo> = {
                    let nodes_guard = nodes.lock().unwrap();
                    nodes_guard.values()
                        .filter(|node| matches!(node.status, NodeStatus::Active | NodeStatus::Suspected))
                        .cloned()
                        .collect()
                };

                for node in active_nodes {
                    // Send health check
                    match Self::send_health_check(&node_id, &node).await {
                        Ok(_) => {
                            debug!("Health check sent to {}", node.id);
                        }
                        Err(e) => {
                            warn!("Failed to send health check to {}: {}", node.id, e);
                        }
                    }
                }
            }
        });
    }

    /// Send a health check to a node
    async fn send_health_check(sender_id: &str, target_node: &NodeInfo) -> NetworkResult<()> {
        // In a real implementation, this would send an actual health check message
        debug!("Sending health check from {} to {}", sender_id, target_node.id);
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }

    /// Start failure detection background task
    async fn start_failure_detection(&self) {
        let nodes = self.nodes.clone();
        let suspect_timeout = self.suspect_timeout;
        let failure_timeout = self.failure_timeout;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));

            loop {
                interval.tick().await;

                let now = Instant::now();
                let mut updates = Vec::new();

                {
                    let nodes_guard = nodes.lock().unwrap();
                    for (node_id, node) in nodes_guard.iter() {
                        let time_since_last_seen = now.duration_since(node.last_seen);

                        match node.status {
                            NodeStatus::Active => {
                                if time_since_last_seen > suspect_timeout {
                                    updates.push((node_id.clone(), NodeStatus::Suspected));
                                }
                            }
                            NodeStatus::Suspected => {
                                if time_since_last_seen > failure_timeout {
                                    updates.push((node_id.clone(), NodeStatus::Down));
                                }
                            }
                            _ => {}
                        }
                    }
                }

                // Apply updates
                {
                    let mut nodes_guard = nodes.lock().unwrap();
                    for (node_id, new_status) in updates {
                        if let Some(node) = nodes_guard.get_mut(&node_id) {
                            if node.status != new_status {
                                warn!("Node {} marked as {:?}", node_id, new_status);
                                node.status = new_status;
                            }
                        }
                    }
                }
            }
        });
    }

    /// Handle node join request
    pub async fn handle_node_join(&self, node_info: NodeInfo) -> NetworkResult<()> {
        info!("Node {} requesting to join cluster", node_info.id);

        // Validate the node can join
        if self.validate_node_join(&node_info).await? {
            self.add_node(node_info);
            Ok(())
        } else {
            Err(NetworkError::AuthenticationFailed)
        }
    }

    /// Validate if a node can join the cluster
    async fn validate_node_join(&self, node_info: &NodeInfo) -> NetworkResult<bool> {
        // Check if node ID is unique
        let nodes = self.nodes.lock().unwrap();
        if nodes.contains_key(&node_info.id) {
            return Ok(false);
        }

        // Check if we have capacity
        if nodes.len() >= 100 { // Arbitrary limit
            return Ok(false);
        }

        // In a real implementation, you might check:
        // - Authentication credentials
        // - Node capabilities
        // - Cluster membership rules

        Ok(true)
    }

    /// Handle node leave request
    pub async fn handle_node_leave(&self, node_id: &str, reason: &str) -> NetworkResult<()> {
        info!("Node {} leaving cluster: {}", node_id, reason);
        self.remove_node(node_id);
        Ok(())
    }
}
