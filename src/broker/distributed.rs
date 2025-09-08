use crate::auth::{AuthenticationResult, DistributedAuthenticator, ValidationResult};
use crate::broker::consensus::{ConsensusState, RaftConsensus};
use crate::broker::metrics::DistributedMetrics;
use crate::broker::split_brain::{ClusterHealth, SplitBrainPrevention};
use crate::broker::{Broker, cluster::Cluster, replication::ReplicationManager};
use crate::network::error::{NetworkError, NetworkResult};
use crate::network::transport::{HeartbeatHandler, MessageHandler, VoteHandler};
use crate::network::{NetworkTransport, NodeDiscovery, TlsConfig};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};

/// Configuration for distributed broker
#[derive(Debug, Clone)]
pub struct DistributedBrokerConfig {
    pub node_id: String,
    pub cluster_id: String,
    pub listen_address: SocketAddr,
    pub seed_nodes: Vec<SocketAddr>,
    pub replication_factor: usize,
    pub expected_cluster_size: usize,
    pub data_dir: String,

    // Security configuration
    pub tls_config: Option<TlsConfig>,
    pub enable_authentication: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub ca_path: Option<String>,

    // Performance tuning
    pub max_connections: usize,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_ms: u64,
    pub replication_timeout_ms: u64,

    // Monitoring
    pub metrics_enabled: bool,
    pub metrics_port: Option<u16>,
    pub log_level: String,
}

impl DistributedBrokerConfig {
    /// Create a new configuration builder
    pub fn builder(node_id: String, cluster_id: String) -> DistributedBrokerConfigBuilder {
        DistributedBrokerConfigBuilder::new(node_id, cluster_id)
    }
}

/// Builder for DistributedBrokerConfig
pub struct DistributedBrokerConfigBuilder {
    node_id: String,
    cluster_id: String,
    listen_address: Option<SocketAddr>,
    seed_nodes: Vec<SocketAddr>,
    replication_factor: usize,
    expected_cluster_size: usize,
    data_dir: String,
    tls_config: Option<TlsConfig>,
    enable_authentication: bool,
    cert_path: Option<String>,
    key_path: Option<String>,
    ca_path: Option<String>,
    max_connections: usize,
    heartbeat_interval_ms: u64,
    election_timeout_ms: u64,
    replication_timeout_ms: u64,
    metrics_enabled: bool,
    metrics_port: Option<u16>,
    log_level: String,
}

impl DistributedBrokerConfigBuilder {
    pub fn new(node_id: String, cluster_id: String) -> Self {
        Self {
            node_id,
            cluster_id,
            listen_address: None,
            seed_nodes: Vec::new(),
            replication_factor: 3,
            expected_cluster_size: 3,
            data_dir: "data".to_string(),
            tls_config: None,
            enable_authentication: false,
            cert_path: None,
            key_path: None,
            ca_path: None,
            max_connections: 1000,
            heartbeat_interval_ms: 200,
            election_timeout_ms: 1000,
            replication_timeout_ms: 5000,
            metrics_enabled: true,
            metrics_port: None,
            log_level: "info".to_string(),
        }
    }

    pub fn listen_address(mut self, addr: SocketAddr) -> Self {
        self.listen_address = Some(addr);
        self
    }

    pub fn seed_nodes(mut self, nodes: Vec<SocketAddr>) -> Self {
        self.seed_nodes = nodes;
        self
    }

    pub fn replication_factor(mut self, factor: usize) -> Self {
        self.replication_factor = factor;
        self
    }

    pub fn expected_cluster_size(mut self, size: usize) -> Self {
        self.expected_cluster_size = size;
        self
    }

    pub fn data_dir<S: Into<String>>(mut self, dir: S) -> Self {
        self.data_dir = dir.into();
        self
    }

    pub fn with_tls(mut self, config: TlsConfig) -> Self {
        self.tls_config = Some(config);
        self
    }

    pub fn with_tls_files<S: Into<String>>(
        mut self,
        cert_path: S,
        key_path: S,
        ca_path: Option<S>,
    ) -> Self {
        self.cert_path = Some(cert_path.into());
        self.key_path = Some(key_path.into());
        self.ca_path = ca_path.map(|s| s.into());
        self
    }

    pub fn enable_authentication(mut self) -> Self {
        self.enable_authentication = true;
        self
    }

    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    pub fn timings(mut self, heartbeat_ms: u64, election_ms: u64, replication_ms: u64) -> Self {
        self.heartbeat_interval_ms = heartbeat_ms;
        self.election_timeout_ms = election_ms;
        self.replication_timeout_ms = replication_ms;
        self
    }

    pub fn enable_metrics(mut self, port: Option<u16>) -> Self {
        self.metrics_enabled = true;
        self.metrics_port = port;
        self
    }

    pub fn log_level<S: Into<String>>(mut self, level: S) -> Self {
        self.log_level = level.into();
        self
    }

    pub fn build(self) -> Result<DistributedBrokerConfig, String> {
        let listen_address = self
            .listen_address
            .ok_or_else(|| "listen_address is required".to_string())?;

        // Validate configuration
        if self.replication_factor == 0 {
            return Err("replication_factor must be greater than 0".to_string());
        }

        if self.expected_cluster_size < self.replication_factor {
            return Err("expected_cluster_size must be >= replication_factor".to_string());
        }

        if self.heartbeat_interval_ms == 0 || self.election_timeout_ms == 0 {
            return Err("timing intervals must be greater than 0".to_string());
        }

        // Build TLS config if paths are provided
        let tls_config =
            if let (Some(cert_path), Some(key_path)) = (&self.cert_path, &self.key_path) {
                match std::fs::read(cert_path) {
                    Ok(cert_pem) => {
                        match std::fs::read(key_path) {
                            Ok(key_pem) => {
                                match TlsConfig::new().with_server_cert(&cert_pem, &key_pem) {
                                    Ok(mut config) => {
                                        // Add client cert support if CA is provided
                                        if self.ca_path.is_some() {
                                            config = config.with_client_auth();
                                        }
                                        Some(config)
                                    }
                                    Err(e) => {
                                        warn!("Failed to create TLS config: {}", e);
                                        None
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to read key file {}: {}", key_path, e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to read cert file {}: {}", cert_path, e);
                        None
                    }
                }
            } else {
                self.tls_config
            };

        Ok(DistributedBrokerConfig {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            listen_address,
            seed_nodes: self.seed_nodes,
            replication_factor: self.replication_factor,
            expected_cluster_size: self.expected_cluster_size,
            data_dir: self.data_dir,
            tls_config,
            enable_authentication: self.enable_authentication,
            cert_path: self.cert_path,
            key_path: self.key_path,
            ca_path: self.ca_path,
            max_connections: self.max_connections,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            election_timeout_ms: self.election_timeout_ms,
            replication_timeout_ms: self.replication_timeout_ms,
            metrics_enabled: self.metrics_enabled,
            metrics_port: self.metrics_port,
            log_level: self.log_level,
        })
    }
}

/// A distributed message broker with consensus, replication, and split-brain prevention
#[derive(Clone)]
#[allow(dead_code)]
pub struct DistributedBroker {
    config: DistributedBrokerConfig,

    // Core broker functionality
    local_broker: Arc<Broker>,
    cluster: Arc<Cluster>,

    // Network layer
    transport: Arc<NetworkTransport>,
    discovery: Arc<NodeDiscovery>,

    // Distributed algorithms
    consensus: Arc<RaftConsensus>,
    split_brain_prevention: Arc<SplitBrainPrevention>,
    replication_manager: Arc<StdMutex<ReplicationManager>>,

    // Security and authentication
    authenticator: Option<Arc<StdMutex<DistributedAuthenticator>>>,

    // Monitoring and metrics
    metrics: Option<Arc<DistributedMetrics>>,

    // State
    is_running: Arc<Mutex<bool>>,

    // Event channels
    cluster_events_tx: mpsc::Sender<ClusterEvent>,
    cluster_events_rx: Arc<Mutex<mpsc::Receiver<ClusterEvent>>>,
}

#[derive(Debug, Clone)]
pub enum ClusterEvent {
    NodeJoined {
        node_id: String,
        address: SocketAddr,
    },
    NodeLeft {
        node_id: String,
        reason: String,
    },
    LeadershipChanged {
        new_leader: Option<String>,
    },
    PartitionDetected {
        affected_nodes: Vec<String>,
    },
    QuorumLost,
    QuorumRestored,
}

impl DistributedBroker {
    /// Create a new distributed broker
    pub async fn new(config: DistributedBrokerConfig) -> NetworkResult<Self> {
        info!("Creating distributed broker with ID: {}", config.node_id);

        // Initialize authentication if enabled
        let authenticator = if config.enable_authentication {
            let jwt_secret = DistributedAuthenticator::generate_jwt_secret();
            let mut auth = DistributedAuthenticator::new(jwt_secret, config.cluster_id.clone());

            // Add default node permissions
            auth.add_node_permissions(
                config.node_id.clone(),
                vec![
                    "broker".to_string(),
                    "replication".to_string(),
                    "consensus".to_string(),
                ],
            );

            // Add a default admin user for testing with all permissions
            auth.add_user("admin", "admin123");
            auth.add_user_permissions(
                "admin".to_string(),
                vec![
                    "read".to_string(),
                    "write".to_string(),
                    "admin".to_string(),
                    "broker".to_string(),
                    "replication".to_string(),
                    "consensus".to_string(),
                ],
            );

            info!(
                "Authentication system initialized for cluster {}",
                config.cluster_id
            );
            Some(Arc::new(StdMutex::new(auth)))
        } else {
            None
        };
        let metrics = if config.metrics_enabled {
            match DistributedMetrics::new() {
                Ok(m) => {
                    info!("Metrics system initialized");
                    Some(Arc::new(m))
                }
                Err(e) => {
                    warn!("Failed to initialize metrics: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Create core broker
        let local_broker = Arc::new(
            Broker::new(
                &config.node_id,
                3, // partitions
                config.replication_factor,
                &config.data_dir,
            )
            .map_err(|e| {
                NetworkError::ConnectionFailed(format!("Failed to create local broker: {}", e))
            })?,
        );

        // Create cluster
        let cluster = Arc::new(Cluster::new());
        cluster.add_broker(config.node_id.clone(), local_broker.clone());

        // Create network transport with optional TLS
        let transport = if let Some(ref tls_config) = config.tls_config {
            info!("Initializing transport with TLS enabled");
            Arc::new(
                NetworkTransport::new_with_tls(config.node_id.clone(), tls_config.clone()).await?,
            )
        } else {
            info!("Initializing transport without TLS");
            Arc::new(NetworkTransport::new(config.node_id.clone()))
        };

        // Create node discovery
        let discovery = Arc::new(NodeDiscovery::new(
            config.node_id.clone(),
            config.listen_address,
            config.seed_nodes.clone(),
        ));

        // Wire discovery into transport so inbound messages refresh liveness
        transport.set_discovery(discovery.clone());

        // Create consensus with custom timeouts
        let consensus = Arc::new(RaftConsensus::new_with_config(
            config.node_id.clone(),
            transport.clone(),
            vec![], // Will be populated by discovery
            Duration::from_millis(config.heartbeat_interval_ms),
            Duration::from_millis(config.election_timeout_ms),
        ));

        // Create split-brain prevention
        let split_brain_prevention = Arc::new(SplitBrainPrevention::new(
            config.node_id.clone(),
            config.cluster_id.clone(),
            transport.clone(),
            discovery.clone(),
            config.expected_cluster_size,
        ));

        // Create replication manager
        let nodes = Arc::new(StdMutex::new(HashMap::new()));
        let mut replication_manager =
            ReplicationManager::new(nodes, config.replication_factor, config.node_id.clone());
        replication_manager.set_transport(transport.clone());
        let replication_manager = Arc::new(StdMutex::new(replication_manager));

        // Create event channels
        let (cluster_events_tx, cluster_events_rx) = mpsc::channel(100);

        Ok(Self {
            config,
            local_broker,
            cluster,
            transport,
            discovery,
            consensus,
            split_brain_prevention,
            replication_manager,
            authenticator,
            metrics,
            is_running: Arc::new(Mutex::new(false)),
            cluster_events_tx,
            cluster_events_rx: Arc::new(Mutex::new(cluster_events_rx)),
        })
    }

    /// Start the distributed broker
    pub async fn start(&self) -> NetworkResult<()> {
        info!("Starting distributed broker {}", self.config.node_id);

        {
            let mut running = self.is_running.lock().await;
            if *running {
                return Err(NetworkError::ConnectionFailed(
                    "Broker already running".to_string(),
                ));
            }
            *running = true;
        }

        // Start network transport
        self.transport
            .start_listening(self.config.listen_address)
            .await?;

        // Register message handlers
        self.register_message_handlers().await;

        // Start node discovery
        self.discovery.start().await?;

        // Connect to seed nodes first so discovery can populate peers before elections start
        // This prevents immediate elections with an empty peer list which leads to split votes
        self.connect_to_seed_nodes().await?;

        // Give discovery a short moment to register active nodes
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start consensus after peers are connected
        self.consensus.start().await?;

        // For single-node clusters, immediately become leader
        if self.config.expected_cluster_size == 1 && self.config.seed_nodes.is_empty() {
            info!("Single-node cluster detected, becoming leader immediately");
            self.consensus.force_become_leader();
        }

        // Start split-brain prevention
        self.split_brain_prevention.start().await?;

        // Start replication manager
        self.start_replication_manager().await;

        // Start cluster monitoring
        self.start_cluster_monitoring().await;

        // Start metrics collection
        self.start_metrics_collection().await;

        // Start metrics HTTP server if configured
        self.start_metrics_server().await?;

        // Connect to seed nodes
        self.connect_to_seed_nodes().await?;

        info!(
            "Distributed broker {} started successfully",
            self.config.node_id
        );
        Ok(())
    }

    /// Stop the distributed broker
    pub async fn stop(&self) -> NetworkResult<()> {
        info!("Stopping distributed broker {}", self.config.node_id);

        {
            let mut running = self.is_running.lock().await;
            *running = false;
        }

        // Stop network transport
        self.transport.stop().await;

        info!("Distributed broker {} stopped", self.config.node_id);
        Ok(())
    }

    /// Get current peer list
    pub fn get_peers(&self) -> Vec<String> {
        self.consensus.get_peers()
    }

    /// Send a message through the transport
    pub async fn send_message(
        &self,
        message: crate::network::protocol::DistributedMessage,
    ) -> NetworkResult<()> {
        self.transport.send_message(message).await
    }

    /// Register message handlers with the transport
    async fn register_message_handlers(&self) {
        // Register heartbeat handler
        let heartbeat_handler = HeartbeatHandler::new(self.config.node_id.clone());
        self.transport.register_handler(
            crate::network::protocol::MessageType::Heartbeat,
            heartbeat_handler.clone(),
        );
        self.transport.register_handler(
            crate::network::protocol::MessageType::HeartbeatResponse,
            heartbeat_handler,
        );

        // Register vote handler
        let vote_handler = VoteHandler::new(self.config.node_id.clone());
        self.transport.register_handler(
            crate::network::protocol::MessageType::VoteRequest,
            vote_handler.clone(),
        );
        self.transport.register_handler(
            crate::network::protocol::MessageType::VoteResponse,
            vote_handler,
        );

        // Register replication handler
        let replication_handler = ReplicationHandler::new(self.replication_manager.clone());
        self.transport.register_handler(
            crate::network::protocol::MessageType::ReplicationRequest,
            replication_handler.clone(),
        );
        self.transport.register_handler(
            crate::network::protocol::MessageType::ReplicationResponse,
            replication_handler,
        );
    }

    /// Start the replication manager
    async fn start_replication_manager(&self) {
        let replication_manager = self.replication_manager.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            loop {
                // Check running flag without holding it across await
                let running = { *is_running.lock().await };
                if !running {
                    break;
                }

                // Fire-and-forget tick for replication manager without holding the guard across await
                if let Ok(mut manager) = replication_manager.lock() {
                    // Run a non-async tick method to avoid await inside the guard scope
                    manager.start();
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

    /// Start cluster monitoring
    async fn start_cluster_monitoring(&self) {
        let split_brain_prevention = self.split_brain_prevention.clone();
        let consensus = self.consensus.clone();
        let discovery = self.discovery.clone();
        let cluster_events_tx = self.cluster_events_tx.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut last_leader: Option<String> = None;
            let mut last_quorum_status = false;

            while *is_running.lock().await {
                // Check leadership changes
                let current_leader = if consensus.is_leader() {
                    Some(consensus.node_id.clone())
                } else {
                    None
                };

                if last_leader != current_leader {
                    let _ = cluster_events_tx
                        .send(ClusterEvent::LeadershipChanged {
                            new_leader: current_leader.clone(),
                        })
                        .await;
                    last_leader = current_leader;
                }

                // Check quorum status
                let health = split_brain_prevention.get_cluster_health();
                if health.has_quorum != last_quorum_status {
                    let event = if health.has_quorum {
                        ClusterEvent::QuorumRestored
                    } else {
                        ClusterEvent::QuorumLost
                    };
                    let _ = cluster_events_tx.send(event).await;
                    last_quorum_status = health.has_quorum;
                }

                // Check for new nodes and update consensus peers
                let active_nodes = discovery.get_active_nodes();
                let peer_ids: Vec<String> = active_nodes
                    .iter()
                    .filter(|node| {
                        // Normalize node ID for comparison
                        let normalized_node_id = match node.id.as_str() {
                            "node_8081" => "node1",
                            "node_8082" => "node2",
                            "node_8083" => "node3",
                            _ => &node.id,
                        };

                        let normalized_self_id = match consensus.node_id.as_str() {
                            "node_8081" => "node1",
                            "node_8082" => "node2",
                            "node_8083" => "node3",
                            _ => &consensus.node_id,
                        };

                        normalized_node_id != normalized_self_id
                    })
                    .map(|node| {
                        // Use normalized ID for peer list
                        match node.id.as_str() {
                            "node_8081" => "node1".to_string(),
                            "node_8082" => "node2".to_string(),
                            "node_8083" => "node3".to_string(),
                            _ => node.id.clone(),
                        }
                    })
                    .collect::<std::collections::HashSet<_>>() // Remove duplicates
                    .into_iter()
                    .collect();

                // Update consensus peers if the list has changed
                if peer_ids != consensus.get_peers() {
                    consensus.update_peers(peer_ids.clone());
                    info!("Updated consensus peers to: {:?}", peer_ids);
                }

                for node in active_nodes {
                    // In a real implementation, we'd track which nodes we've seen before
                    debug!("Active node: {} at {}", node.id, node.address);
                }

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    /// Connect to seed nodes
    async fn connect_to_seed_nodes(&self) -> NetworkResult<()> {
        for seed_addr in &self.config.seed_nodes {
            // Skip connecting to our own address
            if *seed_addr == self.config.listen_address {
                continue;
            }

            info!("Connecting to seed node at {}", seed_addr);

            // Map address to consistent node ID for demo
            let node_id = match seed_addr.port() {
                8081 => "node1".to_string(),
                8082 => "node2".to_string(),
                8083 => "node3".to_string(),
                port => format!("node_{}", port),
            };

            if let Err(e) = self
                .transport
                .connect_to_peer(node_id.clone(), *seed_addr)
                .await
            {
                warn!("Failed to connect to seed node {}: {}", seed_addr, e);
            } else {
                info!("Connected to seed node {} at {}", node_id, seed_addr);

                // Add the connected node to discovery
                use crate::network::discovery::{NodeInfo, NodeStatus};
                let node_info = NodeInfo {
                    id: node_id,
                    address: *seed_addr,
                    capabilities: vec!["broker".to_string()],
                    last_seen: std::time::Instant::now(),
                    status: NodeStatus::Active,
                };
                self.discovery.add_node(node_info);
            }
        }

        Ok(())
    }

    /// Check if this node is the current leader
    pub fn is_leader(&self) -> bool {
        self.consensus.is_leader() && self.split_brain_prevention.can_act_as_leader()
    }

    /// Get cluster health status
    pub fn get_cluster_health(&self) -> ClusterHealth {
        self.split_brain_prevention.get_cluster_health()
    }

    /// Get connected peers
    pub fn get_connected_peers(&self) -> Vec<String> {
        self.transport.get_connected_peers()
    }

    /// Send a broker message (only if leader and have quorum)
    pub async fn send_broker_message(&self, topic: &str, message: Vec<u8>) -> NetworkResult<()> {
        info!(
            "Attempting to send message to topic '{}' ({} bytes)",
            topic,
            message.len()
        );
        info!(
            "Broker status - Is leader: {}, Can act as leader: {}",
            self.is_leader(),
            self.split_brain_prevention.can_act_as_leader()
        );

        if !self.is_leader() {
            warn!(
                "Cannot send message: Node {} is not the leader",
                self.config.node_id
            );
            return Err(NetworkError::NotLeader);
        }

        if !self.split_brain_prevention.can_act_as_leader() {
            warn!("Cannot send message: Network partition detected or insufficient quorum");
            return Err(NetworkError::NetworkPartition);
        }

        // Use local broker to send message
        // In practice, this would also trigger replication
        info!(
            "Successfully sending message to topic {}: {} bytes",
            topic,
            message.len()
        );

        // Trigger replication
        if let Ok(manager) = self.replication_manager.lock() {
            let _ = manager.send_data_update(topic.to_string(), message).await;
        }

        Ok(())
    }

    /// Subscribe to cluster events
    pub async fn next_cluster_event(&self) -> Option<ClusterEvent> {
        // Use try_lock and handle immediately to avoid holding across await
        match self.cluster_events_rx.try_lock() {
            Ok(mut rx) => rx.recv().await,
            Err(_) => None,
        }
    }

    /// Get current consensus state
    pub fn get_consensus_state(&self) -> ConsensusState {
        self.consensus.get_state()
    }

    /// Get current term
    pub fn get_term(&self) -> u64 {
        self.consensus.get_term()
    }

    /// Force a quorum check (for testing or manual intervention)
    pub async fn force_quorum_check(&self) -> NetworkResult<bool> {
        self.split_brain_prevention.force_quorum_check().await
    }

    /// Get metrics registry (for Prometheus integration)
    pub fn get_metrics_registry(&self) -> Option<Arc<prometheus::Registry>> {
        self.metrics.as_ref().map(|m| m.registry())
    }

    /// Export current metrics as text
    pub fn export_metrics(&self) -> String {
        match &self.metrics {
            Some(metrics) => metrics.export_metrics(),
            None => "Metrics not enabled".to_string(),
        }
    }

    /// Get configuration
    pub fn get_config(&self) -> &DistributedBrokerConfig {
        &self.config
    }

    /// Authenticate a client and return JWT token
    pub fn authenticate_client(
        &self,
        username: &str,
        password: &str,
    ) -> Option<AuthenticationResult> {
        if let Some(ref authenticator) = self.authenticator {
            if let Ok(auth) = authenticator.lock() {
                return Some(auth.authenticate_client(username, password));
            }
        }
        None
    }

    /// Authenticate a node and return JWT token
    pub fn authenticate_node(
        &self,
        node_id: &str,
        provided_secret: &str,
    ) -> Option<AuthenticationResult> {
        if let Some(ref authenticator) = self.authenticator {
            if let Ok(auth) = authenticator.lock() {
                return Some(auth.authenticate_node(
                    node_id,
                    &self.config.cluster_id,
                    provided_secret,
                ));
            }
        }
        None
    }

    /// Validate a JWT token
    pub fn validate_token(&self, token: &str) -> Option<ValidationResult> {
        if let Some(ref authenticator) = self.authenticator {
            if let Ok(auth) = authenticator.lock() {
                return Some(auth.validate_token(token));
            }
        }
        None
    }

    /// Check if a token has specific permission
    pub fn has_permission(&self, token: &str, permission: &str) -> bool {
        if let Some(ref authenticator) = self.authenticator {
            if let Ok(auth) = authenticator.lock() {
                return auth.has_permission(token, permission);
            }
        }
        // If authentication is disabled, allow all operations
        !self.config.enable_authentication
    }

    /// Update metrics with current system state
    pub fn update_metrics(&self) {
        if let Some(ref metrics) = self.metrics {
            // Update Raft state
            let state_value = match self.consensus.get_state() {
                ConsensusState::Follower => 0,
                ConsensusState::Candidate => 1,
                ConsensusState::Leader => 2,
                ConsensusState::Shutdown => 3,
            };
            metrics.set_raft_state(state_value);
            metrics.raft_term.set(self.consensus.get_term() as f64);

            // Update cluster health
            let health = self.get_cluster_health();
            metrics.update_cluster_health(
                health.active_nodes,
                health.has_quorum,
                health.suspected_nodes,
                health.isolated_nodes,
            );

            // Update network connections
            let connected_peers = self.get_connected_peers();
            metrics
                .network_connections_active
                .set(connected_peers.len() as f64);
        }
    }

    /// Start metrics collection background task
    async fn start_metrics_collection(&self) {
        if let Some(metrics) = &self.metrics {
            let _metrics = metrics.clone();
            let broker = self.clone();
            let is_running = self.is_running.clone();

            tokio::spawn(async move {
                while *is_running.lock().await {
                    broker.update_metrics();
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });

            info!("Metrics collection started");
        }
    }

    /// Start metrics HTTP server if configured
    async fn start_metrics_server(&self) -> NetworkResult<()> {
        if let (Some(metrics), Some(port)) = (&self.metrics, self.config.metrics_port) {
            let registry = metrics.registry();
            let metrics_addr = format!("0.0.0.0:{}", port);

            tokio::spawn(async move {
                use actix_web::{App, HttpResponse, HttpServer, Result, web};

                async fn metrics_handler(
                    registry: web::Data<Arc<prometheus::Registry>>,
                ) -> Result<HttpResponse> {
                    let encoder = prometheus::TextEncoder::new();
                    let metric_families = registry.gather();

                    match encoder.encode_to_string(&metric_families) {
                        Ok(output) => Ok(HttpResponse::Ok()
                            .content_type("text/plain; charset=utf-8")
                            .body(output)),
                        Err(e) => {
                            error!("Failed to encode metrics: {}", e);
                            Ok(
                                HttpResponse::InternalServerError()
                                    .body("Failed to encode metrics"),
                            )
                        }
                    }
                }

                let registry_clone = registry.clone();
                if let Err(e) = HttpServer::new(move || {
                    App::new()
                        .app_data(web::Data::new(registry_clone.clone()))
                        .route("/metrics", web::get().to(metrics_handler))
                })
                .bind(&metrics_addr)
                {
                    warn!("Failed to bind metrics server to {}: {}", metrics_addr, e);
                } else {
                    info!("Metrics server started on {}", metrics_addr);
                    let _ = HttpServer::new(move || {
                        App::new()
                            .app_data(web::Data::new(registry.clone()))
                            .route("/metrics", web::get().to(metrics_handler))
                    })
                    .bind(&metrics_addr)
                    .unwrap()
                    .run()
                    .await;
                }
            });
        }

        Ok(())
    }
}

/// Message handler for replication messages
pub struct ReplicationHandler {
    replication_manager: Arc<StdMutex<ReplicationManager>>,
}

impl ReplicationHandler {
    pub fn new(replication_manager: Arc<StdMutex<ReplicationManager>>) -> Self {
        Self {
            replication_manager,
        }
    }
}

impl Clone for ReplicationHandler {
    fn clone(&self) -> Self {
        Self {
            replication_manager: self.replication_manager.clone(),
        }
    }
}

impl MessageHandler for ReplicationHandler {
    fn handle(
        &self,
        message: crate::network::protocol::DistributedMessage,
    ) -> NetworkResult<Option<crate::network::protocol::DistributedMessage>> {
        match message.message_type {
            crate::network::protocol::MessageType::ReplicationRequest => {
                info!("Processing replication request from {}", message.sender_id);

                // Handle the replication request synchronously to avoid async issues
                if let Ok(manager) = self.replication_manager.lock() {
                    // Create a blocking runtime for async operation
                    let rt = tokio::runtime::Handle::current();
                    match rt.block_on(manager.handle_replication_request(message)) {
                        Ok(response_opt) => {
                            response_opt.map_or(Ok(None), |response| Ok(Some(response)))
                        }
                        Err(e) => {
                            error!("Failed to handle replication request: {}", e);
                            Err(e)
                        }
                    }
                } else {
                    error!("Failed to acquire replication manager lock");
                    Err(NetworkError::InvalidMessageFormat)
                }
            }
            crate::network::protocol::MessageType::ReplicationResponse => {
                info!("Processing replication response from {}", message.sender_id);

                if let Ok(manager) = self.replication_manager.lock() {
                    let rt = tokio::runtime::Handle::current();
                    match rt.block_on(manager.handle_replication_response(message)) {
                        Ok(_) => Ok(None),
                        Err(e) => {
                            error!("Failed to handle replication response: {}", e);
                            Err(e)
                        }
                    }
                } else {
                    error!("Failed to acquire replication manager lock");
                    Err(NetworkError::InvalidMessageFormat)
                }
            }
            _ => Err(NetworkError::InvalidMessageFormat),
        }
    }
}
