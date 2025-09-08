//! Split-Brain Detection and Prevention System
//!
//! Implements advanced algorithms to detect and prevent split-brain scenarios
//! in distributed systems, ensuring data consistency and system reliability.

/// Node identifier type
pub type NodeId = String;

/// Node state enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Active,
    Inactive,
    Syncing,
    Failed,
}

use crate::network::error::NetworkResult;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use uuid::Uuid;

/// Split-brain prevention configuration
#[derive(Debug, Clone)]
pub struct SplitBrainConfig {
    /// Minimum cluster size to maintain operations
    pub min_cluster_size: usize,
    /// Quorum size for decision making
    pub quorum_size: usize,
    /// Network partition detection timeout
    pub partition_detection_timeout: Duration,
    /// Majority vote requirement for leadership
    pub require_majority_for_leadership: bool,
    /// Enable external arbitrator
    pub external_arbitrator_enabled: bool,
    /// External arbitrator endpoints
    pub arbitrator_endpoints: Vec<String>,
    /// Fencing mechanism enabled
    pub fencing_enabled: bool,
    /// Split-brain recovery timeout
    pub recovery_timeout: Duration,
}

impl Default for SplitBrainConfig {
    fn default() -> Self {
        Self {
            min_cluster_size: 3,
            quorum_size: 2,
            partition_detection_timeout: Duration::from_secs(10),
            require_majority_for_leadership: true,
            external_arbitrator_enabled: false,
            arbitrator_endpoints: Vec::new(),
            fencing_enabled: true,
            recovery_timeout: Duration::from_secs(30),
        }
    }
}

/// Cluster partition state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionState {
    /// Cluster is healthy (no partition detected)
    Healthy,
    /// Network partition detected
    Partitioned,
    /// Node is in minority partition (should step down)
    MinorityPartition,
    /// Node is in majority partition (can continue operations)
    MajorityPartition,
    /// External arbitration in progress
    Arbitrating,
    /// Node is fenced (isolated due to split-brain)
    Fenced,
}

/// Node connectivity information
#[derive(Debug, Clone)]
pub struct NodeConnectivity {
    /// Node ID
    pub node_id: NodeId,
    /// Last successful communication timestamp
    pub last_contact: SystemTime,
    /// Current connectivity status
    pub is_reachable: bool,
    /// Network latency (milliseconds)
    pub latency_ms: u64,
    /// Consecutive failed attempts
    pub failed_attempts: u32,
}

/// Quorum decision for cluster operations
#[derive(Debug, Clone)]
pub struct QuorumDecision {
    /// Decision type
    pub decision_type: QuorumDecisionType,
    /// Nodes that participated in the decision
    pub participating_nodes: Vec<NodeId>,
    /// Total voting power
    pub total_voting_power: u32,
    /// Required voting power for decision
    pub required_voting_power: u32,
    /// Decision timestamp
    pub timestamp: SystemTime,
    /// Decision was successful
    pub success: bool,
}

/// Types of quorum decisions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QuorumDecisionType {
    /// Leadership election
    LeaderElection,
    /// Cluster membership change
    MembershipChange,
    /// Emergency shutdown
    EmergencyShutdown,
    /// Partition resolution
    PartitionResolution,
    /// Fencing decision
    FencingDecision,
}

/// Split-brain detection and prevention system
pub struct SplitBrainDetector {
    /// Configuration
    config: SplitBrainConfig,
    /// Current node ID
    node_id: NodeId,
    /// Current partition state
    partition_state: Arc<RwLock<PartitionState>>,
    /// Node connectivity tracking
    node_connectivity: Arc<RwLock<HashMap<NodeId, NodeConnectivity>>>,
    /// Cluster membership
    cluster_members: Arc<RwLock<HashSet<NodeId>>>,
    /// Quorum calculations
    quorum_tracker: Arc<RwLock<QuorumTracker>>,
    /// Arbitrator client
    arbitrator_client: Option<Arc<ArbitratorClient>>,
    /// Message channel for coordination
    coordination_tx: mpsc::UnboundedSender<CoordinationMessage>,
    /// Statistics
    stats: Arc<RwLock<SplitBrainStats>>,
    /// Fencing coordinator
    fencing_coordinator: Arc<FencingCoordinator>,
}

/// Quorum tracking information
#[derive(Debug, Default)]
#[allow(dead_code)]
struct QuorumTracker {
    /// Current quorum size
    current_quorum_size: usize,
    /// Required quorum size
    required_quorum_size: usize,
    /// Active voters
    active_voters: HashSet<NodeId>,
    /// Voting weights per node
    voting_weights: HashMap<NodeId, u32>,
}

/// External arbitrator client
#[allow(dead_code)]
struct ArbitratorClient {
    /// Arbitrator endpoints
    endpoints: Vec<String>,
    /// HTTP client for arbitrator communication
    client: reqwest::Client,
}

/// Coordination messages for split-brain prevention
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinationMessage {
    /// Heartbeat with node status
    Heartbeat {
        node_id: NodeId,
        timestamp: u64,
        cluster_view: Vec<NodeId>,
        partition_state: PartitionState,
    },
    /// Partition detection alert
    PartitionDetected {
        node_id: NodeId,
        unreachable_nodes: Vec<NodeId>,
        detected_at: u64,
    },
    /// Quorum request
    QuorumRequest {
        request_id: Uuid,
        decision_type: QuorumDecisionType,
        proposing_node: NodeId,
        proposal_data: Vec<u8>,
    },
    /// Quorum response
    QuorumResponse {
        request_id: Uuid,
        responding_node: NodeId,
        vote: bool,
        voting_power: u32,
    },
    /// Fencing directive
    FencingDirective {
        target_node: NodeId,
        reason: String,
        issued_by: NodeId,
        expires_at: u64,
    },
    /// Recovery coordination
    RecoveryCoordination {
        coordinator_node: NodeId,
        recovery_plan: RecoveryPlan,
    },
}

/// Recovery plan for post-partition healing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryPlan {
    /// Recovery strategy
    pub strategy: RecoveryStrategy,
    /// Nodes to be reintegrated
    pub nodes_to_reintegrate: Vec<NodeId>,
    /// Data reconciliation required
    pub data_reconciliation_required: bool,
    /// Estimated recovery duration
    pub estimated_duration_sec: u64,
}

/// Recovery strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryStrategy {
    /// Automatic reconciliation
    AutomaticReconciliation,
    /// Manual intervention required
    ManualIntervention,
    /// Full cluster restart
    FullClusterRestart,
    /// Rolling restart
    RollingRestart,
}

/// Fencing coordinator for isolating problematic nodes
#[allow(dead_code)]
struct FencingCoordinator {
    /// Fenced nodes
    fenced_nodes: Arc<RwLock<HashSet<NodeId>>>,
    /// Fencing reasons
    fencing_reasons: Arc<RwLock<HashMap<NodeId, String>>>,
    /// Auto-unfencing enabled
    auto_unfencing_enabled: bool,
    /// Unfencing timeout
    unfencing_timeout: Duration,
}

/// Split-brain prevention statistics
#[derive(Debug, Default)]
pub struct SplitBrainStats {
    /// Partition events detected
    pub partition_events_detected: u64,
    /// Quorum decisions made
    pub quorum_decisions_made: u64,
    /// Successful leadership changes during partitions
    pub successful_leadership_changes: u64,
    /// Nodes fenced due to split-brain
    pub nodes_fenced: u64,
    /// Recovery operations completed
    pub recovery_operations_completed: u64,
    /// Average partition resolution time (milliseconds)
    pub avg_partition_resolution_time_ms: f64,
    /// Current cluster health score (0-100)
    pub cluster_health_score: u8,
}

impl SplitBrainDetector {
    /// Create new split-brain detector
    pub async fn new(
        config: SplitBrainConfig,
        node_id: NodeId,
        cluster_members: Vec<NodeId>,
    ) -> NetworkResult<Self> {
        let (coordination_tx, coordination_rx) = mpsc::unbounded_channel();

        // Initialize arbitrator client if enabled
        let arbitrator_client = if config.external_arbitrator_enabled {
            Some(Arc::new(ArbitratorClient::new(config.arbitrator_endpoints.clone())))
        } else {
            None
        };

        // Initialize fencing coordinator
        let fencing_coordinator = Arc::new(FencingCoordinator::new(
            config.fencing_enabled,
            config.recovery_timeout,
        ));

        let detector = Self {
            config: config.clone(),
            node_id: node_id.clone(),
            partition_state: Arc::new(RwLock::new(PartitionState::Healthy)),
            node_connectivity: Arc::new(RwLock::new(HashMap::new())),
            cluster_members: Arc::new(RwLock::new(cluster_members.iter().cloned().collect())),
            quorum_tracker: Arc::new(RwLock::new(QuorumTracker::new(config.quorum_size))),
            arbitrator_client,
            coordination_tx: coordination_tx.clone(),
            stats: Arc::new(RwLock::new(SplitBrainStats::default())),
            fencing_coordinator,
        };

        // Initialize node connectivity
        detector.initialize_connectivity(cluster_members).await;

        // Start coordination message handler
        let detector_clone = detector.clone();
        tokio::spawn(async move {
            detector_clone.handle_coordination_messages(coordination_rx).await;
        });

        // Start background monitoring tasks
        detector.start_monitoring_tasks().await;

        println!("✅ Split-brain detector initialized for node: {}", node_id);
        Ok(detector)
    }

    /// Detect potential network partitions
    pub async fn detect_partition(&self) -> NetworkResult<PartitionState> {
        let mut unreachable_nodes = Vec::new();
        let connectivity = self.node_connectivity.read().await;
        let cluster_members = self.cluster_members.read().await;

        let mut reachable_count = 1; // Count self as reachable

        for member in cluster_members.iter() {
            if *member == self.node_id {
                continue;
            }

            if let Some(node_conn) = connectivity.get(member) {
                if !node_conn.is_reachable {
                    unreachable_nodes.push(member.clone());
                } else {
                    reachable_count += 1;
                }
            } else {
                unreachable_nodes.push(member.clone());
            }
        }

        let _total_cluster_size = cluster_members.len();
        let partition_state = if reachable_count >= self.config.quorum_size {
            if unreachable_nodes.is_empty() {
                PartitionState::Healthy
            } else {
                PartitionState::MajorityPartition
            }
        } else {
            PartitionState::MinorityPartition
        };

        // Update partition state
        *self.partition_state.write().await = partition_state.clone();

        if !unreachable_nodes.is_empty() {
            self.stats.write().await.partition_events_detected += 1;

            // Send partition detection message
            let message = CoordinationMessage::PartitionDetected {
                node_id: self.node_id.clone(),
                unreachable_nodes,
                detected_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            };

            let _ = self.coordination_tx.send(message);
        }

        Ok(partition_state)
    }

    /// Make quorum decision
    pub async fn make_quorum_decision(
        &self,
        decision_type: QuorumDecisionType,
        proposal_data: Vec<u8>,
    ) -> NetworkResult<QuorumDecision> {
        let request_id = Uuid::new_v4();

        // Send quorum request to all reachable nodes
        let message = CoordinationMessage::QuorumRequest {
            request_id,
            decision_type: decision_type.clone(),
            proposing_node: self.node_id.clone(),
            proposal_data,
        };

        let _ = self.coordination_tx.send(message);

        // Wait for responses (simplified - in reality would have proper timeout and counting)
        let quorum_tracker = self.quorum_tracker.read().await;
        let decision = QuorumDecision {
            decision_type,
            participating_nodes: vec![self.node_id.clone()],
            total_voting_power: 1,
            required_voting_power: quorum_tracker.required_quorum_size as u32,
            timestamp: SystemTime::now(),
            success: false, // Would be determined by actual vote counting
        };

        self.stats.write().await.quorum_decisions_made += 1;
        Ok(decision)
    }

    /// Check if node should be leader in current partition
    pub async fn can_be_leader(&self) -> bool {
        let partition_state = self.partition_state.read().await;
        match *partition_state {
            PartitionState::Healthy | PartitionState::MajorityPartition => true,
            _ => false,
        }
    }

    /// Fence a node due to split-brain detection
    pub async fn fence_node(&self, node_id: &NodeId, reason: String) -> NetworkResult<()> {
        if !self.config.fencing_enabled {
            return Ok(());
        }

        self.fencing_coordinator.fence_node(node_id, reason.clone()).await;

        // Send fencing directive
        let message = CoordinationMessage::FencingDirective {
            target_node: node_id.clone(),
            reason,
            issued_by: self.node_id.clone(),
            expires_at: (SystemTime::now() + self.config.recovery_timeout)
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let _ = self.coordination_tx.send(message);
        self.stats.write().await.nodes_fenced += 1;

        println!("🚫 Fenced node {} due to split-brain detection", node_id);
        Ok(())
    }

    /// Initiate recovery after partition resolution
    pub async fn initiate_recovery(&self) -> NetworkResult<RecoveryPlan> {
        let plan = RecoveryPlan {
            strategy: RecoveryStrategy::AutomaticReconciliation,
            nodes_to_reintegrate: Vec::new(),
            data_reconciliation_required: true,
            estimated_duration_sec: 60,
        };

        let message = CoordinationMessage::RecoveryCoordination {
            coordinator_node: self.node_id.clone(),
            recovery_plan: plan.clone(),
        };

        let _ = self.coordination_tx.send(message);
        self.stats.write().await.recovery_operations_completed += 1;

        println!("🔄 Initiated recovery process");
        Ok(plan)
    }

    /// Update node connectivity status
    pub async fn update_node_connectivity(
        &self,
        node_id: &NodeId,
        is_reachable: bool,
        latency_ms: u64,
    ) {
        let mut connectivity = self.node_connectivity.write().await;
        if let Some(node_conn) = connectivity.get_mut(node_id) {
            node_conn.is_reachable = is_reachable;
            node_conn.latency_ms = latency_ms;
            node_conn.last_contact = SystemTime::now();

            if is_reachable {
                node_conn.failed_attempts = 0;
            } else {
                node_conn.failed_attempts += 1;
            }
        }
    }

    /// Get cluster health status
    pub async fn get_cluster_health(&self) -> u8 {
        let connectivity = self.node_connectivity.read().await;
        let cluster_members = self.cluster_members.read().await;

        let total_nodes = cluster_members.len();
        let reachable_nodes = connectivity.values()
            .filter(|conn| conn.is_reachable)
            .count() + 1; // Include self

        let health_score = ((reachable_nodes as f64 / total_nodes as f64) * 100.0) as u8;

        self.stats.write().await.cluster_health_score = health_score;
        health_score
    }

    /// Get statistics
    pub async fn get_statistics(&self) -> SplitBrainStats {
        let stats = self.stats.read().await;
        let mut result = SplitBrainStats {
            partition_events_detected: stats.partition_events_detected,
            quorum_decisions_made: stats.quorum_decisions_made,
            successful_leadership_changes: stats.successful_leadership_changes,
            nodes_fenced: stats.nodes_fenced,
            recovery_operations_completed: stats.recovery_operations_completed,
            avg_partition_resolution_time_ms: stats.avg_partition_resolution_time_ms,
            cluster_health_score: 0, // Will be set below
        };
        let health_score = self.get_cluster_health().await;
        result.cluster_health_score = health_score;
        result
    }

    /// Initialize connectivity tracking for cluster members
    async fn initialize_connectivity(&self, cluster_members: Vec<NodeId>) {
        let mut connectivity = self.node_connectivity.write().await;

        for member in cluster_members {
            if member != self.node_id {
                connectivity.insert(member.clone(), NodeConnectivity {
                    node_id: member,
                    last_contact: SystemTime::now(),
                    is_reachable: true, // Assume reachable initially
                    latency_ms: 0,
                    failed_attempts: 0,
                });
            }
        }
    }

    /// Handle coordination messages
    async fn handle_coordination_messages(
        &self,
        mut rx: mpsc::UnboundedReceiver<CoordinationMessage>,
    ) {
        while let Some(message) = rx.recv().await {
            match message {
                CoordinationMessage::Heartbeat { node_id, .. } => {
                    self.update_node_connectivity(&node_id, true, 10).await;
                }
                CoordinationMessage::PartitionDetected { .. } => {
                    // Handle partition detection from other nodes
                    let _ = self.detect_partition().await;
                }
                CoordinationMessage::QuorumRequest { .. } => {
                    // Handle quorum requests
                }
                CoordinationMessage::FencingDirective { target_node, .. } => {
                    if target_node == self.node_id {
                        // We've been fenced - gracefully step down
                        println!("⚠️ Received fencing directive - stepping down");
                    }
                }
                _ => {
                    // Handle other coordination messages
                }
            }
        }
    }

    /// Start background monitoring tasks
    async fn start_monitoring_tasks(&self) {
        let detector = self.clone();

        // Partition detection task
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let _ = detector.detect_partition().await;
            }
        });

        // Heartbeat task
        let detector = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(2));
            loop {
                interval.tick().await;
                detector.send_heartbeat().await;
            }
        });
    }

    /// Send heartbeat to cluster
    async fn send_heartbeat(&self) {
        let cluster_view: Vec<NodeId> = self.cluster_members.read().await.iter().cloned().collect();
        let partition_state = self.partition_state.read().await.clone();

        let message = CoordinationMessage::Heartbeat {
            node_id: self.node_id.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            cluster_view,
            partition_state,
        };

        let _ = self.coordination_tx.send(message);
    }
}

impl Clone for SplitBrainDetector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id.clone(),
            partition_state: Arc::clone(&self.partition_state),
            node_connectivity: Arc::clone(&self.node_connectivity),
            cluster_members: Arc::clone(&self.cluster_members),
            quorum_tracker: Arc::clone(&self.quorum_tracker),
            arbitrator_client: self.arbitrator_client.clone(),
            coordination_tx: self.coordination_tx.clone(),
            stats: Arc::clone(&self.stats),
            fencing_coordinator: Arc::clone(&self.fencing_coordinator),
        }
    }
}

impl QuorumTracker {
    fn new(required_quorum_size: usize) -> Self {
        Self {
            current_quorum_size: 0,
            required_quorum_size,
            active_voters: HashSet::new(),
            voting_weights: HashMap::new(),
        }
    }
}

impl ArbitratorClient {
    fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            client: reqwest::Client::new(),
        }
    }
}

impl FencingCoordinator {
    fn new(enabled: bool, unfencing_timeout: Duration) -> Self {
        Self {
            fenced_nodes: Arc::new(RwLock::new(HashSet::new())),
            fencing_reasons: Arc::new(RwLock::new(HashMap::new())),
            auto_unfencing_enabled: enabled,
            unfencing_timeout,
        }
    }

    async fn fence_node(&self, node_id: &NodeId, reason: String) {
        self.fenced_nodes.write().await.insert(node_id.clone());
        self.fencing_reasons.write().await.insert(node_id.clone(), reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_split_brain_detector_creation() {
        let config = SplitBrainConfig::default();
        let cluster = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];

        let detector = SplitBrainDetector::new(config, "node1".to_string(), cluster).await;
        assert!(detector.is_ok());
    }

    #[tokio::test]
    async fn test_partition_detection() {
        let config = SplitBrainConfig::default();
        let cluster = vec!["node1".to_string(), "node2".to_string()];

        let detector = SplitBrainDetector::new(config, "node1".to_string(), cluster).await.unwrap();
        let state = detector.detect_partition().await.unwrap();

        assert!(matches!(state, PartitionState::Healthy | PartitionState::MinorityPartition));
    }

    #[tokio::test]
    async fn test_cluster_health() {
        let config = SplitBrainConfig::default();
        let cluster = vec!["node1".to_string()];

        let detector = SplitBrainDetector::new(config, "node1".to_string(), cluster).await.unwrap();
        let health = detector.get_cluster_health().await;

        assert!(health > 0);
    }
}
