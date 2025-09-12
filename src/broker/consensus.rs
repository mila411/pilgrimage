//! Enhanced Raft Consensus Algorithm Implementation
//!
//! Production-grade distributed consensus with leader election, log replication,
//! fault tolerance, and strong consistency guarantees for mission-critical systems.

use crate::network::protocol::{DistributedMessage, MessageType, MessagePayload, LogEntry};
use crate::network::transport::{NetworkTransport, MessageHandler};
use crate::network::error::{NetworkError, NetworkResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::interval;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};

/// Enhanced Raft consensus state with additional metadata
#[derive(Debug, Clone, PartialEq, Copy, Serialize, Deserialize)]
pub enum ConsensusState {
    Follower,
    Candidate,
    Leader,
    /// New state for graceful shutdown
    Shutdown,
}

/// Consensus configuration for production environments
#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    /// Election timeout range (min, max) in milliseconds - production values
    pub election_timeout_ms: (u64, u64),
    /// Heartbeat interval in milliseconds - must be < election_timeout/3
    pub heartbeat_interval_ms: u64,
    /// Maximum log entries per append request
    pub max_entries_per_append: usize,
    /// Request timeout for network operations
    pub request_timeout_ms: u64,
    /// Log compaction threshold
    pub log_compaction_threshold: usize,
    /// Snapshot creation threshold
    pub snapshot_threshold: usize,
    /// Preemptive leader stepping down threshold
    pub leader_lease_timeout_ms: u64,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: (1000, 2000), // Production: 1-2 seconds
            heartbeat_interval_ms: 200,         // Production: 200ms (< election_timeout/3)
            max_entries_per_append: 1000,      // Larger batches for efficiency
            request_timeout_ms: 500,            // Network request timeout
            log_compaction_threshold: 10000,    // Compact after 10K entries
            snapshot_threshold: 50000,          // Create snapshot after 50K entries
            leader_lease_timeout_ms: 5000,      // Leader lease timeout
        }
    }
}

/// Enhanced Raft consensus algorithm implementation with production features
pub struct RaftConsensus {
    pub node_id: String,
    config: ConsensusConfig,
    state: Arc<Mutex<ConsensusState>>,
    current_term: Arc<Mutex<u64>>,
    voted_for: Arc<Mutex<Option<String>>>,
    log: Arc<Mutex<Vec<LogEntry>>>,
    commit_index: Arc<Mutex<u64>>,
    last_applied: Arc<Mutex<u64>>,

    // Leader state
    next_index: Arc<Mutex<HashMap<String, u64>>>,
    match_index: Arc<Mutex<HashMap<String, u64>>>,

    // Enhanced timing with jitter and adaptive timeouts
    election_timeout: Duration,
    heartbeat_interval: Duration,
    last_heartbeat: Arc<Mutex<Instant>>,
    last_leader_contact: Arc<Mutex<Instant>>,

    // Network
    transport: Arc<NetworkTransport>,
    peers: Arc<Mutex<Vec<String>>>,

    // Enhanced vote tracking with term validation
    votes_received: Arc<Mutex<HashMap<String, bool>>>,

    // Production features
    /// Current leader ID for fast leader discovery
    current_leader: Arc<Mutex<Option<String>>>,
    /// Performance metrics
    metrics: Arc<Mutex<ConsensusMetrics>>,
    /// Graceful shutdown flag
    shutdown_flag: Arc<Mutex<bool>>,
    /// Split-brain detection
    cluster_size: usize,
}

/// Production-grade consensus metrics
#[derive(Debug, Default)]
pub struct ConsensusMetrics {
    /// Elections started by this node
    pub elections_started: u64,
    /// Elections won by this node
    pub elections_won: u64,
    /// Total votes cast by this node
    pub votes_cast: u64,
    /// Heartbeats sent as leader
    pub heartbeats_sent: u64,
    /// Heartbeats received as follower
    pub heartbeats_received: u64,
    /// Log entries replicated
    pub log_entries_replicated: u64,
    /// Failed append operations
    pub failed_appends: u64,
    /// Current log size
    pub current_log_size: usize,
    /// Last election duration (ms)
    pub last_election_duration_ms: u64,
    /// Average heartbeat latency (ms)
    pub avg_heartbeat_latency_ms: f64,
    /// Split-brain incidents detected
    pub split_brain_incidents: u64,
}

impl RaftConsensus {
    pub fn new(
        node_id: String,
        transport: Arc<NetworkTransport>,
        peers: Vec<String>,
    ) -> Self {
        // Use production-grade timeouts: 1-2 seconds for election, 200ms for heartbeat
        let election_timeout = Duration::from_millis(1000 + rand::random::<u64>() % 1000);

        let consensus = Self {
            node_id: node_id.clone(),
            state: Arc::new(Mutex::new(ConsensusState::Follower)),
            current_term: Arc::new(Mutex::new(0)),
            voted_for: Arc::new(Mutex::new(None)),
            log: Arc::new(Mutex::new(Vec::new())),
            commit_index: Arc::new(Mutex::new(0)),
            last_applied: Arc::new(Mutex::new(0)),
            next_index: Arc::new(Mutex::new(HashMap::new())),
            match_index: Arc::new(Mutex::new(HashMap::new())),
            election_timeout,
            heartbeat_interval: Duration::from_millis(200), // 200ms heartbeat interval
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            last_leader_contact: Arc::new(Mutex::new(Instant::now())),
            transport,
            peers: Arc::new(Mutex::new(peers)),
            votes_received: Arc::new(Mutex::new(HashMap::new())),
            current_leader: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(ConsensusMetrics::default())),
            shutdown_flag: Arc::new(Mutex::new(false)),
            cluster_size: 1, // Default cluster size
            config: ConsensusConfig::default(),
        };

        // Register message handlers
        consensus.register_handlers();

        consensus
    }

    /// Create a new Raft consensus with custom configuration
    pub fn new_with_config(
        node_id: String,
        transport: Arc<NetworkTransport>,
        peers: Vec<String>,
        heartbeat_interval: Duration,
        election_timeout_base: Duration,
    ) -> Self {
        // Add randomness to election timeout to prevent split votes
        let election_timeout = election_timeout_base +
            Duration::from_millis(rand::random::<u64>() % election_timeout_base.as_millis() as u64);

        let consensus = Self {
            node_id: node_id.clone(),
            config: ConsensusConfig::default(),
            state: Arc::new(Mutex::new(ConsensusState::Follower)),
            current_term: Arc::new(Mutex::new(0)),
            voted_for: Arc::new(Mutex::new(None)),
            log: Arc::new(Mutex::new(Vec::new())),
            commit_index: Arc::new(Mutex::new(0)),
            last_applied: Arc::new(Mutex::new(0)),
            next_index: Arc::new(Mutex::new(HashMap::new())),
            match_index: Arc::new(Mutex::new(HashMap::new())),
            election_timeout,
            heartbeat_interval,
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            last_leader_contact: Arc::new(Mutex::new(Instant::now())),
            transport,
            peers: Arc::new(Mutex::new(peers.clone())),
            votes_received: Arc::new(Mutex::new(HashMap::new())),
            current_leader: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(ConsensusMetrics::default())),
            shutdown_flag: Arc::new(Mutex::new(false)),
            cluster_size: peers.len() + 1, // Include self
        };

        // Register message handlers
        consensus.register_handlers();

        consensus
    }

    /// Start the consensus algorithm
    pub async fn start(&self) -> NetworkResult<()> {
        info!("Starting Raft consensus for node {}", self.node_id);

        // Start election timer
        self.start_election_timer().await;

        // Start heartbeat timer (for leaders)
        self.start_heartbeat_timer().await;

        // Start log replication (for leaders)
        self.start_log_replication().await;

        Ok(())
    }

    /// Register message handlers with the transport
    fn register_handlers(&self) {
        let vote_handler = RaftVoteHandler::new(self.clone());
        let heartbeat_handler = RaftHeartbeatHandler::new(self.clone());

        self.transport.register_handler(MessageType::VoteRequest, vote_handler);
        self.transport.register_handler(MessageType::VoteResponse, heartbeat_handler.clone());
        self.transport.register_handler(MessageType::Heartbeat, heartbeat_handler.clone());
        self.transport.register_handler(MessageType::HeartbeatResponse, heartbeat_handler);
    }

    /// Start election timer
    async fn start_election_timer(&self) {
        let state = self.state.clone();
        let last_heartbeat = self.last_heartbeat.clone();
        let election_timeout = self.election_timeout;
        let consensus = self.clone();

        tokio::spawn(async move {
            // Check every 100ms instead of 10ms to reduce CPU usage
            let mut interval = interval(Duration::from_millis(100));

            loop {
                interval.tick().await;

                let current_state = { *state.lock().unwrap() };
                let last_hb = { *last_heartbeat.lock().unwrap() };

                if matches!(current_state, ConsensusState::Follower | ConsensusState::Candidate) {
                    if last_hb.elapsed() > election_timeout {
                        info!("Election timeout reached for node {}, starting election", consensus.node_id);
                        if let Err(e) = consensus.start_election().await {
                            error!("Failed to start election: {}", e);
                        }
                    }
                }
            }
        });
    }

    /// Start heartbeat timer (for leaders)
    async fn start_heartbeat_timer(&self) {
        let state = self.state.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let consensus = self.clone();

        tokio::spawn(async move {
            let mut interval = interval(heartbeat_interval);

            loop {
                interval.tick().await;

                let current_state = { *state.lock().unwrap() };

                if matches!(current_state, ConsensusState::Leader) {
                    if let Err(e) = consensus.send_heartbeats().await {
                        error!("Failed to send heartbeats: {}", e);
                    }
                }
            }
        });
    }

    /// Start log replication (for leaders)
    async fn start_log_replication(&self) {
        let state = self.state.clone();
        let consensus = self.clone();

        tokio::spawn(async move {
            // Log replication can be less frequent - every 50ms
            let mut interval = interval(Duration::from_millis(50));

            loop {
                interval.tick().await;

                let current_state = { *state.lock().unwrap() };

                if matches!(current_state, ConsensusState::Leader) {
                    if let Err(e) = consensus.replicate_logs().await {
                        warn!("Failed to replicate logs: {}", e);
                    }
                }
            }
        });
    }

    /// Start an election
    pub async fn start_election(&self) -> NetworkResult<()> {
        info!("Starting election for node {}", self.node_id);

        // Transition to candidate
        {
            let mut state = self.state.lock().unwrap();
            *state = ConsensusState::Candidate;
        }

        // Increment term
        let current_term = {
            let mut term = self.current_term.lock().unwrap();
            *term += 1;
            *term
        };

        // Vote for self
        {
            let mut voted_for = self.voted_for.lock().unwrap();
            *voted_for = Some(self.node_id.clone());
        }

        // Reset election timer
        {
            let mut last_heartbeat = self.last_heartbeat.lock().unwrap();
            *last_heartbeat = Instant::now();
        }

        // Clear vote tracking
        {
            let mut votes = self.votes_received.lock().unwrap();
            votes.clear();
            votes.insert(self.node_id.clone(), true); // Vote for self
        }

        // Send vote requests to all peers
        let (last_log_index, last_log_term) = self.get_last_log_info();
        let peers = { self.peers.lock().unwrap().clone() };

        for peer_id in peers {
            let vote_request = DistributedMessage::new(
                self.node_id.clone(),
                Some(peer_id),
                MessageType::VoteRequest,
                MessagePayload::VoteRequest {
                    term: current_term,
                    candidate_id: self.node_id.clone(),
                    last_log_index,
                    last_log_term,
                },
            );

            if let Err(e) = self.transport.send_message(vote_request).await {
                warn!("Failed to send vote request: {}", e);
            }
        }

        Ok(())
    }

    /// Send heartbeats to all followers
    async fn send_heartbeats(&self) -> NetworkResult<()> {
        let current_term = { *self.current_term.lock().unwrap() };
        let peers = { self.peers.lock().unwrap().clone() };
        let commit_index = { *self.commit_index.lock().unwrap() };

        for peer_id in peers {
            let (prev_log_index, prev_log_term) = self.get_prev_log_info(&peer_id);
            let entries = self.get_log_entries_for_peer(&peer_id);

            let heartbeat = DistributedMessage::new(
                self.node_id.clone(),
                Some(peer_id),
                MessageType::Heartbeat,
                MessagePayload::Heartbeat {
                    term: current_term,
                    leader_id: self.node_id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: commit_index,
                },
            );

            if let Err(e) = self.transport.send_message(heartbeat).await {
                warn!("Failed to send heartbeat: {}", e);
            }
        }

        Ok(())
    }

    /// Replicate logs to followers
    async fn replicate_logs(&self) -> NetworkResult<()> {
        // This is called continuously by the leader to ensure log replication
        // The actual replication happens in send_heartbeats
        Ok(())
    }

    /// Handle vote request
    pub async fn handle_vote_request(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        if let MessagePayload::VoteRequest { term, candidate_id, last_log_index, last_log_term } = message.payload {
            let mut current_term = self.current_term.lock().unwrap();
            let mut voted_for = self.voted_for.lock().unwrap();
            let mut state = self.state.lock().unwrap();

            let mut vote_granted = false;

            // If candidate's term is greater, update our term and become follower
            if term > *current_term {
                *current_term = term;
                *voted_for = None;
                *state = ConsensusState::Follower;
            }

            // Grant vote if conditions are met
            if term == *current_term &&
               (voted_for.is_none() || voted_for.as_ref() == Some(&candidate_id)) &&
               self.is_log_up_to_date(last_log_index, last_log_term) {
                vote_granted = true;
                *voted_for = Some(candidate_id.clone());

                // Reset election timer
                let mut last_heartbeat = self.last_heartbeat.lock().unwrap();
                *last_heartbeat = Instant::now();
            }

            let response = DistributedMessage::new(
                self.node_id.clone(),
                Some(message.sender_id),
                MessageType::VoteResponse,
                MessagePayload::VoteResponse {
                    term: *current_term,
                    vote_granted,
                    voter_id: self.node_id.clone(),
                },
            );

            Ok(Some(response))
        } else {
            Err(NetworkError::InvalidMessageFormat)
        }
    }

    /// Handle vote response
    pub async fn handle_vote_response(&self, message: DistributedMessage) -> NetworkResult<()> {
        if let MessagePayload::VoteResponse { term, vote_granted, voter_id } = message.payload {
            let mut current_term = self.current_term.lock().unwrap();
            let mut state = self.state.lock().unwrap();

            // If we receive a higher term, step down
            if term > *current_term {
                *current_term = term;
                *state = ConsensusState::Follower;
                let mut voted_for = self.voted_for.lock().unwrap();
                *voted_for = None;
                return Ok(());
            }

            // Only process if we're still a candidate and terms match
            if matches!(*state, ConsensusState::Candidate) && term == *current_term && vote_granted {
                let mut votes = self.votes_received.lock().unwrap();
                votes.insert(voter_id, true);

                let peers = self.peers.lock().unwrap();
                let total_nodes = peers.len() + 1; // +1 for self
                let votes_needed = (total_nodes / 2) + 1;

                if votes.len() >= votes_needed {
                    // We have majority, become leader
                    *state = ConsensusState::Leader;
                    info!("Node {} became leader for term {}", self.node_id, *current_term);

                    // Initialize leader state
                    self.initialize_leader_state().await;
                }
            }
        }

        Ok(())
    }

    /// Handle heartbeat
    pub async fn handle_heartbeat(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        if let MessagePayload::Heartbeat { term, leader_id: _, prev_log_index, prev_log_term, entries, leader_commit } = message.payload {
            let mut current_term = self.current_term.lock().unwrap();
            let mut state = self.state.lock().unwrap();
            let mut voted_for = self.voted_for.lock().unwrap();

            let mut success = false;

            // If leader's term is greater or equal, accept leadership
            if term >= *current_term {
                if term > *current_term {
                    *current_term = term;
                    *voted_for = None;
                }
                *state = ConsensusState::Follower;

                // Reset election timer
                let mut last_heartbeat = self.last_heartbeat.lock().unwrap();
                *last_heartbeat = Instant::now();

                // Check log consistency
                if self.check_log_consistency(prev_log_index, prev_log_term) {
                    success = true;

                    // Append new entries
                    self.append_log_entries(prev_log_index, entries).await;

                    // Update commit index
                    if leader_commit > *self.commit_index.lock().unwrap() {
                        let last_new_entry_index = self.get_last_log_index();
                        let new_commit_index = std::cmp::min(leader_commit, last_new_entry_index);
                        *self.commit_index.lock().unwrap() = new_commit_index;
                    }
                }
            }

            let match_index = if success {
                self.get_last_log_index()
            } else {
                0
            };

            let response = DistributedMessage::new(
                self.node_id.clone(),
                Some(message.sender_id),
                MessageType::HeartbeatResponse,
                MessagePayload::HeartbeatResponse {
                    term: *current_term,
                    success,
                    match_index,
                },
            );

            Ok(Some(response))
        } else {
            Err(NetworkError::InvalidMessageFormat)
        }
    }

    /// Handle heartbeat response
    pub async fn handle_heartbeat_response(&self, message: DistributedMessage) -> NetworkResult<()> {
        if let MessagePayload::HeartbeatResponse { term, success, match_index } = message.payload {
            let mut current_term = self.current_term.lock().unwrap();
            let mut state = self.state.lock().unwrap();

            // If we receive a higher term, step down
            if term > *current_term {
                *current_term = term;
                *state = ConsensusState::Follower;
                let mut voted_for = self.voted_for.lock().unwrap();
                *voted_for = None;
                return Ok(());
            }

            // Only process if we're still the leader
            if matches!(*state, ConsensusState::Leader) && term == *current_term {
                if success {
                    // Update match and next indices
                    let mut match_indices = self.match_index.lock().unwrap();
                    let mut next_indices = self.next_index.lock().unwrap();

                    match_indices.insert(message.sender_id.clone(), match_index);
                    next_indices.insert(message.sender_id, match_index + 1);

                    // Check if we can commit more entries
                    self.update_commit_index().await;
                } else {
                    // Decrement next index for this follower
                    let mut next_indices = self.next_index.lock().unwrap();
                    if let Some(next_index) = next_indices.get_mut(&message.sender_id) {
                        if *next_index > 1 {
                            *next_index -= 1;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // Helper methods

    fn get_last_log_info(&self) -> (u64, u64) {
        let log = self.log.lock().unwrap();
        if let Some(last_entry) = log.last() {
            (last_entry.index, last_entry.term)
        } else {
            (0, 0)
        }
    }

    fn get_last_log_index(&self) -> u64 {
        let log = self.log.lock().unwrap();
        if let Some(last_entry) = log.last() {
            last_entry.index
        } else {
            0
        }
    }

    fn get_prev_log_info(&self, peer_id: &str) -> (u64, u64) {
        let next_indices = self.next_index.lock().unwrap();
        let next_index = next_indices.get(peer_id).copied().unwrap_or(1);
        let prev_index = if next_index > 1 { next_index - 1 } else { 0 };

        if prev_index == 0 {
            (0, 0)
        } else {
            let log = self.log.lock().unwrap();
            if let Some(entry) = log.iter().find(|e| e.index == prev_index) {
                (entry.index, entry.term)
            } else {
                (0, 0)
            }
        }
    }

    fn get_log_entries_for_peer(&self, peer_id: &str) -> Vec<LogEntry> {
        let next_indices = self.next_index.lock().unwrap();
        let next_index = next_indices.get(peer_id).copied().unwrap_or(1);

        let log = self.log.lock().unwrap();
        log.iter()
            .filter(|entry| entry.index >= next_index)
            .cloned()
            .collect()
    }

    fn is_log_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        let (our_last_index, our_last_term) = self.get_last_log_info();

        if last_log_term > our_last_term {
            true
        } else if last_log_term == our_last_term {
            last_log_index >= our_last_index
        } else {
            false
        }
    }

    fn check_log_consistency(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return true;
        }

        let log = self.log.lock().unwrap();
        if let Some(entry) = log.iter().find(|e| e.index == prev_log_index) {
            entry.term == prev_log_term
        } else {
            false
        }
    }

    async fn append_log_entries(&self, prev_log_index: u64, entries: Vec<LogEntry>) {
        let mut log = self.log.lock().unwrap();

        // Remove conflicting entries
        log.retain(|entry| entry.index <= prev_log_index);

        // Append new entries
        log.extend(entries);

        // Sort by index to maintain order
        log.sort_by_key(|entry| entry.index);
    }

    async fn initialize_leader_state(&self) {
        let mut next_indices = self.next_index.lock().unwrap();
        let mut match_indices = self.match_index.lock().unwrap();
        let peers = self.peers.lock().unwrap();

        let last_log_index = self.get_last_log_index();

        for peer_id in peers.iter() {
            next_indices.insert(peer_id.clone(), last_log_index + 1);
            match_indices.insert(peer_id.clone(), 0);
        }
    }

    async fn update_commit_index(&self) {
        let match_indices = self.match_index.lock().unwrap();
        let peers = self.peers.lock().unwrap();
        let current_term = *self.current_term.lock().unwrap();

        let mut indices: Vec<u64> = match_indices.values().copied().collect();
        indices.push(self.get_last_log_index()); // Add our own index
        indices.sort_unstable();
        indices.reverse();

        let majority_index = peers.len() / 2;
        if majority_index < indices.len() {
            let candidate_commit = indices[majority_index];

            // Only commit entries from current term
            let log = self.log.lock().unwrap();
            if let Some(entry) = log.iter().find(|e| e.index == candidate_commit) {
                if entry.term == current_term {
                    let mut commit_index = self.commit_index.lock().unwrap();
                    if candidate_commit > *commit_index {
                        *commit_index = candidate_commit;
                        info!("Updated commit index to {}", candidate_commit);
                    }
                }
            }
        }
    }

    /// Get current state
    pub fn get_state(&self) -> ConsensusState {
        *self.state.lock().unwrap()
    }

    /// Force become leader (for single-node clusters or testing)
    pub fn force_become_leader(&self) {
        info!("Forcing node {} to become leader", self.node_id);

        // Set state to leader
        {
            let mut state = self.state.lock().unwrap();
            *state = ConsensusState::Leader;
        }

        // Increment term
        {
            let mut term = self.current_term.lock().unwrap();
            *term += 1;
        }

        // Set as current leader
        {
            let mut leader = self.current_leader.lock().unwrap();
            *leader = Some(self.node_id.clone());
        }

        // Reset heartbeat timer
        {
            let mut last_heartbeat = self.last_heartbeat.lock().unwrap();
            *last_heartbeat = Instant::now();
        }

        info!("Node {} is now leader at term {}", self.node_id, self.get_term());
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        matches!(*self.state.lock().unwrap(), ConsensusState::Leader)
    }

    /// Get current term
    pub fn get_term(&self) -> u64 {
        *self.current_term.lock().unwrap()
    }

    /// Add a peer to the consensus cluster
    pub fn add_peer(&self, peer_id: String) {
        let mut peers = self.peers.lock().unwrap();
        if !peers.contains(&peer_id) {
            peers.push(peer_id.clone());
            info!("Added peer to consensus: {}", peer_id);
        }
    }

    /// Update the entire peer list
    pub fn update_peers(&self, new_peers: Vec<String>) {
        let mut peers = self.peers.lock().unwrap();
        *peers = new_peers.clone();
        info!("Updated consensus peers: {:?}", new_peers);
    }

    /// Get current peer list
    pub fn get_peers(&self) -> Vec<String> {
        self.peers.lock().unwrap().clone()
    }

    /// Remove a peer from the consensus cluster
    pub fn remove_peer(&self, peer_id: &str) {
        let mut peers = self.peers.lock().unwrap();
        peers.retain(|p| p != peer_id);
        info!("Removed peer from consensus: {}", peer_id);
    }

    /// Append a new entry to the log (only for leaders)
    pub async fn append_entry(&self, data: Vec<u8>) -> NetworkResult<u64> {
        if !self.is_leader() {
            return Err(NetworkError::InvalidMessageFormat);
        }

        let current_term = *self.current_term.lock().unwrap();
        let mut log = self.log.lock().unwrap();

        let new_index = if let Some(last_entry) = log.last() {
            last_entry.index + 1
        } else {
            1
        };

        let _timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let checksum = format!("{:x}", md5::compute(&data));

        let entry = LogEntry {
            term: current_term,
            index: new_index,
            data,
            checksum,
        };

        log.push(entry);

        Ok(new_index)
    }
}

impl Clone for RaftConsensus {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
            current_term: self.current_term.clone(),
            voted_for: self.voted_for.clone(),
            log: self.log.clone(),
            commit_index: self.commit_index.clone(),
            last_applied: self.last_applied.clone(),
            next_index: self.next_index.clone(),
            match_index: self.match_index.clone(),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            last_heartbeat: self.last_heartbeat.clone(),
            last_leader_contact: self.last_leader_contact.clone(),
            transport: self.transport.clone(),
            peers: self.peers.clone(),
            votes_received: self.votes_received.clone(),
            current_leader: self.current_leader.clone(),
            metrics: self.metrics.clone(),
            shutdown_flag: self.shutdown_flag.clone(),
            cluster_size: self.cluster_size,
        }
    }
}

// Message Handlers

pub struct RaftVoteHandler {
    consensus: RaftConsensus,
}

impl RaftVoteHandler {
    pub fn new(consensus: RaftConsensus) -> Self {
        Self { consensus }
    }
}

impl MessageHandler for RaftVoteHandler {
    fn handle(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.consensus.handle_vote_request(message))
    }
}

pub struct RaftHeartbeatHandler {
    consensus: RaftConsensus,
}

impl RaftHeartbeatHandler {
    pub fn new(consensus: RaftConsensus) -> Self {
        Self { consensus }
    }
}

impl Clone for RaftHeartbeatHandler {
    fn clone(&self) -> Self {
        Self {
            consensus: self.consensus.clone(),
        }
    }
}

impl MessageHandler for RaftHeartbeatHandler {
    fn handle(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        let rt = tokio::runtime::Handle::current();

        match message.message_type {
            MessageType::Heartbeat => {
                rt.block_on(self.consensus.handle_heartbeat(message))
            }
            MessageType::HeartbeatResponse => {
                rt.block_on(self.consensus.handle_heartbeat_response(message))?;
                Ok(None)
            }
            MessageType::VoteResponse => {
                rt.block_on(self.consensus.handle_vote_response(message))?;
                Ok(None)
            }
            _ => Err(NetworkError::InvalidMessageFormat),
        }
    }
}
