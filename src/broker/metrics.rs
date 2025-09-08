use std::time::Instant;
use std::sync::Arc;
use prometheus::{Counter, Gauge, Histogram, Registry, HistogramOpts};
use log::{info, warn, error};

#[derive(Debug, Clone)]
pub struct SystemMetrics {
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub network_io: f32,
    pub timestamp: Instant,
}

impl SystemMetrics {
    pub fn new(cpu_usage: f32, memory_usage: f32, network_io: f32) -> Self {
        Self {
            cpu_usage,
            memory_usage,
            network_io,
            timestamp: Instant::now(),
        }
    }

    pub fn is_overloaded(&self, threshold: f32) -> bool {
        self.cpu_usage > threshold || self.memory_usage > threshold
    }

    pub fn is_underutilized(&self, threshold: f32) -> bool {
        self.cpu_usage < threshold && self.memory_usage < threshold
    }
}

/// Distributed system metrics collector
#[derive(Clone)]
pub struct DistributedMetrics {
    // Consensus metrics
    pub raft_term: Arc<Gauge>,
    pub raft_state: Arc<Gauge>, // 0=Follower, 1=Candidate, 2=Leader
    pub election_count: Arc<Counter>,
    pub heartbeat_sent: Arc<Counter>,
    pub heartbeat_received: Arc<Counter>,
    pub vote_requests_sent: Arc<Counter>,
    pub vote_requests_received: Arc<Counter>,

    // Replication metrics
    pub replication_requests: Arc<Counter>,
    pub replication_success: Arc<Counter>,
    pub replication_failures: Arc<Counter>,
    pub replication_latency: Arc<Histogram>,

    // Network metrics
    pub network_connections_active: Arc<Gauge>,
    pub network_connections_total: Arc<Counter>,
    pub network_bytes_sent: Arc<Counter>,
    pub network_bytes_received: Arc<Counter>,
    pub network_errors: Arc<Counter>,
    pub tls_connections: Arc<Counter>,

    // Cluster health metrics
    pub cluster_size: Arc<Gauge>,
    pub quorum_status: Arc<Gauge>, // 0=No quorum, 1=Has quorum
    pub partition_suspects: Arc<Gauge>,
    pub isolated_nodes: Arc<Gauge>,

    // Performance metrics
    pub message_processing_latency: Arc<Histogram>,
    pub log_entries: Arc<Gauge>,
    pub log_size_bytes: Arc<Gauge>,

    registry: Arc<Registry>,
}

impl DistributedMetrics {
    /// Create a new distributed metrics collector
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let registry = Registry::new();

        // Consensus metrics
        let raft_term = Gauge::new("raft_current_term", "Current Raft term")?;
        let raft_state = Gauge::new("raft_state", "Current Raft state (0=Follower, 1=Candidate, 2=Leader)")?;
        let election_count = Counter::new("raft_elections_total", "Total number of elections started")?;
        let heartbeat_sent = Counter::new("raft_heartbeats_sent_total", "Total heartbeats sent")?;
        let heartbeat_received = Counter::new("raft_heartbeats_received_total", "Total heartbeats received")?;
        let vote_requests_sent = Counter::new("raft_vote_requests_sent_total", "Total vote requests sent")?;
        let vote_requests_received = Counter::new("raft_vote_requests_received_total", "Total vote requests received")?;

        // Replication metrics
        let replication_requests = Counter::new("replication_requests_total", "Total replication requests")?;
        let replication_success = Counter::new("replication_success_total", "Successful replications")?;
        let replication_failures = Counter::new("replication_failures_total", "Failed replications")?;
        let replication_latency = Histogram::with_opts(
            HistogramOpts::new("replication_latency_seconds", "Replication latency in seconds")
        )?;

        // Network metrics
        let network_connections_active = Gauge::new("network_connections_active", "Active network connections")?;
        let network_connections_total = Counter::new("network_connections_total", "Total network connections")?;
        let network_bytes_sent = Counter::new("network_bytes_sent_total", "Total bytes sent")?;
        let network_bytes_received = Counter::new("network_bytes_received_total", "Total bytes received")?;
        let network_errors = Counter::new("network_errors_total", "Total network errors")?;
        let tls_connections = Counter::new("tls_connections_total", "Total TLS connections")?;

        // Cluster health metrics
        let cluster_size = Gauge::new("cluster_size", "Current cluster size")?;
        let quorum_status = Gauge::new("cluster_quorum_status", "Cluster quorum status (0=No, 1=Yes)")?;
        let partition_suspects = Gauge::new("cluster_partition_suspects", "Number of suspected partitioned nodes")?;
        let isolated_nodes = Gauge::new("cluster_isolated_nodes", "Number of isolated nodes")?;

        // Performance metrics
        let message_processing_latency = Histogram::with_opts(
            HistogramOpts::new("message_processing_latency_seconds", "Message processing latency")
        )?;
        let log_entries = Gauge::new("raft_log_entries", "Number of log entries")?;
        let log_size_bytes = Gauge::new("raft_log_size_bytes", "Log size in bytes")?;

        // Register all metrics
        registry.register(Box::new(raft_term.clone()))?;
        registry.register(Box::new(raft_state.clone()))?;
        registry.register(Box::new(election_count.clone()))?;
        registry.register(Box::new(heartbeat_sent.clone()))?;
        registry.register(Box::new(heartbeat_received.clone()))?;
        registry.register(Box::new(vote_requests_sent.clone()))?;
        registry.register(Box::new(vote_requests_received.clone()))?;
        registry.register(Box::new(replication_requests.clone()))?;
        registry.register(Box::new(replication_success.clone()))?;
        registry.register(Box::new(replication_failures.clone()))?;
        registry.register(Box::new(replication_latency.clone()))?;
        registry.register(Box::new(network_connections_active.clone()))?;
        registry.register(Box::new(network_connections_total.clone()))?;
        registry.register(Box::new(network_bytes_sent.clone()))?;
        registry.register(Box::new(network_bytes_received.clone()))?;
        registry.register(Box::new(network_errors.clone()))?;
        registry.register(Box::new(tls_connections.clone()))?;
        registry.register(Box::new(cluster_size.clone()))?;
        registry.register(Box::new(quorum_status.clone()))?;
        registry.register(Box::new(partition_suspects.clone()))?;
        registry.register(Box::new(isolated_nodes.clone()))?;
        registry.register(Box::new(message_processing_latency.clone()))?;
        registry.register(Box::new(log_entries.clone()))?;
        registry.register(Box::new(log_size_bytes.clone()))?;

        Ok(Self {
            raft_term: Arc::new(raft_term),
            raft_state: Arc::new(raft_state),
            election_count: Arc::new(election_count),
            heartbeat_sent: Arc::new(heartbeat_sent),
            heartbeat_received: Arc::new(heartbeat_received),
            vote_requests_sent: Arc::new(vote_requests_sent),
            vote_requests_received: Arc::new(vote_requests_received),
            replication_requests: Arc::new(replication_requests),
            replication_success: Arc::new(replication_success),
            replication_failures: Arc::new(replication_failures),
            replication_latency: Arc::new(replication_latency),
            network_connections_active: Arc::new(network_connections_active),
            network_connections_total: Arc::new(network_connections_total),
            network_bytes_sent: Arc::new(network_bytes_sent),
            network_bytes_received: Arc::new(network_bytes_received),
            network_errors: Arc::new(network_errors),
            tls_connections: Arc::new(tls_connections),
            cluster_size: Arc::new(cluster_size),
            quorum_status: Arc::new(quorum_status),
            partition_suspects: Arc::new(partition_suspects),
            isolated_nodes: Arc::new(isolated_nodes),
            message_processing_latency: Arc::new(message_processing_latency),
            log_entries: Arc::new(log_entries),
            log_size_bytes: Arc::new(log_size_bytes),
            registry: Arc::new(registry),
        })
    }

    /// Get the Prometheus registry for metrics export
    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    /// Record a Raft state change
    pub fn set_raft_state(&self, state: u8) {
        self.raft_state.set(state as f64);
        info!("Raft state changed to: {}", state);
    }

    /// Record a successful replication
    pub fn record_replication_success(&self, latency_seconds: f64) {
        self.replication_success.inc();
        self.replication_latency.observe(latency_seconds);
    }

    /// Record a failed replication
    pub fn record_replication_failure(&self) {
        self.replication_failures.inc();
        warn!("Replication failure recorded");
    }

    /// Update cluster health metrics
    pub fn update_cluster_health(&self, size: usize, has_quorum: bool, suspects: usize, isolated: usize) {
        self.cluster_size.set(size as f64);
        self.quorum_status.set(if has_quorum { 1.0 } else { 0.0 });
        self.partition_suspects.set(suspects as f64);
        self.isolated_nodes.set(isolated as f64);

        if !has_quorum {
            warn!("Cluster does not have quorum! Size: {}, Suspects: {}, Isolated: {}",
                  size, suspects, isolated);
        }
    }

    /// Record network activity
    pub fn record_network_activity(&self, bytes_sent: u64, bytes_received: u64, is_tls: bool) {
        self.network_bytes_sent.inc_by(bytes_sent as f64);
        self.network_bytes_received.inc_by(bytes_received as f64);

        if is_tls {
            self.tls_connections.inc();
        }
    }

    /// Record message processing latency
    pub fn record_message_processing(&self, latency_seconds: f64) {
        self.message_processing_latency.observe(latency_seconds);
    }

    /// Export metrics as text for Prometheus
    pub fn export_metrics(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();

        match encoder.encode_to_string(&metric_families) {
            Ok(output) => output,
            Err(e) => {
                error!("Failed to encode metrics: {}", e);
                String::new()
            }
        }
    }
}
