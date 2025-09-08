//! Enhanced observability and metrics for network operations
//!
//! This module provides comprehensive monitoring and metrics collection
//! for production network operations.

use prometheus::{
    Gauge, Histogram, HistogramOpts, IntCounter, IntGauge, register_gauge, register_histogram,
    register_int_counter, register_int_gauge,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::network::error::NetworkError;

/// Network metrics collector
#[derive(Debug, Clone)]
pub struct NetworkMetrics {
    // Connection metrics
    pub connections_total: IntCounter,
    pub connections_active: IntGauge,
    pub connections_failed: IntCounter,
    pub connection_duration: Histogram,

    // Message metrics
    pub messages_sent_total: IntCounter,
    pub messages_received_total: IntCounter,
    pub messages_failed_total: IntCounter,
    pub message_size_bytes: Histogram,
    pub message_latency: Histogram,

    // Flow control metrics
    pub flow_control_backpressure: IntGauge,
    pub flow_control_window_utilization: Gauge,
    pub rate_limit_throttled: IntCounter,

    // Error metrics
    pub network_errors_total: IntCounter,
    pub timeout_errors_total: IntCounter,
    pub protocol_errors_total: IntCounter,

    // Resource metrics
    pub memory_usage_bytes: IntGauge,
    pub cpu_usage_percent: Gauge,
    pub goroutines_count: IntGauge,

    // TLS metrics
    pub tls_handshakes_total: IntCounter,
    pub tls_handshake_duration: Histogram,
    pub tls_errors_total: IntCounter,

    // Cluster metrics
    pub cluster_nodes_total: IntGauge,
    pub cluster_nodes_healthy: IntGauge,
    pub cluster_partitions: IntGauge,
}

impl NetworkMetrics {
    /// Create new network metrics
    pub fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            // Connection metrics
            connections_total: register_int_counter!(
                "pilgrimage_network_connections_total",
                "Total number of network connections established"
            )?,
            connections_active: register_int_gauge!(
                "pilgrimage_network_connections_active",
                "Number of currently active network connections"
            )?,
            connections_failed: register_int_counter!(
                "pilgrimage_network_connections_failed_total",
                "Total number of failed connection attempts"
            )?,
            connection_duration: register_histogram!(
                HistogramOpts::new(
                    "pilgrimage_network_connection_duration_seconds",
                    "Duration of network connections"
                )
                .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0])
            )?,

            // Message metrics
            messages_sent_total: register_int_counter!(
                "pilgrimage_network_messages_sent_total",
                "Total number of messages sent"
            )?,
            messages_received_total: register_int_counter!(
                "pilgrimage_network_messages_received_total",
                "Total number of messages received"
            )?,
            messages_failed_total: register_int_counter!(
                "pilgrimage_network_messages_failed_total",
                "Total number of messages that failed to send"
            )?,
            message_size_bytes: register_histogram!(
                HistogramOpts::new(
                    "pilgrimage_network_message_size_bytes",
                    "Size of network messages in bytes"
                )
                .buckets(vec![
                    64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0
                ])
            )?,
            message_latency: register_histogram!(
                HistogramOpts::new(
                    "pilgrimage_network_message_latency_seconds",
                    "Latency of message round trips"
                )
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0])
            )?,

            // Flow control metrics
            flow_control_backpressure: register_int_gauge!(
                "pilgrimage_network_flow_control_backpressure",
                "Number of connections under backpressure"
            )?,
            flow_control_window_utilization: register_gauge!(
                "pilgrimage_network_flow_control_window_utilization",
                "Flow control window utilization ratio"
            )?,
            rate_limit_throttled: register_int_counter!(
                "pilgrimage_network_rate_limit_throttled_total",
                "Total number of rate-limited operations"
            )?,

            // Error metrics
            network_errors_total: register_int_counter!(
                "pilgrimage_network_errors_total",
                "Total number of network errors"
            )?,
            timeout_errors_total: register_int_counter!(
                "pilgrimage_network_timeout_errors_total",
                "Total number of timeout errors"
            )?,
            protocol_errors_total: register_int_counter!(
                "pilgrimage_network_protocol_errors_total",
                "Total number of protocol errors"
            )?,

            // Resource metrics
            memory_usage_bytes: register_int_gauge!(
                "pilgrimage_network_memory_usage_bytes",
                "Memory usage of network components"
            )?,
            cpu_usage_percent: register_gauge!(
                "pilgrimage_network_cpu_usage_percent",
                "CPU usage percentage of network components"
            )?,
            goroutines_count: register_int_gauge!(
                "pilgrimage_network_goroutines_count",
                "Number of active goroutines/tasks in network components"
            )?,

            // TLS metrics
            tls_handshakes_total: register_int_counter!(
                "pilgrimage_network_tls_handshakes_total",
                "Total number of TLS handshakes"
            )?,
            tls_handshake_duration: register_histogram!(
                HistogramOpts::new(
                    "pilgrimage_network_tls_handshake_duration_seconds",
                    "Duration of TLS handshakes"
                )
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.0])
            )?,
            tls_errors_total: register_int_counter!(
                "pilgrimage_network_tls_errors_total",
                "Total number of TLS errors"
            )?,

            // Cluster metrics
            cluster_nodes_total: register_int_gauge!(
                "pilgrimage_cluster_nodes_total",
                "Total number of nodes in the cluster"
            )?,
            cluster_nodes_healthy: register_int_gauge!(
                "pilgrimage_cluster_nodes_healthy",
                "Number of healthy nodes in the cluster"
            )?,
            cluster_partitions: register_int_gauge!(
                "pilgrimage_cluster_partitions",
                "Number of network partitions detected"
            )?,
        })
    }

    /// Record a connection establishment
    pub fn record_connection(&self, duration: Duration) {
        self.connections_total.inc();
        self.connections_active.inc();
        self.connection_duration.observe(duration.as_secs_f64());
    }

    /// Record a connection closure
    pub fn record_connection_closed(&self) {
        self.connections_active.dec();
    }

    /// Record a failed connection
    pub fn record_connection_failed(&self) {
        self.connections_failed.inc();
    }

    /// Record a sent message
    pub fn record_message_sent(&self, size: usize) {
        self.messages_sent_total.inc();
        self.message_size_bytes.observe(size as f64);
    }

    /// Record a received message
    pub fn record_message_received(&self, size: usize) {
        self.messages_received_total.inc();
        self.message_size_bytes.observe(size as f64);
    }

    /// Record a failed message
    pub fn record_message_failed(&self) {
        self.messages_failed_total.inc();
    }

    /// Record message latency
    pub fn record_message_latency(&self, latency: Duration) {
        self.message_latency.observe(latency.as_secs_f64());
    }

    /// Record flow control state
    pub fn record_flow_control(&self, backpressure_count: i64, window_utilization: f64) {
        self.flow_control_backpressure.set(backpressure_count);
        self.flow_control_window_utilization.set(window_utilization);
    }

    /// Record rate limiting
    pub fn record_rate_limited(&self) {
        self.rate_limit_throttled.inc();
    }

    /// Record network error
    pub fn record_error(&self, error: &NetworkError) {
        self.network_errors_total.inc();

        match error {
            NetworkError::Timeout | NetworkError::TimeoutError(_) => {
                self.timeout_errors_total.inc();
            }
            NetworkError::ProtocolError(_) | NetworkError::InvalidMessageFormat => {
                self.protocol_errors_total.inc();
            }
            NetworkError::TlsError(_) => {
                self.tls_errors_total.inc();
            }
            _ => {}
        }
    }

    /// Record TLS handshake
    pub fn record_tls_handshake(&self, duration: Duration) {
        self.tls_handshakes_total.inc();
        self.tls_handshake_duration.observe(duration.as_secs_f64());
    }

    /// Update cluster state
    pub fn update_cluster_state(&self, total_nodes: i64, healthy_nodes: i64, partitions: i64) {
        self.cluster_nodes_total.set(total_nodes);
        self.cluster_nodes_healthy.set(healthy_nodes);
        self.cluster_partitions.set(partitions);
    }

    /// Update resource usage
    pub fn update_resource_usage(&self, memory_bytes: i64, cpu_percent: f64, task_count: i64) {
        self.memory_usage_bytes.set(memory_bytes);
        self.cpu_usage_percent.set(cpu_percent);
        self.goroutines_count.set(task_count);
    }

    /// Create metrics for testing without registry registration
    pub fn new_for_testing() -> Self {
        use prometheus::{Gauge, Histogram, HistogramOpts, IntCounter, IntGauge};

        Self {
            connections_total: IntCounter::new("test_connections_total", "test").unwrap(),
            connections_active: IntGauge::new("test_connections_active", "test").unwrap(),
            connections_failed: IntCounter::new("test_connections_failed", "test").unwrap(),
            connection_duration: Histogram::with_opts(HistogramOpts::new(
                "test_connection_duration",
                "test",
            ))
            .unwrap(),
            messages_sent_total: IntCounter::new("test_messages_sent_total", "test").unwrap(),
            messages_received_total: IntCounter::new("test_messages_received_total", "test")
                .unwrap(),
            messages_failed_total: IntCounter::new("test_messages_failed_total", "test").unwrap(),
            message_size_bytes: Histogram::with_opts(HistogramOpts::new(
                "test_message_size_bytes",
                "test",
            ))
            .unwrap(),
            message_latency: Histogram::with_opts(HistogramOpts::new(
                "test_message_latency",
                "test",
            ))
            .unwrap(),
            flow_control_backpressure: IntGauge::new("test_flow_control_backpressure", "test")
                .unwrap(),
            flow_control_window_utilization: Gauge::new(
                "test_flow_control_window_utilization",
                "test",
            )
            .unwrap(),
            rate_limit_throttled: IntCounter::new("test_rate_limit_throttled", "test").unwrap(),
            network_errors_total: IntCounter::new("test_network_errors_total", "test").unwrap(),
            timeout_errors_total: IntCounter::new("test_timeout_errors_total", "test").unwrap(),
            protocol_errors_total: IntCounter::new("test_protocol_errors_total", "test").unwrap(),
            memory_usage_bytes: IntGauge::new("test_memory_usage_bytes", "test").unwrap(),
            cpu_usage_percent: Gauge::new("test_cpu_usage_percent", "test").unwrap(),
            goroutines_count: IntGauge::new("test_goroutines_count", "test").unwrap(),
            tls_handshakes_total: IntCounter::new("test_tls_handshakes_total", "test").unwrap(),
            tls_handshake_duration: Histogram::with_opts(HistogramOpts::new(
                "test_tls_handshake_duration",
                "test",
            ))
            .unwrap(),
            tls_errors_total: IntCounter::new("test_tls_errors_total", "test").unwrap(),
            cluster_nodes_total: IntGauge::new("test_cluster_nodes_total", "test").unwrap(),
            cluster_nodes_healthy: IntGauge::new("test_cluster_nodes_healthy", "test").unwrap(),
            cluster_partitions: IntGauge::new("test_cluster_partitions", "test").unwrap(),
        }
    }
}

impl Default for NetworkMetrics {
    fn default() -> Self {
        // Try to create metrics, but if registry already exists (common in tests),
        // create a new registry-less instance for testing
        Self::new().unwrap_or_else(|_| Self::new_for_testing())
    }
}

/// Connection health checker
#[derive(Debug)]
pub struct HealthChecker {
    /// Health check configuration
    config: HealthCheckConfig,
    /// Connection health states
    health_states: Arc<RwLock<HashMap<String, ConnectionHealth>>>,
    /// Metrics collector
    metrics: Arc<NetworkMetrics>,
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Health check interval
    pub check_interval: Duration,
    /// Health check timeout
    pub check_timeout: Duration,
    /// Failure threshold before marking unhealthy
    pub failure_threshold: u32,
    /// Success threshold before marking healthy
    pub success_threshold: u32,
    /// Maximum consecutive failures before giving up
    pub max_failures: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            check_timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 2,
            max_failures: 10,
        }
    }
}

/// Connection health state
#[derive(Debug, Clone)]
pub struct ConnectionHealth {
    /// Whether the connection is healthy
    pub is_healthy: bool,
    /// Last health check time
    pub last_check: Instant,
    /// Consecutive failures
    pub consecutive_failures: u32,
    /// Consecutive successes
    pub consecutive_successes: u32,
    /// Total health checks performed
    pub total_checks: u64,
    /// Average response time
    pub avg_response_time: Duration,
    /// Last error message
    pub last_error: Option<String>,
}

impl Default for ConnectionHealth {
    fn default() -> Self {
        Self {
            is_healthy: true,
            last_check: Instant::now(),
            consecutive_failures: 0,
            consecutive_successes: 0,
            total_checks: 0,
            avg_response_time: Duration::from_millis(0),
            last_error: None,
        }
    }
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(config: HealthCheckConfig, metrics: Arc<NetworkMetrics>) -> Self {
        Self {
            config,
            health_states: Arc::new(RwLock::new(HashMap::new())),
            metrics,
        }
    }

    /// Start health checking for a connection
    pub async fn start_health_check(&self, connection_id: String) {
        let mut health_states = self.health_states.write().await;
        health_states.insert(connection_id, ConnectionHealth::default());
    }

    /// Stop health checking for a connection
    pub async fn stop_health_check(&self, connection_id: &str) {
        let mut health_states = self.health_states.write().await;
        health_states.remove(connection_id);
    }

    /// Get health state for a connection
    pub async fn get_health(&self, connection_id: &str) -> Option<ConnectionHealth> {
        let health_states = self.health_states.read().await;
        health_states.get(connection_id).cloned()
    }

    /// Record health check result
    pub async fn record_health_check(
        &self,
        connection_id: &str,
        success: bool,
        response_time: Duration,
        error: Option<String>,
    ) {
        let mut health_states = self.health_states.write().await;

        if let Some(health) = health_states.get_mut(connection_id) {
            health.last_check = Instant::now();
            health.total_checks += 1;
            health.last_error = error;

            // Update average response time
            let total_time = health.avg_response_time.as_nanos() as u64 * (health.total_checks - 1)
                + response_time.as_nanos() as u64;
            health.avg_response_time = Duration::from_nanos(total_time / health.total_checks);

            if success {
                health.consecutive_successes += 1;
                health.consecutive_failures = 0;

                // Mark healthy if we reach success threshold
                if health.consecutive_successes >= self.config.success_threshold {
                    health.is_healthy = true;
                }
            } else {
                health.consecutive_failures += 1;
                health.consecutive_successes = 0;

                // Mark unhealthy if we reach failure threshold
                if health.consecutive_failures >= self.config.failure_threshold {
                    health.is_healthy = false;
                }
            }
        }
    }

    /// Get all connection health states
    pub async fn get_all_health(&self) -> HashMap<String, ConnectionHealth> {
        self.health_states.read().await.clone()
    }

    /// Get healthy connection count
    pub async fn get_healthy_count(&self) -> usize {
        let health_states = self.health_states.read().await;
        health_states.values().filter(|h| h.is_healthy).count()
    }

    /// Start background health checking
    pub fn start_background_checks(&self) {
        let health_states = self.health_states.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);

            loop {
                interval.tick().await;

                let connection_ids: Vec<String> = {
                    let states = health_states.read().await;
                    states.keys().cloned().collect()
                };

                for _connection_id in connection_ids {
                    // Perform health check (simplified - in production this would
                    // actually ping the connection)
                    let start = Instant::now();
                    let success = true; // Placeholder
                    let response_time = start.elapsed();

                    // Record the result
                    // This would be done through the public API in real usage

                    // Update metrics
                    if success {
                        metrics.record_message_latency(response_time);
                    }
                }

                // Update cluster metrics
                let states = health_states.read().await;
                let total_nodes = states.len() as i64;
                let healthy_nodes = states.values().filter(|h| h.is_healthy).count() as i64;
                metrics.update_cluster_state(total_nodes, healthy_nodes, 0);
            }
        });
    }
}

/// Network diagnostics and troubleshooting
#[derive(Debug)]
pub struct NetworkDiagnostics {
    /// Connection latency measurements
    latencies: Arc<RwLock<HashMap<String, Vec<Duration>>>>,
    /// Packet loss tracking
    packet_loss: Arc<RwLock<HashMap<String, f64>>>,
    /// Bandwidth measurements
    bandwidth: Arc<RwLock<HashMap<String, f64>>>,
}

impl NetworkDiagnostics {
    /// Create new network diagnostics
    pub fn new() -> Self {
        Self {
            latencies: Arc::new(RwLock::new(HashMap::new())),
            packet_loss: Arc::new(RwLock::new(HashMap::new())),
            bandwidth: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record latency measurement
    pub async fn record_latency(&self, connection_id: &str, latency: Duration) {
        let mut latencies = self.latencies.write().await;
        let entry = latencies
            .entry(connection_id.to_string())
            .or_insert_with(Vec::new);

        entry.push(latency);

        // Keep only last 100 measurements
        if entry.len() > 100 {
            entry.remove(0);
        }
    }

    /// Get average latency for a connection
    pub async fn get_average_latency(&self, connection_id: &str) -> Option<Duration> {
        let latencies = self.latencies.read().await;

        if let Some(measurements) = latencies.get(connection_id) {
            if !measurements.is_empty() {
                let total: Duration = measurements.iter().sum();
                Some(total / measurements.len() as u32)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Record packet loss
    pub async fn record_packet_loss(&self, connection_id: &str, loss_rate: f64) {
        let mut packet_loss = self.packet_loss.write().await;
        packet_loss.insert(connection_id.to_string(), loss_rate);
    }

    /// Record bandwidth measurement
    pub async fn record_bandwidth(&self, connection_id: &str, bytes_per_second: f64) {
        let mut bandwidth = self.bandwidth.write().await;
        bandwidth.insert(connection_id.to_string(), bytes_per_second);
    }

    /// Generate diagnostics report
    pub async fn generate_report(&self) -> DiagnosticsReport {
        let latencies = self.latencies.read().await;
        let packet_loss = self.packet_loss.read().await;
        let bandwidth = self.bandwidth.read().await;

        DiagnosticsReport {
            connection_count: latencies.len(),
            average_latencies: latencies
                .iter()
                .filter_map(|(id, measurements)| {
                    if !measurements.is_empty() {
                        let total: Duration = measurements.iter().sum();
                        let avg = total / measurements.len() as u32;
                        Some((id.clone(), avg))
                    } else {
                        None
                    }
                })
                .collect(),
            packet_loss_rates: packet_loss.clone(),
            bandwidth_measurements: bandwidth.clone(),
            timestamp: Instant::now(),
        }
    }
}

impl Default for NetworkDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}

/// Diagnostics report
#[derive(Debug, Clone)]
pub struct DiagnosticsReport {
    pub connection_count: usize,
    pub average_latencies: HashMap<String, Duration>,
    pub packet_loss_rates: HashMap<String, f64>,
    pub bandwidth_measurements: HashMap<String, f64>,
    pub timestamp: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_checker() {
        let config = HealthCheckConfig::default();
        let metrics = Arc::new(NetworkMetrics::default());
        let checker = HealthChecker::new(config, metrics);

        // Start health check for a connection
        checker.start_health_check("test_conn".to_string()).await;

        // Get initial health
        let health = checker.get_health("test_conn").await.unwrap();
        assert!(health.is_healthy);
        assert_eq!(health.consecutive_failures, 0);

        // Record a failure
        checker
            .record_health_check(
                "test_conn",
                false,
                Duration::from_millis(100),
                Some("test error".to_string()),
            )
            .await;

        let health = checker.get_health("test_conn").await.unwrap();
        assert_eq!(health.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn test_network_diagnostics() {
        let diagnostics = NetworkDiagnostics::new();

        // Record some measurements
        diagnostics
            .record_latency("conn1", Duration::from_millis(50))
            .await;
        diagnostics
            .record_latency("conn1", Duration::from_millis(60))
            .await;
        diagnostics.record_packet_loss("conn1", 0.05).await;
        diagnostics.record_bandwidth("conn1", 1000000.0).await;

        // Check average latency
        let avg_latency = diagnostics.get_average_latency("conn1").await.unwrap();
        assert_eq!(avg_latency, Duration::from_millis(55));

        // Generate report
        let report = diagnostics.generate_report().await;
        assert_eq!(report.connection_count, 1);
        assert!(report.average_latencies.contains_key("conn1"));
    }
}
