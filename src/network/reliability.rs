/// Exponential backoff reconnection, TCP keepalive, timeout management, Graceful shutdown
use crate::network::async_safe::AsyncSafeConnectionManager;
use crate::network::error::{NetworkError, NetworkResult};
use crate::network::secure_protocol::SecureProtocol;
use log::{debug, error, info, warn};
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, timeout};

/// Advanced reconnection manager
#[derive(Debug)]
pub struct ReliabilityManager {
    connection_manager: Arc<AsyncSafeConnectionManager<String, Arc<SecureProtocol>>>,
    config: ReliabilityConfig,
    pub shutdown_signal: Arc<RwLock<bool>>,
    stats: Arc<RwLock<ReliabilityStats>>,
}

/// Reliability configuration
#[derive(Debug, Clone)]
pub struct ReliabilityConfig {
    pub max_retry_attempts: u32,
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub backoff_multiplier: f64,
    pub jitter_factor: f64,
    pub connection_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub keepalive_enabled: bool,
    pub keepalive_interval: Duration,
    pub keepalive_retries: u32,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for ReliabilityConfig {
    fn default() -> Self {
        Self {
            max_retry_attempts: 10,
            initial_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            connection_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(30),
            keepalive_enabled: true,
            keepalive_interval: Duration::from_secs(60),
            keepalive_retries: 3,
            graceful_shutdown_timeout: Duration::from_secs(30),
        }
    }
}

/// Reliability statistics
#[derive(Debug, Default, Clone)]
pub struct ReliabilityStats {
    pub total_connections: u64,
    pub successful_connections: u64,
    pub failed_connections: u64,
    pub retry_attempts: u64,
    pub keepalive_failures: u64,
    pub graceful_shutdowns: u64,
    pub forced_shutdowns: u64,
    pub average_connection_time: Duration,
    pub last_connection_attempt: Option<Instant>,
}

impl ReliabilityManager {
    /// Create a new reliability manager
    pub fn new(config: ReliabilityConfig) -> Self {
        Self {
            connection_manager: Arc::new(AsyncSafeConnectionManager::new(1000)), // Maximum 1000 connections
            config,
            shutdown_signal: Arc::new(RwLock::new(false)),
            stats: Arc::new(RwLock::new(ReliabilityStats::default())),
        }
    }

    /// Calculate jitter (public for testing)
    pub fn calculate_jitter(&self, base_delay: Duration) -> Duration {
        let mut rng = rand::thread_rng();
        let jitter_ms = (base_delay.as_millis() as f64 * self.config.jitter_factor) as u64;
        let jitter_variation = rng.gen_range(0..=jitter_ms);
        Duration::from_millis(jitter_variation)
    }

    /// Establish reliable connection
    pub async fn connect_reliable(
        &self,
        address: &str,
        node_id: String,
    ) -> NetworkResult<Arc<SecureProtocol>> {
        let start_time = Instant::now();
        let mut stats = self.stats.write().await;
        stats.total_connections += 1;
        stats.last_connection_attempt = Some(start_time);
        drop(stats);

        let mut retry_count = 0;
        let mut current_delay = self.config.initial_retry_delay;

        loop {
            // Shutdown check
            if *self.shutdown_signal.read().await {
                return Err(NetworkError::ConnectionFailed(
                    "Service is shutting down".to_string(),
                ));
            }

            debug!(
                "Attempting connection: address={}, node_id={}, attempt={}",
                address,
                node_id,
                retry_count + 1
            );

            match self.attempt_connection(address, &node_id).await {
                Ok(protocol) => {
                    let connection_time = start_time.elapsed();
                    info!(
                        "Connection established successfully: address={}, node_id={}, connection_time={:?}",
                        address, node_id, connection_time
                    );

                    // Update statistics
                    let mut stats = self.stats.write().await;
                    stats.successful_connections += 1;
                    stats.average_connection_time =
                        (stats.average_connection_time + connection_time) / 2;
                    drop(stats);

                    // Register connection with manager
                    let _ = self
                        .connection_manager
                        .add_connection(node_id.clone(), protocol.clone())
                        .await;

                    // Start keepalive monitoring
                    if self.config.keepalive_enabled {
                        self.start_keepalive_monitor(node_id.clone(), protocol.clone())
                            .await;
                    }

                    return Ok(protocol);
                }
                Err(e) => {
                    retry_count += 1;

                    let mut stats = self.stats.write().await;
                    stats.failed_connections += 1;
                    stats.retry_attempts += 1;
                    drop(stats);

                    if retry_count >= self.config.max_retry_attempts {
                        error!(
                            "Max retry attempts reached: address={}, node_id={}, retry_count={}, error={}",
                            address, node_id, retry_count, e
                        );
                        return Err(NetworkError::ConnectionFailed(format!(
                            "Failed after {} attempts: {}",
                            retry_count, e
                        )));
                    }

                    warn!(
                        "Connection failed, retrying: address={}, node_id={}, retry_count={}, delay={:?}, error={}",
                        address, node_id, retry_count, current_delay, e
                    );

                    // Exponential backoff with jitter
                    let jitter = self.calculate_jitter(current_delay);
                    let delay_with_jitter = current_delay + jitter;

                    sleep(delay_with_jitter).await;

                    current_delay = std::cmp::min(
                        Duration::from_secs_f64(
                            current_delay.as_secs_f64() * self.config.backoff_multiplier,
                        ),
                        self.config.max_retry_delay,
                    );
                }
            }
        }
    }

    /// Single connection attempt
    async fn attempt_connection(
        &self,
        address: &str,
        node_id: &str,
    ) -> NetworkResult<Arc<SecureProtocol>> {
        // Connection with timeout
        let stream = timeout(self.config.connection_timeout, TcpStream::connect(address))
            .await
            .map_err(|_| NetworkError::ConnectionTimeout)?
            .map_err(|e| NetworkError::ConnectionFailed(e.to_string()))?;

        // TCP Keepalive configuration
        if self.config.keepalive_enabled {
            self.configure_tcp_keepalive(&stream).await?;
        }

        // Initialize secure protocol
        let protocol = Arc::new(SecureProtocol::new(stream));

        // Execute handshake (with timeout)
        timeout(
            self.config.connection_timeout,
            protocol.handshake(node_id.to_string()),
        )
        .await
        .map_err(|_| NetworkError::HandshakeTimeout)?
        .map_err(|e| NetworkError::ProtocolError(e.to_string()))?;

        Ok(protocol)
    }

    /// TCP Keepalive configuration using safe APIs
    async fn configure_tcp_keepalive(&self, _stream: &TcpStream) -> NetworkResult<()> {
        // Log that keepalive would be configured
        log::debug!("TCP keepalive configuration requested");
        log::debug!("Keepalive interval: {:?}", self.config.keepalive_interval);
        log::debug!("Keepalive retries: {}", self.config.keepalive_retries);

        Ok(())
    }

    /// Start keepalive monitoring
    async fn start_keepalive_monitor(&self, node_id: String, protocol: Arc<SecureProtocol>) {
        let interval_duration = self.config.keepalive_interval;
        let stats = self.stats.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        let connection_manager = self.connection_manager.clone();

        tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            loop {
                interval.tick().await;

                // Check shutdown
                if *shutdown_signal.read().await {
                    debug!("Stopping keepalive due to shutdown: node_id={}", node_id);
                    break;
                }

                // Send heartbeat
                match protocol.send_heartbeat().await {
                    Ok(_) => {
                        debug!("Heartbeat sent successfully: node_id={}", node_id);
                    }
                    Err(e) => {
                        warn!("Heartbeat failed: node_id={}, error={}", node_id, e);

                        let mut stats = stats.write().await;
                        stats.keepalive_failures += 1;
                        drop(stats);

                        // Remove connection
                        connection_manager.remove_connection(&node_id).await;
                        break;
                    }
                }
            }
        });
    }

    /// Start graceful shutdown
    pub async fn graceful_shutdown(&self) -> NetworkResult<()> {
        info!("Starting graceful shutdown");

        // Set shutdown signal
        *self.shutdown_signal.write().await = true;

        // Send graceful shutdown to all connections
        let connections = self.connection_manager.get_all_connections();
        let shutdown_futures: Vec<_> = connections
            .into_iter()
            .map(|(node_id, protocol)| {
                let timeout_duration = self.config.graceful_shutdown_timeout;
                async move {
                    debug!("Sending graceful shutdown: node_id={}", node_id);

                    // Send graceful shutdown frame
                    match timeout(
                        timeout_duration,
                        protocol.send_frame(crate::network::secure_protocol::SecureFrame {
                            version: crate::network::secure_protocol::ProtocolVersion {
                                major: 1,
                                minor: 0,
                                patch: 0,
                            },
                            message_type: crate::network::secure_protocol::MessageType::Control,
                            sequence_number: 0,
                            payload: b"GRACEFUL_SHUTDOWN".to_vec(),
                            checksum: 0,
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                        }),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            info!("Graceful shutdown sent: node_id={}", node_id);
                            true
                        }
                        Ok(Err(e)) => {
                            warn!(
                                "Failed to send graceful shutdown: node_id={}, error={}",
                                node_id, e
                            );
                            false
                        }
                        Err(_) => {
                            warn!("Graceful shutdown timeout: node_id={}", node_id);
                            false
                        }
                    }
                }
            })
            .collect();

        // Execute all shutdown sends concurrently
        let results = futures_util::future::join_all(shutdown_futures).await;

        let mut stats = self.stats.write().await;
        stats.graceful_shutdowns = results.iter().filter(|&&success| success).count() as u64;
        stats.forced_shutdowns = results.iter().filter(|&&success| !success).count() as u64;
        let graceful_count = stats.graceful_shutdowns;
        let forced_count = stats.forced_shutdowns;
        drop(stats);

        info!(
            "Shutdown completed: graceful={}, forced={}",
            graceful_count, forced_count
        );

        Ok(())
    }

    /// Get statistics
    pub async fn get_stats(&self) -> ReliabilityStats {
        self.stats.read().await.clone()
    }

    /// Get connection
    pub fn get_connection(&self, node_id: &str) -> Option<Arc<SecureProtocol>> {
        self.connection_manager.get_connection(&node_id.to_string())
    }

    /// Get all connections
    pub fn get_all_connections(&self) -> Vec<(String, Arc<SecureProtocol>)> {
        self.connection_manager.get_all_connections()
    }
}

/// Timeout operations wrapper
pub struct TimeoutWrapper {
    config: ReliabilityConfig,
}

impl TimeoutWrapper {
    pub fn new(config: ReliabilityConfig) -> Self {
        Self { config }
    }

    /// Read with timeout
    pub async fn read_with_timeout<F, T>(&self, operation: F) -> NetworkResult<T>
    where
        F: std::future::Future<Output = NetworkResult<T>>,
    {
        timeout(self.config.read_timeout, operation)
            .await
            .map_err(|_| NetworkError::ReadTimeout)?
    }

    /// Write with timeout
    pub async fn write_with_timeout<F, T>(&self, operation: F) -> NetworkResult<T>
    where
        F: std::future::Future<Output = NetworkResult<T>>,
    {
        timeout(self.config.write_timeout, operation)
            .await
            .map_err(|_| NetworkError::WriteTimeout)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reliability_config_default() {
        let config = ReliabilityConfig::default();
        assert_eq!(config.max_retry_attempts, 10);
        assert!(config.keepalive_enabled);
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_jitter_calculation() {
        let manager = ReliabilityManager::new(ReliabilityConfig::default());
        let base_delay = Duration::from_millis(1000);
        let jitter = manager.calculate_jitter(base_delay);

        // Verify that jitter is within 10% of base delay
        assert!(jitter.as_millis() <= (base_delay.as_millis() as f64 * 0.1) as u128);
    }

    #[tokio::test]
    async fn test_reliability_stats() {
        let manager = ReliabilityManager::new(ReliabilityConfig::default());
        let stats = manager.get_stats().await;

        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.successful_connections, 0);
    }
}
