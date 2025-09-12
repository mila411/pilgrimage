use crate::network::async_safe::{AsyncSafeConnectionManager, ConcurrencyError};
use crate::network::secure_protocol::{ProtocolError, SecureProtocol};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, sleep, timeout};
use tracing::{debug, error, info, warn};

/// Reconnection configuration
#[derive(Debug, Clone)]
pub struct ReconnectionConfig {
    /// Maximum reconnection attempts
    pub max_attempts: u32,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Handshake timeout
    pub handshake_timeout: Duration,
    /// Enable jitter
    pub jitter_enabled: bool,
}

/// Reconnection statistics
#[derive(Debug, Clone)]
pub struct ReconnectionStats {
    pub total_attempts: u32,
    pub successful_connections: u32,
    pub failed_connections: u32,
    pub last_attempt_time: Option<Instant>,
    pub last_success_time: Option<Instant>,
    pub current_backoff: Duration,
}

/// Reconnection manager
#[derive(Debug)]
pub struct ReconnectionManager {
    config: ReconnectionConfig,
    connection_manager: Arc<AsyncSafeConnectionManager<String, Arc<SecureProtocol>>>,
    stats: Arc<tokio::sync::RwLock<ReconnectionStats>>,
}

/// Reconnection errors
#[derive(Debug, thiserror::Error)]
pub enum ReconnectionError {
    #[error("Max reconnection attempts exceeded: {attempts}")]
    MaxAttemptsExceeded { attempts: u32 },

    #[error("Connection timeout: {timeout:?}")]
    ConnectionTimeout { timeout: Duration },

    #[error("Handshake timeout: {timeout:?}")]
    HandshakeTimeout { timeout: Duration },

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Concurrency error: {0}")]
    Concurrency(#[from] ConcurrencyError),

    #[error("Network error: {0}")]
    Network(#[from] std::io::Error),
}

pub type ReconnectionResult<T> = Result<T, ReconnectionError>;

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            connect_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(5),
            jitter_enabled: true,
        }
    }
}

impl Default for ReconnectionStats {
    fn default() -> Self {
        Self {
            total_attempts: 0,
            successful_connections: 0,
            failed_connections: 0,
            last_attempt_time: None,
            last_success_time: None,
            current_backoff: Duration::from_millis(100),
        }
    }
}

impl ReconnectionManager {
    /// Create a new reconnection manager
    pub fn new(config: ReconnectionConfig, max_connections: u64) -> Self {
        Self {
            config,
            connection_manager: Arc::new(AsyncSafeConnectionManager::new(max_connections)),
            stats: Arc::new(tokio::sync::RwLock::new(ReconnectionStats::default())),
        }
    }

    /// Attempt to reconnect to the specified address
    pub async fn connect_with_retry(
        &self,
        node_id: String,
        addr: SocketAddr,
    ) -> ReconnectionResult<Arc<SecureProtocol>> {
        let mut attempt = 0;
        let mut current_backoff = self.config.initial_backoff;

        while attempt < self.config.max_attempts {
            // Statistical Update
            {
                let mut stats = self.stats.write().await;
                stats.total_attempts += 1;
                stats.last_attempt_time = Some(Instant::now());
                stats.current_backoff = current_backoff;
            }

            info!(
                node_id = %node_id,
                addr = %addr,
                attempt = attempt + 1,
                max_attempts = self.config.max_attempts,
                backoff = ?current_backoff,
                "Attempting connection"
            );

            match self.attempt_connection(&node_id, addr).await {
                Ok(protocol) => {
                    info!(
                        node_id = %node_id,
                        addr = %addr,
                        attempt = attempt + 1,
                        "Connection successful"
                    );

                    // Successful connection statistics update
                    {
                        let mut stats = self.stats.write().await;
                        stats.successful_connections += 1;
                        stats.last_success_time = Some(Instant::now());
                    }

                    return Ok(protocol);
                }
                Err(e) => {
                    warn!(
                        node_id = %node_id,
                        addr = %addr,
                        attempt = attempt + 1,
                        error = %e,
                        "Connection attempt failed"
                    );

                    // Failed connection statistics update
                    {
                        let mut stats = self.stats.write().await;
                        stats.failed_connections += 1;
                    }

                    attempt += 1;

                    // Backoff if not the last attempt
                    if attempt < self.config.max_attempts {
                        self.backoff(current_backoff).await;
                        current_backoff = self.calculate_next_backoff(current_backoff);
                    }
                }
            }
        }

        error!(
            node_id = %node_id,
            addr = %addr,
            attempts = self.config.max_attempts,
            "Max reconnection attempts exceeded"
        );

        Err(ReconnectionError::MaxAttemptsExceeded {
            attempts: self.config.max_attempts,
        })
    }

    /// Attempt a single connection
    async fn attempt_connection(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> ReconnectionResult<Arc<SecureProtocol>> {
        debug!(node_id = %node_id, addr = %addr, "Starting connection attempt");

        // Timeout-enabled TCP connection
        let stream = timeout(self.config.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ReconnectionError::ConnectionTimeout {
                timeout: self.config.connect_timeout,
            })??;

        debug!(node_id = %node_id, addr = %addr, "TCP connection established");

        // Initialize secure protocol
        let protocol = SecureProtocol::new(stream);

        // Timeout-enabled handshake
        timeout(
            self.config.handshake_timeout,
            protocol.handshake(node_id.to_string()),
        )
        .await
        .map_err(|_| ReconnectionError::HandshakeTimeout {
            timeout: self.config.handshake_timeout,
        })??;

        debug!(node_id = %node_id, addr = %addr, "Handshake completed");

        let protocol = Arc::new(protocol);

        // Add to connection manager
        self.connection_manager
            .add_connection(node_id.to_string(), protocol.clone())
            .await?;

        Ok(protocol)
    }

    /// Execute backoff
    async fn backoff(&self, duration: Duration) {
        let actual_duration = if self.config.jitter_enabled {
            self.add_jitter(duration)
        } else {
            duration
        };

        debug!(duration = ?actual_duration, "Backing off before retry");
        sleep(actual_duration).await;
    }

    /// Calculate the next backoff duration
    fn calculate_next_backoff(&self, current: Duration) -> Duration {
        let next = Duration::from_secs_f64(current.as_secs_f64() * self.config.backoff_multiplier);

        std::cmp::min(next, self.config.max_backoff)
    }

    /// Add jitter (±25% range)
    fn add_jitter(&self, duration: Duration) -> Duration {
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let jitter_factor = rng.gen_range(0.75..=1.25);

        Duration::from_secs_f64(duration.as_secs_f64() * jitter_factor)
    }

    /// Get a connection
    pub fn get_connection(&self, node_id: &str) -> Option<Arc<SecureProtocol>> {
        self.connection_manager.get_connection(&node_id.to_string())
    }

    /// Remove a connection
    pub async fn remove_connection(&self, node_id: &str) -> Option<Arc<SecureProtocol>> {
        self.connection_manager
            .remove_connection(&node_id.to_string())
            .await
    }

    /// Get all connections
    pub fn get_all_connections(&self) -> Vec<(String, Arc<SecureProtocol>)> {
        self.connection_manager.get_all_connections()
    }

    /// Get active connection count
    pub async fn active_connection_count(&self) -> u64 {
        self.connection_manager.active_count().await
    }

    /// Get statistics
    pub async fn get_stats(&self) -> ReconnectionStats {
        self.stats.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = ReconnectionStats::default();
    }

    /// Get configuration
    pub fn config(&self) -> &ReconnectionConfig {
        &self.config
    }

    /// Execute health check
    pub async fn health_check(&self, node_id: &str) -> bool {
        if let Some(protocol) = self.get_connection(node_id) {
            // Send heartbeat for health check
            match timeout(Duration::from_secs(5), protocol.send_heartbeat()).await {
                Ok(Ok(_)) => true,
                Ok(Err(e)) => {
                    warn!(node_id = %node_id, error = %e, "Health check failed");
                    false
                }
                Err(_) => {
                    warn!(node_id = %node_id, "Health check timeout");
                    false
                }
            }
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reconnection_config() {
        let config = ReconnectionConfig::default();
        assert_eq!(config.max_attempts, 10);
        assert!(config.jitter_enabled);
    }

    #[tokio::test]
    async fn test_backoff_calculation() {
        let config = ReconnectionConfig::default();
        let manager = ReconnectionManager::new(config, 10);

        let initial = Duration::from_millis(100);
        let next = manager.calculate_next_backoff(initial);
        assert_eq!(next, Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let config = ReconnectionConfig::default();
        let manager = ReconnectionManager::new(config, 10);

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_attempts, 0);
        assert_eq!(stats.successful_connections, 0);
    }
}
