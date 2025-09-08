//! Enhanced Network Configuration
//!
//! Production-level network configuration with security, reliability, and performance settings

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Timeout configuration for network operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutConfig {
    pub connection_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

/// Retry policy configuration with exponential backoff
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f64,
    pub jitter_enabled: bool,
    pub jitter_factor: f64,
}

/// Flow control configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlConfig {
    pub max_connections: usize,
    pub connection_rate_limit: f64,
    pub message_rate_limit: f64,
    pub bandwidth_limit: u64,
}

/// Keep-alive configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeepAliveConfig {
    pub enabled: bool,
    pub heartbeat_interval: Duration,
    pub timeout: Duration,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub mutual_tls: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub ca_path: Option<String>,
}

/// Enhanced network configuration for production use
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedNetworkConfig {
    /// Maximum message frame size (prevents DoS attacks)
    pub max_frame_size: usize,

    /// Timeout configuration
    pub timeouts: TimeoutConfig,

    /// Retry policy configuration
    pub retry_policy: RetryPolicy,

    /// Flow control configuration
    pub flow_control: FlowControlConfig,

    /// Keep-alive configuration
    pub keepalive_settings: KeepAliveConfig,

    /// TLS configuration
    pub tls_config: Option<TlsConfig>,

    /// Protocol version
    pub protocol_version: u16,

    /// Compression settings
    pub compression_enabled: bool,

    /// Health check interval
    pub health_check_interval: Duration,

    /// Discovery settings
    pub discovery_enabled: bool,

    /// Observability settings
    pub observability_enabled: bool,
}

impl Default for EnhancedNetworkConfig {
    fn default() -> Self {
        Self {
            // Message limits (prevents oversized frame attacks)
            max_frame_size: 16 * 1024 * 1024, // 16MB max

            // Timeouts (for production environments)
            timeouts: TimeoutConfig {
                connection_timeout: Duration::from_secs(30),
                read_timeout: Duration::from_secs(60),
                write_timeout: Duration::from_secs(30),
            },

            // Retry configuration (exponential backoff)
            retry_policy: RetryPolicy {
                max_retries: 5,
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(60),
                backoff_multiplier: 2.0,
                jitter_enabled: true,
                jitter_factor: 0.1,
            },

            // Flow control (memory protection)
            flow_control: FlowControlConfig {
                max_connections: 1000,
                connection_rate_limit: 100.0,
                message_rate_limit: 10000.0,
                bandwidth_limit: 100 * 1024 * 1024, // 100MB/s
            },

            // Keep-alive settings (TCP layer)
            keepalive_settings: KeepAliveConfig {
                enabled: true,
                heartbeat_interval: Duration::from_secs(30),
                timeout: Duration::from_secs(90),
            },

            // TLS disabled by default
            tls_config: None,

            // Protocol settings
            protocol_version: 1,
            compression_enabled: false,

            // Health and discovery
            health_check_interval: Duration::from_secs(60),
            discovery_enabled: false,
            observability_enabled: true,
        }
    }
}
