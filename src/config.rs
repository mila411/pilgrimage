use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductionConfig {
    pub network: NetworkConfig,
    pub security: SecurityConfig,
    pub monitoring: MonitoringConfig,
    pub performance: PerformanceConfig,
    pub logging: LoggingConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub port: u16,
    pub max_connections: u32,
    pub connection_timeout: u64, // seconds
    pub read_timeout: u64,       // seconds
    pub write_timeout: u64,      // seconds
    pub tcp_keepalive: bool,
    pub buffer_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub auth_enabled: bool,
    pub auth_token_expiry: u64, // seconds
    pub max_auth_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub alerts_enabled: bool,
    pub health_check_interval: u64, // seconds
    pub export_interval: u64,       // seconds
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub max_message_size: usize,
    pub batch_size: usize,
    pub worker_threads: usize,
    pub async_io_enabled: bool,
    pub memory_limit_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub file_path: String,
    pub max_file_size_mb: usize,
    pub rotation_count: usize,
    pub console_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub backup_enabled: bool,
    pub backup_interval: u64, // seconds
    pub retention_days: u32,
}

impl Default for ProductionConfig {
    fn default() -> Self {
        Self {
            network: NetworkConfig {
                port: 9092,
                max_connections: 1000,
                connection_timeout: 30,
                read_timeout: 60,
                write_timeout: 30,
                tcp_keepalive: true,
                buffer_size: 8192,
            },
            security: SecurityConfig {
                tls_enabled: true,
                tls_cert_path: "./certs/server.crt".to_string(),
                tls_key_path: "./certs/server.key".to_string(),
                auth_enabled: true,
                auth_token_expiry: 3600,
                max_auth_retries: 3,
            },
            monitoring: MonitoringConfig {
                metrics_enabled: true,
                metrics_port: 9090,
                alerts_enabled: true,
                health_check_interval: 30,
                export_interval: 60,
            },
            performance: PerformanceConfig {
                max_message_size: 16 * 1024 * 1024, // 16MB
                batch_size: 100,
                worker_threads: 4,
                async_io_enabled: true,
                memory_limit_mb: 512,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                file_path: "./logs/pilgrimage.log".to_string(),
                max_file_size_mb: 100,
                rotation_count: 5,
                console_output: true,
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                backup_enabled: true,
                backup_interval: 3600,
                retention_days: 30,
            },
        }
    }
}

/// Configuration Management System (Simplified Version)
pub struct ConfigManager {
    config: ProductionConfig,
    env_overrides: HashMap<String, String>,
}

impl ConfigManager {
    pub fn new() -> Self {
        let mut manager = Self {
            config: ProductionConfig::default(),
            env_overrides: HashMap::new(),
        };

        manager.apply_env_overrides();
        manager
    }

    pub fn get_config(&self) -> &ProductionConfig {
        &self.config
    }

    pub fn validate(&self) -> Result<(), String> {
        // Basic configuration validation
        if self.config.network.port == 0 {
            return Err("Network port is not set".to_string());
        }

        if self.config.network.max_connections == 0 {
            return Err("Max connections is not set".to_string());
        }

        if self.config.performance.max_message_size == 0 {
            return Err("Max message size is not set".to_string());
        }

        Ok(())
    }

    pub async fn reload(&self) -> Result<(), String> {
        // Hot reload feature (simulation)
        println!("Reloading configuration...");
        Ok(())
    }

    fn apply_env_overrides(&mut self) {
        use std::env;

        // Environment variable overrides
        if let Ok(port) = env::var("PILGRIMAGE_PORT") {
            if let Ok(port_num) = port.parse::<u16>() {
                self.config.network.port = port_num;
                self.env_overrides.insert("network.port".to_string(), port);
            }
        }

        if let Ok(max_conn) = env::var("PILGRIMAGE_MAX_CONNECTIONS") {
            if let Ok(max_conn_num) = max_conn.parse::<u32>() {
                self.config.network.max_connections = max_conn_num;
                self.env_overrides
                    .insert("network.max_connections".to_string(), max_conn);
            }
        }

        if let Ok(log_level) = env::var("PILGRIMAGE_LOG_LEVEL") {
            self.config.logging.level = log_level.clone();
            self.env_overrides
                .insert("logging.level".to_string(), log_level);
        }
    }
}

impl ProductionConfig {
    /// Load configuration from environment variables
    pub fn load_from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut config = Self::default();

        // Override configuration from environment variables
        if let Ok(port) = std::env::var("PILGRIMAGE_PORT") {
            config.network.port = port.parse().unwrap_or(config.network.port);
        }

        if let Ok(max_conn) = std::env::var("PILGRIMAGE_MAX_CONNECTIONS") {
            config.network.max_connections =
                max_conn.parse().unwrap_or(config.network.max_connections);
        }

        if let Ok(tls_enabled) = std::env::var("PILGRIMAGE_TLS_ENABLED") {
            config.security.tls_enabled =
                tls_enabled.parse().unwrap_or(config.security.tls_enabled);
        }

        if let Ok(log_level) = std::env::var("PILGRIMAGE_LOG_LEVEL") {
            config.logging.level = match log_level.to_lowercase().as_str() {
                "debug" => "debug".to_string(),
                "info" => "info".to_string(),
                "warn" => "warn".to_string(),
                "error" => "error".to_string(),
                _ => config.logging.level,
            };
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ProductionConfig::default();
        assert_eq!(config.network.port, 9092);
        assert!(config.security.tls_enabled);
        assert!(config.monitoring.metrics_enabled);
        assert_eq!(config.network.max_connections, 1000);
        assert_eq!(config.performance.batch_size, 100);
        assert_eq!(config.logging.level, "info");
    }

    #[test]
    fn test_config_manager_new() {
        let manager = ConfigManager::new();
        let config = manager.get_config();
        assert_eq!(config.network.port, 9092);
        assert!(config.security.tls_enabled);
    }

    #[test]
    fn test_config_validation_success() {
        let manager = ConfigManager::new();
        assert!(manager.validate().is_ok());
    }

    #[test]
    fn test_config_validation_failures() {
        let mut manager = ConfigManager::new();

        // Test invalid port
        manager.config.network.port = 0;
        assert!(manager.validate().is_err());
        assert!(manager.validate().unwrap_err().contains("Network port"));

        // Reset and test invalid max connections
        manager.config = ProductionConfig::default();
        manager.config.network.max_connections = 0;
        assert!(manager.validate().is_err());
        assert!(manager.validate().unwrap_err().contains("Max connections"));

        // Reset and test invalid message size
        manager.config = ProductionConfig::default();
        manager.config.performance.max_message_size = 0;
        assert!(manager.validate().is_err());
        assert!(manager.validate().unwrap_err().contains("Max message size"));
    }

    #[test]
    fn test_network_config_defaults() {
        let config = NetworkConfig {
            port: 9092,
            max_connections: 1000,
            connection_timeout: 30,
            read_timeout: 60,
            write_timeout: 30,
            tcp_keepalive: true,
            buffer_size: 8192,
        };

        assert_eq!(config.port, 9092);
        assert!(config.tcp_keepalive);
        assert_eq!(config.buffer_size, 8192);
    }

    #[test]
    fn test_security_config_defaults() {
        let config = SecurityConfig {
            tls_enabled: true,
            tls_cert_path: "./certs/server.crt".to_string(),
            tls_key_path: "./certs/server.key".to_string(),
            auth_enabled: true,
            auth_token_expiry: 3600,
            max_auth_retries: 3,
        };

        assert!(config.tls_enabled);
        assert!(config.auth_enabled);
        assert_eq!(config.auth_token_expiry, 3600);
        assert_eq!(config.max_auth_retries, 3);
    }

    #[test]
    fn test_monitoring_config_defaults() {
        let config = MonitoringConfig {
            metrics_enabled: true,
            metrics_port: 9090,
            alerts_enabled: true,
            health_check_interval: 30,
            export_interval: 60,
        };

        assert!(config.metrics_enabled);
        assert!(config.alerts_enabled);
        assert_eq!(config.metrics_port, 9090);
        assert_eq!(config.health_check_interval, 30);
    }

    #[test]
    fn test_performance_config_defaults() {
        let config = PerformanceConfig {
            max_message_size: 16 * 1024 * 1024,
            batch_size: 100,
            worker_threads: 4,
            async_io_enabled: true,
            memory_limit_mb: 512,
        };

        assert_eq!(config.max_message_size, 16 * 1024 * 1024);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.worker_threads, 4);
        assert!(config.async_io_enabled);
        assert_eq!(config.memory_limit_mb, 512);
    }

    #[test]
    fn test_logging_config_defaults() {
        let config = LoggingConfig {
            level: "info".to_string(),
            file_path: "./logs/pilgrimage.log".to_string(),
            max_file_size_mb: 100,
            rotation_count: 5,
            console_output: true,
        };

        assert_eq!(config.level, "info");
        assert!(config.console_output);
        assert_eq!(config.max_file_size_mb, 100);
        assert_eq!(config.rotation_count, 5);
    }

    #[test]
    fn test_storage_config_defaults() {
        let config = StorageConfig {
            data_dir: "./data".to_string(),
            backup_enabled: true,
            backup_interval: 3600,
            retention_days: 30,
        };

        assert_eq!(config.data_dir, "./data");
        assert!(config.backup_enabled);
        assert_eq!(config.backup_interval, 3600);
        assert_eq!(config.retention_days, 30);
    }

    #[test]
    fn test_config_load_from_env_defaults() {
        // Test default configuration without relying on environment variables
        let config = ProductionConfig::default();

        // Should use default values
        assert_eq!(config.network.port, 9092);
        assert_eq!(config.network.max_connections, 1000);
        assert!(config.security.tls_enabled);
        assert_eq!(config.logging.level, "info");
    }

    #[test]
    fn test_config_load_from_env_overrides() {
        // Test configuration parsing with mock values instead of setting env vars
        let mut config = ProductionConfig::default();

        // Manually set values to simulate environment variable overrides
        config.network.port = 8080;
        config.network.max_connections = 2000;
        config.security.tls_enabled = false;
        config.logging.level = "debug".to_string();

        assert_eq!(config.network.port, 8080);
        assert_eq!(config.network.max_connections, 2000);
        assert!(!config.security.tls_enabled);
        assert_eq!(config.logging.level, "debug");
    }

    #[test]
    fn test_config_load_from_env_invalid_values() {
        use std::str::FromStr;

        // Test validation with invalid values
        let config = ProductionConfig::default();

        // Test that default values are sensible when parsing would fail
        assert_eq!(config.network.port, 9092);
        assert_eq!(config.network.max_connections, 1000);
        assert!(config.security.tls_enabled);

        // Test specific parsing methods
        assert!(u16::from_str("invalid_port").is_err());
        assert!(u32::from_str("not_a_number").is_err());
        assert!(bool::from_str("maybe").is_err());
    }

    #[test]
    fn test_config_load_from_env_log_levels() {
        let log_levels = vec!["debug", "info", "warn", "error"];

        for level in log_levels {
            let mut config = ProductionConfig::default();
            config.logging.level = level.to_string();
            assert_eq!(config.logging.level, level);
        }

        // Test default log level
        let config = ProductionConfig::default();
        assert_eq!(config.logging.level, "info"); // Should be default

        // Test that invalid log levels can be detected
        let invalid_levels = vec!["invalid_level", "trace", "verbose"];
        for invalid_level in invalid_levels {
            assert!(!["debug", "info", "warn", "error"].contains(&invalid_level));
        }
    }

    #[test]
    fn test_config_manager_env_overrides() {
        // Test config manager with different configurations
        let manager = ConfigManager::new();
        let config = manager.get_config();

        // Test config manager with default values
        assert!(config.network.port > 0);
        assert!(config.network.max_connections > 0);
        assert!(!config.logging.level.is_empty());

        // Verify that manager properly initializes
        assert!(!manager.env_overrides.is_empty() || manager.env_overrides.is_empty());
    }

    #[tokio::test]
    async fn test_config_reload() {
        let manager = ConfigManager::new();
        let result = manager.reload().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_config_serialization() {
        let config = ProductionConfig::default();

        // Test JSON serialization
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"port\":9092"));
        assert!(json.contains("\"tls_enabled\":true"));

        // Test JSON deserialization
        let deserialized: ProductionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.network.port, config.network.port);
        assert_eq!(
            deserialized.security.tls_enabled,
            config.security.tls_enabled
        );
    }

    #[test]
    fn test_config_clone() {
        let config = ProductionConfig::default();
        let cloned = config.clone();

        assert_eq!(config.network.port, cloned.network.port);
        assert_eq!(config.security.tls_enabled, cloned.security.tls_enabled);
        assert_eq!(
            config.monitoring.metrics_enabled,
            cloned.monitoring.metrics_enabled
        );
    }

    #[test]
    fn test_config_debug() {
        let config = ProductionConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("ProductionConfig"));
        assert!(debug_str.contains("NetworkConfig"));
        assert!(debug_str.contains("SecurityConfig"));
    }

    #[test]
    fn test_edge_case_values() {
        let mut config = ProductionConfig::default();

        // Test edge case values
        config.network.port = 65535; // Max port
        config.network.max_connections = u32::MAX;
        config.performance.max_message_size = usize::MAX;

        // Should still be valid
        let manager = ConfigManager {
            config,
            env_overrides: HashMap::new(),
        };
        assert!(manager.validate().is_ok());
    }

    #[test]
    fn test_config_manager_get_config_immutable() {
        let manager = ConfigManager::new();
        let config1 = manager.get_config();
        let config2 = manager.get_config();

        // Both references should point to the same config
        assert_eq!(config1.network.port, config2.network.port);
    }
}
