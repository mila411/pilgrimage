use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

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

        // Overwrite settings from environment variables
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ProductionConfig::default();
        assert_eq!(config.network.port, 9092);
        assert!(config.security.tls_enabled);
        assert!(config.monitoring.metrics_enabled);
    }

    #[test]
    fn test_config_manager() {
        let manager = ConfigManager::new();
        let config = manager.get_config();
        assert_eq!(config.network.port, 9092);
    }

    #[test]
    fn test_config_validation() {
        let manager = ConfigManager::new();
        assert!(manager.validate().is_ok());
    }
}
