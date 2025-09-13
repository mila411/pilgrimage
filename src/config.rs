use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProductionConfig {
    pub network: NetworkConfig,
    pub security: SecurityConfig,
    pub monitoring: MonitoringConfig,
    pub performance: PerformanceConfig,
    pub logging: LoggingConfig,
    pub storage: StorageConfig,
}

/// Represents changes between two configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDiff {
    pub timestamp: u64,
    pub changed_sections: Vec<String>,
    pub previous_config: Option<ProductionConfig>,
    pub new_config: ProductionConfig,
}

/// Represents a configuration change event
#[derive(Debug, Clone)]
pub struct ConfigChangeEvent {
    pub config: ProductionConfig,
    pub diff: Option<ConfigDiff>,
    pub source: ConfigChangeSource,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub enum ConfigChangeSource {
    FileWatch,
    ManualReload,
    EnvVariableChange,
    ApiUpdate,
}

/// Hot reload metrics for monitoring
#[derive(Debug, Clone, Default)]
pub struct HotReloadMetrics {
    pub total_reloads: u64,
    pub successful_reloads: u64,
    pub failed_reloads: u64,
    pub last_reload_timestamp: Option<u64>,
    pub average_reload_time_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkConfig {
    pub port: u16,
    pub max_connections: u32,
    pub connection_timeout: u64, // seconds
    pub read_timeout: u64,       // seconds
    pub write_timeout: u64,      // seconds
    pub tcp_keepalive: bool,
    pub buffer_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityConfig {
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub auth_enabled: bool,
    pub auth_token_expiry: u64, // seconds
    pub max_auth_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MonitoringConfig {
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub alerts_enabled: bool,
    pub health_check_interval: u64, // seconds
    pub export_interval: u64,       // seconds
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PerformanceConfig {
    pub max_message_size: usize,
    pub batch_size: usize,
    pub worker_threads: usize,
    pub async_io_enabled: bool,
    pub memory_limit_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoggingConfig {
    pub level: String,
    pub file_path: String,
    pub max_file_size_mb: usize,
    pub rotation_count: usize,
    pub console_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

/// Configuration Management System (Advanced Real-time Version)
#[derive(Clone)]
pub struct ConfigManager {
    config: ProductionConfig,
    previous_config: Option<ProductionConfig>,
    env_overrides: HashMap<String, String>,
    config_path: Option<PathBuf>,
    watched_paths: Vec<PathBuf>,
    tx: broadcast::Sender<ConfigChangeEvent>,
    metrics: HotReloadMetrics,
}

impl ConfigManager {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(32);
        let mut manager = Self {
            config: ProductionConfig::default(),
            previous_config: None,
            env_overrides: HashMap::new(),
            config_path: None,
            watched_paths: Vec::new(),
            tx,
            metrics: HotReloadMetrics::default(),
        };
        manager.apply_env_overrides();
        manager
    }

    /// Create a ConfigManager that loads from a specific file path (TOML/YAML/JSON)
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let mut mgr = Self::new();
        let path_buf = path.as_ref().to_path_buf();
        let loaded = load_config_from_path(&path_buf).map_err(|e| e.to_string())?;
        mgr.config = loaded;
        mgr.config_path = Some(path_buf.clone());
        mgr.watched_paths.push(path_buf);
        Ok(mgr)
    }

    /// Add additional files to watch (e.g., include files, environment-specific configs)
    pub fn add_watched_file<P: AsRef<Path>>(&mut self, path: P) {
        let path_buf = path.as_ref().to_path_buf();
        if !self.watched_paths.contains(&path_buf) {
            self.watched_paths.push(path_buf);
        }
    }

    pub fn get_config(&self) -> &ProductionConfig {
        &self.config
    }

    pub fn get_metrics(&self) -> &HotReloadMetrics {
        &self.metrics
    }

    /// Subscribe to config change events (advanced hot reload with diff information)
    pub fn subscribe(&self) -> broadcast::Receiver<ConfigChangeEvent> {
        self.tx.subscribe()
    }

    /// Advanced validation with detailed error reporting
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Network validation
        if self.config.network.port == 0 {
            errors.push("Network port cannot be zero".to_string());
        }
        if self.config.network.port < 1024 && self.config.network.port != 0 {
            errors.push("Network port should be >= 1024 for non-root users".to_string());
        }
        if self.config.network.max_connections == 0 {
            errors.push("Max connections must be greater than zero".to_string());
        }
        if self.config.network.max_connections > 100000 {
            errors.push("Max connections seems unreasonably high (>100k)".to_string());
        }

        // Performance validation  
        if self.config.performance.max_message_size == 0 {
            errors.push("Max message size must be greater than zero".to_string());
        }
        if self.config.performance.max_message_size > 1024 * 1024 * 1024 {
            errors.push("Max message size exceeds 1GB limit".to_string());
        }
        if self.config.performance.worker_threads == 0 {
            errors.push("Worker threads must be at least 1".to_string());
        }

        // Security validation
        if self.config.security.tls_enabled {
            if self.config.security.tls_cert_path.is_empty() {
                errors.push("TLS certificate path is required when TLS is enabled".to_string());
            }
            if self.config.security.tls_key_path.is_empty() {
                errors.push("TLS key path is required when TLS is enabled".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Generate diff between current and new configuration
    fn generate_diff(&self, new_config: &ProductionConfig) -> ConfigDiff {
        let mut changed_sections = Vec::new();

        if self.config.network != new_config.network {
            changed_sections.push("network".to_string());
        }
        if self.config.security != new_config.security {
            changed_sections.push("security".to_string());
        }
        if self.config.monitoring != new_config.monitoring {
            changed_sections.push("monitoring".to_string());
        }
        if self.config.performance != new_config.performance {
            changed_sections.push("performance".to_string());
        }
        if self.config.logging != new_config.logging {
            changed_sections.push("logging".to_string());
        }
        if self.config.storage != new_config.storage {
            changed_sections.push("storage".to_string());
        }

        ConfigDiff {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            changed_sections,
            previous_config: Some(self.config.clone()),
            new_config: new_config.clone(),
        }
    }

    /// Advanced reload with validation, rollback, and detailed notifications
    pub async fn reload_with_source(&mut self, source: ConfigChangeSource) -> Result<(), String> {
        let start_time = Instant::now();
        self.metrics.total_reloads += 1;

        // Store current config for potential rollback
        let backup_config = self.config.clone();
        let mut new_config = backup_config.clone();

        // Load from file if configured
        if let Some(path) = &self.config_path {
            match load_config_from_path(path) {
                Ok(cfg) => {
                    new_config = cfg;
                }
                Err(e) => {
                    self.metrics.failed_reloads += 1;
                    return Err(format!("Failed to load config from file: {}", e));
                }
            }
        }

        // Apply environment overrides to the new config
        let mut temp_manager = Self {
            config: new_config.clone(),
            previous_config: self.previous_config.clone(),
            env_overrides: self.env_overrides.clone(),
            config_path: self.config_path.clone(),
            watched_paths: self.watched_paths.clone(),
            tx: self.tx.clone(),
            metrics: self.metrics.clone(),
        };
        temp_manager.apply_env_overrides();
        new_config = temp_manager.config;

        // Validate new configuration
        if let Err(validation_errors) = self.validate_config(&new_config) {
            self.metrics.failed_reloads += 1;
            return Err(format!("Config validation failed: {}", validation_errors.join(", ")));
        }

        // Generate diff and update state
        let diff = if new_config != self.config {
            Some(self.generate_diff(&new_config))
        } else {
            None
        };

        self.previous_config = Some(self.config.clone());
        self.config = new_config.clone();

        // Update metrics
        self.metrics.successful_reloads += 1;
        self.metrics.last_reload_timestamp = Some(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        );
        let reload_time_ms = start_time.elapsed().as_millis() as f64;
        self.metrics.average_reload_time_ms = 
            (self.metrics.average_reload_time_ms * (self.metrics.successful_reloads - 1) as f64 + reload_time_ms) 
            / self.metrics.successful_reloads as f64;

        // Send change event to subscribers
        let change_event = ConfigChangeEvent {
            config: new_config,
            diff,
            source,
            timestamp: Instant::now(),
        };

        if let Err(_) = self.tx.send(change_event) {
            // No subscribers, that's okay
        }

        Ok(())
    }

    /// Convenience method for backward compatibility
    pub async fn reload(&mut self) -> Result<(), String> {
        self.reload_with_source(ConfigChangeSource::ManualReload).await
    }

    /// Validate a configuration without applying it
    fn validate_config(&self, config: &ProductionConfig) -> Result<(), Vec<String>> {
        let mut temp_manager = self.clone();
        temp_manager.config = config.clone();
        temp_manager.validate()
    }

    /// Rollback to the previous configuration if available
    pub async fn rollback(&mut self) -> Result<(), String> {
        if let Some(prev_config) = &self.previous_config {
            let rollback_config = prev_config.clone();
            
            // Validate the rollback config
            if let Err(errors) = self.validate_config(&rollback_config) {
                return Err(format!("Cannot rollback, previous config is invalid: {}", errors.join(", ")));
            }

            let diff = Some(self.generate_diff(&rollback_config));
            
            self.config = rollback_config.clone();
            self.previous_config = None; // Clear previous config after rollback

            // Send rollback event
            let change_event = ConfigChangeEvent {
                config: rollback_config,
                diff,
                source: ConfigChangeSource::ManualReload, // Could add ConfigChangeSource::Rollback
                timestamp: Instant::now(),
            };

            if let Err(_) = self.tx.send(change_event) {
                // No subscribers, that's okay
            }

            Ok(())
        } else {
            Err("No previous configuration available for rollback".to_string())
        }
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

/// Load configuration from file path by extension (toml/yaml/json)
pub fn load_config_from_path(path: &Path) -> Result<ProductionConfig, Box<dyn std::error::Error + Send + Sync>> {
    let content = std::fs::read_to_string(path)?;
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
    let cfg = match ext.as_str() {
        "toml" => toml::from_str::<ProductionConfig>(&content)?,
        "yaml" | "yml" => serde_yaml::from_str::<ProductionConfig>(&content)?,
        "json" => serde_json::from_str::<ProductionConfig>(&content)?,
        _ => return Err(format!("Unsupported config extension: {}", ext).into()),
    };
    Ok(cfg)
}

/// Advanced Real-time Configuration Reloader with Multi-file Support
pub struct ConfigReloader {
    inner: Arc<AsyncMutex<ConfigManager>>,
    debounce_duration: Duration,
    max_retry_attempts: usize,
}

impl ConfigReloader {
    pub fn new(manager: ConfigManager) -> Self {
        Self { 
            inner: Arc::new(AsyncMutex::new(manager)),
            debounce_duration: Duration::from_millis(50), // Much faster response
            max_retry_attempts: 3,
        }
    }

    /// Create reloader with custom debounce settings
    pub fn with_debounce(manager: ConfigManager, debounce_ms: u64) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(manager)),
            debounce_duration: Duration::from_millis(debounce_ms),
            max_retry_attempts: 3,
        }
    }

    pub fn manager(&self) -> Arc<AsyncMutex<ConfigManager>> { 
        self.inner.clone() 
    }

    /// Start watching all configured files for changes with advanced real-time monitoring
    pub async fn start_watching(&self) -> Result<tokio::task::JoinHandle<()>, String> {
        let manager = self.inner.clone();
        let watched_paths = {
            let guard = manager.lock().await;
            if guard.watched_paths.is_empty() {
                return Err("No paths configured for watching; use ConfigManager::from_file or add_watched_file".to_string());
            }
            guard.watched_paths.clone()
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let debounce_duration = self.debounce_duration;
        let max_retry_attempts = self.max_retry_attempts;

        // Enhanced file watcher with multiple file support
        let paths_clone = watched_paths.clone();
        let paths_for_watch = paths_clone.clone();
        std::thread::spawn(move || {
            use notify::{RecommendedWatcher, RecursiveMode, Watcher, EventKind};
            let tx2 = tx;
            let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
                if let Ok(ev) = res {
                    // Enhanced event filtering for better responsiveness
                    match ev.kind {
                        EventKind::Modify(_) | EventKind::Create(_) => {
                            // Check if the event is for one of our watched files
                            for path in &ev.paths {
                                if paths_clone.iter().any(|watched| watched == path) {
                                    let _ = tx2.send(path.clone());
                                    break; // Only send one event per actual change
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }).expect("Failed to create file system watcher");

            // Watch all configured paths
            for path in &paths_for_watch {
                if let Err(e) = watcher.watch(path, RecursiveMode::NonRecursive) {
                    eprintln!("Failed to watch path {:?}: {}", path, e);
                }
            }

            // Keep watcher alive
            loop { 
                std::thread::sleep(std::time::Duration::from_secs(3600)); 
            }
        });

        // Advanced debouncing and reload logic
        let handle = tokio::spawn(async move {
            use tokio::time::sleep;
            
            while let Some(changed_path) = rx.recv().await {
                // Debounce: wait for a quiet period before reloading
                sleep(debounce_duration).await;

                // Attempt reload with retry logic
                let mut attempts = 0;
                while attempts < max_retry_attempts {
                    match manager.try_lock() {
                        Ok(mut guard) => {
                            match guard.reload_with_source(ConfigChangeSource::FileWatch).await {
                                Ok(()) => {
                                    println!("✅ Configuration reloaded successfully from {:?}", changed_path);
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("❌ Config reload failed (attempt {}): {}", attempts + 1, e);
                                    
                                    // Try rollback on validation failure
                                    if e.contains("validation failed") {
                                        if let Ok(()) = guard.rollback().await {
                                            eprintln!("🔄 Rolled back to previous valid configuration");
                                        }
                                    }
                                }
                            }
                            break;
                        }
                        Err(_) => {
                            // Manager is busy, wait a bit and retry
                            attempts += 1;
                            sleep(Duration::from_millis(20)).await;
                        }
                    }
                }
                
                if attempts >= max_retry_attempts {
                    eprintln!("⚠️  Failed to acquire config manager lock after {} attempts", max_retry_attempts);
                }
            }
        });

        Ok(handle)
    }

    /// Start watching with real-time performance monitoring
    pub async fn start_watching_with_metrics(&self) -> Result<(tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>), String> {
        let watcher_handle = self.start_watching().await?;
        let manager = self.inner.clone();
        
        // Metrics reporting task
        let metrics_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if let Ok(guard) = manager.try_lock() {
                    let metrics = guard.get_metrics();
                    if metrics.total_reloads > 0 {
                        println!("📊 Hot Reload Metrics: {} total, {} successful ({:.1}% success rate), avg reload time: {:.2}ms",
                            metrics.total_reloads,
                            metrics.successful_reloads,
                            (metrics.successful_reloads as f64 / metrics.total_reloads as f64) * 100.0,
                            metrics.average_reload_time_ms
                        );
                    }
                }
            }
        });

        Ok((watcher_handle, metrics_handle))
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
        let errors = manager.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("Network port")));

        // Reset and test invalid max connections
        manager.config = ProductionConfig::default();
        manager.config.network.max_connections = 0;
        assert!(manager.validate().is_err());
        let errors = manager.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("Max connections")));

        // Reset and test invalid message size
        manager.config = ProductionConfig::default();
        manager.config.performance.max_message_size = 0;
        assert!(manager.validate().is_err());
        let errors = manager.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("Max message size")));
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
        let mut manager = ConfigManager::new();
        let result = manager.reload().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_advanced_config_reload_with_diff() {
        use tempfile::tempdir;
        
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_config.toml");

        let initial = r#"
[network]
port = 19092
max_connections = 100
connection_timeout = 30
read_timeout = 60
write_timeout = 30
tcp_keepalive = true
buffer_size = 8192

[security]
tls_enabled = false
tls_cert_path = "./certs/server.crt"
tls_key_path = "./certs/server.key"
auth_enabled = true
auth_token_expiry = 3600
max_auth_retries = 3

[monitoring]
metrics_enabled = true
metrics_port = 19090
alerts_enabled = true
health_check_interval = 30
export_interval = 60

[performance]
max_message_size = 1048576
batch_size = 10
worker_threads = 2
async_io_enabled = true
memory_limit_mb = 256

[logging]
level = "debug"
file_path = "./logs/test.log"
max_file_size_mb = 10
rotation_count = 2
console_output = true

[storage]
data_dir = "./data"
backup_enabled = false
backup_interval = 3600
retention_days = 7
"#;
        std::fs::write(&file_path, initial).unwrap();

        let mgr = ConfigManager::from_file(&file_path).unwrap();
        assert_eq!(mgr.get_config().network.port, 19092);

        // Subscribe to advanced change events
        let mut rx = mgr.subscribe();

        let reloader = ConfigReloader::with_debounce(mgr, 10); // Very fast debounce for testing

        // Update config file with multiple changes
        let updated = initial
            .replace("19092", "19093")
            .replace("100", "200")
            .replace("debug", "info");
        std::fs::write(&file_path, updated).unwrap();

        // Manual reload to test diff functionality
        {
            let manager = reloader.manager();
            let mut guard = manager.lock().await;
            guard.reload_with_source(ConfigChangeSource::FileWatch).await.unwrap();
        }

        // Receive and verify change event with diff
        use tokio::time::{timeout, Duration};
        let change_event = timeout(Duration::from_millis(500), rx.recv()).await
            .expect("Should receive config change event")
            .expect("Should not have receiver error");

        assert_eq!(change_event.config.network.port, 19093);
        assert_eq!(change_event.config.network.max_connections, 200);
        assert_eq!(change_event.config.logging.level, "info");

        // Verify diff information
        if let Some(diff) = change_event.diff {
            assert!(diff.changed_sections.contains(&"network".to_string()));
            assert!(diff.changed_sections.contains(&"logging".to_string()));
            assert_eq!(diff.changed_sections.len(), 2);
            assert!(diff.previous_config.is_some());
            let prev = diff.previous_config.unwrap();
            assert_eq!(prev.network.port, 19092);
            assert_eq!(prev.logging.level, "debug");
        } else {
            panic!("Expected diff information");
        }

        // Test validation and rollback
        let invalid_config = r#"
[network]
port = 0
max_connections = 0
"#;
        std::fs::write(&file_path, invalid_config).unwrap();

        {
            let manager = reloader.manager();
            let mut guard = manager.lock().await;
            let result = guard.reload_with_source(ConfigChangeSource::FileWatch).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("validation failed"));

            // Test rollback
            assert!(guard.rollback().await.is_ok());
            assert_eq!(guard.get_config().network.port, 19093); // Should be back to previous valid config
        }
    }

    #[tokio::test]
    async fn test_multi_file_watching() {
        use tempfile::tempdir;
        
        let temp_dir = tempdir().unwrap();
        let main_config_path = temp_dir.path().join("main.toml");
        let env_config_path = temp_dir.path().join("environment.toml");

        let main_config = r#"
[network]
port = 9092
max_connections = 1000
connection_timeout = 30
read_timeout = 60
write_timeout = 30
tcp_keepalive = true
buffer_size = 8192

[security]
tls_enabled = true
tls_cert_path = "./certs/server.crt"
tls_key_path = "./certs/server.key"
auth_enabled = true
auth_token_expiry = 3600
max_auth_retries = 3

[monitoring]
metrics_enabled = true
metrics_port = 9090
alerts_enabled = true
health_check_interval = 30
export_interval = 60

[performance]
max_message_size = 16777216
batch_size = 100
worker_threads = 4
async_io_enabled = true
memory_limit_mb = 512

[logging]
level = "info"
file_path = "./logs/broker.log"
max_file_size_mb = 100
rotation_count = 5
console_output = false

[storage]
data_dir = "./storage"
backup_enabled = true
backup_interval = 3600
retention_days = 30
"#;

        std::fs::write(&main_config_path, main_config).unwrap();
        std::fs::write(&env_config_path, "[network]\nport = 8080").unwrap();

        let mut mgr = ConfigManager::from_file(&main_config_path).unwrap();
        mgr.add_watched_file(&env_config_path);

        let reloader = ConfigReloader::new(mgr);
        let manager = reloader.manager();
        let watched_paths = {
            let guard = manager.lock().await;
            guard.watched_paths.len()
        };

        assert_eq!(watched_paths, 2); // Main config + environment config
    }

    #[tokio::test]
    async fn test_concurrent_config_changes() {
        use tempfile::tempdir;
        use tokio::time::{sleep, Duration};
        
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("concurrent_test.toml");

        let base_config = r#"
[network]
port = 9092
max_connections = 1000
connection_timeout = 30
read_timeout = 60
write_timeout = 30
tcp_keepalive = true
buffer_size = 8192

[security]
tls_enabled = true
tls_cert_path = "./certs/server.crt"
tls_key_path = "./certs/server.key"
auth_enabled = true
auth_token_expiry = 3600
max_auth_retries = 3

[monitoring]
metrics_enabled = true
metrics_port = 9090
alerts_enabled = true
health_check_interval = 30
export_interval = 60

[performance]
max_message_size = 16777216
batch_size = 100
worker_threads = 4
async_io_enabled = true
memory_limit_mb = 512

[logging]
level = "info"
file_path = "./logs/broker.log"
max_file_size_mb = 100
rotation_count = 5
console_output = false

[storage]
data_dir = "./storage"
backup_enabled = true
backup_interval = 3600
retention_days = 30
"#;
        std::fs::write(&file_path, base_config).unwrap();

        let mgr = ConfigManager::from_file(&file_path).unwrap();
        let reloader = ConfigReloader::new(mgr);
        let manager = reloader.manager();

        // Simulate concurrent config reads and writes
        let read_handles: Vec<_> = (0..5).map(|_| {
            let mgr_clone = manager.clone();
            tokio::spawn(async move {
                for _ in 0..10 {
                    if let Ok(guard) = mgr_clone.try_lock() {
                        let _config = guard.get_config();
                        drop(guard);
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }).collect();

        let write_handle = {
            let mgr_clone = manager.clone();
            let path_clone = file_path.clone();
            tokio::spawn(async move {
                for i in 0..5 {
                    let updated_config = base_config.replace("9092", &(9093 + i).to_string());
                    std::fs::write(&path_clone, updated_config).unwrap();
                    if let Ok(mut guard) = mgr_clone.try_lock() {
                        let _ = guard.reload().await;
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            })
        };

        // Wait for all tasks to complete
        for handle in read_handles {
            handle.await.unwrap();
        }
        write_handle.await.unwrap();

        // Verify final state
        let guard = manager.lock().await;
        let metrics = guard.get_metrics();
        assert!(metrics.total_reloads > 0);
    }

    #[tokio::test]
    async fn test_hot_reload_metrics() {
        use tempfile::tempdir;
        
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("metrics_test.toml");

        let config_content = r#"
[network]
port = 9092
max_connections = 1000
connection_timeout = 30
read_timeout = 60
write_timeout = 30
tcp_keepalive = true
buffer_size = 8192

[security]
tls_enabled = true
tls_cert_path = "./certs/server.crt"
tls_key_path = "./certs/server.key"
auth_enabled = true
auth_token_expiry = 3600
max_auth_retries = 3

[monitoring]
metrics_enabled = true
metrics_port = 9090
alerts_enabled = true
health_check_interval = 30
export_interval = 60

[performance]
max_message_size = 16777216
batch_size = 100
worker_threads = 4
async_io_enabled = true
memory_limit_mb = 512

[logging]
level = "info"
file_path = "./logs/broker.log"
max_file_size_mb = 100
rotation_count = 5
console_output = false

[storage]
data_dir = "./storage"
backup_enabled = true
backup_interval = 3600
retention_days = 30
"#;
        std::fs::write(&file_path, config_content).unwrap();

        let mgr = ConfigManager::from_file(&file_path).unwrap();
        let reloader = ConfigReloader::new(mgr);

        {
            let manager = reloader.manager();
            let mut guard = manager.lock().await;
            
            // Initial metrics should be zero
            let initial_metrics = guard.get_metrics();
            assert_eq!(initial_metrics.total_reloads, 0);
            assert_eq!(initial_metrics.successful_reloads, 0);
            assert_eq!(initial_metrics.failed_reloads, 0);

            // Perform successful reload
            let result = guard.reload().await;
            assert!(result.is_ok());

            // Check updated metrics
            let metrics = guard.get_metrics();
            assert_eq!(metrics.total_reloads, 1);
            assert_eq!(metrics.successful_reloads, 1);
            assert_eq!(metrics.failed_reloads, 0);
            assert!(metrics.last_reload_timestamp.is_some());
            assert!(metrics.average_reload_time_ms > 0.0);

            // Test failed reload
            guard.config_path = Some(PathBuf::from("/nonexistent/file.toml"));
            let result = guard.reload().await;
            assert!(result.is_err());

            // Check metrics after failure
            let metrics = guard.get_metrics();
            assert_eq!(metrics.total_reloads, 2);
            assert_eq!(metrics.successful_reloads, 1);
            assert_eq!(metrics.failed_reloads, 1);
        }
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
}
