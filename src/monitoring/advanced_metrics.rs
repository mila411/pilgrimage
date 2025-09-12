/// Real-time metrics, performance monitoring, and alerting capabilities
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;

/// Advanced metrics management system
pub struct AdvancedMetricsSystem {
    metrics: Arc<RwLock<SystemMetrics>>,
    alerts: Arc<RwLock<AlertManager>>,
    collectors: Arc<RwLock<Vec<MetricsCollector>>>,
    config: MetricsConfig,
}

/// Metrics configuration
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Metrics collection interval
    pub collection_interval: Duration,
    /// Data retention period
    pub retention_period: Duration,
    /// Alert thresholds
    pub alert_thresholds: AlertThresholds,
    /// Export configuration
    pub export_config: ExportConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(10),
            retention_period: Duration::from_secs(3600), // 1 hour
            alert_thresholds: AlertThresholds::default(),
            export_config: ExportConfig::default(),
        }
    }
}

/// Alert threshold configuration
#[derive(Debug, Clone)]
pub struct AlertThresholds {
    /// Maximum CPU usage (%)
    pub max_cpu_usage: f64,
    /// Maximum memory usage (%)
    pub max_memory_usage: f64,
    /// Maximum latency (ms)
    pub max_latency: u64,
    /// Minimum throughput (ops/sec)
    pub min_throughput: f64,
    /// Maximum error rate (%)
    pub max_error_rate: f64,
    /// Maximum connections
    pub max_connections: u32,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            max_cpu_usage: 80.0,
            max_memory_usage: 85.0,
            max_latency: 1000,
            min_throughput: 100.0,
            max_error_rate: 5.0,
            max_connections: 1000,
        }
    }
}

/// Export configuration
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// Prometheus export enabled
    pub prometheus_enabled: bool,
    /// Prometheus port
    pub prometheus_port: u16,
    /// File export enabled
    pub file_export_enabled: bool,
    /// File path
    pub export_file_path: String,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            prometheus_enabled: false,
            prometheus_port: 9090,
            file_export_enabled: true,
            export_file_path: "./metrics.json".to_string(),
        }
    }
}

impl AdvancedMetricsSystem {
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(SystemMetrics::new())),
            alerts: Arc::new(RwLock::new(AlertManager::new(
                config.alert_thresholds.clone(),
            ))),
            collectors: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// Start metrics collection
    pub async fn start_collection(&self) {
        println!("📊 Starting advanced metrics collection");

        let metrics = self.metrics.clone();
        let alerts = self.alerts.clone();
        let collectors = self.collectors.clone();
        let config = self.config.clone();

        // Periodic collection task
        tokio::spawn(async move {
            let mut interval = interval(config.collection_interval);

            loop {
                interval.tick().await;

                // Collect system metrics
                Self::collect_system_metrics(metrics.clone()).await;

                // Collect application metrics
                Self::collect_application_metrics(metrics.clone(), &collectors).await;

                // Check alerts
                Self::check_alerts(metrics.clone(), alerts.clone()).await;

                // Manage data retention period
                Self::cleanup_old_data(metrics.clone(), config.retention_period).await;
            }
        });

        // Export task
        if config.export_config.file_export_enabled {
            let export_metrics = self.metrics.clone();
            let export_config = config.export_config.clone();

            tokio::spawn(async move {
                let mut export_interval = interval(Duration::from_secs(60));

                loop {
                    export_interval.tick().await;
                    Self::export_metrics(export_metrics.clone(), &export_config).await;
                }
            });
        }
    }

    /// Collect system metrics
    async fn collect_system_metrics(metrics: Arc<RwLock<SystemMetrics>>) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Get system information (mock implementation)
        let cpu_usage = Self::get_cpu_usage().await;
        let memory_usage = Self::get_memory_usage().await;
        let disk_usage = Self::get_disk_usage().await;
        let network_io = Self::get_network_io().await;

        let mut metrics_guard = metrics.write().await;
        metrics_guard.record_system_metrics(SystemSnapshot {
            timestamp,
            cpu_usage,
            memory_usage,
            disk_usage,
            network_io,
        });
    }

    /// Collect application metrics
    async fn collect_application_metrics(
        metrics: Arc<RwLock<SystemMetrics>>,
        collectors: &Arc<RwLock<Vec<MetricsCollector>>>,
    ) {
        let collectors_guard = collectors.read().await;

        for collector in collectors_guard.iter() {
            if let Some(app_metrics) = collector.collect().await {
                let mut metrics_guard = metrics.write().await;
                metrics_guard.record_application_metrics(app_metrics);
            }
        }
    }

    /// Check alerts
    async fn check_alerts(metrics: Arc<RwLock<SystemMetrics>>, alerts: Arc<RwLock<AlertManager>>) {
        let metrics_guard = metrics.read().await;
        let current_snapshot = metrics_guard.get_latest_snapshot();

        if let Some(snapshot) = current_snapshot {
            let mut alerts_guard = alerts.write().await;
            alerts_guard.check_thresholds(&snapshot).await;
        }
    }

    /// Clean up old data
    async fn cleanup_old_data(metrics: Arc<RwLock<SystemMetrics>>, retention_period: Duration) {
        let mut metrics_guard = metrics.write().await;
        metrics_guard.cleanup_old_data(retention_period);
    }

    /// Export metrics
    async fn export_metrics(metrics: Arc<RwLock<SystemMetrics>>, config: &ExportConfig) {
        if config.file_export_enabled {
            let metrics_guard = metrics.read().await;
            let export_data = metrics_guard.to_export_format();

            if let Ok(json_data) = serde_json::to_string_pretty(&export_data) {
                match tokio::fs::write(&config.export_file_path, json_data).await {
                    Ok(_) => println!("📁 Metrics export completed: {}", config.export_file_path),
                    Err(e) => println!("❌ Metrics export failed: {}", e),
                }
            }
        }
    }

    /// Register metrics collector
    pub async fn register_collector(&self, collector: MetricsCollector) {
        let collector_name = collector.name.clone();
        let mut collectors = self.collectors.write().await;
        collectors.push(collector);
        println!("📊 Metrics collector registered: {}", collector_name);
    }

    /// Get current metrics
    pub async fn get_current_metrics(&self) -> SystemMetrics {
        self.metrics.read().await.clone()
    }

    /// Get alert history
    pub async fn get_alert_history(&self) -> Vec<Alert> {
        self.alerts.read().await.get_history()
    }

    // System information acquisition methods (mock implementation)
    async fn get_cpu_usage() -> f64 {
        // In actual implementation, use psutil or system-stats crate
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    async fn get_memory_usage() -> f64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (std::time::SystemTime::now(), "memory").hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    async fn get_disk_usage() -> f64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (std::time::SystemTime::now(), "disk").hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    async fn get_network_io() -> NetworkIO {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut hasher);
        let base = hasher.finish();

        NetworkIO {
            bytes_sent: (base % 1000000),
            bytes_received: ((base >> 8) % 1000000),
            packets_sent: ((base >> 16) % 10000),
            packets_received: ((base >> 24) % 10000),
        }
    }
}

/// System Metrics
#[derive(Debug, Clone)]
pub struct SystemMetrics {
    system_snapshots: VecDeque<SystemSnapshot>,
    application_metrics: HashMap<String, ApplicationMetrics>,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self {
            system_snapshots: VecDeque::new(),
            application_metrics: HashMap::new(),
        }
    }

    pub fn record_system_metrics(&mut self, snapshot: SystemSnapshot) {
        self.system_snapshots.push_back(snapshot);

        // Keep maximum 1000 entries
        if self.system_snapshots.len() > 1000 {
            self.system_snapshots.pop_front();
        }
    }

    pub fn record_application_metrics(&mut self, metrics: ApplicationMetrics) {
        self.application_metrics
            .insert(metrics.component_name.clone(), metrics);
    }

    pub fn get_latest_snapshot(&self) -> Option<&SystemSnapshot> {
        self.system_snapshots.back()
    }

    pub fn cleanup_old_data(&mut self, retention_period: Duration) {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - retention_period.as_secs();

        self.system_snapshots
            .retain(|snapshot| snapshot.timestamp > cutoff_time);
    }

    pub fn to_export_format(&self) -> MetricsExport {
        MetricsExport {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            system_snapshots: self.system_snapshots.iter().cloned().collect(),
            application_metrics: self.application_metrics.clone(),
        }
    }

    pub fn calculate_averages(&self, duration: Duration) -> MetricsSummary {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - duration.as_secs();

        let recent_snapshots: Vec<&SystemSnapshot> = self
            .system_snapshots
            .iter()
            .filter(|snapshot| snapshot.timestamp > cutoff_time)
            .collect();

        if recent_snapshots.is_empty() {
            return MetricsSummary::default();
        }

        let avg_cpu = recent_snapshots.iter().map(|s| s.cpu_usage).sum::<f64>()
            / recent_snapshots.len() as f64;
        let avg_memory = recent_snapshots.iter().map(|s| s.memory_usage).sum::<f64>()
            / recent_snapshots.len() as f64;
        let avg_disk = recent_snapshots.iter().map(|s| s.disk_usage).sum::<f64>()
            / recent_snapshots.len() as f64;

        MetricsSummary {
            duration,
            average_cpu_usage: avg_cpu,
            average_memory_usage: avg_memory,
            average_disk_usage: avg_disk,
            total_network_bytes_sent: recent_snapshots
                .iter()
                .map(|s| s.network_io.bytes_sent)
                .sum(),
            total_network_bytes_received: recent_snapshots
                .iter()
                .map(|s| s.network_io.bytes_received)
                .sum(),
        }
    }
}

/// System snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemSnapshot {
    pub timestamp: u64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub network_io: NetworkIO,
}

/// Network I/O information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkIO {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
}

/// Application metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationMetrics {
    pub component_name: String,
    pub timestamp: u64,
    pub connections_active: u32,
    pub messages_processed: u64,
    pub messages_failed: u64,
    pub average_latency: u64,
    pub throughput: f64,
    pub memory_usage_mb: f64,
    pub custom_metrics: HashMap<String, f64>,
}

/// Metrics collector
#[derive(Clone)]
pub struct MetricsCollector {
    pub name: String,
    pub component: String,
    collection_fn: Arc<dyn Fn() -> Option<ApplicationMetrics> + Send + Sync>,
}

impl MetricsCollector {
    pub fn new<F>(name: String, component: String, collection_fn: F) -> Self
    where
        F: Fn() -> Option<ApplicationMetrics> + Send + Sync + 'static,
    {
        Self {
            name,
            component,
            collection_fn: Arc::new(collection_fn),
        }
    }

    pub async fn collect(&self) -> Option<ApplicationMetrics> {
        (self.collection_fn)()
    }
}

/// Alert management
#[derive(Debug, Clone)]
pub struct AlertManager {
    thresholds: AlertThresholds,
    alert_history: VecDeque<Alert>,
    active_alerts: HashMap<String, Alert>,
}

impl AlertManager {
    pub fn new(thresholds: AlertThresholds) -> Self {
        Self {
            thresholds,
            alert_history: VecDeque::new(),
            active_alerts: HashMap::new(),
        }
    }

    pub async fn check_thresholds(&mut self, snapshot: &SystemSnapshot) {
        // CPU usage check
        if snapshot.cpu_usage > self.thresholds.max_cpu_usage {
            self.trigger_alert(Alert {
                id: format!("cpu_high_{}", snapshot.timestamp),
                alert_type: AlertType::HighCpuUsage,
                severity: AlertSeverity::Warning,
                message: format!("High CPU usage: {:.1}%", snapshot.cpu_usage),
                timestamp: snapshot.timestamp,
                value: snapshot.cpu_usage,
                threshold: self.thresholds.max_cpu_usage,
            })
            .await;
        }

        // Memory usage check
        if snapshot.memory_usage > self.thresholds.max_memory_usage {
            self.trigger_alert(Alert {
                id: format!("memory_high_{}", snapshot.timestamp),
                alert_type: AlertType::HighMemoryUsage,
                severity: AlertSeverity::Critical,
                message: format!("High memory usage: {:.1}%", snapshot.memory_usage),
                timestamp: snapshot.timestamp,
                value: snapshot.memory_usage,
                threshold: self.thresholds.max_memory_usage,
            })
            .await;
        }
    }

    pub async fn trigger_alert(&mut self, alert: Alert) {
        println!(
            "🚨 Alert triggered: {} - {}",
            alert.alert_type.as_str(),
            alert.message
        );

        // Add to active alerts
        self.active_alerts.insert(alert.id.clone(), alert.clone());

        // Add to history
        self.alert_history.push_back(alert);

        // Keep maximum 100 entries in history
        if self.alert_history.len() > 100 {
            self.alert_history.pop_front();
        }
    }

    pub fn get_history(&self) -> Vec<Alert> {
        self.alert_history.iter().cloned().collect()
    }

    pub fn get_active_alerts(&self) -> Vec<Alert> {
        self.active_alerts.values().cloned().collect()
    }
}

/// Alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub message: String,
    pub timestamp: u64,
    pub value: f64,
    pub threshold: f64,
}

/// Alert type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertType {
    HighCpuUsage,
    HighMemoryUsage,
    HighLatency,
    LowThroughput,
    HighErrorRate,
    TooManyConnections,
    SystemFailure,
}

impl AlertType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertType::HighCpuUsage => "High CPU Usage",
            AlertType::HighMemoryUsage => "High Memory Usage",
            AlertType::HighLatency => "High Latency",
            AlertType::LowThroughput => "Low Throughput",
            AlertType::HighErrorRate => "High Error Rate",
            AlertType::TooManyConnections => "Too Many Connections",
            AlertType::SystemFailure => "System Failure",
        }
    }
}

/// Alert severity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

/// Metrics summary
#[derive(Debug, Clone, Default)]
pub struct MetricsSummary {
    pub duration: Duration,
    pub average_cpu_usage: f64,
    pub average_memory_usage: f64,
    pub average_disk_usage: f64,
    pub total_network_bytes_sent: u64,
    pub total_network_bytes_received: u64,
}

/// Metrics export format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsExport {
    pub timestamp: u64,
    pub system_snapshots: Vec<SystemSnapshot>,
    pub application_metrics: HashMap<String, ApplicationMetrics>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_system_creation() {
        let config = MetricsConfig::default();
        let metrics_system = AdvancedMetricsSystem::new(config);

        let current_metrics = metrics_system.get_current_metrics().await;
        assert!(current_metrics.system_snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_alert_creation() {
        let thresholds = AlertThresholds::default();
        let mut alert_manager = AlertManager::new(thresholds);

        let alert = Alert {
            id: "test_alert".to_string(),
            alert_type: AlertType::HighCpuUsage,
            severity: AlertSeverity::Warning,
            message: "Test alert".to_string(),
            timestamp: 1000000,
            value: 90.0,
            threshold: 80.0,
        };

        alert_manager.trigger_alert(alert).await;
        assert_eq!(alert_manager.get_active_alerts().len(), 1);
    }

    #[test]
    fn test_metrics_summary() {
        let mut metrics = SystemMetrics::new();

        // Use current timestamp to ensure it's not filtered out
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot = SystemSnapshot {
            timestamp: current_time,
            cpu_usage: 50.0,
            memory_usage: 60.0,
            disk_usage: 70.0,
            network_io: NetworkIO {
                bytes_sent: 1000,
                bytes_received: 2000,
                packets_sent: 10,
                packets_received: 20,
            },
        };

        metrics.record_system_metrics(snapshot);

        let summary = metrics.calculate_averages(Duration::from_secs(3600));
        assert_eq!(summary.average_cpu_usage, 50.0);
        assert_eq!(summary.average_memory_usage, 60.0);
    }
}
