//! Metrics collection and aggregation system
//!
//! Provides comprehensive metrics collection, processing, and storage
//! for system monitoring and observability.

use crate::network::error::NetworkResult;

use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;

/// Metric types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

/// Metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram {
        buckets: Vec<(f64, u64)>, // (upper_bound, count)
        sum: f64,
        count: u64,
    },
    Summary {
        quantiles: Vec<(f64, f64)>, // (quantile, value)
        sum: f64,
        count: u64,
    },
}

/// Metric metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricMetadata {
    pub name: String,
    pub help: String,
    pub metric_type: MetricType,
    pub labels: HashMap<String, String>,
}

/// Metric sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    pub metadata: MetricMetadata,
    pub value: MetricValue,
    pub timestamp: u64,
    pub node_id: String,
}

/// Metric collection configuration
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub collection_interval: Duration,
    pub retention_period: Duration,
    pub max_metrics: usize,
    pub export_enabled: bool,
    pub export_interval: Duration,
    pub prometheus_endpoint: Option<String>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval: Duration::from_secs(15),
            retention_period: Duration::from_secs(24 * 3600), // 24 hours
            max_metrics: 100000,
            export_enabled: false,
            export_interval: Duration::from_secs(60),
            prometheus_endpoint: None,
        }
    }
}

/// Metrics collector
pub struct MetricsCollector {
    config: MetricsConfig,
    node_id: String,
    metrics: Arc<RwLock<HashMap<String, MetricTimeSeries>>>,
    metric_registry: Arc<RwLock<HashMap<String, MetricMetadata>>>,
    collection_stats: Arc<Mutex<CollectionStats>>,
}

/// Time series for a single metric
#[derive(Debug, Clone)]
struct MetricTimeSeries {
    metadata: MetricMetadata,
    samples: VecDeque<(u64, MetricValue)>, // (timestamp, value)
    last_updated: Instant,
}

/// Collection statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CollectionStats {
    pub total_samples_collected: u64,
    pub metrics_registered: u64,
    pub collection_errors: u64,
    pub export_count: u64,
    pub last_collection_duration_micros: u64,
}

/// Built-in metric collectors
pub trait MetricCollector: Send + Sync {
    fn collect(&self) -> Vec<MetricSample>;
    fn name(&self) -> &str;
}

/// System metrics collector
pub struct SystemMetricsCollector {
    node_id: String,
}

/// Broker metrics collector
pub struct BrokerMetricsCollector {
    node_id: String,
}

/// Network metrics collector
pub struct NetworkMetricsCollector {
    node_id: String,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(config: MetricsConfig, node_id: String) -> Self {
        Self {
            config,
            node_id,
            metrics: Arc::new(RwLock::new(HashMap::new())),
            metric_registry: Arc::new(RwLock::new(HashMap::new())),
            collection_stats: Arc::new(Mutex::new(CollectionStats::default())),
        }
    }

    /// Start the metrics collection service
    pub async fn start(&self) -> NetworkResult<()> {
        if !self.config.enabled {
            info!("Metrics collection is disabled");
            return Ok(());
        }

        info!(
            "Starting metrics collection service for node {}",
            self.node_id
        );

        // Register built-in metric collectors
        self.register_builtin_collectors().await?;

        // Start collection task
        self.start_collection_task().await;

        // Start export task if enabled
        if self.config.export_enabled {
            self.start_export_task().await;
        }

        // Start cleanup task
        self.start_cleanup_task().await;

        Ok(())
    }

    /// Register a metric
    pub async fn register_metric(&self, metadata: MetricMetadata) -> NetworkResult<()> {
        let mut registry = self.metric_registry.write().await;
        registry.insert(metadata.name.clone(), metadata.clone());

        let mut stats = self.collection_stats.lock().unwrap();
        stats.metrics_registered += 1;

        debug!("Registered metric: {} ({})", metadata.name, metadata.help);
        Ok(())
    }

    /// Record a metric sample
    pub async fn record_sample(&self, sample: MetricSample) -> NetworkResult<()> {
        let mut metrics = self.metrics.write().await;

        let time_series = metrics
            .entry(sample.metadata.name.clone())
            .or_insert_with(|| MetricTimeSeries {
                metadata: sample.metadata.clone(),
                samples: VecDeque::new(),
                last_updated: Instant::now(),
            });

        time_series
            .samples
            .push_back((sample.timestamp, sample.value));
        time_series.last_updated = Instant::now();

        // Limit sample history
        if time_series.samples.len() > 1000 {
            time_series.samples.pop_front();
        }

        let mut stats = self.collection_stats.lock().unwrap();
        stats.total_samples_collected += 1;

        Ok(())
    }

    /// Increment a counter metric
    pub async fn increment_counter(
        &self,
        name: &str,
        labels: Option<HashMap<String, String>>,
    ) -> NetworkResult<()> {
        let sample = MetricSample {
            metadata: MetricMetadata {
                name: name.to_string(),
                help: format!("Counter metric: {}", name),
                metric_type: MetricType::Counter,
                labels: labels.unwrap_or_default(),
            },
            value: MetricValue::Counter(1),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            node_id: self.node_id.clone(),
        };

        self.record_sample(sample).await
    }

    /// Set a gauge metric
    pub async fn set_gauge(
        &self,
        name: &str,
        value: f64,
        labels: Option<HashMap<String, String>>,
    ) -> NetworkResult<()> {
        let sample = MetricSample {
            metadata: MetricMetadata {
                name: name.to_string(),
                help: format!("Gauge metric: {}", name),
                metric_type: MetricType::Gauge,
                labels: labels.unwrap_or_default(),
            },
            value: MetricValue::Gauge(value),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            node_id: self.node_id.clone(),
        };

        self.record_sample(sample).await
    }

    /// Record histogram observation
    pub async fn observe_histogram(
        &self,
        name: &str,
        value: f64,
        buckets: Vec<f64>,
        labels: Option<HashMap<String, String>>,
    ) -> NetworkResult<()> {
        // Calculate bucket counts
        let mut bucket_counts = Vec::new();
        for &bucket in &buckets {
            let count = if value <= bucket { 1 } else { 0 };
            bucket_counts.push((bucket, count));
        }

        let sample = MetricSample {
            metadata: MetricMetadata {
                name: name.to_string(),
                help: format!("Histogram metric: {}", name),
                metric_type: MetricType::Histogram,
                labels: labels.unwrap_or_default(),
            },
            value: MetricValue::Histogram {
                buckets: bucket_counts,
                sum: value,
                count: 1,
            },
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            node_id: self.node_id.clone(),
        };

        self.record_sample(sample).await
    }

    /// Get all metrics
    pub async fn get_all_metrics(&self) -> HashMap<String, Vec<(u64, f64)>> {
        let metrics = self.metrics.read().await;
        let mut result = HashMap::new();

        for (name, time_series) in metrics.iter() {
            let values: Vec<(u64, f64)> = time_series
                .samples
                .iter()
                .map(|(timestamp, value)| {
                    let numeric_value = match value {
                        MetricValue::Counter(v) => *v as f64,
                        MetricValue::Gauge(v) => *v,
                        MetricValue::Histogram { sum, count, .. } => {
                            if *count > 0 {
                                sum / (*count as f64)
                            } else {
                                0.0
                            }
                        }
                        MetricValue::Summary { sum, count, .. } => {
                            if *count > 0 {
                                sum / (*count as f64)
                            } else {
                                0.0
                            }
                        }
                    };
                    (*timestamp, numeric_value)
                })
                .collect();

            result.insert(name.clone(), values);
        }

        result
    }

    /// Get metric by name
    pub async fn get_metric(&self, name: &str) -> Option<Vec<MetricSample>> {
        let metrics = self.metrics.read().await;

        if let Some(time_series) = metrics.get(name) {
            let samples: Vec<MetricSample> = time_series
                .samples
                .iter()
                .map(|(timestamp, value)| MetricSample {
                    metadata: time_series.metadata.clone(),
                    value: value.clone(),
                    timestamp: *timestamp,
                    node_id: self.node_id.clone(),
                })
                .collect();
            Some(samples)
        } else {
            None
        }
    }

    /// Get collection statistics
    pub fn get_stats(&self) -> CollectionStats {
        let stats = self.collection_stats.lock().unwrap();
        CollectionStats {
            total_samples_collected: stats.total_samples_collected,
            metrics_registered: stats.metrics_registered,
            collection_errors: stats.collection_errors,
            export_count: stats.export_count,
            last_collection_duration_micros: stats.last_collection_duration_micros,
        }
    }

    /// Register built-in metric collectors
    async fn register_builtin_collectors(&self) -> NetworkResult<()> {
        // System metrics
        let system_metadata = vec![
            MetricMetadata {
                name: "cpu_usage_percent".to_string(),
                help: "CPU usage percentage".to_string(),
                metric_type: MetricType::Gauge,
                labels: HashMap::new(),
            },
            MetricMetadata {
                name: "memory_usage_percent".to_string(),
                help: "Memory usage percentage".to_string(),
                metric_type: MetricType::Gauge,
                labels: HashMap::new(),
            },
            MetricMetadata {
                name: "disk_usage_percent".to_string(),
                help: "Disk usage percentage".to_string(),
                metric_type: MetricType::Gauge,
                labels: HashMap::new(),
            },
        ];

        // Broker metrics
        let broker_metadata = vec![
            MetricMetadata {
                name: "messages_sent_total".to_string(),
                help: "Total messages sent".to_string(),
                metric_type: MetricType::Counter,
                labels: HashMap::new(),
            },
            MetricMetadata {
                name: "messages_received_total".to_string(),
                help: "Total messages received".to_string(),
                metric_type: MetricType::Counter,
                labels: HashMap::new(),
            },
            MetricMetadata {
                name: "active_connections".to_string(),
                help: "Number of active connections".to_string(),
                metric_type: MetricType::Gauge,
                labels: HashMap::new(),
            },
        ];

        // Network metrics
        let network_metadata = vec![
            MetricMetadata {
                name: "network_bytes_sent_total".to_string(),
                help: "Total network bytes sent".to_string(),
                metric_type: MetricType::Counter,
                labels: HashMap::new(),
            },
            MetricMetadata {
                name: "network_bytes_received_total".to_string(),
                help: "Total network bytes received".to_string(),
                metric_type: MetricType::Counter,
                labels: HashMap::new(),
            },
            MetricMetadata {
                name: "connection_errors_total".to_string(),
                help: "Total connection errors".to_string(),
                metric_type: MetricType::Counter,
                labels: HashMap::new(),
            },
        ];

        // Register all metrics
        for metadata in system_metadata
            .into_iter()
            .chain(broker_metadata.into_iter())
            .chain(network_metadata.into_iter())
        {
            self.register_metric(metadata).await?;
        }

        info!("Registered built-in metrics");
        Ok(())
    }

    /// Start metrics collection task
    async fn start_collection_task(&self) {
        let metrics_collector = Arc::new(SystemMetricsCollector {
            node_id: self.node_id.clone(),
        });
        let broker_collector = Arc::new(BrokerMetricsCollector {
            node_id: self.node_id.clone(),
        });
        let network_collector = Arc::new(NetworkMetricsCollector {
            node_id: self.node_id.clone(),
        });

        let collectors: Vec<Arc<dyn MetricCollector>> =
            vec![metrics_collector, broker_collector, network_collector];

        let metrics = self.metrics.clone();
        let stats = self.collection_stats.clone();
        let interval_duration = self.config.collection_interval;

        tokio::task::spawn(async move {
            let mut timer = interval(interval_duration);
            loop {
                timer.tick().await;

                let start_time = Instant::now();
                for collector in &collectors {
                    let samples = collector.collect();
                    for sample in samples {
                        if let Err(e) = Self::record_sample_internal(&metrics, sample).await {
                            error!("Failed to record metric sample: {}", e);
                            let mut stats = stats.lock().unwrap();
                            stats.collection_errors += 1;
                        }
                    }
                }

                let duration = start_time.elapsed();
                let mut stats = stats.lock().unwrap();
                stats.last_collection_duration_micros = duration.as_micros() as u64;
            }
        });
    }

    /// Internal method to record sample
    async fn record_sample_internal(
        metrics: &Arc<RwLock<HashMap<String, MetricTimeSeries>>>,
        sample: MetricSample,
    ) -> NetworkResult<()> {
        let mut metrics = metrics.write().await;

        let time_series = metrics
            .entry(sample.metadata.name.clone())
            .or_insert_with(|| MetricTimeSeries {
                metadata: sample.metadata.clone(),
                samples: VecDeque::new(),
                last_updated: Instant::now(),
            });

        time_series
            .samples
            .push_back((sample.timestamp, sample.value));
        time_series.last_updated = Instant::now();

        // Limit sample history
        if time_series.samples.len() > 1000 {
            time_series.samples.pop_front();
        }

        Ok(())
    }

    /// Start export task
    async fn start_export_task(&self) {
        let metrics = self.metrics.clone();
        let stats = self.collection_stats.clone();
        let interval_duration = self.config.export_interval;
        let prometheus_endpoint = self.config.prometheus_endpoint.clone();

        tokio::task::spawn(async move {
            let mut timer = interval(interval_duration);
            loop {
                timer.tick().await;

                if let Err(e) = Self::export_metrics(&metrics, &prometheus_endpoint).await {
                    error!("Failed to export metrics: {}", e);
                } else {
                    let mut stats = stats.lock().unwrap();
                    stats.export_count += 1;
                }
            }
        });
    }

    /// Export metrics to external systems
    async fn export_metrics(
        metrics: &Arc<RwLock<HashMap<String, MetricTimeSeries>>>,
        _prometheus_endpoint: &Option<String>,
    ) -> NetworkResult<()> {
        let metrics = metrics.read().await;

        debug!("Exporting {} metrics", metrics.len());

        // In a real implementation, this would export to Prometheus or other systems
        for (name, time_series) in metrics.iter() {
            if let Some((timestamp, value)) = time_series.samples.back() {
                debug!("Metric: {} = {:?} at {}", name, value, timestamp);
            }
        }

        Ok(())
    }

    /// Start cleanup task
    async fn start_cleanup_task(&self) {
        let metrics = self.metrics.clone();
        let retention_period = self.config.retention_period;

        tokio::task::spawn(async move {
            let mut timer = interval(Duration::from_secs(3600)); // Every hour
            loop {
                timer.tick().await;

                let cutoff_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    - retention_period.as_secs();

                let mut metrics = metrics.write().await;
                for (name, time_series) in metrics.iter_mut() {
                    let original_len = time_series.samples.len();

                    while let Some(front) = time_series.samples.front() {
                        if front.0 < cutoff_time {
                            time_series.samples.pop_front();
                        } else {
                            break;
                        }
                    }

                    let removed = original_len - time_series.samples.len();
                    if removed > 0 {
                        debug!("Cleaned up {} old samples for metric: {}", removed, name);
                    }
                }
            }
        });
    }
}

impl SystemMetricsCollector {
    fn collect_cpu_usage(&self) -> f64 {
        // Simplified CPU usage collection
        // In a real implementation, this would use system APIs
        25.5 // Sample value
    }

    fn collect_memory_usage(&self) -> f64 {
        // Simplified memory usage collection
        60.2 // Sample value
    }

    fn collect_disk_usage(&self) -> f64 {
        // Simplified disk usage collection
        45.8 // Sample value
    }
}

impl MetricCollector for SystemMetricsCollector {
    fn collect(&self) -> Vec<MetricSample> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        vec![
            MetricSample {
                metadata: MetricMetadata {
                    name: "cpu_usage_percent".to_string(),
                    help: "CPU usage percentage".to_string(),
                    metric_type: MetricType::Gauge,
                    labels: HashMap::new(),
                },
                value: MetricValue::Gauge(self.collect_cpu_usage()),
                timestamp,
                node_id: self.node_id.clone(),
            },
            MetricSample {
                metadata: MetricMetadata {
                    name: "memory_usage_percent".to_string(),
                    help: "Memory usage percentage".to_string(),
                    metric_type: MetricType::Gauge,
                    labels: HashMap::new(),
                },
                value: MetricValue::Gauge(self.collect_memory_usage()),
                timestamp,
                node_id: self.node_id.clone(),
            },
            MetricSample {
                metadata: MetricMetadata {
                    name: "disk_usage_percent".to_string(),
                    help: "Disk usage percentage".to_string(),
                    metric_type: MetricType::Gauge,
                    labels: HashMap::new(),
                },
                value: MetricValue::Gauge(self.collect_disk_usage()),
                timestamp,
                node_id: self.node_id.clone(),
            },
        ]
    }

    fn name(&self) -> &str {
        "system_metrics"
    }
}

impl MetricCollector for BrokerMetricsCollector {
    fn collect(&self) -> Vec<MetricSample> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        vec![
            MetricSample {
                metadata: MetricMetadata {
                    name: "messages_sent_total".to_string(),
                    help: "Total messages sent".to_string(),
                    metric_type: MetricType::Counter,
                    labels: HashMap::new(),
                },
                value: MetricValue::Counter(100), // Sample value
                timestamp,
                node_id: self.node_id.clone(),
            },
            MetricSample {
                metadata: MetricMetadata {
                    name: "messages_received_total".to_string(),
                    help: "Total messages received".to_string(),
                    metric_type: MetricType::Counter,
                    labels: HashMap::new(),
                },
                value: MetricValue::Counter(95), // Sample value
                timestamp,
                node_id: self.node_id.clone(),
            },
            MetricSample {
                metadata: MetricMetadata {
                    name: "active_connections".to_string(),
                    help: "Number of active connections".to_string(),
                    metric_type: MetricType::Gauge,
                    labels: HashMap::new(),
                },
                value: MetricValue::Gauge(15.0), // Sample value
                timestamp,
                node_id: self.node_id.clone(),
            },
        ]
    }

    fn name(&self) -> &str {
        "broker_metrics"
    }
}

impl MetricCollector for NetworkMetricsCollector {
    fn collect(&self) -> Vec<MetricSample> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        vec![
            MetricSample {
                metadata: MetricMetadata {
                    name: "network_bytes_sent_total".to_string(),
                    help: "Total network bytes sent".to_string(),
                    metric_type: MetricType::Counter,
                    labels: HashMap::new(),
                },
                value: MetricValue::Counter(1024000), // Sample value
                timestamp,
                node_id: self.node_id.clone(),
            },
            MetricSample {
                metadata: MetricMetadata {
                    name: "network_bytes_received_total".to_string(),
                    help: "Total network bytes received".to_string(),
                    metric_type: MetricType::Counter,
                    labels: HashMap::new(),
                },
                value: MetricValue::Counter(950000), // Sample value
                timestamp,
                node_id: self.node_id.clone(),
            },
            MetricSample {
                metadata: MetricMetadata {
                    name: "connection_errors_total".to_string(),
                    help: "Total connection errors".to_string(),
                    metric_type: MetricType::Counter,
                    labels: HashMap::new(),
                },
                value: MetricValue::Counter(5), // Sample value
                timestamp,
                node_id: self.node_id.clone(),
            },
        ]
    }

    fn name(&self) -> &str {
        "network_metrics"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[test]
    fn test_metric_metadata_creation() {
        let mut labels = HashMap::new();
        labels.insert("instance".to_string(), "test".to_string());
        labels.insert("job".to_string(), "broker".to_string());

        let metadata = MetricMetadata {
            name: "test_metric".to_string(),
            help: "Test metric for testing".to_string(),
            metric_type: MetricType::Counter,
            labels,
        };

        assert_eq!(metadata.name, "test_metric");
        assert_eq!(metadata.help, "Test metric for testing");
        assert_eq!(metadata.metric_type, MetricType::Counter);
        assert_eq!(metadata.labels.get("instance").unwrap(), "test");
        assert_eq!(metadata.labels.get("job").unwrap(), "broker");
    }

    #[test]
    fn test_metric_value_counter() {
        let counter_value = MetricValue::Counter(42);

        if let MetricValue::Counter(value) = counter_value {
            assert_eq!(value, 42);
        } else {
            panic!("Expected Counter value");
        }
    }

    #[test]
    fn test_metric_value_gauge() {
        let gauge_value = MetricValue::Gauge(3.14159);

        if let MetricValue::Gauge(value) = gauge_value {
            assert!((value - 3.14159).abs() < f64::EPSILON);
        } else {
            panic!("Expected Gauge value");
        }
    }

    #[test]
    fn test_metric_value_histogram() {
        let buckets = vec![(1.0, 10), (5.0, 25), (10.0, 50), (f64::INFINITY, 60)];

        let histogram_value = MetricValue::Histogram {
            buckets: buckets.clone(),
            sum: 234.5,
            count: 60,
        };

        if let MetricValue::Histogram {
            buckets: h_buckets,
            sum,
            count,
        } = histogram_value
        {
            assert_eq!(h_buckets, buckets);
            assert!((sum - 234.5).abs() < f64::EPSILON);
            assert_eq!(count, 60);
        } else {
            panic!("Expected Histogram value");
        }
    }

    #[test]
    fn test_metric_value_summary() {
        let quantiles = vec![
            (0.5, 100.0),  // median
            (0.9, 500.0),  // 90th percentile
            (0.99, 900.0), // 99th percentile
        ];

        let summary_value = MetricValue::Summary {
            quantiles: quantiles.clone(),
            sum: 12345.6,
            count: 1000,
        };

        if let MetricValue::Summary {
            quantiles: s_quantiles,
            sum,
            count,
        } = summary_value
        {
            assert_eq!(s_quantiles, quantiles);
            assert!((sum - 12345.6).abs() < f64::EPSILON);
            assert_eq!(count, 1000);
        } else {
            panic!("Expected Summary value");
        }
    }

    #[test]
    fn test_metric_sample_creation() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let sample = MetricSample {
            metadata: MetricMetadata {
                name: "test_counter".to_string(),
                help: "Test counter metric".to_string(),
                metric_type: MetricType::Counter,
                labels: HashMap::new(),
            },
            value: MetricValue::Counter(123),
            timestamp,
            node_id: "test_node".to_string(),
        };

        assert_eq!(sample.metadata.name, "test_counter");
        assert_eq!(sample.node_id, "test_node");
        assert_eq!(sample.timestamp, timestamp);

        if let MetricValue::Counter(value) = sample.value {
            assert_eq!(value, 123);
        } else {
            panic!("Expected Counter value");
        }
    }

    #[tokio::test]
    async fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new("test_node".to_string());

        assert_eq!(collector.node_id, "test_node");
        assert_eq!(collector.collection_interval, Duration::from_secs(10));

        let registry = collector.registry.read().await;
        assert!(registry.is_empty());
    }

    #[tokio::test]
    async fn test_register_metric() {
        let collector = MetricsCollector::new("test_node".to_string());

        let metadata = MetricMetadata {
            name: "test_metric".to_string(),
            help: "Test metric".to_string(),
            metric_type: MetricType::Gauge,
            labels: HashMap::new(),
        };

        collector.register_metric(metadata.clone()).await;

        let registry = collector.registry.read().await;
        assert!(registry.contains_key("test_metric"));
        assert_eq!(registry.get("test_metric").unwrap(), &metadata);
    }

    #[tokio::test]
    async fn test_update_metric_counter() {
        let collector = MetricsCollector::new("test_node".to_string());

        let metadata = MetricMetadata {
            name: "test_counter".to_string(),
            help: "Test counter".to_string(),
            metric_type: MetricType::Counter,
            labels: HashMap::new(),
        };

        collector.register_metric(metadata).await;

        // Update counter
        collector
            .update_metric("test_counter", MetricValue::Counter(10))
            .await;
        collector
            .update_metric("test_counter", MetricValue::Counter(25))
            .await;

        let values = collector.metric_values.read().await;
        if let Some(MetricValue::Counter(value)) = values.get("test_counter") {
            assert_eq!(*value, 25); // Last value wins for counter
        } else {
            panic!("Expected counter value");
        }
    }

    #[tokio::test]
    async fn test_update_metric_gauge() {
        let collector = MetricsCollector::new("test_node".to_string());

        let metadata = MetricMetadata {
            name: "test_gauge".to_string(),
            help: "Test gauge".to_string(),
            metric_type: MetricType::Gauge,
            labels: HashMap::new(),
        };

        collector.register_metric(metadata).await;

        // Update gauge with different values
        collector
            .update_metric("test_gauge", MetricValue::Gauge(1.5))
            .await;
        collector
            .update_metric("test_gauge", MetricValue::Gauge(2.7))
            .await;
        collector
            .update_metric("test_gauge", MetricValue::Gauge(0.8))
            .await;

        let values = collector.metric_values.read().await;
        if let Some(MetricValue::Gauge(value)) = values.get("test_gauge") {
            assert!((value - 0.8).abs() < f64::EPSILON);
        } else {
            panic!("Expected gauge value");
        }
    }

    #[tokio::test]
    async fn test_increment_counter() {
        let collector = MetricsCollector::new("test_node".to_string());

        let metadata = MetricMetadata {
            name: "increment_test".to_string(),
            help: "Increment test counter".to_string(),
            metric_type: MetricType::Counter,
            labels: HashMap::new(),
        };

        collector.register_metric(metadata).await;

        // Increment counter multiple times
        collector.increment_counter("increment_test", 5).await;
        collector.increment_counter("increment_test", 3).await;
        collector.increment_counter("increment_test", 2).await;

        let values = collector.metric_values.read().await;
        if let Some(MetricValue::Counter(value)) = values.get("increment_test") {
            assert_eq!(*value, 10); // 5 + 3 + 2
        } else {
            panic!("Expected counter value");
        }
    }

    #[tokio::test]
    async fn test_set_gauge() {
        let collector = MetricsCollector::new("test_node".to_string());

        let metadata = MetricMetadata {
            name: "gauge_test".to_string(),
            help: "Gauge test metric".to_string(),
            metric_type: MetricType::Gauge,
            labels: HashMap::new(),
        };

        collector.register_metric(metadata).await;

        // Set gauge to different values
        collector.set_gauge("gauge_test", 42.5).await;
        collector.set_gauge("gauge_test", 17.3).await;

        let values = collector.metric_values.read().await;
        if let Some(MetricValue::Gauge(value)) = values.get("gauge_test") {
            assert!((value - 17.3).abs() < f64::EPSILON);
        } else {
            panic!("Expected gauge value");
        }
    }

    #[tokio::test]
    async fn test_observe_histogram() {
        let collector = MetricsCollector::new("test_node".to_string());

        let metadata = MetricMetadata {
            name: "response_time".to_string(),
            help: "Response time histogram".to_string(),
            metric_type: MetricType::Histogram,
            labels: HashMap::new(),
        };

        collector.register_metric(metadata).await;

        // Observe values
        collector.observe_histogram("response_time", 0.5).await;
        collector.observe_histogram("response_time", 1.2).await;
        collector.observe_histogram("response_time", 0.8).await;
        collector.observe_histogram("response_time", 2.1).await;

        let values = collector.metric_values.read().await;
        if let Some(MetricValue::Histogram {
            buckets,
            sum,
            count,
        }) = values.get("response_time")
        {
            assert_eq!(*count, 4);
            assert!((sum - 4.6).abs() < f64::EPSILON); // 0.5 + 1.2 + 0.8 + 2.1
            assert!(!buckets.is_empty());
        } else {
            panic!("Expected histogram value");
        }
    }

    #[tokio::test]
    async fn test_collect_samples() {
        let collector = MetricsCollector::new("test_node".to_string());

        // Register some metrics
        collector
            .register_metric(MetricMetadata {
                name: "counter_metric".to_string(),
                help: "Counter metric".to_string(),
                metric_type: MetricType::Counter,
                labels: HashMap::new(),
            })
            .await;

        collector
            .register_metric(MetricMetadata {
                name: "gauge_metric".to_string(),
                help: "Gauge metric".to_string(),
                metric_type: MetricType::Gauge,
                labels: HashMap::new(),
            })
            .await;

        // Update values
        collector.increment_counter("counter_metric", 10).await;
        collector.set_gauge("gauge_metric", 3.14).await;

        // Collect samples
        let samples = collector.collect_samples().await;

        assert_eq!(samples.len(), 2);

        // Check that all samples have correct node_id
        for sample in &samples {
            assert_eq!(sample.node_id, "test_node");
        }

        // Check counter sample
        let counter_sample = samples
            .iter()
            .find(|s| s.metadata.name == "counter_metric")
            .expect("Counter sample should exist");

        if let MetricValue::Counter(value) = &counter_sample.value {
            assert_eq!(*value, 10);
        } else {
            panic!("Expected counter value");
        }

        // Check gauge sample
        let gauge_sample = samples
            .iter()
            .find(|s| s.metadata.name == "gauge_metric")
            .expect("Gauge sample should exist");

        if let MetricValue::Gauge(value) = &gauge_sample.value {
            assert!((value - 3.14).abs() < f64::EPSILON);
        } else {
            panic!("Expected gauge value");
        }
    }

    #[tokio::test]
    async fn test_export_prometheus_format() {
        let collector = MetricsCollector::new("test_node".to_string());

        // Register and update metrics
        collector
            .register_metric(MetricMetadata {
                name: "http_requests_total".to_string(),
                help: "Total HTTP requests".to_string(),
                metric_type: MetricType::Counter,
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("method".to_string(), "GET".to_string());
                    labels.insert("status".to_string(), "200".to_string());
                    labels
                },
            })
            .await;

        collector.increment_counter("http_requests_total", 42).await;

        let prometheus_output = collector.export_prometheus_format().await;

        assert!(prometheus_output.contains("# HELP http_requests_total Total HTTP requests"));
        assert!(prometheus_output.contains("# TYPE http_requests_total counter"));
        assert!(
            prometheus_output.contains("http_requests_total{method=\"GET\",status=\"200\"} 42")
        );
    }

    #[test]
    fn test_metric_type_equality() {
        assert_eq!(MetricType::Counter, MetricType::Counter);
        assert_eq!(MetricType::Gauge, MetricType::Gauge);
        assert_eq!(MetricType::Histogram, MetricType::Histogram);
        assert_eq!(MetricType::Summary, MetricType::Summary);

        assert_ne!(MetricType::Counter, MetricType::Gauge);
        assert_ne!(MetricType::Histogram, MetricType::Summary);
    }

    #[test]
    fn test_metric_serialization() {
        let metric = MetricSample {
            metadata: MetricMetadata {
                name: "test_metric".to_string(),
                help: "Test metric for serialization".to_string(),
                metric_type: MetricType::Counter,
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("env".to_string(), "test".to_string());
                    labels
                },
            },
            value: MetricValue::Counter(100),
            timestamp: 1234567890,
            node_id: "node1".to_string(),
        };

        // Test JSON serialization
        let json_result = serde_json::to_string(&metric);
        assert!(json_result.is_ok());

        let json_str = json_result.unwrap();
        assert!(json_str.contains("test_metric"));
        assert!(json_str.contains("node1"));
        assert!(json_str.contains("Counter"));

        // Test JSON deserialization
        let deserialized_result: Result<MetricSample, _> = serde_json::from_str(&json_str);
        assert!(deserialized_result.is_ok());

        let deserialized = deserialized_result.unwrap();
        assert_eq!(deserialized.metadata.name, metric.metadata.name);
        assert_eq!(deserialized.node_id, metric.node_id);
        assert_eq!(deserialized.timestamp, metric.timestamp);
    }

    #[test]
    fn test_system_metrics_provider() {
        let provider = SystemMetricsProvider::new("test_node".to_string());

        assert_eq!(provider.name(), "system_metrics");
        assert_eq!(provider.node_id, "test_node");

        let metrics = provider.collect();

        // Should provide CPU, memory, and disk metrics
        assert_eq!(metrics.len(), 3);

        let metric_names: Vec<&str> = metrics.iter().map(|m| m.metadata.name.as_str()).collect();

        assert!(metric_names.contains(&"cpu_usage_percent"));
        assert!(metric_names.contains(&"memory_usage_bytes"));
        assert!(metric_names.contains(&"disk_usage_bytes"));

        // Check that all metrics have correct node_id
        for metric in &metrics {
            assert_eq!(metric.node_id, "test_node");
        }
    }

    #[test]
    fn test_network_metrics_provider() {
        let provider = NetworkMetricsProvider::new("network_node".to_string());

        assert_eq!(provider.name(), "network_metrics");
        assert_eq!(provider.node_id, "network_node");

        let metrics = provider.collect();

        // Should provide network-related metrics
        assert_eq!(metrics.len(), 4);

        let metric_names: Vec<&str> = metrics.iter().map(|m| m.metadata.name.as_str()).collect();

        assert!(metric_names.contains(&"network_bytes_sent_total"));
        assert!(metric_names.contains(&"network_bytes_received_total"));
        assert!(metric_names.contains(&"network_packets_sent_total"));
        assert!(metric_names.contains(&"connection_errors_total"));
    }

    #[tokio::test]
    async fn test_unregistered_metric_update() {
        let collector = MetricsCollector::new("test_node".to_string());

        // Try to update a metric that hasn't been registered
        collector.increment_counter("unregistered_counter", 5).await;
        collector.set_gauge("unregistered_gauge", 2.5).await;

        // These operations should not panic and should not create entries
        let values = collector.metric_values.read().await;
        assert!(!values.contains_key("unregistered_counter"));
        assert!(!values.contains_key("unregistered_gauge"));
    }

    #[tokio::test]
    async fn test_empty_metrics_collection() {
        let collector = MetricsCollector::new("empty_node".to_string());

        // Collect samples with no registered metrics
        let samples = collector.collect_samples().await;
        assert!(samples.is_empty());

        // Export prometheus format with no metrics
        let prometheus_output = collector.export_prometheus_format().await;
        assert!(prometheus_output.is_empty() || prometheus_output.trim().is_empty());
    }

    #[test]
    fn test_metric_value_debug_format() {
        let counter = MetricValue::Counter(42);
        let gauge = MetricValue::Gauge(3.14);

        let counter_debug = format!("{:?}", counter);
        let gauge_debug = format!("{:?}", gauge);

        assert!(counter_debug.contains("Counter"));
        assert!(counter_debug.contains("42"));
        assert!(gauge_debug.contains("Gauge"));
        assert!(gauge_debug.contains("3.14"));
    }
}
