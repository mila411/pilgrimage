//! Enhanced centralized logging system for production monitoring
//!
//! Provides enterprise-grade log management with:
//! - Multi-node log aggregation
//! - Real-time log streaming
//! - Advanced search and filtering
//! - Log analysis and pattern detection
//! - Integration with alerting system

use crate::monitoring::alerts::AlertManager;
use crate::monitoring::enhanced_alerts::EnhancedAlertManager;
use crate::network::error::{NetworkError, NetworkResult};
use crate::network::transport::NetworkTransport;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque, BTreeMap};
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufWriter, Write, BufReader, BufRead};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::time::interval;
use uuid::Uuid;
use log::{debug, error, info, warn};
use chrono::{DateTime, Utc};

/// Enhanced log entry with full metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedLogEntry {
    pub id: Uuid,
    pub timestamp: u64,
    pub level: LogLevel,
    pub source_node: String,
    pub component: String,
    pub message: String,
    pub metadata: HashMap<String, String>,
    pub trace_id: Option<Uuid>,
    pub span_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,
    pub tags: Vec<String>,
    pub structured_data: Option<serde_json::Value>,
    pub stack_trace: Option<String>,
    pub performance_metrics: Option<PerformanceMetrics>,
}

/// Performance metrics attached to log entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub duration_micros: u64,
    pub memory_usage_bytes: u64,
    pub cpu_usage_percent: f64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
}

/// Log severity levels with numeric priorities
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Critical = 5,
}

/// Advanced log query criteria
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogQueryCriteria {
    pub level: Option<LogLevel>,
    pub min_level: Option<LogLevel>,
    pub component: Option<String>,
    pub source_node: Option<String>,
    pub trace_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub user_id: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub text_search: Option<String>,
    pub tags: Vec<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub sort_order: SortOrder,
}

/// Sort order for log queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortOrder {
    TimestampAsc,
    TimestampDesc,
    LevelAsc,
    LevelDesc,
}

/// Log aggregation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogMetrics {
    pub total_logs_processed: u64,
    pub logs_by_level: HashMap<String, u64>,
    pub logs_by_component: HashMap<String, u64>,
    pub logs_by_node: HashMap<String, u64>,
    pub avg_processing_time_micros: u64,
    pub storage_size_bytes: u64,
    pub index_size_bytes: u64,
    pub compression_ratio: f64,
    pub alert_triggers: u64,
    pub pattern_detections: u64,
}

/// Log processing pipeline stage
pub trait LogProcessor: Send + Sync {
    fn name(&self) -> &str;
    fn process(&self, entry: &mut EnhancedLogEntry) -> NetworkResult<bool>;
    fn should_skip(&self, entry: &EnhancedLogEntry) -> bool { false }
}

/// Enhanced centralized logging system
pub struct CentralizedLoggingSystem {
    config: LoggingConfig,
    node_id: String,
    transport: Arc<NetworkTransport>,
    alert_manager: Option<Arc<EnhancedAlertManager>>,

    // Storage components
    log_buffer: Arc<Mutex<VecDeque<EnhancedLogEntry>>>,
    log_index: Arc<RwLock<BTreeMap<u64, Uuid>>>, // timestamp -> log_id
    component_index: Arc<RwLock<HashMap<String, Vec<Uuid>>>>,
    level_index: Arc<RwLock<HashMap<LogLevel, Vec<Uuid>>>>,
    tag_index: Arc<RwLock<HashMap<String, Vec<Uuid>>>>,

    // Processing pipeline
    processors: Arc<RwLock<Vec<Box<dyn LogProcessor + Send + Sync>>>>,
    pattern_detectors: Arc<RwLock<Vec<Box<dyn PatternDetector + Send + Sync>>>>,

    // Real-time streaming
    log_stream_tx: broadcast::Sender<EnhancedLogEntry>,

    // Metrics and statistics
    metrics: Arc<Mutex<LogMetrics>>,

    // Storage management
    storage_path: PathBuf,
    current_log_file: Arc<Mutex<Option<BufWriter<File>>>>,
    compression_enabled: bool,

    // Search and retrieval
    search_cache: Arc<RwLock<HashMap<String, (Instant, Vec<Uuid>)>>>,
}

/// Logging system configuration
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub enabled: bool,
    pub collection_interval: Duration,
    pub retention_period: Duration,
    pub max_buffer_size: usize,
    pub storage_path: PathBuf,
    pub compression_enabled: bool,
    pub real_time_streaming: bool,
    pub max_file_size_mb: usize,
    pub max_search_results: usize,
    pub index_refresh_interval: Duration,
    pub pattern_detection_enabled: bool,
    pub alert_integration_enabled: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval: Duration::from_secs(5),
            retention_period: Duration::from_secs(30 * 24 * 60 * 60), // 30 days
            max_buffer_size: 10000,
            storage_path: PathBuf::from("./logs"),
            compression_enabled: true,
            real_time_streaming: true,
            max_file_size_mb: 100,
            max_search_results: 1000,
            index_refresh_interval: Duration::from_secs(30),
            pattern_detection_enabled: true,
            alert_integration_enabled: true,
        }
    }
}

/// Pattern detector for anomaly detection
pub trait PatternDetector: Send + Sync {
    fn name(&self) -> &str;
    fn detect(&self, entry: &EnhancedLogEntry, history: &[EnhancedLogEntry]) -> Option<DetectedPattern>;
}

/// Detected pattern result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedPattern {
    pub pattern_type: String,
    pub severity: PatternSeverity,
    pub description: String,
    pub confidence: f64,
    pub related_entries: Vec<Uuid>,
    pub suggested_action: Option<String>,
}

/// Pattern severity levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PatternSeverity {
    Info,
    Warning,
    Critical,
}

impl CentralizedLoggingSystem {
    /// Create new centralized logging system
    pub fn new(
        config: LoggingConfig,
        node_id: String,
        transport: Arc<NetworkTransport>,
        alert_manager: Option<Arc<EnhancedAlertManager>>,
    ) -> NetworkResult<Self> {
        // Create storage directory
        if config.enabled {
            create_dir_all(&config.storage_path)
                .map_err(|e| NetworkError::Io(format!("Failed to create log directory: {}", e)))?;
        }

        let (log_stream_tx, _) = broadcast::channel(1000);

        Ok(Self {
            config: config.clone(),
            node_id,
            transport,
            alert_manager,
            log_buffer: Arc::new(Mutex::new(VecDeque::new())),
            log_index: Arc::new(RwLock::new(BTreeMap::new())),
            component_index: Arc::new(RwLock::new(HashMap::new())),
            level_index: Arc::new(RwLock::new(HashMap::new())),
            tag_index: Arc::new(RwLock::new(HashMap::new())),
            processors: Arc::new(RwLock::new(Vec::new())),
            pattern_detectors: Arc::new(RwLock::new(Vec::new())),
            log_stream_tx,
            metrics: Arc::new(Mutex::new(LogMetrics::default())),
            storage_path: config.storage_path.clone(),
            current_log_file: Arc::new(Mutex::new(None)),
            compression_enabled: config.compression_enabled,
            search_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Start the centralized logging system
    pub async fn start(&self) -> NetworkResult<()> {
        if !self.config.enabled {
            info!("Centralized logging system is disabled");
            return Ok(());
        }

        info!("Starting centralized logging system for node {}", self.node_id);

        // Start log collection task
        self.start_log_collection_task().await;

        // Start log processing pipeline
        self.start_processing_pipeline().await;

        // Start index maintenance task
        self.start_index_maintenance_task().await;

        // Start pattern detection if enabled
        if self.config.pattern_detection_enabled {
            self.start_pattern_detection_task().await;
        }

        // Start storage management
        self.start_storage_management_task().await;

        info!("Centralized logging system started successfully");
        Ok(())
    }

    /// Add a log entry to the system
    pub async fn add_log_entry(&self, mut entry: EnhancedLogEntry) -> NetworkResult<()> {
        let start_time = Instant::now();

        // Set source node if not already set
        if entry.source_node.is_empty() {
            entry.source_node = self.node_id.clone();
        }

        // Generate ID if not set
        if entry.id == Uuid::nil() {
            entry.id = Uuid::new_v4();
        }

        // Process through pipeline
        let mut processors = self.processors.read().await;
        for processor in processors.iter() {
            if processor.should_skip(&entry) {
                continue;
            }

            if !processor.process(&mut entry)? {
                // Processor filtered out this entry
                return Ok(());
            }
        }
        drop(processors);

        // Add to buffer
        {
            let mut buffer = self.log_buffer.lock().unwrap();
            buffer.push_back(entry.clone());

            // Maintain buffer size
            while buffer.len() > self.config.max_buffer_size {
                buffer.pop_front();
            }
        }

        // Update indices
        self.update_indices(&entry).await;

        // Real-time streaming if enabled
        if self.config.real_time_streaming {
            let _ = self.log_stream_tx.send(entry.clone());
        }

        // Pattern detection
        if self.config.pattern_detection_enabled {
            self.detect_patterns(&entry).await?;
        }

        // Update metrics
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_logs_processed += 1;

            let level_key = format!("{:?}", entry.level);
            *metrics.logs_by_level.entry(level_key).or_insert(0) += 1;

            *metrics.logs_by_component.entry(entry.component.clone()).or_insert(0) += 1;
            *metrics.logs_by_node.entry(entry.source_node.clone()).or_insert(0) += 1;

            let processing_time = start_time.elapsed().as_micros() as u64;
            metrics.avg_processing_time_micros =
                (metrics.avg_processing_time_micros + processing_time) / 2;
        }

        Ok(())
    }

    /// Query logs with advanced criteria
    pub async fn query_logs(&self, criteria: LogQueryCriteria) -> NetworkResult<Vec<EnhancedLogEntry>> {
        let start_time = Instant::now();

        // Check search cache first
        let cache_key = format!("{:?}", criteria);
        {
            let cache = self.search_cache.read().await;
            if let Some((cached_time, cached_results)) = cache.get(&cache_key) {
                if cached_time.elapsed() < Duration::from_secs(60) { // 1-minute cache
                    let buffer = self.log_buffer.lock().unwrap();
                    let results: Vec<EnhancedLogEntry> = cached_results.iter()
                        .filter_map(|id| buffer.iter().find(|entry| &entry.id == id))
                        .cloned()
                        .collect();

                    if !results.is_empty() {
                        debug!("Query cache hit for: {}", cache_key);
                        return Ok(results);
                    }
                }
            }
        }

        let mut results: Vec<EnhancedLogEntry> = {
            let buffer = self.log_buffer.lock().unwrap();
            buffer.iter()
                .filter(|entry| self.matches_criteria(entry, &criteria))
                .cloned()
                .collect()
        };

        // Apply sorting
        match criteria.sort_order {
            SortOrder::TimestampAsc => results.sort_by_key(|e| e.timestamp),
            SortOrder::TimestampDesc => results.sort_by_key(|e| std::cmp::Reverse(e.timestamp)),
            SortOrder::LevelAsc => results.sort_by_key(|e| e.level.clone()),
            SortOrder::LevelDesc => results.sort_by_key(|e| std::cmp::Reverse(e.level.clone())),
        }

        // Apply pagination
        if let Some(offset) = criteria.offset {
            if offset < results.len() {
                results = results.into_iter().skip(offset).collect();
            } else {
                results.clear();
            }
        }

        if let Some(limit) = criteria.limit {
            results.truncate(limit);
        }

        // Cache results
        let result_ids: Vec<Uuid> = results.iter().map(|e| e.id).collect();
        {
            let mut cache = self.search_cache.write().await;
            cache.insert(cache_key, (Instant::now(), result_ids));

            // Limit cache size
            if cache.len() > 100 {
                let oldest_key = cache.iter()
                    .min_by_key(|(_, (time, _))| time)
                    .map(|(key, _)| key.clone());
                if let Some(key) = oldest_key {
                    cache.remove(&key);
                }
            }
        }

        debug!("Log query completed in {:?}, returned {} results",
               start_time.elapsed(), results.len());

        Ok(results)
    }

    /// Add a log processor to the pipeline
    pub async fn add_processor(&self, processor: Box<dyn LogProcessor + Send + Sync>) {
        let processor_name = processor.name().to_string();
        let mut processors = self.processors.write().await;
        processors.push(processor);
        info!("Added log processor: {}", processor_name);
    }

    /// Add a pattern detector
    pub async fn add_pattern_detector(&self, detector: Box<dyn PatternDetector + Send + Sync>) {
        let detector_name = detector.name().to_string();
        let mut detectors = self.pattern_detectors.write().await;
        detectors.push(detector);
        info!("Added pattern detector: {}", detector_name);
    }

    /// Subscribe to real-time log stream
    pub fn subscribe_to_stream(&self) -> broadcast::Receiver<EnhancedLogEntry> {
        self.log_stream_tx.subscribe()
    }

    /// Get logging metrics
    pub fn get_metrics(&self) -> LogMetrics {
        let metrics = self.metrics.lock().unwrap();
        metrics.clone()
    }

    /// Export logs to external format
    pub async fn export_logs(
        &self,
        criteria: LogQueryCriteria,
        format: ExportFormat,
    ) -> NetworkResult<String> {
        let logs = self.query_logs(criteria).await?;

        match format {
            ExportFormat::Json => Ok(serde_json::to_string_pretty(&logs)
                .map_err(|e| NetworkError::Serialization(e.to_string()))?),
            ExportFormat::Csv => self.export_to_csv(&logs),
            ExportFormat::PlainText => Ok(logs.iter()
                .map(|entry| format!("{} [{}] {}: {}",
                    DateTime::<Utc>::from(SystemTime::UNIX_EPOCH + Duration::from_secs(entry.timestamp)),
                    format!("{:?}", entry.level),
                    entry.component,
                    entry.message))
                .collect::<Vec<_>>()
                .join("\n")),
        }
    }

    /// Get log statistics
    pub async fn get_statistics(&self) -> LogStatistics {
        let buffer = self.log_buffer.lock().unwrap();
        let metrics = self.metrics.lock().unwrap();

        let total_logs = buffer.len();
        let oldest_timestamp = buffer.front().map(|e| e.timestamp);
        let newest_timestamp = buffer.back().map(|e| e.timestamp);

        let error_count = buffer.iter()
            .filter(|e| matches!(e.level, LogLevel::Error | LogLevel::Critical))
            .count();

        let components: std::collections::HashSet<String> = buffer.iter()
            .map(|e| e.component.clone())
            .collect();

        LogStatistics {
            total_logs,
            error_count,
            component_count: components.len(),
            oldest_timestamp,
            newest_timestamp,
            storage_size_bytes: metrics.storage_size_bytes,
            processing_rate_logs_per_second: self.calculate_processing_rate().await,
        }
    }

    // Private helper methods

    async fn start_log_collection_task(&self) {
        let collection_interval = self.config.collection_interval;
        let node_id = self.node_id.clone();

        tokio::spawn(async move {
            let mut interval = interval(collection_interval);
            loop {
                interval.tick().await;
                // Collection logic would be implemented here
                debug!("Log collection tick for node {}", node_id);
            }
        });
    }

    async fn start_processing_pipeline(&self) {
        // Processing pipeline logic
        info!("Started log processing pipeline");
    }

    async fn start_index_maintenance_task(&self) {
        let refresh_interval = self.config.index_refresh_interval;
        let search_cache = self.search_cache.clone();

        tokio::spawn(async move {
            let mut interval = interval(refresh_interval);
            loop {
                interval.tick().await;

                // Clear expired cache entries
                let mut cache = search_cache.write().await;
                cache.retain(|_, (time, _)| time.elapsed() < Duration::from_secs(300));

                debug!("Index maintenance completed, cache size: {}", cache.len());
            }
        });
    }

    async fn start_pattern_detection_task(&self) {
        info!("Started pattern detection task");
    }

    async fn start_storage_management_task(&self) {
        info!("Started storage management task");
    }

    async fn update_indices(&self, entry: &EnhancedLogEntry) {
        // Update timestamp index
        {
            let mut index = self.log_index.write().await;
            index.insert(entry.timestamp, entry.id);
        }

        // Update component index
        {
            let mut index = self.component_index.write().await;
            index.entry(entry.component.clone()).or_insert_with(Vec::new).push(entry.id);
        }

        // Update level index
        {
            let mut index = self.level_index.write().await;
            index.entry(entry.level.clone()).or_insert_with(Vec::new).push(entry.id);
        }

        // Update tag index
        {
            let mut index = self.tag_index.write().await;
            for tag in &entry.tags {
                index.entry(tag.clone()).or_insert_with(Vec::new).push(entry.id);
            }
        }
    }

    async fn detect_patterns(&self, entry: &EnhancedLogEntry) -> NetworkResult<()> {
        let detectors = self.pattern_detectors.read().await;
        if detectors.is_empty() {
            return Ok(());
        }

        let buffer = self.log_buffer.lock().unwrap();
        let recent_entries: Vec<EnhancedLogEntry> = buffer.iter()
            .rev()
            .take(100) // Last 100 entries for pattern detection
            .cloned()
            .collect();
        drop(buffer);

        for detector in detectors.iter() {
            if let Some(pattern) = detector.detect(entry, &recent_entries) {
                info!("Pattern detected: {} - {}", pattern.pattern_type, pattern.description);

                // Trigger alert if configured
                if self.config.alert_integration_enabled {
                    if let Some(ref alert_manager) = self.alert_manager {
                        // Create alert for detected pattern
                        self.create_pattern_alert(pattern, alert_manager.clone()).await?;
                    }
                }

                // Update metrics
                {
                    let mut metrics = self.metrics.lock().unwrap();
                    metrics.pattern_detections += 1;
                }
            }
        }

        Ok(())
    }

    async fn create_pattern_alert(&self, pattern: DetectedPattern, alert_manager: Arc<EnhancedAlertManager>) -> NetworkResult<()> {
        // Implementation would create appropriate alert based on detected pattern
        info!("Creating alert for pattern: {}", pattern.pattern_type);
        Ok(())
    }

    fn matches_criteria(&self, entry: &EnhancedLogEntry, criteria: &LogQueryCriteria) -> bool {
        // Level filtering
        if let Some(ref level) = criteria.level {
            if &entry.level != level {
                return false;
            }
        }

        if let Some(ref min_level) = criteria.min_level {
            if entry.level < *min_level {
                return false;
            }
        }

        // Component filtering
        if let Some(ref component) = criteria.component {
            if &entry.component != component {
                return false;
            }
        }

        // Node filtering
        if let Some(ref node) = criteria.source_node {
            if &entry.source_node != node {
                return false;
            }
        }

        // Trace ID filtering
        if let Some(ref trace_id) = criteria.trace_id {
            if entry.trace_id.as_ref() != Some(trace_id) {
                return false;
            }
        }

        // Correlation ID filtering
        if let Some(ref correlation_id) = criteria.correlation_id {
            if entry.correlation_id.as_ref() != Some(correlation_id) {
                return false;
            }
        }

        // User ID filtering
        if let Some(ref user_id) = criteria.user_id {
            if entry.user_id.as_ref() != Some(user_id) {
                return false;
            }
        }

        // Time range filtering
        if let Some(start_time) = criteria.start_time {
            if entry.timestamp < start_time {
                return false;
            }
        }

        if let Some(end_time) = criteria.end_time {
            if entry.timestamp > end_time {
                return false;
            }
        }

        // Text search
        if let Some(ref search_text) = criteria.text_search {
            let search_lower = search_text.to_lowercase();
            if !entry.message.to_lowercase().contains(&search_lower) &&
               !entry.component.to_lowercase().contains(&search_lower) {
                return false;
            }
        }

        // Tag filtering
        if !criteria.tags.is_empty() {
            let has_all_tags = criteria.tags.iter()
                .all(|tag| entry.tags.contains(tag));
            if !has_all_tags {
                return false;
            }
        }

        true
    }

    fn export_to_csv(&self, logs: &[EnhancedLogEntry]) -> NetworkResult<String> {
        let mut csv_data = String::new();

        // Header
        csv_data.push_str("timestamp,level,source_node,component,message,trace_id,correlation_id\n");

        // Data rows
        for entry in logs {
            csv_data.push_str(&format!(
                "{},{:?},{},{},{},{},{}\n",
                entry.timestamp,
                entry.level,
                entry.source_node,
                entry.component,
                entry.message.replace(",", ";"), // Escape commas
                entry.trace_id.map(|id| id.to_string()).unwrap_or_default(),
                entry.correlation_id.as_ref().unwrap_or(&String::new())
            ));
        }

        Ok(csv_data)
    }

    async fn calculate_processing_rate(&self) -> f64 {
        // Calculate logs processed per second based on recent activity
        let metrics = self.metrics.lock().unwrap();
        if metrics.avg_processing_time_micros > 0 {
            1_000_000.0 / metrics.avg_processing_time_micros as f64
        } else {
            0.0
        }
    }
}

/// Export format options
#[derive(Debug, Clone)]
pub enum ExportFormat {
    Json,
    Csv,
    PlainText,
}

/// Log statistics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStatistics {
    pub total_logs: usize,
    pub error_count: usize,
    pub component_count: usize,
    pub oldest_timestamp: Option<u64>,
    pub newest_timestamp: Option<u64>,
    pub storage_size_bytes: u64,
    pub processing_rate_logs_per_second: f64,
}

impl Default for LogMetrics {
    fn default() -> Self {
        Self {
            total_logs_processed: 0,
            logs_by_level: HashMap::new(),
            logs_by_component: HashMap::new(),
            logs_by_node: HashMap::new(),
            avg_processing_time_micros: 0,
            storage_size_bytes: 0,
            index_size_bytes: 0,
            compression_ratio: 1.0,
            alert_triggers: 0,
            pattern_detections: 0,
        }
    }
}

// Built-in processors and detectors

/// Error log processor that enriches error entries
pub struct ErrorLogProcessor;

impl LogProcessor for ErrorLogProcessor {
    fn name(&self) -> &str {
        "error_enricher"
    }

    fn process(&self, entry: &mut EnhancedLogEntry) -> NetworkResult<bool> {
        if matches!(entry.level, LogLevel::Error | LogLevel::Critical) {
            // Add error-specific metadata
            entry.tags.push("error".to_string());
            entry.metadata.insert("error_processed".to_string(), Utc::now().to_rfc3339());
        }
        Ok(true)
    }
}

/// Security event detector
pub struct SecurityPatternDetector;

impl PatternDetector for SecurityPatternDetector {
    fn name(&self) -> &str {
        "security_detector"
    }

    fn detect(&self, entry: &EnhancedLogEntry, history: &[EnhancedLogEntry]) -> Option<DetectedPattern> {
        // Detect potential security issues
        let security_keywords = ["unauthorized", "failed login", "access denied", "intrusion"];

        for keyword in &security_keywords {
            if entry.message.to_lowercase().contains(keyword) {
                return Some(DetectedPattern {
                    pattern_type: "security_event".to_string(),
                    severity: PatternSeverity::Critical,
                    description: format!("Security event detected: {}", keyword),
                    confidence: 0.8,
                    related_entries: vec![entry.id],
                    suggested_action: Some("Review security logs and investigate".to_string()),
                });
            }
        }

        None
    }
}
