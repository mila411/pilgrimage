//! Centralized log aggregation and management system
//!
//! Provides unified log collection, processing, and storage across
//! all nodes in the distributed messaging system.

use crate::network::error::{NetworkError, NetworkResult};
use crate::network::transport::NetworkTransport;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use uuid::Uuid;
use log::{debug, error, info, warn};

/// Log entry structure for centralized logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: Uuid,
    pub timestamp: u64,
    pub level: LogLevel,
    pub source_node: String,
    pub component: String,
    pub message: String,
    pub metadata: HashMap<String, String>,
    pub trace_id: Option<Uuid>,
    pub span_id: Option<Uuid>,
}

/// Log severity levels
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Critical,
}

/// Log aggregation configuration
#[derive(Debug, Clone)]
pub struct LogAggregationConfig {
    pub enabled: bool,
    pub collection_interval: Duration,
    pub retention_period: Duration,
    pub max_log_entries: usize,
    pub storage_path: PathBuf,
    pub compression_enabled: bool,
    pub forward_to_central: bool,
    pub central_log_server: Option<String>,
}

impl Default for LogAggregationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval: Duration::from_secs(30),
            retention_period: Duration::from_secs(7 * 24 * 3600), // 7 days
            max_log_entries: 100000,
            storage_path: PathBuf::from("logs/aggregated"),
            compression_enabled: true,
            forward_to_central: false,
            central_log_server: None,
        }
    }
}

/// Centralized log aggregator
pub struct LogAggregator {
    config: LogAggregationConfig,
    node_id: String,
    transport: Arc<NetworkTransport>,
    log_buffer: Arc<Mutex<VecDeque<LogEntry>>>,
    log_storage: Arc<RwLock<LogStorage>>,
    log_processors: Arc<RwLock<Vec<Box<dyn LogProcessor + Send + Sync>>>>,
    metrics: Arc<Mutex<LogMetrics>>,
}

/// Log storage backend
#[derive(Debug)]
struct LogStorage {
    file_writer: Option<BufWriter<File>>,
    current_file_path: PathBuf,
    rotation_size: u64,
    current_size: u64,
}

/// Log processing interface
pub trait LogProcessor: Send + Sync {
    fn process(&self, entry: &LogEntry) -> Result<(), Box<dyn std::error::Error>>;
    fn name(&self) -> &str;
}

/// Log metrics for monitoring
#[derive(Debug, Default)]
pub struct LogMetrics {
    total_logs_processed: u64,
    logs_by_level: HashMap<String, u64>,
    logs_by_component: HashMap<String, u64>,
    processing_errors: u64,
    last_processed_timestamp: u64,
}

impl LogAggregator {
    /// Create a new log aggregator instance
    pub fn new(
        config: LogAggregationConfig,
        node_id: String,
        transport: Arc<NetworkTransport>,
    ) -> NetworkResult<Self> {
        std::fs::create_dir_all(&config.storage_path)
            .map_err(|e| NetworkError::InternalError(format!("Failed to create log directory: {}", e)))?;

        let log_storage = LogStorage::new(&config.storage_path)?;

        Ok(Self {
            config,
            node_id,
            transport,
            log_buffer: Arc::new(Mutex::new(VecDeque::new())),
            log_storage: Arc::new(RwLock::new(log_storage)),
            log_processors: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(Mutex::new(LogMetrics::default())),
        })
    }

    /// Start the log aggregation service
    pub async fn start(&self) -> NetworkResult<()> {
        info!("Starting log aggregation service for node {}", self.node_id);

        // Start log collection task
        self.start_collection_task().await;

        // Start log forwarding task if enabled
        if self.config.forward_to_central {
            self.start_forwarding_task().await;
        }

        // Start cleanup task
        self.start_cleanup_task().await;

        Ok(())
    }

    /// Add a log entry to the aggregation system
    pub async fn add_log_entry(&self, entry: LogEntry) -> NetworkResult<()> {
        let mut buffer = self.log_buffer.lock().unwrap();

        // Check buffer limits
        if buffer.len() >= self.config.max_log_entries {
            buffer.pop_front(); // Remove oldest entry
        }

        buffer.push_back(entry);
        Ok(())
    }

    /// Create a log entry from structured data
    pub fn create_log_entry(
        &self,
        level: LogLevel,
        component: &str,
        message: &str,
        metadata: Option<HashMap<String, String>>,
        trace_id: Option<Uuid>,
        span_id: Option<Uuid>,
    ) -> LogEntry {
        LogEntry {
            id: Uuid::new_v4(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            level,
            source_node: self.node_id.clone(),
            component: component.to_string(),
            message: message.to_string(),
            metadata: metadata.unwrap_or_default(),
            trace_id,
            span_id,
        }
    }

    /// Add a log processor
    pub async fn add_processor(&self, processor: Box<dyn LogProcessor + Send + Sync>) {
        let processor_name = processor.name().to_string();
        let mut processors = self.log_processors.write().await;
        processors.push(processor);
        info!("Added log processor: {}", processor_name);
    }

    /// Query logs by criteria
    pub async fn query_logs(
        &self,
        criteria: LogQueryCriteria,
    ) -> NetworkResult<Vec<LogEntry>> {
        // Implementation would query stored logs based on criteria
        // For now, return recent logs from buffer
        let buffer = self.log_buffer.lock().unwrap();
        let logs: Vec<LogEntry> = buffer
            .iter()
            .filter(|entry| self.matches_criteria(entry, &criteria))
            .cloned()
            .collect();

        Ok(logs)
    }

    /// Get log aggregation metrics
    pub fn get_metrics(&self) -> LogMetrics {
        let metrics = self.metrics.lock().unwrap();
        LogMetrics {
            total_logs_processed: metrics.total_logs_processed,
            logs_by_level: metrics.logs_by_level.clone(),
            logs_by_component: metrics.logs_by_component.clone(),
            processing_errors: metrics.processing_errors,
            last_processed_timestamp: metrics.last_processed_timestamp,
        }
    }

    /// Start log collection task
    async fn start_collection_task(&self) {
        let buffer = self.log_buffer.clone();
        let storage = self.log_storage.clone();
        let processors = self.log_processors.clone();
        let metrics = self.metrics.clone();
        let interval_duration = self.config.collection_interval;

        tokio::task::spawn(async move {
            let mut timer = interval(interval_duration);
            loop {
                timer.tick().await;

                if let Err(e) = Self::process_log_batch(
                    &buffer,
                    &storage,
                    &processors,
                    &metrics,
                ).await {
                    error!("Log processing failed: {}", e);
                }
            }
        });
    }

    /// Process a batch of logs
    async fn process_log_batch(
        buffer: &Arc<Mutex<VecDeque<LogEntry>>>,
        storage: &Arc<RwLock<LogStorage>>,
        processors: &Arc<RwLock<Vec<Box<dyn LogProcessor + Send + Sync>>>>,
        metrics: &Arc<Mutex<LogMetrics>>,
    ) -> NetworkResult<()> {
        // Extract logs from buffer
        let logs_to_process: Vec<LogEntry> = {
            let mut buffer = buffer.lock().unwrap();
            let mut logs = Vec::new();
            while let Some(entry) = buffer.pop_front() {
                logs.push(entry);
            }
            logs
        };

        if logs_to_process.is_empty() {
            return Ok(());
        }

        // Process each log entry
        let processors = processors.read().await;
        for entry in &logs_to_process {
            // Store to persistent storage
            {
                let mut storage = storage.write().await;
                if let Err(e) = storage.store_log_entry(entry).await {
                    error!("Failed to store log entry: {}", e);
                }
            }

            // Run through processors
            for processor in processors.iter() {
                if let Err(e) = processor.process(entry) {
                    warn!("Log processor '{}' failed: {}", processor.name(), e);
                }
            }

            // Update metrics
            {
                let mut metrics = metrics.lock().unwrap();
                metrics.total_logs_processed += 1;
                *metrics.logs_by_level
                    .entry(format!("{:?}", entry.level))
                    .or_insert(0) += 1;
                *metrics.logs_by_component
                    .entry(entry.component.clone())
                    .or_insert(0) += 1;
                metrics.last_processed_timestamp = entry.timestamp;
            }
        }

        info!("Processed {} log entries", logs_to_process.len());
        Ok(())
    }

    /// Start log forwarding task
    async fn start_forwarding_task(&self) {
        if let Some(central_server) = &self.config.central_log_server {
            let server = central_server.clone();
            let transport = self.transport.clone();
            let buffer = self.log_buffer.clone();

            tokio::task::spawn(async move {
                let mut timer = interval(Duration::from_secs(60));
                loop {
                    timer.tick().await;

                    // Forward logs to central server
                    if let Err(e) = Self::forward_logs_to_central(&server, &transport, &buffer).await {
                        warn!("Failed to forward logs to central server: {}", e);
                    }
                }
            });
        }
    }

    /// Forward logs to central server
    async fn forward_logs_to_central(
        _server: &str,
        _transport: &Arc<NetworkTransport>,
        _buffer: &Arc<Mutex<VecDeque<LogEntry>>>,
    ) -> NetworkResult<()> {
        // Implementation would forward logs to central logging server
        debug!("Forwarding logs to central server");
        Ok(())
    }

    /// Start cleanup task
    async fn start_cleanup_task(&self) {
        let storage_path = self.config.storage_path.clone();
        let retention_period = self.config.retention_period;

        tokio::task::spawn(async move {
            let mut timer = interval(Duration::from_secs(3600)); // Run every hour
            loop {
                timer.tick().await;

                if let Err(e) = Self::cleanup_old_logs(&storage_path, retention_period).await {
                    error!("Log cleanup failed: {}", e);
                }
            }
        });
    }

    /// Clean up old log files
    async fn cleanup_old_logs(
        storage_path: &Path,
        retention_period: Duration,
    ) -> NetworkResult<()> {
        let cutoff_time = SystemTime::now() - retention_period;

        if let Ok(entries) = std::fs::read_dir(storage_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(created) = metadata.created() {
                            if created < cutoff_time {
                                if let Err(e) = std::fs::remove_file(entry.path()) {
                                    warn!("Failed to remove old log file: {}", e);
                                } else {
                                    debug!("Removed old log file: {:?}", entry.path());
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if log entry matches query criteria
    fn matches_criteria(&self, entry: &LogEntry, criteria: &LogQueryCriteria) -> bool {
        if let Some(ref level) = criteria.level {
            if entry.level != *level {
                return false;
            }
        }

        if let Some(ref component) = criteria.component {
            if entry.component != *component {
                return false;
            }
        }

        if let Some(ref trace_id) = criteria.trace_id {
            if entry.trace_id != Some(*trace_id) {
                return false;
            }
        }

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

        true
    }
}

impl LogStorage {
    fn new(storage_path: &Path) -> NetworkResult<Self> {
        let current_file_path = storage_path.join("current.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_file_path)
            .map_err(|e| NetworkError::InternalError(format!("Failed to open log file: {}", e)))?;

        Ok(Self {
            file_writer: Some(BufWriter::new(file)),
            current_file_path,
            rotation_size: 100 * 1024 * 1024, // 100MB
            current_size: 0,
        })
    }

    async fn store_log_entry(&mut self, entry: &LogEntry) -> NetworkResult<()> {
        let serialized = serde_json::to_string(entry)
            .map_err(|e| NetworkError::InternalError(format!("Failed to serialize log entry: {}", e)))?;

        if let Some(ref mut writer) = self.file_writer {
            writeln!(writer, "{}", serialized)
                .map_err(|e| NetworkError::InternalError(format!("Failed to write log entry: {}", e)))?;

            writer.flush()
                .map_err(|e| NetworkError::InternalError(format!("Failed to flush log file: {}", e)))?;

            self.current_size += serialized.len() as u64;

            // Check if rotation is needed
            if self.current_size > self.rotation_size {
                self.rotate_log_file().await?;
            }
        }

        Ok(())
    }

    async fn rotate_log_file(&mut self) -> NetworkResult<()> {
        // Close current file
        self.file_writer = None;

        // Rename current file with timestamp
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rotated_path = self.current_file_path
            .with_file_name(format!("rotated_{}.log", timestamp));

        std::fs::rename(&self.current_file_path, &rotated_path)
            .map_err(|e| NetworkError::InternalError(format!("Failed to rotate log file: {}", e)))?;

        // Create new current file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.current_file_path)
            .map_err(|e| NetworkError::InternalError(format!("Failed to create new log file: {}", e)))?;

        self.file_writer = Some(BufWriter::new(file));
        self.current_size = 0;

        info!("Log file rotated to: {:?}", rotated_path);
        Ok(())
    }
}

/// Log query criteria
#[derive(Debug, Clone)]
pub struct LogQueryCriteria {
    pub level: Option<LogLevel>,
    pub component: Option<String>,
    pub trace_id: Option<Uuid>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<usize>,
}

/// JSON log processor
pub struct JsonLogProcessor {
    name: String,
}

impl JsonLogProcessor {
    pub fn new() -> Self {
        Self {
            name: "json_processor".to_string(),
        }
    }
}

impl LogProcessor for JsonLogProcessor {
    fn process(&self, entry: &LogEntry) -> Result<(), Box<dyn std::error::Error>> {
        // Process log entry as JSON (could send to external system)
        debug!("Processing log entry: {}", serde_json::to_string(entry)?);
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Alert log processor
pub struct AlertLogProcessor {
    name: String,
}

impl AlertLogProcessor {
    pub fn new() -> Self {
        Self {
            name: "alert_processor".to_string(),
        }
    }
}

impl LogProcessor for AlertLogProcessor {
    fn process(&self, entry: &LogEntry) -> Result<(), Box<dyn std::error::Error>> {
        // Check for alert conditions
        if matches!(entry.level, LogLevel::Error | LogLevel::Critical) {
            warn!("Alert condition detected: {} - {}", entry.component, entry.message);
            // Could trigger alert system here
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}
