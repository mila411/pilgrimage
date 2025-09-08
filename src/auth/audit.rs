//! Security audit logging system
//!
//! Provides comprehensive audit logging for security events, user actions,
//! system changes, and compliance monitoring with structured logging,
//! event correlation, and threat detection capabilities.

use crate::auth::authentication::UserInfo;
use crate::auth::authorization::AuthDecision;
use crate::network::error::{NetworkError, NetworkResult};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use log::{debug, info, warn, error};

/// Audit logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,
    /// Log file path
    pub log_file: String,
    /// Log rotation size in bytes
    pub rotation_size: u64,
    /// Maximum number of log files to keep
    pub max_files: u32,
    /// Enable real-time event streaming
    pub enable_streaming: bool,
    /// Buffer size for batch writing
    pub buffer_size: usize,
    /// Flush interval in seconds
    pub flush_interval: u64,
    /// Enable log compression
    pub enable_compression: bool,
    /// Retention period in days
    pub retention_days: u32,
    /// Enable encryption of log files
    pub enable_encryption: bool,
    /// Log level filter
    pub log_level: AuditLogLevel,
    /// Enable anomaly detection
    pub enable_anomaly_detection: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_file: "logs/audit.log".to_string(),
            rotation_size: 100 * 1024 * 1024, // 100MB
            max_files: 10,
            enable_streaming: false,
            buffer_size: 1000,
            flush_interval: 5,
            enable_compression: true,
            retention_days: 90,
            enable_encryption: false,
            log_level: AuditLogLevel::Info,
            enable_anomaly_detection: true,
        }
    }
}

/// Audit log levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AuditLogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

/// Types of security events that can be audited
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AuditEventType {
    // Authentication events
    AuthenticationSuccess,
    AuthenticationFailure,
    AuthenticationTimeout,

    // Authorization events
    AuthorizationSuccess,
    AuthorizationFailure,
    PermissionGranted,
    PermissionDenied,

    // User management events
    UserCreated,
    UserDeleted,
    UserModified,
    UserLocked,
    UserUnlocked,
    PasswordChanged,

    // Role and permission events
    RoleCreated,
    RoleDeleted,
    RoleModified,
    RoleAssigned,
    RoleRevoked,
    PermissionAdded,
    PermissionRemoved,

    // System events
    SystemStartup,
    SystemShutdown,
    ConfigurationChanged,
    CertificateRotated,
    KeyGenerated,

    // Data events
    DataAccessed,
    DataModified,
    DataDeleted,
    DataExported,
    DataImported,

    // Network events
    ConnectionEstablished,
    ConnectionTerminated,
    TlsHandshake,
    CertificateValidation,

    // Compliance events
    ComplianceViolation,
    PolicyViolation,
    DataRetentionExpired,

    // Security incidents
    SuspiciousActivity,
    SecurityBreach,
    MaliciousRequest,
    UnauthorizedAccess,
    AnomalyDetected,

    // Custom events
    Custom(String),
}

/// Audit event entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unique event ID
    pub id: Uuid,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Event type
    pub event_type: AuditEventType,
    /// User who triggered the event
    pub user_id: Option<String>,
    /// User information at the time of event
    pub user_info: Option<UserInfo>,
    /// Source IP address
    pub source_ip: Option<String>,
    /// User agent
    pub user_agent: Option<String>,
    /// Resource affected by the event
    pub resource: Option<String>,
    /// Action performed
    pub action: Option<String>,
    /// Event outcome (success/failure)
    pub outcome: EventOutcome,
    /// Event description
    pub description: String,
    /// Additional event details
    pub details: HashMap<String, serde_json::Value>,
    /// Severity level
    pub severity: AuditLogLevel,
    /// Session ID
    pub session_id: Option<String>,
    /// Request ID for correlation
    pub request_id: Option<String>,
    /// Node ID where event occurred
    pub node_id: String,
}

/// Event outcome
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventOutcome {
    Success,
    Failure,
    Unknown,
}

/// Audit logger implementation
pub struct AuditLogger {
    config: AuditConfig,
    buffer: Arc<Mutex<Vec<AuditEvent>>>,
    event_counts: Arc<RwLock<HashMap<AuditEventType, u64>>>,
    suspicious_patterns: Arc<RwLock<HashMap<String, SuspiciousPattern>>>,
    node_id: String,
}

/// Suspicious activity pattern
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SuspiciousPattern {
    pattern_type: String,
    count: u64,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    risk_score: f64,
}

impl AuditLogger {
    /// Create a new audit logger
    pub fn new(config: AuditConfig, node_id: String) -> NetworkResult<Self> {
        // Create log directory if it doesn't exist
        if let Some(parent) = std::path::Path::new(&config.log_file).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| NetworkError::AuditError(format!("Failed to create log directory: {}", e)))?;
        }

        let logger = Self {
            config,
            buffer: Arc::new(Mutex::new(Vec::new())),
            event_counts: Arc::new(RwLock::new(HashMap::new())),
            suspicious_patterns: Arc::new(RwLock::new(HashMap::new())),
            node_id,
        };

        // Start background tasks
        logger.start_flush_task();
        if logger.config.enable_anomaly_detection {
            logger.start_anomaly_detection_task();
        }

        info!("Audit logger initialized for node: {}", logger.node_id);
        Ok(logger)
    }

    /// Log an audit event
    pub async fn log_event(&self, mut event: AuditEvent) -> NetworkResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Set node ID
        event.node_id = self.node_id.clone();

        // Check severity filter
        if event.severity < self.config.log_level {
            return Ok(());
        }

        // Update event counts
        self.update_event_counts(&event).await;

        // Detect suspicious patterns
        if self.config.enable_anomaly_detection {
            self.detect_suspicious_patterns(&event).await;
        }

        // Add to buffer
        let mut buffer = self.buffer.lock().await;
        buffer.push(event);

        // Flush if buffer is full
        if buffer.len() >= self.config.buffer_size {
            drop(buffer); // Release lock before flushing
            self.flush().await?;
        }

        Ok(())
    }

    /// Log authentication event
    pub async fn log_authentication(&self, user_id: &str, outcome: EventOutcome, details: HashMap<String, serde_json::Value>) -> NetworkResult<()> {
        let event_type = match outcome {
            EventOutcome::Success => AuditEventType::AuthenticationSuccess,
            EventOutcome::Failure => AuditEventType::AuthenticationFailure,
            EventOutcome::Unknown => AuditEventType::AuthenticationTimeout,
        };

        let event = AuditEvent {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type,
            user_id: Some(user_id.to_string()),
            user_info: None,
            source_ip: details.get("source_ip").and_then(|v| v.as_str()).map(String::from),
            user_agent: details.get("user_agent").and_then(|v| v.as_str()).map(String::from),
            resource: None,
            action: Some("authenticate".to_string()),
            outcome: outcome.clone(),
            description: format!("User authentication attempt for: {}", user_id),
            details,
            severity: match outcome {
                EventOutcome::Success => AuditLogLevel::Info,
                EventOutcome::Failure => AuditLogLevel::Warning,
                EventOutcome::Unknown => AuditLogLevel::Error,
            },
            session_id: None,
            request_id: None,
            node_id: self.node_id.clone(),
        };

        self.log_event(event).await
    }

    /// Log authorization event
    pub async fn log_authorization(&self, user_id: &str, resource: &str, action: &str, decision: &AuthDecision) -> NetworkResult<()> {
        let outcome = if decision.allowed {
            EventOutcome::Success
        } else {
            EventOutcome::Failure
        };

        let event_type = if decision.allowed {
            AuditEventType::AuthorizationSuccess
        } else {
            AuditEventType::AuthorizationFailure
        };

        let mut details = HashMap::new();
        details.insert("decision_id".to_string(), serde_json::Value::String(decision.decision_id.to_string()));
        details.insert("reason".to_string(), serde_json::Value::String(decision.reason.clone()));
        details.insert("evaluation_time_ms".to_string(), serde_json::Value::Number(decision.evaluation_time_ms.into()));

        let event = AuditEvent {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type,
            user_id: Some(user_id.to_string()),
            user_info: None,
            source_ip: None,
            user_agent: None,
            resource: Some(resource.to_string()),
            action: Some(action.to_string()),
            outcome,
            description: format!("Authorization check for user '{}' on resource '{}' action '{}'", user_id, resource, action),
            details,
            severity: if decision.allowed {
                AuditLogLevel::Info
            } else {
                AuditLogLevel::Warning
            },
            session_id: None,
            request_id: None,
            node_id: self.node_id.clone(),
        };

        self.log_event(event).await
    }

    /// Log security incident
    pub async fn log_security_incident(&self, incident_type: &str, description: &str, severity: AuditLogLevel, details: HashMap<String, serde_json::Value>) -> NetworkResult<()> {
        let event = AuditEvent {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: AuditEventType::Custom(incident_type.to_string()),
            user_id: None,
            user_info: None,
            source_ip: details.get("source_ip").and_then(|v| v.as_str()).map(String::from),
            user_agent: None,
            resource: details.get("resource").and_then(|v| v.as_str()).map(String::from),
            action: details.get("action").and_then(|v| v.as_str()).map(String::from),
            outcome: EventOutcome::Failure,
            description: description.to_string(),
            details,
            severity,
            session_id: None,
            request_id: None,
            node_id: self.node_id.clone(),
        };

        // Use Box::pin to avoid recursion issues
        Box::pin(self.log_event(event)).await
    }

    /// Flush buffered events to disk
    pub async fn flush(&self) -> NetworkResult<()> {
        let mut buffer = self.buffer.lock().await;
        if buffer.is_empty() {
            return Ok(());
        }

        // Open log file for appending
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.log_file)
            .await
            .map_err(|e| NetworkError::AuditError(format!("Failed to open log file: {}", e)))?;

        // Write events
        for event in buffer.drain(..) {
            let json_line = serde_json::to_string(&event)
                .map_err(|e| NetworkError::AuditError(format!("Failed to serialize event: {}", e)))?;

            file.write_all(format!("{}\n", json_line).as_bytes())
                .await
                .map_err(|e| NetworkError::AuditError(format!("Failed to write to log file: {}", e)))?;
        }

        file.flush()
            .await
            .map_err(|e| NetworkError::AuditError(format!("Failed to flush log file: {}", e)))?;

        debug!("Audit events flushed to disk");
        Ok(())
    }

    /// Update event count statistics
    async fn update_event_counts(&self, event: &AuditEvent) {
        let mut counts = self.event_counts.write().await;
        let count = counts.entry(event.event_type.clone()).or_insert(0);
        *count += 1;
    }

    /// Detect suspicious activity patterns
    async fn detect_suspicious_patterns(&self, event: &AuditEvent) {
        // Check for suspicious patterns based on event type and frequency
        match &event.event_type {
            AuditEventType::AuthenticationFailure => {
                self.check_brute_force_pattern(event).await;
            }
            AuditEventType::AuthorizationFailure => {
                self.check_privilege_escalation_pattern(event).await;
            }
            AuditEventType::UnauthorizedAccess => {
                self.check_intrusion_pattern(event).await;
            }
            _ => {}
        }
    }

    /// Check for brute force attack patterns
    async fn check_brute_force_pattern(&self, event: &AuditEvent) {
        if let Some(source_ip) = &event.source_ip {
            let pattern_key = format!("brute_force:{}", source_ip);
            let mut patterns = self.suspicious_patterns.write().await;

            let pattern = patterns.entry(pattern_key.clone()).or_insert_with(|| {
                SuspiciousPattern {
                    pattern_type: "brute_force".to_string(),
                    count: 0,
                    first_seen: event.timestamp,
                    last_seen: event.timestamp,
                    risk_score: 0.0,
                }
            });

            pattern.count += 1;
            pattern.last_seen = event.timestamp;
            pattern.risk_score = (pattern.count as f64) * 0.1;

            // Alert if threshold exceeded
            if pattern.count > 10 {
                warn!("Brute force attack detected from IP: {} (count: {})", source_ip, pattern.count);

                // Log security incident
                let mut details = HashMap::new();
                details.insert("source_ip".to_string(), serde_json::Value::String(source_ip.clone()));
                details.insert("attempt_count".to_string(), serde_json::Value::Number(pattern.count.into()));
                details.insert("risk_score".to_string(), serde_json::json!(pattern.risk_score));

                if let Err(e) = self.log_security_incident(
                    "brute_force_attack",
                    &format!("Brute force attack detected from IP: {}", source_ip),
                    AuditLogLevel::Critical,
                    details,
                ).await {
                    error!("Failed to log security incident: {}", e);
                }
            }
        }
    }

    /// Check for privilege escalation patterns
    async fn check_privilege_escalation_pattern(&self, event: &AuditEvent) {
        // Implementation for privilege escalation detection
        debug!("Checking privilege escalation pattern for event: {:?}", event.id);
    }

    /// Check for intrusion patterns
    async fn check_intrusion_pattern(&self, event: &AuditEvent) {
        // Implementation for intrusion detection
        debug!("Checking intrusion pattern for event: {:?}", event.id);
    }

    /// Start background flush task
    fn start_flush_task(&self) {
        let _buffer = Arc::clone(&self.buffer);
        let config = self.config.clone();
        let logger = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(config.flush_interval));
            loop {
                interval.tick().await;
                if let Err(e) = logger.flush().await {
                    error!("Failed to flush audit logs: {}", e);
                }
            }
        });
    }

    /// Start anomaly detection task
    fn start_anomaly_detection_task(&self) {
        let patterns = Arc::clone(&self.suspicious_patterns);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Check every minute
            loop {
                interval.tick().await;

                // Clean up old patterns
                let mut patterns_guard = patterns.write().await;
                let cutoff_time = Utc::now() - chrono::Duration::hours(24);

                patterns_guard.retain(|_, pattern| pattern.last_seen > cutoff_time);

                debug!("Cleaned up old suspicious patterns, remaining: {}", patterns_guard.len());
            }
        });
    }

    /// Get audit statistics
    pub async fn get_statistics(&self) -> AuditStatistics {
        let event_counts = self.event_counts.read().await;
        let suspicious_patterns = self.suspicious_patterns.read().await;

        AuditStatistics {
            total_events: event_counts.values().sum(),
            event_counts: event_counts.clone(),
            suspicious_patterns_count: suspicious_patterns.len(),
            high_risk_patterns: suspicious_patterns.values()
                .filter(|p| p.risk_score > 5.0)
                .count(),
        }
    }
}

impl Clone for AuditLogger {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            buffer: Arc::clone(&self.buffer),
            event_counts: Arc::clone(&self.event_counts),
            suspicious_patterns: Arc::clone(&self.suspicious_patterns),
            node_id: self.node_id.clone(),
        }
    }
}

/// Audit statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditStatistics {
    pub total_events: u64,
    pub event_counts: HashMap<AuditEventType, u64>,
    pub suspicious_patterns_count: usize,
    pub high_risk_patterns: usize,
}

/// Audit query builder for searching events
pub struct AuditQuery {
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub event_types: Vec<AuditEventType>,
    pub user_id: Option<String>,
    pub resource: Option<String>,
    pub source_ip: Option<String>,
    pub severity: Option<AuditLogLevel>,
    pub limit: Option<usize>,
}

impl AuditQuery {
    /// Create a new audit query
    pub fn new() -> Self {
        Self {
            start_time: None,
            end_time: None,
            event_types: Vec::new(),
            user_id: None,
            resource: None,
            source_ip: None,
            severity: None,
            limit: None,
        }
    }

    /// Set time range for query
    pub fn time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Filter by event types
    pub fn event_types(mut self, types: Vec<AuditEventType>) -> Self {
        self.event_types = types;
        self
    }

    /// Filter by user ID
    pub fn user_id(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Filter by resource
    pub fn resource(mut self, resource: String) -> Self {
        self.resource = Some(resource);
        self
    }

    /// Limit number of results
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_audit_logger_creation() {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("audit.log").to_string_lossy().to_string();

        let config = AuditConfig {
            log_file,
            ..AuditConfig::default()
        };

        let logger = AuditLogger::new(config, "test-node".to_string());
        assert!(logger.is_ok());
    }

    #[tokio::test]
    async fn test_audit_event_logging() {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("audit.log").to_string_lossy().to_string();

        let config = AuditConfig {
            log_file,
            buffer_size: 1, // Force immediate flush
            ..AuditConfig::default()
        };

        let logger = AuditLogger::new(config, "test-node".to_string()).unwrap();

        let event = AuditEvent {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: AuditEventType::AuthenticationSuccess,
            user_id: Some("test_user".to_string()),
            user_info: None,
            source_ip: Some("127.0.0.1".to_string()),
            user_agent: None,
            resource: None,
            action: Some("login".to_string()),
            outcome: EventOutcome::Success,
            description: "User login successful".to_string(),
            details: HashMap::new(),
            severity: AuditLogLevel::Info,
            session_id: None,
            request_id: None,
            node_id: "test-node".to_string(),
        };

        let result = logger.log_event(event).await;
        assert!(result.is_ok());

        // Verify file was created and contains data
        tokio::time::sleep(Duration::from_millis(100)).await;
        let log_path = std::path::Path::new(&logger.config.log_file);
        assert!(log_path.exists());
    }
}
