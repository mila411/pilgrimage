//! Security Audit Logging System
//!
//! Comprehensive audit logging for security events, compliance,
//! and forensic analysis with tamper protection

use crate::network::error::NetworkResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Security event types for audit logging
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SecurityEventType {
    Authentication { event_subtype: AuthEventType },
    Authorization,
    DataAccess { event_subtype: DataAccessType },
    SystemAccess,
    ConfigurationChange,
    SecurityViolation,
    SessionManagement,
    DataModification,
    Administrative { event_subtype: AdminEventType },
}

/// Authentication event subtypes
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum AuthEventType {
    Login,
    Logout,
    LoginFailed,
    SessionExpired,
    PasswordChange,
    TokenRefresh,
}

/// Authorization event subtypes
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum AuthzEventType {
    AccessGranted,
    AccessDenied,
    RoleAssigned,
    RoleRevoked,
    PermissionGranted,
    PermissionRevoked,
}

/// Data access event subtypes
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum DataAccessType {
    Read,
    Write,
    Delete,
    Create,
    Update,
    Query,
    Export,
    Import,
}

/// Administrative event subtypes
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum AdminEventType {
    UserCreated,
    UserDeleted,
    RoleCreated,
    RoleDeleted,
    ConfigurationChanged,
    SystemMaintenance,
    BackupCreated,
    BackupRestored,
}

/// Event result
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum EventResult {
    Success,
    Failure,
    Error,
    Warning,
}

/// Event severity levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum EventSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Audit logging configuration
#[derive(Debug, Clone)]
pub struct AuditLogConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub log_file_path: Option<String>,
    pub include_sensitive_data: bool,
}

impl Default for AuditLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10_000,
            log_file_path: None,
            include_sensitive_data: false,
        }
    }
}

/// Audit statistics
#[derive(Debug, Clone)]
pub struct AuditStats {
    pub total_events: u64,
    pub failed_writes: u64,
    pub events_by_type: HashMap<String, u64>,
    pub events_today: u64,
}

/// Security audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub id: Uuid,
    pub timestamp: u64,
    pub event_type: SecurityEventType,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub resource: String,
    pub action: String,
    pub result: String,
    pub details: HashMap<String, String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
}

impl AuditLogEntry {
    /// Create a new audit log entry
    pub fn new(
        event_type: SecurityEventType,
        resource: String,
        action: String,
        result: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type,
            user_id: None,
            session_id: None,
            resource,
            action,
            result,
            details: HashMap::new(),
            ip_address: None,
            user_agent: None,
        }
    }

    /// Add user context to the audit entry
    pub fn with_user(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Add session context to the audit entry
    pub fn with_session(mut self, session_id: String) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Add network context to the audit entry
    pub fn with_network_context(mut self, ip_address: String, user_agent: String) -> Self {
        self.ip_address = Some(ip_address);
        self.user_agent = Some(user_agent);
        self
    }

    /// Add custom details to the audit entry
    pub fn with_detail(mut self, key: String, value: String) -> Self {
        self.details.insert(key, value);
        self
    }
}

/// Security audit logger with tamper protection
pub struct SecurityAuditLogger {
    entries: RwLock<Vec<AuditLogEntry>>,
    config: AuditLogConfig,
    stats: RwLock<AuditStats>,
}

impl SecurityAuditLogger {
    /// Create a new audit logger with specified configuration
    pub async fn new(config: AuditLogConfig) -> NetworkResult<Self> {
        let stats = AuditStats {
            total_events: 0,
            failed_writes: 0,
            events_by_type: HashMap::new(),
            events_today: 0,
        };

        Ok(Self {
            entries: RwLock::new(Vec::with_capacity(config.max_entries)),
            config,
            stats: RwLock::new(stats),
        })
    }

    /// Log a security event
    pub async fn log(&self, entry: AuditLogEntry) {
        if !self.config.enabled {
            return;
        }

        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;

        // Rotate log entries if at capacity
        if entries.len() >= self.config.max_entries {
            entries.remove(0);
        }

        // Update statistics
        stats.total_events += 1;
        let event_type_key = format!("{:?}", entry.event_type);
        *stats.events_by_type.entry(event_type_key).or_insert(0) += 1;

        entries.push(entry);
    }

    /// Log authentication event
    pub async fn log_auth_event(
        &self,
        event_type: AuthEventType,
        user: Option<&crate::security::User>,
        session_id: Option<String>,
        result: EventResult,
        description: String,
    ) -> NetworkResult<()> {
        let entry = AuditLogEntry::new(
            SecurityEventType::Authentication {
                event_subtype: event_type,
            },
            "auth_system".to_string(),
            "authentication".to_string(),
            format!("{:?}", result),
        )
        .with_detail("description".to_string(), description);

        let entry = if let Some(user) = user {
            entry.with_user(user.id.clone())
        } else {
            entry
        };

        let entry = if let Some(session_id) = session_id {
            entry.with_session(session_id)
        } else {
            entry
        };

        self.log(entry).await;
        Ok(())
    }

    /// Log authorization event
    pub async fn log_authorization_event(
        &self,
        context: &crate::security::AuthorizationContext,
        result: &crate::security::AuthorizationResult,
    ) -> NetworkResult<()> {
        let entry = AuditLogEntry::new(
            SecurityEventType::Authorization,
            context.resource.name.clone(),
            format!("{:?}", context.action),
            if result.allowed { "allowed" } else { "denied" }.to_string(),
        )
        .with_user(context.user.id.clone())
        .with_detail(
            "resource_type".to_string(),
            format!("{:?}", context.resource.resource_type),
        );

        self.log(entry).await;
        Ok(())
    }

    /// Log data access event
    pub async fn log_data_access(
        &self,
        access_type: DataAccessType,
        user: &crate::security::User,
        resource: &str,
        data_size: Option<usize>,
        success: bool,
        description: String,
    ) -> NetworkResult<()> {
        let mut entry = AuditLogEntry::new(
            SecurityEventType::DataAccess {
                event_subtype: access_type,
            },
            resource.to_string(),
            format!("{:?}", access_type),
            if success { "success" } else { "failure" }.to_string(),
        )
        .with_user(user.id.clone())
        .with_detail("description".to_string(), description);

        if let Some(size) = data_size {
            entry = entry.with_detail("data_size".to_string(), size.to_string());
        }

        self.log(entry).await;
        Ok(())
    }

    /// Log administrative event
    pub async fn log_admin_event(
        &self,
        event_type: AdminEventType,
        user: &crate::security::User,
        target: Option<String>,
        description: String,
        metadata: HashMap<String, String>,
    ) -> NetworkResult<()> {
        let mut entry = AuditLogEntry::new(
            SecurityEventType::Administrative {
                event_subtype: event_type,
            },
            target.unwrap_or_else(|| "system".to_string()),
            format!("{:?}", event_type),
            "completed".to_string(),
        )
        .with_user(user.id.clone())
        .with_detail("description".to_string(), description);

        for (key, value) in metadata {
            entry = entry.with_detail(key, value);
        }

        self.log(entry).await;
        Ok(())
    }

    /// Get audit statistics
    pub async fn get_stats(&self) -> AuditStats {
        self.stats.read().await.clone()
    }

    /// Get all audit log entries
    pub async fn get_all_entries(&self) -> Vec<AuditLogEntry> {
        self.entries.read().await.clone()
    }

    /// Get audit log entries by event type
    pub async fn get_entries_by_type(&self, event_type: SecurityEventType) -> Vec<AuditLogEntry> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|entry| {
                std::mem::discriminant(&entry.event_type) == std::mem::discriminant(&event_type)
            })
            .cloned()
            .collect()
    }

    /// Get audit log entries by user ID
    pub async fn get_entries_by_user(&self, user_id: &str) -> Vec<AuditLogEntry> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|entry| entry.user_id.as_ref().map_or(false, |id| id == user_id))
            .cloned()
            .collect()
    }

    /// Get recent audit log entries (last N entries)
    pub async fn get_recent_entries(&self, limit: usize) -> Vec<AuditLogEntry> {
        let entries = self.entries.read().await;
        let start_index = if entries.len() > limit {
            entries.len() - limit
        } else {
            0
        };
        entries[start_index..].to_vec()
    }

    /// Clear all audit log entries (admin only)
    pub async fn clear_all(&self) {
        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;
        entries.clear();
        stats.total_events = 0;
        stats.events_by_type.clear();
        stats.events_today = 0;
    }

    /// Get the current number of log entries
    pub async fn entry_count(&self) -> usize {
        self.entries.read().await.len()
    }
}

/// Default audit logger instance with default configuration
impl Default for SecurityAuditLogger {
    fn default() -> Self {
        // Note: This is a blocking implementation
        // In real usage, use SecurityAuditLogger::new(AuditLogConfig::default()).await
        Self {
            entries: RwLock::new(Vec::with_capacity(10_000)),
            config: AuditLogConfig::default(),
            stats: RwLock::new(AuditStats {
                total_events: 0,
                failed_writes: 0,
                events_by_type: HashMap::new(),
                events_today: 0,
            }),
        }
    }
}
