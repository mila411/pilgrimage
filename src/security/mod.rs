//! Security Module
//!
//! Comprehensive security implementation with TLS/SSL encryption,
//! fine-grained authorization, and comprehensive audit logging

pub mod audit_logger;
pub mod authorization;
pub mod modern_tls;
pub mod tls_manager;

#[cfg(test)]
mod tls_manager_test;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use audit_logger::{
    AdminEventType, AuditLogConfig, AuditLogEntry, AuditStats, AuthEventType, AuthzEventType,
    DataAccessType, EventResult, EventSeverity, SecurityAuditLogger, SecurityEventType,
};
pub use authorization::{
    Action, AuthorizationContext, AuthorizationResult, Condition, EnhancedAuthorizationManager,
    Permission, Resource, ResourceType, Role,
};
pub use modern_tls::{
    ClientCertVerificationResult, ModernTlsConfig, ModernTlsManager, MutualTlsStatus,
};
pub use tls_manager::{
    CertRotationConfig, CertValidationConfig, EnhancedTlsConfig, EnhancedTlsManager,
    TlsConnectionInfo, TlsVersion,
};
// pub use tls_manager_simple::{EnhancedTlsManager as SimpleTlsManager, TlsConfig};

use crate::network::error::NetworkResult;

/// User representation for security operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub email: Option<String>,
    pub roles: Vec<String>,
    pub attributes: HashMap<String, String>,
}

/// Comprehensive security manager integrating all security components
pub struct SecurityManager {
    /// TLS/SSL manager
    tls_manager: Option<EnhancedTlsManager>,
    /// Authorization manager
    authorization_manager: EnhancedAuthorizationManager,
    /// Audit logger
    audit_logger: SecurityAuditLogger,
    /// Security configuration
    config: SecurityConfig,
}

/// Security configuration
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// TLS configuration
    pub tls_config: EnhancedTlsConfig,
    /// Audit logging configuration
    pub audit_config: AuditLogConfig,
    /// Enable security features
    pub security_enabled: bool,
    /// Enforce authentication
    pub require_authentication: bool,
    /// Enforce authorization
    pub require_authorization: bool,
    /// Audit all operations
    pub audit_all_operations: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tls_config: EnhancedTlsConfig::default(),
            audit_config: AuditLogConfig::default(),
            security_enabled: true,
            require_authentication: false,
            require_authorization: false,
            audit_all_operations: true,
        }
    }
}

impl SecurityManager {
    /// Create new security manager
    pub async fn new(config: SecurityConfig) -> NetworkResult<Self> {
        // Initialize TLS manager if enabled
        let tls_manager = if config.tls_config.enabled {
            Some(EnhancedTlsManager::new(config.tls_config.clone()).await?)
        } else {
            None
        };

        // Initialize authorization manager
        let authorization_manager = EnhancedAuthorizationManager::new();

        // Initialize audit logger
        let audit_logger = SecurityAuditLogger::new(config.audit_config.clone()).await?;

        let manager = Self {
            tls_manager,
            authorization_manager,
            audit_logger,
            config,
        };

        println!("✅ Security manager initialized with comprehensive security features");
        Ok(manager)
    }

    /// Get TLS manager
    pub fn tls_manager(&self) -> Option<&EnhancedTlsManager> {
        self.tls_manager.as_ref()
    }

    /// Get authorization manager
    pub fn authorization_manager(&self) -> &EnhancedAuthorizationManager {
        &self.authorization_manager
    }

    /// Get audit logger
    pub fn audit_logger(&self) -> &SecurityAuditLogger {
        &self.audit_logger
    }

    /// Check if operation is authorized and log the attempt
    pub async fn authorize_and_log(
        &self,
        context: &AuthorizationContext,
    ) -> NetworkResult<AuthorizationResult> {
        // Perform authorization check
        let result = self
            .authorization_manager
            .check_authorization(context)
            .await?;

        // Log the authorization attempt
        if self.config.audit_all_operations {
            self.audit_logger
                .log_authorization_event(context, &result)
                .await?;
        }

        Ok(result)
    }

    /// Create default security roles
    pub async fn setup_default_roles(&self) -> NetworkResult<()> {
        // Create admin role
        let admin_role_id = self
            .authorization_manager
            .create_role(
                "admin".to_string(),
                "Full administrative access".to_string(),
            )
            .await?;

        // Add admin permissions
        use std::collections::HashSet;
        let mut admin_actions = HashSet::new();
        admin_actions.insert(Action::Admin);
        admin_actions.insert(Action::Read);
        admin_actions.insert(Action::Write);
        admin_actions.insert(Action::Delete);
        admin_actions.insert(Action::Create);
        admin_actions.insert(Action::Update);
        admin_actions.insert(Action::Execute);
        admin_actions.insert(Action::List);
        admin_actions.insert(Action::Monitor);
        admin_actions.insert(Action::ClusterManage);

        self.authorization_manager
            .add_permission_to_role(
                admin_role_id,
                Resource {
                    resource_type: ResourceType::Custom("*".to_string()),
                    name: "*".to_string(),
                    attributes: std::collections::HashMap::new(),
                },
                admin_actions,
                vec![],
            )
            .await?;

        // Create user role
        let user_role_id = self
            .authorization_manager
            .create_role("user".to_string(), "Standard user access".to_string())
            .await?;

        let mut user_actions = HashSet::new();
        user_actions.insert(Action::Read);
        user_actions.insert(Action::Write);
        user_actions.insert(Action::Subscribe);
        user_actions.insert(Action::Publish);

        self.authorization_manager
            .add_permission_to_role(
                user_role_id,
                Resource {
                    resource_type: ResourceType::Topic,
                    name: "user.*".to_string(),
                    attributes: std::collections::HashMap::new(),
                },
                user_actions,
                vec![],
            )
            .await?;

        // Create read-only role
        let readonly_role_id = self
            .authorization_manager
            .create_role("readonly".to_string(), "Read-only access".to_string())
            .await?;

        let mut readonly_actions = HashSet::new();
        readonly_actions.insert(Action::Read);
        readonly_actions.insert(Action::List);
        readonly_actions.insert(Action::Subscribe);

        self.authorization_manager
            .add_permission_to_role(
                readonly_role_id,
                Resource {
                    resource_type: ResourceType::Custom("*".to_string()),
                    name: "*".to_string(),
                    attributes: std::collections::HashMap::new(),
                },
                readonly_actions,
                vec![],
            )
            .await?;

        println!("✅ Default security roles created: admin, user, readonly");
        Ok(())
    }

    /// Log security event
    pub async fn log_security_event(
        &self,
        event_type: SecurityEventType,
        _severity: EventSeverity,
        user: Option<&User>,
        description: String,
        metadata: std::collections::HashMap<String, String>,
    ) -> NetworkResult<()> {
        if !self.config.audit_all_operations {
            return Ok(());
        }

        // Create basic audit entry and log through audit logger
        match event_type {
            SecurityEventType::Authentication { event_subtype } => {
                self.audit_logger
                    .log_auth_event(event_subtype, user, None, EventResult::Success, description)
                    .await
            }
            SecurityEventType::DataAccess { event_subtype } => {
                if let Some(user) = user {
                    self.audit_logger
                        .log_data_access(event_subtype, user, "unknown", None, true, description)
                        .await
                } else {
                    Ok(())
                }
            }
            SecurityEventType::Administrative { event_subtype } => {
                if let Some(user) = user {
                    self.audit_logger
                        .log_admin_event(event_subtype, user, None, description, metadata)
                        .await
                } else {
                    Ok(())
                }
            }
            _ => {
                // For other event types, we would implement specific logging
                println!(
                    "🔒 Security event logged: {:?} - {}",
                    event_type, description
                );
                Ok(())
            }
        }
    }

    /// Log a simple security event
    pub async fn log_simple_security_event(
        &self,
        event_type: &str,
        user_id: &str,
        description: &str,
        success: bool,
    ) -> NetworkResult<()> {
        // For now, just log to stdout
        println!(
            "🔒 Security Event: {} | User: {} | {} | Success: {}",
            event_type, user_id, description, success
        );
        Ok(())
    }
    pub async fn get_security_stats(&self) -> SecurityStats {
        let audit_stats = self.audit_logger.get_stats().await;
        let roles = self.authorization_manager.get_roles().await;

        SecurityStats {
            tls_enabled: self.tls_manager.is_some(),
            total_roles: roles.len(),
            audit_events_total: audit_stats.total_events,
            audit_failures: audit_stats.failed_writes,
        }
    }

    /// Validate security configuration
    pub async fn validate_configuration(&self) -> NetworkResult<SecurityValidationResult> {
        let mut issues = Vec::new();
        let mut warnings = Vec::new();

        // Check TLS configuration
        if !self.config.tls_config.enabled {
            warnings.push("TLS not configured - communications are not encrypted".to_string());
        }

        // Check audit configuration
        if !self.config.audit_config.enabled {
            issues.push(
                "Audit logging is disabled - compliance requirements may not be met".to_string(),
            );
        }

        // Check authentication requirement
        if !self.config.require_authentication {
            issues.push("Authentication not required - security risk".to_string());
        }

        // Check authorization requirement
        if !self.config.require_authorization {
            issues.push("Authorization not required - access control not enforced".to_string());
        }

        let is_valid = issues.is_empty();

        Ok(SecurityValidationResult {
            is_valid,
            issues,
            warnings,
        })
    }

    /// Shutdown security manager
    pub async fn shutdown(&mut self) {
        if let Some(ref mut _tls_manager) = self.tls_manager {
            // tls_manager.shutdown().await;
            // Note: shutdown method not implemented for EnhancedTlsManager
        }

        // Authorization manager doesn't need explicit shutdown

        // Shutdown audit logger
        // Note: We can't call shutdown on audit_logger due to ownership
        // In a real implementation, we'd use Arc<Mutex<>> or similar

        println!("🛑 Security manager shutdown complete");
    }
}

/// Security statistics
#[derive(Debug, Clone)]
pub struct SecurityStats {
    pub tls_enabled: bool,
    pub total_roles: usize,
    pub audit_events_total: u64,
    pub audit_failures: u64,
}

impl SecurityStats {
    /// Convert to HashMap for compatibility
    pub fn to_hashmap(&self) -> std::collections::HashMap<String, f64> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "tls_enabled".to_string(),
            if self.tls_enabled { 1.0 } else { 0.0 },
        );
        map.insert("total_roles".to_string(), self.total_roles as f64);
        map.insert(
            "audit_events_total".to_string(),
            self.audit_events_total as f64,
        );
        map.insert("audit_failures".to_string(), self.audit_failures as f64);
        map.insert("roles_configured".to_string(), self.total_roles as f64);
        map.insert("active_connections".to_string(), 0.0);
        map.insert(
            "audit_events_today".to_string(),
            self.audit_events_total as f64,
        );
        map
    }
}

/// Security validation result
#[derive(Debug, Clone)]
pub struct SecurityValidationResult {
    pub is_valid: bool,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
}
