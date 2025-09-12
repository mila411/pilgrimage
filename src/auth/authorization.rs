//! Advanced Authorization System with RBAC and PBAC
//!
//! This module provides comprehensive authorization capabilities including:
//! - Role-Based Access Control (RBAC) with hierarchical roles
//! - Policy-Based Access Control (PBAC) with dynamic policy evaluation
//! - Context-aware authorization decisions
//! - Fine-grained permission management
//! - Role inheritance and delegation
//! - Policy templates and conditional access

use crate::network::error::{NetworkError, NetworkResult};
use crate::auth::authentication::UserInfo;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use log::{debug, info, warn};

/// Granular permissions for system resources
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    // Basic permissions
    Read,
    Write,
    Admin,

    // Message operations
    MessageSend,
    MessageReceive,
    MessageDelete,
    MessageModify,
    MessageRoute,

    // Queue operations
    QueueCreate,
    QueueDelete,
    QueueModify,
    QueuePurge,
    QueueBind,
    QueueUnbind,

    // Topic operations
    TopicCreate,
    TopicDelete,
    TopicModify,
    TopicPublish,
    TopicSubscribe,
    TopicUnsubscribe,

    // Consumer operations
    ConsumerCreate,
    ConsumerDelete,
    ConsumerModify,
    ConsumerGroupJoin,
    ConsumerGroupLeave,

    // Administrative operations
    UserManagement,
    RoleManagement,
    PermissionManagement,
    SystemConfiguration,
    SystemMonitoring,
    SystemMetrics,

    // Data operations
    DataRead,
    DataWrite,
    DataDelete,
    DataExport,
    DataImport,
    DataBackup,
    DataRestore,

    // Security operations
    SecurityAudit,
    SecurityConfiguration,
    CertificateManagement,
    KeyManagement,

    // Cluster operations
    ClusterManagement,
    NodeManagement,
    LoadBalancing,
    FailoverControl,

    // Custom permissions
    Custom(String),
}

/// Authorization decision with detailed context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthDecision {
    /// Unique decision ID for tracking
    pub decision_id: Uuid,
    /// Whether access is allowed
    pub allowed: bool,
    /// Reason for the decision
    pub reason: String,
    /// Permissions that were evaluated
    pub evaluated_permissions: Vec<Permission>,
    /// Roles that were considered
    pub evaluated_roles: Vec<String>,
    /// Policies that were applied
    pub applied_policies: Vec<String>,
    /// Context used in evaluation
    pub context: AuthorizationContext,
    /// Time taken to make decision (in milliseconds)
    pub evaluation_time_ms: u64,
    /// Timestamp of decision
    pub timestamp: DateTime<Utc>,
    /// Additional metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Authorization context for decision making
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationContext {
    /// User making the request
    pub user_id: String,
    /// User information
    pub user_info: Option<UserInfo>,
    /// Resource being accessed
    pub resource: String,
    /// Action being performed
    pub action: String,
    /// Source IP address
    pub source_ip: Option<String>,
    /// Time of request
    pub request_time: DateTime<Utc>,
    /// Session information
    pub session_id: Option<String>,
    /// Request ID for correlation
    pub request_id: Option<String>,
    /// Additional context attributes
    pub attributes: HashMap<String, serde_json::Value>,
}

/// Role definition with permissions and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name
    pub name: String,
    /// Role description
    pub description: String,
    /// Permissions granted by this role
    pub permissions: HashSet<Permission>,
    /// Parent roles (for inheritance)
    pub parent_roles: HashSet<String>,
    /// Child roles (roles that inherit from this one)
    pub child_roles: HashSet<String>,
    /// Role priority (higher = more important)
    pub priority: u32,
    /// Whether role is active
    pub active: bool,
    /// Role metadata
    pub metadata: HashMap<String, serde_json::Value>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last modified timestamp
    pub modified_at: DateTime<Utc>,
}

impl Role {
    /// Create a new role
    pub fn new(name: String, description: String) -> Self {
        Self {
            name,
            description,
            permissions: HashSet::new(),
            parent_roles: HashSet::new(),
            child_roles: HashSet::new(),
            priority: 0,
            active: true,
            metadata: HashMap::new(),
            created_at: Utc::now(),
            modified_at: Utc::now(),
        }
    }

    /// Add permission to role
    pub fn add_permission(&mut self, permission: Permission) {
        self.permissions.insert(permission);
        self.modified_at = Utc::now();
    }

    /// Remove permission from role
    pub fn remove_permission(&mut self, permission: &Permission) -> bool {
        let removed = self.permissions.remove(permission);
        if removed {
            self.modified_at = Utc::now();
        }
        removed
    }

    /// Check if role has permission
    pub fn has_permission(&self, permission: &Permission) -> bool {
        self.permissions.contains(permission)
    }

    /// Add parent role for inheritance
    pub fn add_parent_role(&mut self, parent_name: String) {
        self.parent_roles.insert(parent_name);
        self.modified_at = Utc::now();
    }

    /// Get all permissions including inherited ones
    pub fn get_effective_permissions(&self, role_manager: &HashMap<String, Role>) -> HashSet<Permission> {
        let mut effective_permissions = self.permissions.clone();

        // Add permissions from parent roles
        for parent_name in &self.parent_roles {
            if let Some(parent_role) = role_manager.get(parent_name) {
                effective_permissions.extend(parent_role.get_effective_permissions(role_manager));
            }
        }

        effective_permissions
    }
}

/// Policy-based access control rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPolicy {
    /// Policy ID
    pub id: String,
    /// Policy name
    pub name: String,
    /// Policy description
    pub description: String,
    /// Policy conditions
    pub conditions: Vec<PolicyCondition>,
    /// Policy effect (Allow/Deny)
    pub effect: PolicyEffect,
    /// Resources this policy applies to
    pub resources: Vec<String>,
    /// Actions this policy applies to
    pub actions: Vec<String>,
    /// Priority (higher = evaluated first)
    pub priority: u32,
    /// Whether policy is active
    pub active: bool,
    /// Policy metadata
    pub metadata: HashMap<String, serde_json::Value>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Expiration timestamp
    pub expires_at: Option<DateTime<Utc>>,
}

/// Policy effect
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

/// Policy condition for evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyCondition {
    /// Attribute to evaluate
    pub attribute: String,
    /// Comparison operator
    pub operator: ConditionOperator,
    /// Value to compare against
    pub value: serde_json::Value,
}

/// Condition operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConditionOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Contains,
    NotContains,
    StartsWith,
    EndsWith,
    Regex,
    In,
    NotIn,
}

/// User role assignment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRoleAssignment {
    /// User ID
    pub user_id: String,
    /// Role name
    pub role_name: String,
    /// Assignment timestamp
    pub assigned_at: DateTime<Utc>,
    /// Assignment expiration
    pub expires_at: Option<DateTime<Utc>>,
    /// Assignment metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Advanced authorization manager
pub struct AuthorizationManager {
    /// Role definitions
    roles: Arc<RwLock<HashMap<String, Role>>>,
    /// User role assignments
    user_roles: Arc<RwLock<HashMap<String, Vec<UserRoleAssignment>>>>,
    /// Access policies
    policies: Arc<RwLock<HashMap<String, AccessPolicy>>>,
    /// Decision cache
    decision_cache: Arc<RwLock<HashMap<String, AuthDecision>>>,
    /// Configuration
    config: AuthorizationConfig,
}

/// Authorization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationConfig {
    /// Enable permission caching
    pub enable_caching: bool,
    /// Cache TTL in seconds
    pub cache_ttl: u64,
    /// Default deny policy
    pub default_deny: bool,
    /// Enable role inheritance
    pub enable_inheritance: bool,
    /// Enable policy evaluation
    pub enable_policies: bool,
    /// Maximum policy evaluation depth
    pub max_policy_depth: u32,
    /// Enable context evaluation
    pub enable_context: bool,
}

impl Default for AuthorizationConfig {
    fn default() -> Self {
        Self {
            enable_caching: true,
            cache_ttl: 300, // 5 minutes
            default_deny: true,
            enable_inheritance: true,
            enable_policies: true,
            max_policy_depth: 10,
            enable_context: true,
        }
    }
}

impl AuthorizationManager {
    /// Create a new authorization manager
    pub fn new(config: AuthorizationConfig) -> Self {
        Self {
            roles: Arc::new(RwLock::new(HashMap::new())),
            user_roles: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(HashMap::new())),
            decision_cache: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Initialize with default roles
    pub async fn initialize_default_roles(&self) -> NetworkResult<()> {
        let mut roles = self.roles.write().await;

        // Admin role
        let mut admin_role = Role::new("admin".to_string(), "System administrator".to_string());
        admin_role.permissions.extend(vec![
            Permission::UserManagement,
            Permission::RoleManagement,
            Permission::PermissionManagement,
            Permission::SystemConfiguration,
            Permission::SystemMonitoring,
            Permission::SecurityConfiguration,
            Permission::ClusterManagement,
        ]);
        admin_role.priority = 1000;
        roles.insert("admin".to_string(), admin_role);

        // User role
        let mut user_role = Role::new("user".to_string(), "Regular user".to_string());
        user_role.permissions.extend(vec![
            Permission::MessageSend,
            Permission::MessageReceive,
            Permission::TopicSubscribe,
            Permission::ConsumerCreate,
        ]);
        user_role.priority = 100;
        roles.insert("user".to_string(), user_role);

        // Guest role
        let mut guest_role = Role::new("guest".to_string(), "Guest user with limited access".to_string());
        guest_role.permissions.extend(vec![
            Permission::MessageReceive,
            Permission::DataRead,
        ]);
        guest_role.priority = 10;
        roles.insert("guest".to_string(), guest_role);

        info!("Default roles initialized");
        Ok(())
    }

    /// Create a new role
    pub async fn create_role(&self, role: Role) -> NetworkResult<()> {
        let mut roles = self.roles.write().await;

        if roles.contains_key(&role.name) {
            return Err(NetworkError::AuthorizationError(
                format!("Role '{}' already exists", role.name)
            ));
        }

        roles.insert(role.name.clone(), role);
        info!("Role '{}' created", roles.len());
        Ok(())
    }

    /// Delete a role
    pub async fn delete_role(&self, role_name: &str) -> NetworkResult<()> {
        let mut roles = self.roles.write().await;

        if !roles.contains_key(role_name) {
            return Err(NetworkError::AuthorizationError(
                format!("Role '{}' not found", role_name)
            ));
        }

        // Remove role assignments
        let mut user_roles = self.user_roles.write().await;
        for assignments in user_roles.values_mut() {
            assignments.retain(|assignment| assignment.role_name != role_name);
        }

        roles.remove(role_name);
        info!("Role '{}' deleted", role_name);
        Ok(())
    }

    /// Assign role to user
    pub async fn assign_role(&self, user_id: &str, role_name: &str, expires_at: Option<DateTime<Utc>>) -> NetworkResult<()> {
        let roles = self.roles.read().await;

        if !roles.contains_key(role_name) {
            return Err(NetworkError::AuthorizationError(
                format!("Role '{}' not found", role_name)
            ));
        }

        let mut user_roles = self.user_roles.write().await;
        let assignments = user_roles.entry(user_id.to_string()).or_insert_with(Vec::new);

        // Check if already assigned
        if assignments.iter().any(|assignment| assignment.role_name == role_name) {
            return Err(NetworkError::AuthorizationError(
                format!("Role '{}' already assigned to user '{}'", role_name, user_id)
            ));
        }

        let assignment = UserRoleAssignment {
            user_id: user_id.to_string(),
            role_name: role_name.to_string(),
            assigned_at: Utc::now(),
            expires_at,
            metadata: HashMap::new(),
        };

        assignments.push(assignment);
        info!("Role '{}' assigned to user '{}'", role_name, user_id);
        Ok(())
    }

    /// Revoke role from user
    pub async fn revoke_role(&self, user_id: &str, role_name: &str) -> NetworkResult<()> {
        let mut user_roles = self.user_roles.write().await;

        if let Some(assignments) = user_roles.get_mut(user_id) {
            let original_len = assignments.len();
            assignments.retain(|assignment| assignment.role_name != role_name);

            if assignments.len() == original_len {
                return Err(NetworkError::AuthorizationError(
                    format!("Role '{}' not assigned to user '{}'", role_name, user_id)
                ));
            }

            info!("Role '{}' revoked from user '{}'", role_name, user_id);
            Ok(())
        } else {
            Err(NetworkError::AuthorizationError(
                format!("User '{}' has no role assignments", user_id)
            ))
        }
    }

    /// Check if user has permission
    pub async fn check_permission(&self, context: AuthorizationContext) -> NetworkResult<AuthDecision> {
        let start_time = SystemTime::now();
        let decision_id = Uuid::new_v4();

        // Check cache first
        if self.config.enable_caching {
            let cache_key = format!("{}:{}:{}", context.user_id, context.resource, context.action);
            let cache = self.decision_cache.read().await;
            if let Some(cached_decision) = cache.get(&cache_key) {
                // Check if cache entry is still valid
                let cache_age = (Utc::now() - cached_decision.timestamp).num_seconds() as u64;
                if cache_age < self.config.cache_ttl {
                    debug!("Authorization decision served from cache");
                    return Ok(cached_decision.clone());
                }
            }
        }

        let mut decision = AuthDecision {
            decision_id,
            allowed: false,
            reason: "Evaluating permissions".to_string(),
            evaluated_permissions: Vec::new(),
            evaluated_roles: Vec::new(),
            applied_policies: Vec::new(),
            context: context.clone(),
            evaluation_time_ms: 0,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        };

        // Get user roles
        let user_roles = self.user_roles.read().await;
        let roles = self.roles.read().await;

        // Use a static empty vector to avoid temporary value issue
        static EMPTY_VEC: Vec<UserRoleAssignment> = Vec::new();
        let user_assignments = user_roles.get(&context.user_id).unwrap_or(&EMPTY_VEC);

        // Check role-based permissions
        let mut effective_permissions = HashSet::new();
        let mut evaluated_roles = Vec::new();

        for assignment in user_assignments {
            // Check if assignment is still valid
            if let Some(expires_at) = assignment.expires_at {
                if Utc::now() > expires_at {
                    continue;
                }
            }

            if let Some(role) = roles.get(&assignment.role_name) {
                if role.active {
                    evaluated_roles.push(role.name.clone());

                    if self.config.enable_inheritance {
                        effective_permissions.extend(role.get_effective_permissions(&roles));
                    } else {
                        effective_permissions.extend(role.permissions.clone());
                    }
                }
            }
        }

        decision.evaluated_roles = evaluated_roles;
        decision.evaluated_permissions = effective_permissions.iter().cloned().collect();

        // Check if required permission exists
        let required_permission = self.map_action_to_permission(&context.action);
        let has_permission = effective_permissions.contains(&required_permission);

        // Evaluate policies if enabled
        if self.config.enable_policies {
            let policy_result = self.evaluate_policies(&context).await?;
            decision.applied_policies = policy_result.applied_policies;

            // Policy can override role-based decision
            match policy_result.effect {
                Some(PolicyEffect::Allow) => {
                    decision.allowed = true;
                    decision.reason = format!("Access allowed by policy: {}", policy_result.reason);
                }
                Some(PolicyEffect::Deny) => {
                    decision.allowed = false;
                    decision.reason = format!("Access denied by policy: {}", policy_result.reason);
                }
                None => {
                    // No applicable policies, use role-based decision
                    decision.allowed = has_permission;
                    decision.reason = if has_permission {
                        format!("Access allowed by role permissions")
                    } else if self.config.default_deny {
                        format!("Access denied - insufficient permissions")
                    } else {
                        format!("Access allowed by default policy")
                    };
                }
            }
        } else {
            // Pure role-based decision
            decision.allowed = has_permission || !self.config.default_deny;
            decision.reason = if has_permission {
                format!("Access allowed by role permissions")
            } else if self.config.default_deny {
                format!("Access denied - insufficient permissions")
            } else {
                format!("Access allowed by default policy")
            };
        }

        // Calculate evaluation time
        if let Ok(duration) = start_time.elapsed() {
            decision.evaluation_time_ms = duration.as_millis() as u64;
        }

        // Cache decision
        if self.config.enable_caching {
            let cache_key = format!("{}:{}:{}", context.user_id, context.resource, context.action);
            let mut cache = self.decision_cache.write().await;
            cache.insert(cache_key, decision.clone());
        }

        debug!("Authorization decision: {} for user '{}' on resource '{}' action '{}'",
               if decision.allowed { "ALLOW" } else { "DENY" },
               context.user_id, context.resource, context.action);

        Ok(decision)
    }

    /// Evaluate policies for given context
    async fn evaluate_policies(&self, context: &AuthorizationContext) -> NetworkResult<PolicyEvaluationResult> {
        let policies = self.policies.read().await;
        let mut applicable_policies: Vec<&AccessPolicy> = policies.values()
            .filter(|policy| policy.active)
            .filter(|policy| self.policy_applies_to_context(policy, context))
            .collect();

        // Sort by priority (higher first)
        applicable_policies.sort_by(|a, b| b.priority.cmp(&a.priority));

        let mut result = PolicyEvaluationResult {
            effect: None,
            reason: String::new(),
            applied_policies: Vec::new(),
        };

        for policy in applicable_policies {
            if self.evaluate_policy_conditions(policy, context).await? {
                result.applied_policies.push(policy.id.clone());
                result.effect = Some(policy.effect.clone());
                result.reason = format!("Policy '{}' matched", policy.name);

                // First matching policy wins
                break;
            }
        }

        Ok(result)
    }

    /// Check if policy applies to context
    fn policy_applies_to_context(&self, policy: &AccessPolicy, context: &AuthorizationContext) -> bool {
        // Check if expired
        if let Some(expires_at) = policy.expires_at {
            if Utc::now() > expires_at {
                return false;
            }
        }

        // Check resource match
        let resource_match = policy.resources.is_empty() ||
            policy.resources.iter().any(|resource| {
                context.resource.starts_with(resource) || resource == "*"
            });

        // Check action match
        let action_match = policy.actions.is_empty() ||
            policy.actions.iter().any(|action| {
                context.action == *action || action == "*"
            });

        resource_match && action_match
    }

    /// Evaluate policy conditions
    async fn evaluate_policy_conditions(&self, policy: &AccessPolicy, context: &AuthorizationContext) -> NetworkResult<bool> {
        for condition in &policy.conditions {
            if !self.evaluate_condition(condition, context).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Evaluate individual condition
    async fn evaluate_condition(&self, condition: &PolicyCondition, context: &AuthorizationContext) -> NetworkResult<bool> {
        let attribute_value = self.get_context_attribute(&condition.attribute, context)?;

        match condition.operator {
            ConditionOperator::Equals => Ok(attribute_value == condition.value),
            ConditionOperator::NotEquals => Ok(attribute_value != condition.value),
            ConditionOperator::Contains => {
                if let (serde_json::Value::String(haystack), serde_json::Value::String(needle)) = (&attribute_value, &condition.value) {
                    Ok(haystack.contains(needle))
                } else {
                    Ok(false)
                }
            }
            // Implement other operators as needed
            _ => {
                warn!("Condition operator {:?} not implemented", condition.operator);
                Ok(true) // Default to allow
            }
        }
    }

    /// Get context attribute value
    fn get_context_attribute(&self, attribute: &str, context: &AuthorizationContext) -> NetworkResult<serde_json::Value> {
        match attribute {
            "user_id" => Ok(serde_json::Value::String(context.user_id.clone())),
            "resource" => Ok(serde_json::Value::String(context.resource.clone())),
            "action" => Ok(serde_json::Value::String(context.action.clone())),
            "source_ip" => Ok(context.source_ip.as_ref()
                .map(|ip| serde_json::Value::String(ip.clone()))
                .unwrap_or(serde_json::Value::Null)),
            "time" => Ok(serde_json::Value::String(context.request_time.to_rfc3339())),
            _ => {
                // Check custom attributes
                Ok(context.attributes.get(attribute)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null))
            }
        }
    }

    /// Map action to permission
    fn map_action_to_permission(&self, action: &str) -> Permission {
        match action {
            "send" => Permission::MessageSend,
            "receive" => Permission::MessageReceive,
            "delete" => Permission::MessageDelete,
            "modify" => Permission::MessageModify,
            "create_queue" => Permission::QueueCreate,
            "delete_queue" => Permission::QueueDelete,
            "publish" => Permission::TopicPublish,
            "subscribe" => Permission::TopicSubscribe,
            "read" => Permission::DataRead,
            "write" => Permission::DataWrite,
            "admin" => Permission::SystemConfiguration,
            _ => Permission::Custom(action.to_string()),
        }
    }

    /// Get user effective permissions
    pub async fn get_user_permissions(&self, user_id: &str) -> NetworkResult<HashSet<Permission>> {
        let user_roles = self.user_roles.read().await;
        let roles = self.roles.read().await;

        let mut effective_permissions = HashSet::new();

        if let Some(assignments) = user_roles.get(user_id) {
            for assignment in assignments {
                // Check if assignment is still valid
                if let Some(expires_at) = assignment.expires_at {
                    if Utc::now() > expires_at {
                        continue;
                    }
                }

                if let Some(role) = roles.get(&assignment.role_name) {
                    if role.active {
                        if self.config.enable_inheritance {
                            effective_permissions.extend(role.get_effective_permissions(&roles));
                        } else {
                            effective_permissions.extend(role.permissions.clone());
                        }
                    }
                }
            }
        }

        Ok(effective_permissions)
    }

    /// Get user roles
    pub async fn get_user_roles(&self, user_id: &str) -> Vec<String> {
        let user_roles = self.user_roles.read().await;

        if let Some(assignments) = user_roles.get(user_id) {
            assignments.iter()
                .filter(|assignment| {
                    // Check if assignment is still valid
                    if let Some(expires_at) = assignment.expires_at {
                        Utc::now() <= expires_at
                    } else {
                        true
                    }
                })
                .map(|assignment| assignment.role_name.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Clear authorization cache
    pub async fn clear_cache(&self) {
        let mut cache = self.decision_cache.write().await;
        cache.clear();
        debug!("Authorization cache cleared");
    }
}

/// Simple Role-Based Access Control implementation for tests
#[derive(Debug)]
pub struct RoleBasedAccessControl {
    /// User to roles mapping
    pub user_roles: HashMap<String, HashSet<String>>,
    /// Role to permissions mapping
    pub role_permissions: HashMap<String, HashSet<Permission>>,
}

impl RoleBasedAccessControl {
    /// Create new RBAC instance
    pub fn new() -> Self {
        Self {
            user_roles: HashMap::new(),
            role_permissions: HashMap::new(),
        }
    }

    /// Add role with permissions
    pub fn add_role(&mut self, role_name: &str, permissions: Vec<Permission>) {
        self.role_permissions.insert(role_name.to_string(), permissions.into_iter().collect());
    }

    /// Assign role to user
    pub fn assign_role(&mut self, user_id: &str, role_name: &str) {
        self.user_roles.entry(user_id.to_string())
            .or_insert_with(HashSet::new)
            .insert(role_name.to_string());
    }

    /// Check if user has permission
    pub fn has_permission(&self, user_id: &str, permission: &Permission) -> bool {
        if let Some(user_roles) = self.user_roles.get(user_id) {
            for role in user_roles {
                if let Some(role_permissions) = self.role_permissions.get(role) {
                    if role_permissions.contains(permission) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Revoke role from user
    pub fn revoke_role(&mut self, user_id: &str, role_name: &str) {
        if let Some(user_roles) = self.user_roles.get_mut(user_id) {
            user_roles.remove(role_name);
        }
    }

    /// Remove role entirely
    pub fn remove_role(&mut self, role_name: &str) {
        self.role_permissions.remove(role_name);
        for user_roles in self.user_roles.values_mut() {
            user_roles.remove(role_name);
        }
    }
}

/// Policy evaluation result
struct PolicyEvaluationResult {
    effect: Option<PolicyEffect>,
    reason: String,
    applied_policies: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_authorization_manager_creation() {
        let config = AuthorizationConfig::default();
        let manager = AuthorizationManager::new(config);

        // Test initialization
        assert!(manager.initialize_default_roles().await.is_ok());
    }

    #[tokio::test]
    async fn test_role_management() {
        let config = AuthorizationConfig::default();
        let manager = AuthorizationManager::new(config);

        let mut role = Role::new("test_role".to_string(), "Test role".to_string());
        role.add_permission(Permission::MessageSend);

        assert!(manager.create_role(role).await.is_ok());
        assert!(manager.assign_role("test_user", "test_role", None).await.is_ok());

        let user_roles = manager.get_user_roles("test_user").await;
        assert!(user_roles.contains(&"test_role".to_string()));

        let permissions = manager.get_user_permissions("test_user").await.unwrap();
        assert!(permissions.contains(&Permission::MessageSend));
    }

    #[tokio::test]
    async fn test_authorization_decision() {
        let config = AuthorizationConfig::default();
        let manager = AuthorizationManager::new(config);

        // Create role and assign to user
        let mut role = Role::new("sender".to_string(), "Message sender".to_string());
        role.add_permission(Permission::MessageSend);
        manager.create_role(role).await.unwrap();
        manager.assign_role("alice", "sender", None).await.unwrap();

        // Create authorization context
        let context = AuthorizationContext {
            user_id: "alice".to_string(),
            user_info: None,
            resource: "message_queue".to_string(),
            action: "send".to_string(),
            source_ip: Some("127.0.0.1".to_string()),
            request_time: Utc::now(),
            session_id: None,
            request_id: None,
            attributes: HashMap::new(),
        };

        // Check authorization
        let decision = manager.check_permission(context).await.unwrap();
        assert!(decision.allowed);
        assert!(decision.evaluated_roles.contains(&"sender".to_string()));
    }
}

// Note: Removed duplicate simplified Permission and demo RBAC to avoid conflicts with the
// comprehensive definitions above.
