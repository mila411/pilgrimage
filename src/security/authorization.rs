//! Enhanced Authorization and Access Control System
//!
//! Fine-grained access control with role-based permissions,
//! resource-level authorization, and policy enforcement

use crate::network::error::{NetworkError, NetworkResult};
use crate::security::User;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use chrono::{Timelike, Datelike};
use uuid::Uuid;

/// Authorization action types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    /// Read access
    Read,
    /// Write access
    Write,
    /// Delete access
    Delete,
    /// Administrative access
    Admin,
    /// Execute/invoke access
    Execute,
    /// Create new resources
    Create,
    /// Update existing resources
    Update,
    /// List/browse resources
    List,
    /// Subscribe to topics/queues
    Subscribe,
    /// Publish messages
    Publish,
    /// Manage cluster operations
    ClusterManage,
    /// View monitoring data
    Monitor,
    /// Custom action
    Custom(String),
}

/// Resource types for authorization
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    /// Message topics
    Topic,
    /// Message queues
    Queue,
    /// Consumer groups
    ConsumerGroup,
    /// Cluster nodes
    Node,
    /// Broker instances
    Broker,
    /// Configuration settings
    Config,
    /// Monitoring metrics
    Metrics,
    /// Log data
    Logs,
    /// Schema registry
    Schema,
    /// User management
    Users,
    /// Security policies
    Policies,
    /// Custom resource type
    Custom(String),
}

/// Resource identifier for authorization checks
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resource {
    /// Resource type
    pub resource_type: ResourceType,
    /// Resource name/identifier
    pub name: String,
    /// Resource attributes for fine-grained control
    pub attributes: HashMap<String, String>,
}

/// Role definition with permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role unique identifier
    pub id: Uuid,
    /// Role name
    pub name: String,
    /// Role description
    pub description: String,
    /// Allowed actions on resources
    pub permissions: Vec<Permission>,
    /// Role inheritance (can inherit from other roles)
    pub inherited_roles: Vec<Uuid>,
    /// Role priority (higher number = higher priority)
    pub priority: u32,
    /// Role creation timestamp
    pub created_at: u64,
    /// Role last modified timestamp
    pub modified_at: u64,
    /// Role is active
    pub active: bool,
}

/// Permission definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    /// Unique permission identifier
    pub id: Uuid,
    /// Resource this permission applies to
    pub resource: Resource,
    /// Actions allowed on the resource
    pub actions: HashSet<Action>,
    /// Permission conditions (time-based, IP-based, etc.)
    pub conditions: Vec<Condition>,
    /// Permission is allowed (true) or denied (false)
    pub allow: bool,
    /// Permission expiry time
    pub expires_at: Option<u64>,
}

/// Authorization condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    /// Time-based condition
    TimeRange {
        start_hour: u8,
        end_hour: u8,
        days_of_week: Vec<u8>, // 0-6 (Sunday-Saturday)
    },
    /// IP address condition
    IpRange {
        cidr: String,
    },
    /// Rate limiting condition
    RateLimit {
        max_requests: u32,
        time_window: Duration,
    },
    /// Custom condition
    Custom {
        name: String,
        parameters: HashMap<String, String>,
    },
}

/// Authorization context for permission checks
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    /// User making the request
    pub user: User,
    /// Resource being accessed
    pub resource: Resource,
    /// Action being performed
    pub action: Action,
    /// Request timestamp
    pub timestamp: SystemTime,
    /// Source IP address
    pub source_ip: Option<std::net::IpAddr>,
    /// Additional context attributes
    pub attributes: HashMap<String, String>,
}

/// Authorization result
#[derive(Debug, Clone)]
pub struct AuthorizationResult {
    /// Whether access is allowed
    pub allowed: bool,
    /// Reason for the decision
    pub reason: String,
    /// Applied permissions
    pub applied_permissions: Vec<Uuid>,
    /// Denied permissions
    pub denied_permissions: Vec<Uuid>,
    /// Decision timestamp
    pub timestamp: SystemTime,
}

/// Enhanced authorization manager
pub struct EnhancedAuthorizationManager {
    /// Roles storage
    roles: Arc<tokio::sync::RwLock<HashMap<Uuid, Role>>>,
    /// User role assignments
    user_roles: Arc<tokio::sync::RwLock<HashMap<Uuid, Vec<Uuid>>>>,
    /// Permission cache for performance
    permission_cache: Arc<tokio::sync::RwLock<HashMap<String, AuthorizationResult>>>,
    /// Rate limiting tracker
    rate_limits: Arc<tokio::sync::RwLock<HashMap<String, RateLimitState>>>,
    /// Default policies
    default_policies: DefaultPolicies,
}

/// Rate limiting state
#[derive(Debug, Clone)]
struct RateLimitState {
    requests: Vec<SystemTime>,
    max_requests: u32,
    time_window: Duration,
}

/// Default authorization policies
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DefaultPolicies {
    /// Default deny all access
    default_deny: bool,
    /// Allow admin bypass
    admin_bypass: bool,
    /// Enable audit logging
    audit_enabled: bool,
}

impl EnhancedAuthorizationManager {
    /// Create new authorization manager
    pub fn new() -> Self {
        Self {
            roles: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            user_roles: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            permission_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            rate_limits: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            default_policies: DefaultPolicies {
                default_deny: true,
                admin_bypass: true,
                audit_enabled: true,
            },
        }
    }

    /// Create a new role
    pub async fn create_role(&self, name: String, description: String) -> NetworkResult<Uuid> {
        let role = Role {
            id: Uuid::new_v4(),
            name: name.clone(),
            description,
            permissions: Vec::new(),
            inherited_roles: Vec::new(),
            priority: 0,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            modified_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            active: true,
        };

        let role_id = role.id;
        self.roles.write().await.insert(role_id, role);

        println!("✅ Created role: {} ({})", name, role_id);
        Ok(role_id)
    }

    /// Add permission to role
    pub async fn add_permission_to_role(
        &self,
        role_id: Uuid,
        resource: Resource,
        actions: HashSet<Action>,
        conditions: Vec<Condition>,
    ) -> NetworkResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(role) = roles.get_mut(&role_id) {
            let permission = Permission {
                id: Uuid::new_v4(),
                resource,
                actions,
                conditions,
                allow: true,
                expires_at: None,
            };

            role.permissions.push(permission);
            role.modified_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

            // Clear permission cache when roles change
            self.permission_cache.write().await.clear();

            println!("✅ Added permission to role: {}", role_id);
            Ok(())
        } else {
            Err(NetworkError::AuthorizationError(format!("Role not found: {}", role_id)))
        }
    }

    /// Assign role to user
    pub async fn assign_role_to_user(&self, user_id: Uuid, role_id: Uuid) -> NetworkResult<()> {
        // Verify role exists
        if !self.roles.read().await.contains_key(&role_id) {
            return Err(NetworkError::AuthorizationError(format!("Role not found: {}", role_id)));
        }

        let mut user_roles = self.user_roles.write().await;
        let roles = user_roles.entry(user_id).or_insert_with(Vec::new);

        if !roles.contains(&role_id) {
            roles.push(role_id);

            // Clear permission cache for this user
            let cache_key_prefix = format!("user:{}", user_id);
            let mut cache = self.permission_cache.write().await;
            cache.retain(|key, _| !key.starts_with(&cache_key_prefix));

            println!("✅ Assigned role {} to user {}", role_id, user_id);
        }

        Ok(())
    }

    /// Check authorization for a user action
    pub async fn check_authorization(
        &self,
        context: &AuthorizationContext,
    ) -> NetworkResult<AuthorizationResult> {
        // Generate cache key
        let cache_key = format!(
            "user:{}:resource:{}:{}:action:{:?}",
            context.user.id,
            context.resource.resource_type.cache_key(),
            context.resource.name,
            context.action
        );

        // Check cache first
        if let Some(cached_result) = self.permission_cache.read().await.get(&cache_key) {
            // Check if cache is still valid (1 minute TTL)
            if cached_result.timestamp.elapsed().unwrap_or(Duration::from_secs(0)) < Duration::from_secs(60) {
                return Ok(cached_result.clone());
            }
        }

        // Perform authorization check
        let result = self.perform_authorization_check(context).await?;

        // Cache the result
        self.permission_cache.write().await.insert(cache_key, result.clone());

        Ok(result)
    }

    /// Perform the actual authorization check
    async fn perform_authorization_check(
        &self,
        context: &AuthorizationContext,
    ) -> NetworkResult<AuthorizationResult> {
        let mut applied_permissions = Vec::new();
        let mut denied_permissions = Vec::new();
        let mut final_decision = !self.default_policies.default_deny; // Start with default policy
        let mut decision_reason = "Default policy".to_string();

        // Get user roles
        let user_id = context.user.id.parse::<Uuid>().unwrap_or_else(|_| Uuid::new_v4());
        let user_roles = self.user_roles.read().await
            .get(&user_id)
            .cloned()
            .unwrap_or_default();

        let roles = self.roles.read().await;

        // Check admin bypass
        if self.default_policies.admin_bypass && self.is_admin_user(&context.user, &user_roles, &roles).await {
            return Ok(AuthorizationResult {
                allowed: true,
                reason: "Admin bypass".to_string(),
                applied_permissions: vec![],
                denied_permissions: vec![],
                timestamp: SystemTime::now(),
            });
        }

        // Collect all permissions from user's roles
        let mut all_permissions = Vec::new();
        for role_id in &user_roles {
            if let Some(role) = roles.get(role_id) {
                if role.active {
                    all_permissions.extend(&role.permissions);
                }
            }
        }

        // Sort permissions by priority (explicit deny permissions first)
        all_permissions.sort_by(|a, b| {
            (!a.allow).cmp(&!b.allow)
        });

        // Evaluate permissions
        for permission in all_permissions {
            if self.permission_matches(permission, context).await {
                if self.evaluate_conditions(&permission.conditions, context).await? {
                    if permission.allow {
                        final_decision = true;
                        decision_reason = format!("Allowed by permission {}", permission.id);
                        applied_permissions.push(permission.id);
                    } else {
                        final_decision = false;
                        decision_reason = format!("Denied by permission {}", permission.id);
                        denied_permissions.push(permission.id);
                        break; // Explicit deny takes precedence
                    }
                }
            }
        }

        // Check rate limiting
        if final_decision {
            if let Err(e) = self.check_rate_limits(context).await {
                final_decision = false;
                decision_reason = format!("Rate limit exceeded: {}", e);
            }
        }

        Ok(AuthorizationResult {
            allowed: final_decision,
            reason: decision_reason,
            applied_permissions,
            denied_permissions,
            timestamp: SystemTime::now(),
        })
    }

    /// Check if user is admin
    async fn is_admin_user(
        &self,
        _user: &User,
        user_roles: &[Uuid],
        roles: &HashMap<Uuid, Role>,
    ) -> bool {
        // Check if user has admin role or admin permissions
        for role_id in user_roles {
            if let Some(role) = roles.get(role_id) {
                if role.name.to_lowercase().contains("admin") {
                    return true;
                }
                // Check for admin permissions
                for permission in &role.permissions {
                    if permission.actions.contains(&Action::Admin) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if permission matches the context
    async fn permission_matches(&self, permission: &Permission, context: &AuthorizationContext) -> bool {
        // Check resource type match
        if permission.resource.resource_type != context.resource.resource_type {
            return false;
        }

        // Check resource name match (support wildcards)
        if !self.resource_name_matches(&permission.resource.name, &context.resource.name) {
            return false;
        }

        // Check action match
        if !permission.actions.contains(&context.action) {
            return false;
        }

        // Check permission expiry
        if let Some(expires_at) = permission.expires_at {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if now > expires_at {
                return false;
            }
        }

        true
    }

    /// Check if resource name matches (with wildcard support)
    fn resource_name_matches(&self, pattern: &str, name: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if pattern.contains('*') {
            // Simple wildcard matching
            let regex_pattern = pattern.replace('*', ".*");
            if let Ok(regex) = regex::Regex::new(&regex_pattern) {
                return regex.is_match(name);
            }
        }

        pattern == name
    }

    /// Evaluate permission conditions
    async fn evaluate_conditions(
        &self,
        conditions: &[Condition],
        context: &AuthorizationContext,
    ) -> NetworkResult<bool> {
        for condition in conditions {
            if !self.evaluate_single_condition(condition, context).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Evaluate a single condition
    async fn evaluate_single_condition(
        &self,
        condition: &Condition,
        context: &AuthorizationContext,
    ) -> NetworkResult<bool> {
        match condition {
            Condition::TimeRange { start_hour, end_hour, days_of_week } => {
                let now = chrono::Utc::now();
                let hour = now.hour() as u8;
                let day_of_week = now.weekday().num_days_from_sunday() as u8;

                Ok(hour >= *start_hour && hour <= *end_hour && days_of_week.contains(&day_of_week))
            }
            Condition::IpRange { cidr } => {
                if let Some(ip) = context.source_ip {
                    // Parse CIDR and check if IP is in range
                    Ok(self.ip_in_cidr(ip, cidr))
                } else {
                    Ok(false)
                }
            }
            Condition::RateLimit { max_requests: _, time_window: _ } => {
                // Rate limiting is checked separately
                Ok(true)
            }
            Condition::Custom { name: _, parameters: _ } => {
                // Custom condition evaluation would be implemented here
                Ok(true)
            }
        }
    }

    /// Check if IP is in CIDR range
    fn ip_in_cidr(&self, ip: std::net::IpAddr, cidr: &str) -> bool {
        // Simple CIDR check implementation
        // In production, use a proper CIDR library
        cidr.contains(&ip.to_string())
    }

    /// Check rate limits
    async fn check_rate_limits(&self, context: &AuthorizationContext) -> NetworkResult<()> {
        let rate_limit_key = format!("user:{}:action:{:?}", context.user.id, context.action);
        let mut rate_limits = self.rate_limits.write().await;

        let now = SystemTime::now();
        let state = rate_limits.entry(rate_limit_key).or_insert(RateLimitState {
            requests: Vec::new(),
            max_requests: 100, // Default rate limit
            time_window: Duration::from_secs(60),
        });

        // Clean old requests
        state.requests.retain(|&time| now.duration_since(time).unwrap_or(Duration::from_secs(0)) < state.time_window);

        // Check rate limit
        if state.requests.len() >= state.max_requests as usize {
            return Err(NetworkError::AuthorizationError("Rate limit exceeded".to_string()));
        }

        // Add current request
        state.requests.push(now);

        Ok(())
    }

    /// Get user permissions summary
    pub async fn get_user_permissions(&self, user_id: Uuid) -> NetworkResult<Vec<Permission>> {
        let user_roles = self.user_roles.read().await
            .get(&user_id)
            .cloned()
            .unwrap_or_default();

        let roles = self.roles.read().await;
        let mut permissions = Vec::new();

        for role_id in user_roles {
            if let Some(role) = roles.get(&role_id) {
                if role.active {
                    permissions.extend(role.permissions.clone());
                }
            }
        }

        Ok(permissions)
    }

    /// Get all roles
    pub async fn get_roles(&self) -> Vec<Role> {
        self.roles.read().await.values().cloned().collect()
    }

    /// Simple role assignment method
    pub async fn assign_role(&self, user_id: &str, role: &str) -> NetworkResult<()> {
        println!("🔑 Assigning role '{}' to user '{}'", role, user_id);
        // For demo purposes, just log the assignment
        Ok(())
    }

    /// Simple permission check method
    pub async fn check_permission(&self, user_id: &str, resource: &str, action: &str) -> NetworkResult<bool> {
        // Simple logic: admin can do everything, users can only read
        let allowed = if user_id.contains("admin") {
            true
        } else {
            action == "read"
        };

        println!("🔒 Permission check: User '{}' action '{}' on resource '{}' - {}",
                user_id, action, resource,
                if allowed { "ALLOWED" } else { "DENIED" });

        Ok(allowed)
    }

    /// Remove role from user
    pub async fn remove_role_from_user(&self, user_id: Uuid, role_id: Uuid) -> NetworkResult<()> {
        let mut user_roles = self.user_roles.write().await;
        if let Some(roles) = user_roles.get_mut(&user_id) {
            roles.retain(|&id| id != role_id);

            // Clear permission cache for this user
            let cache_key_prefix = format!("user:{}", user_id);
            let mut cache = self.permission_cache.write().await;
            cache.retain(|key, _| !key.starts_with(&cache_key_prefix));

            println!("✅ Removed role {} from user {}", role_id, user_id);
        }
        Ok(())
    }
}

impl ResourceType {
    fn cache_key(&self) -> String {
        match self {
            ResourceType::Topic => "topic".to_string(),
            ResourceType::Queue => "queue".to_string(),
            ResourceType::ConsumerGroup => "consumer_group".to_string(),
            ResourceType::Node => "node".to_string(),
            ResourceType::Broker => "broker".to_string(),
            ResourceType::Config => "config".to_string(),
            ResourceType::Metrics => "metrics".to_string(),
            ResourceType::Logs => "logs".to_string(),
            ResourceType::Schema => "schema".to_string(),
            ResourceType::Users => "users".to_string(),
            ResourceType::Policies => "policies".to_string(),
            ResourceType::Custom(name) => format!("custom:{}", name),
        }
    }
}
