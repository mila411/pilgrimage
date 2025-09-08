//! Enhanced security features for network operations
//!
//! This module provides comprehensive security features including
//! certificate validation, mTLS, rate limiting, and intrusion detection.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use sha2::{Sha256, Digest};
use uuid::Uuid;

use crate::network::error::{NetworkError, NetworkResult};

/// Security configuration
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// Enable mutual TLS
    pub enable_mtls: bool,
    /// Certificate validation mode
    pub cert_validation: CertValidationMode,
    /// Rate limiting configuration
    pub rate_limiting: RateLimitConfig,
    /// Intrusion detection configuration
    pub intrusion_detection: IntrusionDetectionConfig,
    /// Security token lifetime
    pub token_lifetime: Duration,
    /// Maximum authentication attempts
    pub max_auth_attempts: u32,
    /// Audit logging enabled
    pub audit_logging: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enable_mtls: true,
            cert_validation: CertValidationMode::Strict,
            rate_limiting: RateLimitConfig::default(),
            intrusion_detection: IntrusionDetectionConfig::default(),
            token_lifetime: Duration::from_secs(24 * 60 * 60), // 24 hours
            max_auth_attempts: 3,
            audit_logging: true,
        }
    }
}

/// Certificate validation modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertValidationMode {
    /// Strict validation (recommended for production)
    Strict,
    /// Relaxed validation (development only)
    Relaxed,
    /// No validation (dangerous, test only)
    None,
}

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per IP per minute
    pub max_requests_per_minute: u32,
    /// Maximum connections per IP
    pub max_connections_per_ip: u32,
    /// Burst allowance
    pub burst_allowance: u32,
    /// Rate limit window
    pub window_duration: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_minute: 1000,
            max_connections_per_ip: 10,
            burst_allowance: 100,
            window_duration: Duration::from_secs(60),
        }
    }
}

/// Intrusion detection configuration
#[derive(Debug, Clone)]
pub struct IntrusionDetectionConfig {
    /// Enable anomaly detection
    pub enable_anomaly_detection: bool,
    /// Maximum failed authentication attempts before blocking
    pub max_failed_auth: u32,
    /// Block duration for suspicious IPs
    pub block_duration: Duration,
    /// Suspicious activity patterns to detect
    pub patterns: Vec<SuspiciousPattern>,
}

impl Default for IntrusionDetectionConfig {
    fn default() -> Self {
        Self {
            enable_anomaly_detection: true,
            max_failed_auth: 5,
            block_duration: Duration::from_secs(60 * 60), // 1 hour
            patterns: vec![
                SuspiciousPattern::RapidConnections,
                SuspiciousPattern::PortScanning,
                SuspiciousPattern::AuthenticationBruteForce,
            ],
        }
    }
}

/// Suspicious activity patterns
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuspiciousPattern {
    /// Too many connections in short time
    RapidConnections,
    /// Port scanning attempts
    PortScanning,
    /// Brute force authentication attempts
    AuthenticationBruteForce,
    /// Unusual message patterns
    UnusualMessagePatterns,
    /// Protocol violations
    ProtocolViolations,
}

/// Security token for authentication
#[derive(Debug, Clone)]
pub struct SecurityToken {
    /// Token ID
    pub id: Uuid,
    /// Node ID this token belongs to
    pub node_id: String,
    /// Token value (hash)
    pub token: String,
    /// Issued at timestamp
    pub issued_at: SystemTime,
    /// Expires at timestamp
    pub expires_at: SystemTime,
    /// Permissions granted by this token
    pub permissions: Vec<Permission>,
    /// Token scope
    pub scope: TokenScope,
}

/// Token permissions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    /// Read operations
    Read,
    /// Write operations
    Write,
    /// Administrative operations
    Admin,
    /// Cluster management
    ClusterManagement,
    /// Metrics access
    Metrics,
}

/// Token scope
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenScope {
    /// Node-to-node communication
    NodeToNode,
    /// Client access
    Client,
    /// Administrative access
    Admin,
    /// Service account
    Service,
}

impl SecurityToken {
    /// Create a new security token
    pub fn new(
        node_id: String,
        permissions: Vec<Permission>,
        scope: TokenScope,
        lifetime: Duration,
    ) -> Self {
        let id = Uuid::new_v4();
        let issued_at = SystemTime::now();
        let expires_at = issued_at + lifetime;

        // Generate token hash
        let mut hasher = Sha256::new();
        hasher.update(id.as_bytes());
        hasher.update(node_id.as_bytes());
        hasher.update(&issued_at.duration_since(UNIX_EPOCH).unwrap().as_secs().to_be_bytes());
        let token = format!("{:x}", hasher.finalize());

        Self {
            id,
            node_id,
            token,
            issued_at,
            expires_at,
            permissions,
            scope,
        }
    }

    /// Check if token is valid
    pub fn is_valid(&self) -> bool {
        SystemTime::now() < self.expires_at
    }

    /// Check if token has permission
    pub fn has_permission(&self, permission: &Permission) -> bool {
        self.permissions.contains(permission)
    }

    /// Get remaining lifetime
    pub fn remaining_lifetime(&self) -> Option<Duration> {
        if self.is_valid() {
            self.expires_at.duration_since(SystemTime::now()).ok()
        } else {
            None
        }
    }
}

/// Rate limiter for IP-based limiting
#[derive(Debug)]
pub struct IpRateLimiter {
    /// Configuration
    config: RateLimitConfig,
    /// Per-IP request counters
    request_counters: Arc<RwLock<HashMap<IpAddr, RequestCounter>>>,
    /// Per-IP connection counters
    connection_counters: Arc<RwLock<HashMap<IpAddr, u32>>>,
}

/// Request counter for rate limiting
#[derive(Debug, Clone)]
struct RequestCounter {
    count: u32,
    window_start: Instant,
    burst_used: u32,
}

impl IpRateLimiter {
    /// Create new IP rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            request_counters: Arc::new(RwLock::new(HashMap::new())),
            connection_counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if request is allowed for IP
    pub async fn is_request_allowed(&self, ip: IpAddr) -> bool {
        let mut counters = self.request_counters.write().await;
        let now = Instant::now();

        let counter = counters.entry(ip).or_insert_with(|| RequestCounter {
            count: 0,
            window_start: now,
            burst_used: 0,
        });

        // Reset window if expired
        if now.duration_since(counter.window_start) >= self.config.window_duration {
            counter.count = 0;
            counter.window_start = now;
            counter.burst_used = 0;
        }

        // Check rate limit
        if counter.count >= self.config.max_requests_per_minute {
            // Check burst allowance
            if counter.burst_used < self.config.burst_allowance {
                counter.burst_used += 1;
                true
            } else {
                false
            }
        } else {
            counter.count += 1;
            true
        }
    }

    /// Check if connection is allowed for IP
    pub async fn is_connection_allowed(&self, ip: IpAddr) -> bool {
        let counters = self.connection_counters.read().await;
        let current_connections = counters.get(&ip).copied().unwrap_or(0);

        current_connections < self.config.max_connections_per_ip
    }

    /// Record new connection for IP
    pub async fn record_connection(&self, ip: IpAddr) {
        let mut counters = self.connection_counters.write().await;
        *counters.entry(ip).or_insert(0) += 1;
    }

    /// Record connection closure for IP
    pub async fn record_connection_closed(&self, ip: IpAddr) {
        let mut counters = self.connection_counters.write().await;
        if let Some(count) = counters.get_mut(&ip) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                counters.remove(&ip);
            }
        }
    }

    /// Clean up old entries
    pub async fn cleanup(&self) {
        let now = Instant::now();
        let mut request_counters = self.request_counters.write().await;

        request_counters.retain(|_, counter| {
            now.duration_since(counter.window_start) < self.config.window_duration
        });
    }

    /// Start cleanup task
    pub fn start_cleanup_task(&self) {
        let request_counters = self.request_counters.clone();
        let window_duration = self.config.window_duration;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(window_duration);

            loop {
                interval.tick().await;

                let now = Instant::now();
                let mut counters = request_counters.write().await;

                counters.retain(|_, counter| {
                    now.duration_since(counter.window_start) < window_duration
                });
            }
        });
    }
}

/// Intrusion detection system
#[derive(Debug)]
pub struct IntrusionDetectionSystem {
    /// Configuration
    config: IntrusionDetectionConfig,
    /// Blocked IPs
    blocked_ips: Arc<RwLock<HashMap<IpAddr, Instant>>>,
    /// Failed authentication attempts
    failed_auth_attempts: Arc<RwLock<HashMap<IpAddr, VecDeque<Instant>>>>,
    /// Suspicious activity log
    activity_log: Arc<RwLock<Vec<SuspiciousActivity>>>,
}

/// Suspicious activity record
#[derive(Debug, Clone)]
pub struct SuspiciousActivity {
    pub ip: IpAddr,
    pub pattern: SuspiciousPattern,
    pub timestamp: Instant,
    pub details: String,
    pub severity: Severity,
}

/// Severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

impl IntrusionDetectionSystem {
    /// Create new intrusion detection system
    pub fn new(config: IntrusionDetectionConfig) -> Self {
        Self {
            config,
            blocked_ips: Arc::new(RwLock::new(HashMap::new())),
            failed_auth_attempts: Arc::new(RwLock::new(HashMap::new())),
            activity_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Check if IP is blocked
    pub async fn is_ip_blocked(&self, ip: IpAddr) -> bool {
        let blocked_ips = self.blocked_ips.read().await;

        if let Some(&blocked_at) = blocked_ips.get(&ip) {
            Instant::now().duration_since(blocked_at) < self.config.block_duration
        } else {
            false
        }
    }

    /// Block an IP address
    pub async fn block_ip(&self, ip: IpAddr, reason: String) {
        let mut blocked_ips = self.blocked_ips.write().await;
        blocked_ips.insert(ip, Instant::now());

        // Log suspicious activity
        self.log_suspicious_activity(
            ip,
            SuspiciousPattern::AuthenticationBruteForce,
            reason,
            Severity::High
        ).await;
    }

    /// Record failed authentication attempt
    pub async fn record_failed_auth(&self, ip: IpAddr) -> bool {
        let mut attempts = self.failed_auth_attempts.write().await;
        let now = Instant::now();

        let ip_attempts = attempts.entry(ip).or_insert_with(VecDeque::new);

        // Clean old attempts (older than 1 hour)
        while let Some(&front_time) = ip_attempts.front() {
            if now.duration_since(front_time) > Duration::from_secs(60 * 60) {
                ip_attempts.pop_front();
            } else {
                break;
            }
        }

        // Add new attempt
        ip_attempts.push_back(now);

        // Check if we should block
        if ip_attempts.len() >= self.config.max_failed_auth as usize {
            self.block_ip(ip, format!("Too many failed authentication attempts: {}", ip_attempts.len())).await;
            true
        } else {
            false
        }
    }

    /// Detect rapid connections
    pub async fn detect_rapid_connections(&self, ip: IpAddr, connection_count: u32, time_window: Duration) {
        if connection_count > 50 && time_window < Duration::from_secs(10) {
            self.log_suspicious_activity(
                ip,
                SuspiciousPattern::RapidConnections,
                format!("{} connections in {:?}", connection_count, time_window),
                Severity::Medium
            ).await;
        }
    }

    /// Log suspicious activity
    pub async fn log_suspicious_activity(
        &self,
        ip: IpAddr,
        pattern: SuspiciousPattern,
        details: String,
        severity: Severity,
    ) {
        let activity = SuspiciousActivity {
            ip,
            pattern,
            timestamp: Instant::now(),
            details,
            severity,
        };

        let mut log = self.activity_log.write().await;
        log.push(activity);

        // Keep only last 1000 entries
        if log.len() > 1000 {
            log.remove(0);
        }
    }

    /// Get recent suspicious activities
    pub async fn get_recent_activities(&self, since: Instant) -> Vec<SuspiciousActivity> {
        let log = self.activity_log.read().await;
        log.iter()
            .filter(|activity| activity.timestamp >= since)
            .cloned()
            .collect()
    }

    /// Clean up old blocked IPs
    pub async fn cleanup_blocked_ips(&self) {
        let mut blocked_ips = self.blocked_ips.write().await;
        let now = Instant::now();

        blocked_ips.retain(|_, &mut blocked_at| {
            now.duration_since(blocked_at) < self.config.block_duration
        });
    }

    /// Start cleanup task
    pub fn start_cleanup_task(&self) {
        let blocked_ips = self.blocked_ips.clone();
        let failed_auth_attempts = self.failed_auth_attempts.clone();
        let block_duration = self.config.block_duration;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10 * 60)); // 10 minutes

            loop {
                interval.tick().await;

                let now = Instant::now();

                // Clean blocked IPs
                {
                    let mut blocked = blocked_ips.write().await;
                    blocked.retain(|_, &mut blocked_at| {
                        now.duration_since(blocked_at) < block_duration
                    });
                }

                // Clean failed auth attempts
                {
                    let mut attempts = failed_auth_attempts.write().await;
                    for (_, ip_attempts) in attempts.iter_mut() {
                        while let Some(&front_time) = ip_attempts.front() {
                            if now.duration_since(front_time) > Duration::from_secs(60 * 60) {
                                ip_attempts.pop_front();
                            } else {
                                break;
                            }
                        }
                    }
                    attempts.retain(|_, attempts| !attempts.is_empty());
                }
            }
        });
    }
}

/// Security manager combining all security features
#[derive(Debug)]
pub struct SecurityManager {
    /// Configuration
    config: SecurityConfig,
    /// Rate limiter
    rate_limiter: IpRateLimiter,
    /// Intrusion detection system
    ids: IntrusionDetectionSystem,
    /// Active security tokens
    tokens: Arc<RwLock<HashMap<String, SecurityToken>>>,
}

impl SecurityManager {
    /// Create new security manager
    pub fn new(config: SecurityConfig) -> Self {
        let rate_limiter = IpRateLimiter::new(config.rate_limiting.clone());
        let ids = IntrusionDetectionSystem::new(config.intrusion_detection.clone());

        Self {
            config,
            rate_limiter,
            ids,
            tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Validate incoming connection
    pub async fn validate_connection(&self, ip: IpAddr) -> NetworkResult<()> {
        // Check if IP is blocked
        if self.ids.is_ip_blocked(ip).await {
            return Err(NetworkError::SecurityError(
                format!("IP {} is blocked due to suspicious activity", ip)
            ));
        }

        // Check rate limits
        if !self.rate_limiter.is_connection_allowed(ip).await {
            return Err(NetworkError::SecurityError(
                format!("Too many connections from IP {}", ip)
            ));
        }

        // Record connection
        self.rate_limiter.record_connection(ip).await;

        Ok(())
    }

    /// Validate request
    pub async fn validate_request(&self, ip: IpAddr) -> NetworkResult<()> {
        // Check rate limits
        if !self.rate_limiter.is_request_allowed(ip).await {
            return Err(NetworkError::SecurityError(
                format!("Rate limit exceeded for IP {}", ip)
            ));
        }

        Ok(())
    }

    /// Create security token
    pub async fn create_token(
        &self,
        node_id: String,
        permissions: Vec<Permission>,
        scope: TokenScope,
    ) -> SecurityToken {
        let token = SecurityToken::new(
            node_id.clone(),
            permissions,
            scope,
            self.config.token_lifetime,
        );

        // Store token
        {
            let mut tokens = self.tokens.write().await;
            tokens.insert(token.token.clone(), token.clone());
        }

        token
    }

    /// Validate security token
    pub async fn validate_token(&self, token_value: &str) -> NetworkResult<SecurityToken> {
        let tokens = self.tokens.read().await;

        if let Some(token) = tokens.get(token_value) {
            if token.is_valid() {
                Ok(token.clone())
            } else {
                Err(NetworkError::AuthenticationError("Token expired".to_string()))
            }
        } else {
            Err(NetworkError::AuthenticationError("Invalid token".to_string()))
        }
    }

    /// Revoke security token
    pub async fn revoke_token(&self, token_value: &str) -> bool {
        let mut tokens = self.tokens.write().await;
        tokens.remove(token_value).is_some()
    }

    /// Record failed authentication
    pub async fn record_failed_auth(&self, ip: IpAddr) -> bool {
        self.ids.record_failed_auth(ip).await
    }

    /// Start security background tasks
    pub fn start_background_tasks(&self) {
        self.rate_limiter.start_cleanup_task();
        self.ids.start_cleanup_task();

        // Token cleanup task
        let tokens = self.tokens.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60 * 60)); // 1 hour

            loop {
                interval.tick().await;

                let mut token_map = tokens.write().await;
                token_map.retain(|_, token| token.is_valid());
            }
        });
    }

    /// Get security statistics
    pub async fn get_security_stats(&self) -> SecurityStats {
        let blocked_ips = self.ids.blocked_ips.read().await;
        let tokens = self.tokens.read().await;
        let recent_activities = self.ids.get_recent_activities(
            Instant::now() - Duration::from_secs(24 * 60 * 60) // 24 hours
        ).await;

        SecurityStats {
            blocked_ips_count: blocked_ips.len(),
            active_tokens_count: tokens.len(),
            suspicious_activities_24h: recent_activities.len(),
            high_severity_activities: recent_activities.iter()
                .filter(|a| a.severity >= Severity::High)
                .count(),
        }
    }
}

/// Security statistics
#[derive(Debug, Clone)]
pub struct SecurityStats {
    pub blocked_ips_count: usize,
    pub active_tokens_count: usize,
    pub suspicious_activities_24h: usize,
    pub high_severity_activities: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_security_token_creation() {
        let token = SecurityToken::new(
            "node1".to_string(),
            vec![Permission::Read, Permission::Write],
            TokenScope::NodeToNode,
            Duration::from_secs(60 * 60), // 1 hour
        );

        assert!(token.is_valid());
        assert!(token.has_permission(&Permission::Read));
        assert!(token.has_permission(&Permission::Write));
        assert!(!token.has_permission(&Permission::Admin));
    }

    #[tokio::test]
    async fn test_ip_rate_limiter() {
        let config = RateLimitConfig {
            max_requests_per_minute: 5,
            max_connections_per_ip: 2,
            burst_allowance: 2,
            window_duration: Duration::from_secs(60),
        };

        let limiter = IpRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // Should allow initial requests
        for _ in 0..5 {
            assert!(limiter.is_request_allowed(ip).await);
        }

        // Should allow burst
        for _ in 0..2 {
            assert!(limiter.is_request_allowed(ip).await);
        }

        // Should deny further requests
        assert!(!limiter.is_request_allowed(ip).await);
    }

    #[tokio::test]
    async fn test_intrusion_detection() {
        let config = IntrusionDetectionConfig {
            max_failed_auth: 3,
            block_duration: Duration::from_secs(60),
            enable_anomaly_detection: true,
            patterns: vec![SuspiciousPattern::AuthenticationBruteForce],
        };

        let ids = IntrusionDetectionSystem::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));

        // Should not be blocked initially
        assert!(!ids.is_ip_blocked(ip).await);

        // Record failed attempts
        assert!(!ids.record_failed_auth(ip).await);
        assert!(!ids.record_failed_auth(ip).await);
        assert!(ids.record_failed_auth(ip).await); // Should trigger block

        // Should be blocked now
        assert!(ids.is_ip_blocked(ip).await);
    }

    #[tokio::test]
    async fn test_security_manager() {
        let config = SecurityConfig::default();
        let manager = SecurityManager::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        // Should validate clean connection
        assert!(manager.validate_connection(ip).await.is_ok());

        // Create and validate token
        let token = manager.create_token(
            "test_node".to_string(),
            vec![Permission::Read],
            TokenScope::Client,
        ).await;

        let validated = manager.validate_token(&token.token).await.unwrap();
        assert_eq!(validated.node_id, "test_node");

        // Revoke token
        assert!(manager.revoke_token(&token.token).await);
        assert!(manager.validate_token(&token.token).await.is_err());
    }
}
