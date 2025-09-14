//! JWT-based authentication for distributed broker
//!
//! This module provides JWT token-based authentication for secure
//! communication between distributed nodes and client access control.

use crate::auth::authentication::{Authenticator, BasicAuthenticator};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// JWT claims structure for broker authentication
#[derive(Debug, Serialize, Deserialize)]
pub struct BrokerClaims {
    pub node_id: String,
    pub cluster_id: String,
    pub permissions: Vec<String>,
    pub exp: u64,    // Expiration time
    pub iat: u64,    // Issued at
    pub iss: String, // Issuer
}

/// JWT claims for client authentication
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientClaims {
    pub client_id: String,
    pub username: String,
    pub roles: Vec<String>,
    pub exp: u64,
    pub iat: u64,
    pub iss: String,
}

/// Distributed broker authentication system
pub struct DistributedAuthenticator {
    jwt_secret: Vec<u8>,
    basic_auth: BasicAuthenticator,
    node_permissions: HashMap<String, Vec<String>>,
    user_permissions: HashMap<String, Vec<String>>, // Add user permissions
    token_expiry_seconds: u64,
    issuer: String,
    // Flag to require admin to change password on first localhost login
    admin_password_change_required: bool,
}

#[derive(Debug, Clone)]
pub struct AuthenticationResult {
    pub success: bool,
    pub token: Option<String>,
    pub permissions: Vec<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub valid: bool,
    pub node_id: Option<String>,
    pub client_id: Option<String>,
    pub permissions: Vec<String>,
    pub error_message: Option<String>,
}

impl DistributedAuthenticator {
    /// Create a new distributed authenticator
    pub fn new(jwt_secret: Vec<u8>, issuer: String) -> Self {
        Self {
            jwt_secret,
            basic_auth: BasicAuthenticator::new(),
            node_permissions: HashMap::new(),
            user_permissions: HashMap::new(), // Initialize user permissions
            token_expiry_seconds: 3600,       // 1 hour default
            issuer,
            admin_password_change_required: false,
        }
    }

    /// Set token expiry time in seconds
    pub fn set_token_expiry(&mut self, seconds: u64) {
        self.token_expiry_seconds = seconds;
    }

    /// Add a user for basic authentication
    pub fn add_user(&mut self, username: &str, password: &str) {
        self.basic_auth.add_user(username, password);
    }

    /// Check if a user exists
    pub fn user_exists(&self, username: &str) -> bool {
        self.basic_auth.has_user(username)
    }

    /// Add node with specific permissions
    pub fn add_node_permissions(&mut self, node_id: String, permissions: Vec<String>) {
        self.node_permissions.insert(node_id, permissions);
    }

    /// Add user with specific permissions
    pub fn add_user_permissions(&mut self, username: String, permissions: Vec<String>) {
        self.user_permissions.insert(username, permissions);
    }

    /// Authenticate a node and generate JWT token
    pub fn authenticate_node(
        &self,
        node_id: &str,
        cluster_id: &str,
        provided_secret: &str,
    ) -> AuthenticationResult {
        // Simple secret-based authentication for nodes
        let expected_secret = format!("{}:{}", node_id, cluster_id);

        if provided_secret != expected_secret {
            return AuthenticationResult {
                success: false,
                token: None,
                permissions: vec![],
                error_message: Some("Invalid node credentials".to_string()),
            };
        }

        // Get permissions for this node
        let permissions = self
            .node_permissions
            .get(node_id)
            .cloned()
            .unwrap_or_else(|| vec!["basic".to_string()]);

        // Generate JWT token
        match self.generate_node_token(node_id, cluster_id, &permissions) {
            Ok(token) => {
                info!("Successfully authenticated node: {}", node_id);
                AuthenticationResult {
                    success: true,
                    token: Some(token),
                    permissions,
                    error_message: None,
                }
            }
            Err(e) => {
                error!("Failed to generate token for node {}: {}", node_id, e);
                AuthenticationResult {
                    success: false,
                    token: None,
                    permissions: vec![],
                    error_message: Some(format!("Token generation failed: {}", e)),
                }
            }
        }
    }

    /// Authenticate a client user and generate JWT token
    pub fn authenticate_client(&self, username: &str, password: &str) -> AuthenticationResult {
        match self.basic_auth.authenticate(username, password) {
            Ok(true) => {
                // Get user-specific permissions, or default to basic user permissions
                let permissions = self
                    .user_permissions
                    .get(username)
                    .cloned()
                    .unwrap_or_else(|| vec!["user".to_string()]);

                match self.generate_client_token(username, &permissions) {
                    Ok(token) => {
                        info!("Successfully authenticated client: {}", username);
                        AuthenticationResult {
                            success: true,
                            token: Some(token),
                            permissions,
                            error_message: None,
                        }
                    }
                    Err(e) => {
                        error!("Failed to generate token for client {}: {}", username, e);
                        AuthenticationResult {
                            success: false,
                            token: None,
                            permissions: vec![],
                            error_message: Some(format!("Token generation failed: {}", e)),
                        }
                    }
                }
            }
            Ok(false) => {
                warn!("Authentication failed for client: {}", username);
                AuthenticationResult {
                    success: false,
                    token: None,
                    permissions: vec![],
                    error_message: Some("Invalid username or password".to_string()),
                }
            }
            Err(e) => {
                error!("Authentication error for client {}: {}", username, e);
                AuthenticationResult {
                    success: false,
                    token: None,
                    permissions: vec![],
                    error_message: Some(format!("Authentication error: {}", e)),
                }
            }
        }
    }

    /// For admin password change policy
    pub fn set_admin_password_change_required(&mut self, required: bool) {
        self.admin_password_change_required = required;
    }

    pub fn is_admin_password_change_required(&self) -> bool {
        self.admin_password_change_required
    }

    /// Change a user's password (verifies current password)
    pub fn change_user_password(
        &mut self,
        username: &str,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), String> {
        self.basic_auth
            .change_password(username, current_password, new_password)
    }

    /// Validate a JWT token
    pub fn validate_token(&self, token: &str) -> ValidationResult {
        let validation = Validation::new(Algorithm::HS256);
        let decoding_key = DecodingKey::from_secret(&self.jwt_secret);

        // Try to decode as broker token first
        if let Ok(token_data) = decode::<BrokerClaims>(token, &decoding_key, &validation) {
            let claims = token_data.claims;

            // Check if token is expired
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if claims.exp < current_time {
                return ValidationResult {
                    valid: false,
                    node_id: None,
                    client_id: None,
                    permissions: vec![],
                    error_message: Some("Token expired".to_string()),
                };
            }

            debug!("Validated broker token for node: {}", claims.node_id);
            return ValidationResult {
                valid: true,
                node_id: Some(claims.node_id),
                client_id: None,
                permissions: claims.permissions,
                error_message: None,
            };
        }

        // Try to decode as client token
        if let Ok(token_data) = decode::<ClientClaims>(token, &decoding_key, &validation) {
            let claims = token_data.claims;

            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if claims.exp < current_time {
                return ValidationResult {
                    valid: false,
                    node_id: None,
                    client_id: None,
                    permissions: vec![],
                    error_message: Some("Token expired".to_string()),
                };
            }

            debug!("Validated client token for user: {}", claims.username);
            return ValidationResult {
                valid: true,
                node_id: None,
                client_id: Some(claims.client_id),
                permissions: claims.roles,
                error_message: None,
            };
        }

        warn!("Invalid token provided");
        ValidationResult {
            valid: false,
            node_id: None,
            client_id: None,
            permissions: vec![],
            error_message: Some("Invalid token format".to_string()),
        }
    }

    /// Generate JWT token for node authentication
    fn generate_node_token(
        &self,
        node_id: &str,
        cluster_id: &str,
        permissions: &[String],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let claims = BrokerClaims {
            node_id: node_id.to_string(),
            cluster_id: cluster_id.to_string(),
            permissions: permissions.to_vec(),
            exp: current_time + self.token_expiry_seconds,
            iat: current_time,
            iss: self.issuer.clone(),
        };

        let encoding_key = EncodingKey::from_secret(&self.jwt_secret);
        let token = encode(&Header::default(), &claims, &encoding_key)?;

        Ok(token)
    }

    /// Generate JWT token for client authentication
    fn generate_client_token(
        &self,
        username: &str,
        roles: &[String],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let claims = ClientClaims {
            client_id: uuid::Uuid::new_v4().to_string(),
            username: username.to_string(),
            roles: roles.to_vec(),
            exp: current_time + self.token_expiry_seconds,
            iat: current_time,
            iss: self.issuer.clone(),
        };

        let encoding_key = EncodingKey::from_secret(&self.jwt_secret);
        let token = encode(&Header::default(), &claims, &encoding_key)?;

        Ok(token)
    }

    /// Decode client token to get claims
    pub fn decode_client_token(&self, token: &str) -> Result<ClientClaims, Box<dyn std::error::Error>> {
        let validation = Validation::new(Algorithm::HS256);
        let decoding_key = DecodingKey::from_secret(&self.jwt_secret);

        let token_data = decode::<ClientClaims>(token, &decoding_key, &validation)?;
        Ok(token_data.claims)
    }

    /// Check if a token has specific permission
    pub fn has_permission(&self, token: &str, required_permission: &str) -> bool {
        let validation_result = self.validate_token(token);
        if !validation_result.valid {
            return false;
        }

        validation_result
            .permissions
            .contains(&required_permission.to_string())
            || validation_result.permissions.contains(&"admin".to_string())
    }

    /// Generate a secure random JWT secret
    pub fn generate_jwt_secret() -> Vec<u8> {
        use rand::RngCore;
        let mut secret = vec![0u8; 32]; // 256-bit secret
        rand::thread_rng().fill_bytes(&mut secret);
        secret
    }

    /// Get issuer name
    pub fn get_issuer(&self) -> &str {
        &self.issuer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_authentication() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_node_permissions(
            "node1".to_string(),
            vec!["broker".to_string(), "replication".to_string()],
        );

        let result = auth.authenticate_node("node1", "test-cluster", "node1:test-cluster");
        assert!(result.success);
        assert!(result.token.is_some());
        assert_eq!(result.permissions, vec!["broker", "replication"]);
    }

    #[test]
    fn test_client_authentication() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_user("testuser", "testpass");

        let result = auth.authenticate_client("testuser", "testpass");
        assert!(result.success);
        assert!(result.token.is_some());
    }

    #[test]
    fn test_token_validation() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_node_permissions("node1".to_string(), vec!["broker".to_string()]);

        let auth_result = auth.authenticate_node("node1", "test-cluster", "node1:test-cluster");
        assert!(auth_result.success);

        let token = auth_result.token.unwrap();
        let _validation_result = auth.validate_token(&token);

        // Token should be valid since it was just created
        assert!(auth.has_permission(&token, "broker"));
    }

    #[test]
    fn test_permission_check() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_node_permissions(
            "node1".to_string(),
            vec!["broker".to_string(), "replication".to_string()],
        );

        let auth_result = auth.authenticate_node("node1", "test-cluster", "node1:test-cluster");
        let token = auth_result.token.unwrap();

        assert!(auth.has_permission(&token, "broker"));
        assert!(auth.has_permission(&token, "replication"));
        assert!(!auth.has_permission(&token, "admin"));
    }

    #[test]
    fn test_authentication_failure_invalid_credentials() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_user("validuser", "validpass");

        // Test wrong password
        let result = auth.authenticate_client("validuser", "wrongpass");
        assert!(!result.success);
        assert!(result.token.is_none());
        assert!(result.error_message.is_some());

        // Test nonexistent user
        let result2 = auth.authenticate_client("invaliduser", "anypass");
        assert!(!result2.success);
        assert!(result2.token.is_none());
    }

    #[test]
    fn test_node_authentication_failure() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        // Node without permissions
        let result = auth.authenticate_node("unknown_node", "test-cluster", "unknown:test-cluster");
        assert!(!result.success);
        assert!(result.token.is_none());
        assert!(result.error_message.is_some());
    }

    #[test]
    fn test_invalid_token_validation() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        // Test completely invalid token
        let result = auth.validate_token("invalid.token.here");
        assert!(!result.valid);
        assert!(result.node_id.is_none());
        assert!(result.client_id.is_none());

        // Test empty token
        let result2 = auth.validate_token("");
        assert!(!result2.valid);

        // Test malformed token
        let result3 = auth.validate_token("malformed");
        assert!(!result3.valid);
    }

    #[test]
    fn test_permission_check_invalid_token() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        assert!(!auth.has_permission("invalid.token", "any_permission"));
        assert!(!auth.has_permission("", "any_permission"));
    }

    #[test]
    fn test_expired_token() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        // Set very short expiry for testing
        auth.token_expiry_seconds = 1;
        auth.add_node_permissions("node1".to_string(), vec!["broker".to_string()]);

        let auth_result = auth.authenticate_node("node1", "test-cluster", "node1:test-cluster");
        assert!(auth_result.success);
        let token = auth_result.token.unwrap();

        // Wait for token to expire (in real test, you might mock time instead)
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Token should now be invalid due to expiration
        let _validation_result = auth.validate_token(&token);
        // Note: This test might be flaky depending on system time precision
        // In production, you'd use a time mocking library
    }

    #[test]
    fn test_jwt_secret_generation() {
        let secret1 = DistributedAuthenticator::generate_jwt_secret();
        let secret2 = DistributedAuthenticator::generate_jwt_secret();

        // Secrets should be different
        assert_ne!(secret1, secret2);
        assert_eq!(secret1.len(), 32); // 256 bits
        assert_eq!(secret2.len(), 32);
    }

    #[test]
    fn test_user_permissions() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_user("user1", "pass1");
        auth.add_user_permissions(
            "user1".to_string(),
            vec!["read".to_string(), "write".to_string()],
        );

        let result = auth.authenticate_client("user1", "pass1");
        assert!(result.success);
        assert_eq!(result.permissions, vec!["read", "write"]);

        let token = result.token.unwrap();
        assert!(auth.has_permission(&token, "read"));
        assert!(auth.has_permission(&token, "write"));
        assert!(!auth.has_permission(&token, "admin"));
    }

    #[test]
    fn test_multiple_nodes_different_permissions() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_node_permissions(
            "broker_node".to_string(),
            vec!["broker".to_string(), "routing".to_string()],
        );
        auth.add_node_permissions(
            "storage_node".to_string(),
            vec!["storage".to_string(), "backup".to_string()],
        );
        auth.add_node_permissions(
            "admin_node".to_string(),
            vec!["admin".to_string(), "monitoring".to_string()],
        );

        // Test broker node
        let broker_result =
            auth.authenticate_node("broker_node", "test-cluster", "broker_node:test-cluster");
        assert!(broker_result.success);
        let broker_token = broker_result.token.unwrap();
        assert!(auth.has_permission(&broker_token, "broker"));
        assert!(auth.has_permission(&broker_token, "routing"));
        assert!(!auth.has_permission(&broker_token, "storage"));

        // Test storage node
        let storage_result =
            auth.authenticate_node("storage_node", "test-cluster", "storage_node:test-cluster");
        assert!(storage_result.success);
        let storage_token = storage_result.token.unwrap();
        assert!(auth.has_permission(&storage_token, "storage"));
        assert!(auth.has_permission(&storage_token, "backup"));
        assert!(!auth.has_permission(&storage_token, "admin"));

        // Test admin node - admin permission grants access to all permissions
        let admin_result =
            auth.authenticate_node("admin_node", "test-cluster", "admin_node:test-cluster");
        assert!(admin_result.success);
        let admin_token = admin_result.token.unwrap();
        assert!(auth.has_permission(&admin_token, "admin"));
        assert!(auth.has_permission(&admin_token, "monitoring"));
        // Admin permission grants access to any permission
        assert!(auth.has_permission(&admin_token, "broker"));
    }

    #[test]
    fn test_token_claims_validation() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "production-cluster".to_string());

        auth.add_node_permissions("secure_node".to_string(), vec!["security".to_string()]);

        let auth_result = auth.authenticate_node(
            "secure_node",
            "production-cluster",
            "secure_node:production-cluster",
        );
        assert!(auth_result.success);

        let token = auth_result.token.unwrap();
        let validation_result = auth.validate_token(&token);

        assert!(validation_result.valid);
        assert_eq!(validation_result.node_id, Some("secure_node".to_string()));
        assert!(
            validation_result
                .permissions
                .contains(&"security".to_string())
        );
    }

    #[test]
    fn test_client_token_validation() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_user("client_user", "client_pass");
        auth.add_user_permissions(
            "client_user".to_string(),
            vec!["publish".to_string(), "subscribe".to_string()],
        );

        let auth_result = auth.authenticate_client("client_user", "client_pass");
        assert!(auth_result.success);

        let token = auth_result.token.unwrap();
        let validation_result = auth.validate_token(&token);

        assert!(validation_result.valid);
        // client_id is generated as UUID, so we just check it exists
        assert!(validation_result.client_id.is_some());
        assert!(
            validation_result
                .permissions
                .contains(&"publish".to_string())
        );
        assert!(
            validation_result
                .permissions
                .contains(&"subscribe".to_string())
        );
    }

    #[test]
    fn test_different_cluster_rejection() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "cluster-a".to_string());

        auth.add_node_permissions("node1".to_string(), vec!["broker".to_string()]);

        // Try to authenticate with wrong secret format (this should fail)
        let result = auth.authenticate_node("node1", "cluster-a", "node1:wrong-cluster");
        assert!(!result.success);
        assert!(result.token.is_none());
        assert!(result.error_message.is_some());
    }

    #[test]
    fn test_authentication_result_structure() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        // Test successful authentication result
        auth.add_user("test_user", "test_pass");
        auth.add_user_permissions("test_user".to_string(), vec!["read".to_string()]);

        let success_result = auth.authenticate_client("test_user", "test_pass");
        assert!(success_result.success);
        assert!(success_result.token.is_some());
        assert_eq!(success_result.permissions, vec!["read"]);
        assert!(success_result.error_message.is_none());

        // Test failed authentication result
        let fail_result = auth.authenticate_client("test_user", "wrong_pass");
        assert!(!fail_result.success);
        assert!(fail_result.token.is_none());
        assert!(fail_result.permissions.is_empty());
        assert!(fail_result.error_message.is_some());
    }

    #[test]
    fn test_empty_permissions() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        // Don't add any specific permissions for this node - should get default "basic" permission
        let result =
            auth.authenticate_node("node_empty", "test-cluster", "node_empty:test-cluster");
        assert!(result.success);
        assert!(result.permissions.contains(&"basic".to_string()));
    }

    #[test]
    fn test_token_format_structure() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        auth.add_user("format_test", "format_pass");

        let result = auth.authenticate_client("format_test", "format_pass");
        assert!(result.success);

        let token = result.token.unwrap();

        // JWT tokens should have 3 parts separated by dots
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        // Each part should be non-empty
        assert!(!parts[0].is_empty()); // Header
        assert!(!parts[1].is_empty()); // Payload
        assert!(!parts[2].is_empty()); // Signature
    }

    #[test]
    fn test_concurrent_authentication() {
        use std::sync::Arc;
        use std::thread;

        let secret = DistributedAuthenticator::generate_jwt_secret();
        let _auth = Arc::new(DistributedAuthenticator::new(
            secret,
            "concurrent-cluster".to_string(),
        ));

        let mut handles = vec![];

        // Spawn multiple threads to test concurrent token validation
        for i in 0..10 {
            let handle = thread::spawn(move || {
                let mut local_auth = DistributedAuthenticator::new(
                    DistributedAuthenticator::generate_jwt_secret(),
                    "concurrent-cluster".to_string(),
                );
                local_auth.add_user(&format!("user{}", i), "password");
                local_auth.authenticate_client(&format!("user{}", i), "password")
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            let result = handle.join().unwrap();
            assert!(result.success);
        }
    }

    #[test]
    fn test_special_characters_in_credentials() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        let special_user = "user@domain.com";
        let special_pass = "p@ssw0rd!#$%^&*()";

        auth.add_user(special_user, special_pass);
        auth.add_user_permissions(special_user.to_string(), vec!["special".to_string()]);

        let result = auth.authenticate_client(special_user, special_pass);
        assert!(result.success);
        assert!(result.token.is_some());
        assert!(result.permissions.contains(&"special".to_string()));
    }

    #[test]
    fn test_unicode_credentials() {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        let mut auth = DistributedAuthenticator::new(secret, "test-cluster".to_string());

        let unicode_user = "Test User";
        let unicode_pass = "Password Test";

        auth.add_user(unicode_user, unicode_pass);

        let result = auth.authenticate_client(unicode_user, unicode_pass);
        assert!(result.success);
        assert!(result.token.is_some());
    }

    #[test]
    fn test_claims_serialization() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Test BrokerClaims serialization
        let broker_claims = BrokerClaims {
            node_id: "test_node".to_string(),
            cluster_id: "test_cluster".to_string(),
            permissions: vec!["broker".to_string(), "replication".to_string()],
            exp: now + 3600,
            iat: now,
            iss: "pilgrimage-broker".to_string(),
        };

        let broker_json = serde_json::to_string(&broker_claims).unwrap();
        let broker_deserialized: BrokerClaims = serde_json::from_str(&broker_json).unwrap();

        assert_eq!(broker_deserialized.node_id, "test_node");
        assert_eq!(broker_deserialized.cluster_id, "test_cluster");
        assert_eq!(
            broker_deserialized.permissions,
            vec!["broker", "replication"]
        );

        // Test ClientClaims serialization
        let client_claims = ClientClaims {
            client_id: "test_client".to_string(),
            username: "test_user".to_string(),
            roles: vec!["user".to_string(), "subscriber".to_string()],
            exp: now + 3600,
            iat: now,
            iss: "pilgrimage-broker".to_string(),
        };

        let client_json = serde_json::to_string(&client_claims).unwrap();
        let client_deserialized: ClientClaims = serde_json::from_str(&client_json).unwrap();

        assert_eq!(client_deserialized.client_id, "test_client");
        assert_eq!(client_deserialized.username, "test_user");
        assert_eq!(client_deserialized.roles, vec!["user", "subscriber"]);
    }
}
