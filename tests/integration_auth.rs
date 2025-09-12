/// Integration Testing of Authentication and Authorization System
///
/// Tests the core authentication and authorization functionality
use pilgrimage::auth::audit::{AuditConfig, AuditLogger};
use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
use pilgrimage::auth::authorization::{AuthorizationConfig, AuthorizationManager};
use pilgrimage::auth::jwt_auth::DistributedAuthenticator;
use pilgrimage::auth::token::TokenManager;
use tempfile::tempdir;

#[tokio::test]
async fn test_basic_authentication() {
    // Test basic authentication functionality
    let mut authenticator = BasicAuthenticator::new();

    let username = "test_user";
    let password = "secure_password123";

    // Add user
    authenticator.add_user(username, password);

    // Test successful authentication
    let auth_result = authenticator.authenticate(username, password);
    assert!(
        auth_result.is_ok(),
        "Authentication failed: {:?}",
        auth_result.err()
    );
    assert!(auth_result.unwrap(), "Authentication should return true");

    // Test failed authentication with wrong password
    let wrong_auth_result = authenticator.authenticate(username, "wrong_password");
    assert!(wrong_auth_result.is_ok(), "Authentication should not error");
    assert!(
        !wrong_auth_result.unwrap(),
        "Authentication should return false for wrong password"
    );

    // Test authentication with non-existent user
    let nonexistent_auth_result = authenticator.authenticate("nonexistent_user", password);
    assert!(
        nonexistent_auth_result.is_ok(),
        "Authentication should not error"
    );
    assert!(
        !nonexistent_auth_result.unwrap(),
        "Authentication should return false for non-existent user"
    );
}

#[tokio::test]
async fn test_jwt_distributed_authentication() {
    // Test JWT distributed authentication
    let jwt_secret = b"test_secret_key_for_integration_tests".to_vec();
    let issuer = "test_issuer".to_string();
    let mut jwt_auth = DistributedAuthenticator::new(jwt_secret, issuer);

    // Add a user
    let username = "test_user";
    let password = "password123";
    jwt_auth.add_user(username, password);

    // Add user permissions
    jwt_auth.add_user_permissions(
        username.to_string(),
        vec!["read".to_string(), "write".to_string()],
    );

    // Test user authentication
    let auth_result = jwt_auth.authenticate_client(username, password);
    assert!(auth_result.success, "User authentication should succeed");
    assert!(auth_result.token.is_some(), "Token should be generated");
    assert!(
        !auth_result.permissions.is_empty(),
        "User should have permissions"
    );

    let token = auth_result.token.unwrap();

    // Test token validation
    let validation_result = jwt_auth.validate_token(&token);
    assert!(validation_result.valid, "Token validation should succeed");
    assert!(
        !validation_result.permissions.is_empty(),
        "Validation should return permissions"
    );

    // Test node authentication
    let node_id = "test_node";
    let cluster_id = "test_cluster";
    let node_secret = format!("{}:{}", node_id, cluster_id);

    jwt_auth.add_node_permissions(node_id.to_string(), vec!["cluster_admin".to_string()]);

    let node_auth_result = jwt_auth.authenticate_node(node_id, cluster_id, &node_secret);
    assert!(
        node_auth_result.success,
        "Node authentication should succeed"
    );
    assert!(
        node_auth_result.token.is_some(),
        "Node token should be generated"
    );
}

#[tokio::test]
async fn test_token_manager() {
    // Test token manager functionality
    let secret = b"test_secret_key_for_token_manager";
    let token_manager = TokenManager::new(secret);

    let user_id = "test_user";
    let roles = vec!["user".to_string(), "reader".to_string()];

    // Generate token
    let token_result = token_manager.generate_token(user_id, roles.clone());
    assert!(
        token_result.is_ok(),
        "Token generation failed: {:?}",
        token_result.err()
    );

    let token = token_result.unwrap();
    assert!(!token.is_empty(), "Generated token should not be empty");

    // Verify token
    let verify_result = token_manager.verify_token(&token);
    assert!(
        verify_result.is_ok(),
        "Token verification failed: {:?}",
        verify_result.err()
    );

    let claims = verify_result.unwrap();
    assert_eq!(claims.sub, user_id);
    assert_eq!(claims.roles, roles);

    // Test invalid token
    let invalid_verify_result = token_manager.verify_token("invalid_token");
    assert!(
        invalid_verify_result.is_err(),
        "Invalid token should fail verification"
    );
}

#[tokio::test]
async fn test_authorization_manager() {
    // Test authorization manager
    let config = AuthorizationConfig::default();
    let _auth_manager = AuthorizationManager::new(config);

    // Basic test to ensure the manager can be created
    println!("Authorization manager created successfully");

    // Note: More complex tests would require implementing specific role assignment
    // and permission checking methods that match the actual API
}

#[tokio::test]
async fn test_audit_logger() {
    // Test audit logger functionality
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let log_path = temp_dir.path().join("audit.log");

    let audit_config = AuditConfig {
        enabled: true,
        log_file: log_path.to_string_lossy().to_string(),
        rotation_size: 10 * 1024 * 1024, // 10MB
        max_files: 5,
        enable_streaming: false,
        buffer_size: 100,
        flush_interval: 1,
        enable_compression: false,
        retention_days: 30,
        enable_encryption: false,
        log_level: pilgrimage::auth::audit::AuditLogLevel::Info,
        enable_anomaly_detection: false,
    };

    let node_id = "test_node".to_string();
    let audit_logger_result = AuditLogger::new(audit_config, node_id);
    assert!(
        audit_logger_result.is_ok(),
        "Audit logger creation failed: {:?}",
        audit_logger_result.err()
    );

    let _audit_logger = audit_logger_result.unwrap();
    println!("Audit logger created successfully");

    // Cleanup
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
async fn test_integrated_auth_flow() {
    // Test integrated authentication flow
    let secret = b"integrated_test_secret";
    let token_manager = TokenManager::new(secret);
    let mut basic_auth = BasicAuthenticator::new();

    let username = "integrated_user";
    let password = "secure_password";
    let roles = vec!["user".to_string(), "admin".to_string()];

    // Step 1: Add user to basic authenticator
    basic_auth.add_user(username, password);

    // Step 2: Authenticate user
    let auth_result = basic_auth.authenticate(username, password);
    assert!(
        auth_result.is_ok() && auth_result.unwrap(),
        "Basic authentication failed"
    );

    // Step 3: Generate token for authenticated user
    let token_result = token_manager.generate_token(username, roles.clone());
    assert!(token_result.is_ok(), "Token generation failed");

    let token = token_result.unwrap();

    // Step 4: Verify token
    let verify_result = token_manager.verify_token(&token);
    assert!(verify_result.is_ok(), "Token verification failed");

    let claims = verify_result.unwrap();
    assert_eq!(claims.sub, username);
    assert_eq!(claims.roles, roles);

    println!("Integrated authentication flow completed successfully");
}

#[tokio::test]
async fn test_user_registration_and_authentication() {
    // Test user registration and authentication workflow
    let mut authenticator = BasicAuthenticator::new();

    // Test multiple users
    let users = vec![
        ("alice", "alice_password"),
        ("bob", "bob_password"),
        ("charlie", "charlie_password"),
    ];

    // Register users
    for (username, password) in &users {
        authenticator.add_user(username, password);
    }

    // Test authentication for all users
    for (username, password) in &users {
        let auth_result = authenticator.authenticate(username, password);
        assert!(
            auth_result.is_ok(),
            "Authentication failed for user: {}",
            username
        );
        assert!(
            auth_result.unwrap(),
            "User {} should be authenticated",
            username
        );
    }

    // Test cross-authentication (should fail)
    let cross_auth_result = authenticator.authenticate("alice", "bob_password");
    assert!(
        cross_auth_result.is_ok(),
        "Cross authentication should not error"
    );
    assert!(
        !cross_auth_result.unwrap(),
        "Cross authentication should fail"
    );

    println!("User registration and authentication test completed");
}
