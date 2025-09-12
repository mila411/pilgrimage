//! Security system integration test
//!
//! This test verifies that all security components work together correctly.

use pilgrimage::auth::audit::{AuditLogger, AuditConfig, AuditEvent, AuditEventType, EventOutcome, AuditLogLevel};
use pilgrimage::auth::authorization::{AuthorizationManager, AuthorizationConfig, AuthorizationContext, Role, Permission};
use pilgrimage::network::tls::{TlsManager, TlsConfig};

use std::collections::HashMap;
use chrono::Utc;
use uuid::Uuid;
use tempfile::TempDir;

#[tokio::test]
async fn test_security_integration() {
    // Setup temporary directory for logs
    let temp_dir = TempDir::new().unwrap();
    let log_file = temp_dir.path().join("security_test.log").to_string_lossy().to_string();

    // Test TLS Manager
    println!("Testing TLS Manager...");
    let tls_config = TlsConfig {
        enabled: false, // Disable for testing without certificates
        ..TlsConfig::default()
    };
    let tls_manager = TlsManager::new(tls_config).await;
    assert!(tls_manager.is_ok(), "TLS Manager should initialize successfully");
    println!("✅ TLS Manager initialized");

    // Test Authorization Manager
    println!("Testing Authorization Manager...");
    let auth_config = AuthorizationConfig::default();
    let auth_manager = AuthorizationManager::new(auth_config);

    // Initialize default roles
    auth_manager.initialize_default_roles().await.unwrap();

    // Create a test role
    let mut test_role = Role::new("test_role".to_string(), "Test role for security integration".to_string());
    test_role.add_permission(Permission::MessageSend);
    test_role.add_permission(Permission::MessageReceive);

    auth_manager.create_role(test_role).await.unwrap();
    auth_manager.assign_role("test_user", "test_role", None).await.unwrap();

    // Test authorization
    let auth_context = AuthorizationContext {
        user_id: "test_user".to_string(),
        user_info: None,
        resource: "test_queue".to_string(),
        action: "send".to_string(),
        source_ip: Some("127.0.0.1".to_string()),
        request_time: Utc::now(),
        session_id: Some(Uuid::new_v4().to_string()),
        request_id: Some(Uuid::new_v4().to_string()),
        attributes: HashMap::new(),
    };

    let decision = auth_manager.check_permission(auth_context).await.unwrap();
    assert!(decision.allowed, "User should have permission to send messages");
    println!("✅ Authorization Manager working correctly");

    // Test Audit Logger
    println!("Testing Audit Logger...");
    let audit_config = AuditConfig {
        enabled: true,
        log_file,
        buffer_size: 1, // Force immediate flush for testing
        ..AuditConfig::default()
    };

    let audit_logger = AuditLogger::new(audit_config, "test-node".to_string()).unwrap();

    // Log test events
    let test_event = AuditEvent {
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
        description: "Test authentication event".to_string(),
        details: HashMap::new(),
        severity: AuditLogLevel::Info,
        session_id: Some(Uuid::new_v4().to_string()),
        request_id: Some(Uuid::new_v4().to_string()),
        node_id: "test-node".to_string(),
    };

    audit_logger.log_event(test_event).await.unwrap();

    // Test authentication logging
    let mut details = HashMap::new();
    details.insert("test_key".to_string(), serde_json::Value::String("test_value".to_string()));
    audit_logger.log_authentication("test_user", EventOutcome::Success, details).await.unwrap();

    // Test authorization logging
    audit_logger.log_authorization("test_user", "test_resource", "test_action", &decision).await.unwrap();

    // Flush audit events
    audit_logger.flush().await.unwrap();

    // Verify statistics
    let stats = audit_logger.get_statistics().await;
    assert!(stats.total_events >= 3, "Should have at least 3 audit events");
    println!("✅ Audit Logger working correctly");

    println!("🎯 Security Integration Test Completed Successfully!");
    println!("   - TLS Manager: ✅ Initialized");
    println!("   - Authorization Manager: ✅ Role management and permission checking");
    println!("   - Audit Logger: ✅ Event logging and statistics");
    println!("   - Total Audit Events: {}", stats.total_events);
}
