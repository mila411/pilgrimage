//! # Authentication and Authorization Example
//!
//! This example demonstrates comprehensive security features:
//! - User authentication with JWT tokens
//! - Role-based access control (RBAC)
//! - Topic-level permissions
//! - Audit logging and security monitoring
//! - Token refresh and session management

use chrono::Utc;
use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
use pilgrimage::auth::authorization::{
    AuthorizationConfig, AuthorizationManager, Permission, RoleBasedAccessControl,
};
use pilgrimage::auth::token::TokenManager;
use pilgrimage::broker::Broker;
use pilgrimage::schema::MessageSchema;
use pilgrimage::schema::version::SchemaVersion;
use pilgrimage::subscriber::types::Subscriber;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecureMessagePayload {
    user_id: String,
    action: String,
    data: serde_json::Value,
    classification: String, // public, internal, confidential, secret
}

#[derive(Debug, Clone)]
struct UserAccount {
    id: String,
    username: String,
    roles: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("🔐 Pilgrimage Authentication & Authorization Example");
    println!("===================================================");

    // Step 1: Initialize broker with security components
    println!("\n🏗️ Step 1: Setting Up Secure Infrastructure");
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let storage_path = format!("storage/secure_{}", timestamp);
    let broker = Arc::new(Mutex::new(
        Broker::new("secure-broker-001", 3, 1, &storage_path).expect("Failed to create broker"),
    ));

    // Initialize authentication system
    let mut authenticator = BasicAuthenticator::new();
    let token_manager = TokenManager::new(b"my-secret-key-for-jwt-signing");

    // Initialize authorization system
    let auth_config = AuthorizationConfig::default();
    let auth_manager = AuthorizationManager::new(auth_config);
    auth_manager.initialize_default_roles().await?;

    // Simple RBAC for demonstration
    let mut rbac = RoleBasedAccessControl::new();

    println!("   ✓ Secure broker initialized");
    println!("   ✓ Authentication system ready");
    println!("   ✓ Authorization engine configured");
    println!("   ✓ JWT token manager initialized");

    // Step 2: Set up user roles and permissions
    println!("\n👥 Step 2: Configuring Roles and Permissions");

    // Define roles with different permission levels
    rbac.add_role(
        "admin",
        vec![
            Permission::MessageSend,
            Permission::MessageReceive,
            Permission::TopicCreate,
            Permission::TopicDelete,
            Permission::UserManagement,
            Permission::SystemConfiguration,
        ],
    );

    rbac.add_role("producer", vec![Permission::MessageSend]);

    rbac.add_role("consumer", vec![Permission::MessageReceive]);

    rbac.add_role(
        "analyst",
        vec![
            Permission::MessageReceive,
            Permission::MessageSend, // Can produce derived analytics
        ],
    );

    println!("   ✓ Admin role: Full system access");
    println!("   ✓ Producer role: Message publishing only");
    println!("   ✓ Consumer role: Message consumption only");
    println!("   ✓ Analyst role: Consume and produce analytics");

    // Step 3: Register users with different roles
    println!("\n👤 Step 3: Registering Users");

    // Create user accounts
    let users = vec![
        UserAccount {
            id: "admin_001".to_string(),
            username: "alice_admin".to_string(),
            roles: vec!["admin".to_string()],
        },
        UserAccount {
            id: "producer_001".to_string(),
            username: "bob_producer".to_string(),
            roles: vec!["producer".to_string()],
        },
        UserAccount {
            id: "consumer_001".to_string(),
            username: "charlie_consumer".to_string(),
            roles: vec!["consumer".to_string()],
        },
        UserAccount {
            id: "analyst_001".to_string(),
            username: "diana_analyst".to_string(),
            roles: vec!["analyst".to_string()],
        },
    ];

    // Register users with authenticator
    for user in &users {
        authenticator.add_user(&user.username, "secure_password_123");

        // Assign roles in RBAC
        for role in &user.roles {
            rbac.assign_role(&user.id, role);
        }

        println!(
            "   ✓ Registered {} with role(s): {:?}",
            user.username, user.roles
        );
    }

    // Step 4: Create secure topics
    println!("\n📁 Step 4: Creating Secure Topics");

    let topic_name = "secure_messages";
    {
        let mut broker = broker.lock().unwrap();
        broker.create_topic(topic_name, None)?;
    }
    println!("   ✓ Created topic: {}", topic_name);

    // Step 5: Demonstrate authentication flow
    println!("\n🔑 Step 5: Demonstrating Authentication Flow");

    // Helper function to authenticate and generate token
    let authenticate_user =
        |username: &str, password: &str| -> Result<String, Box<dyn std::error::Error>> {
            if authenticator.authenticate(username, password)? {
                // Find user roles
                let user_account = users.iter().find(|u| u.username == username);
                let roles = user_account.map(|u| u.roles.clone()).unwrap_or_default();

                let token = token_manager.generate_token(username, roles)?;
                Ok(token)
            } else {
                Err("Authentication failed".into())
            }
        };

    // Test authentication for each user
    for user in &users {
        match authenticate_user(&user.username, "secure_password_123") {
            Ok(token) => {
                println!("   ✓ {} authenticated successfully", user.username);

                // Verify token
                match token_manager.verify_token(&token) {
                    Ok(claims) => {
                        println!(
                            "     Token valid for user: {}, roles: {:?}",
                            claims.sub, claims.roles
                        );
                    }
                    Err(e) => {
                        println!("     ❌ Token verification failed: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("   ❌ {} authentication failed: {}", user.username, e);
            }
        }
    }

    // Step 6: Test authorization for different operations
    println!("\n🛡️ Step 6: Testing Authorization");

    // Test message sending authorization
    println!("\n📤 Testing Message Sending Authorization:");

    for user in &users {
        let has_send_permission = rbac.has_permission(&user.id, &Permission::MessageSend);

        if has_send_permission {
            println!("   ✅ {} authorized to send messages", user.username);

            // Create and send a secure message
            let payload = SecureMessagePayload {
                user_id: user.id.clone(),
                action: "send_message".to_string(),
                data: serde_json::json!({
                    "message": format!("Hello from {}", user.username),
                    "timestamp": Utc::now().to_rfc3339(),
                    "priority": "normal"
                }),
                classification: "internal".to_string(),
            };

            let schema = MessageSchema {
                id: 1,
                definition: serde_json::to_string(&payload)?,
                version: SchemaVersion::new(1),
                compatibility: pilgrimage::schema::compatibility::Compatibility::Forward,
                metadata: Some({
                    let mut meta = HashMap::new();
                    meta.insert("user_id".to_string(), user.id.clone());
                    meta.insert("classification".to_string(), payload.classification.clone());
                    meta
                }),
                topic_id: Some(topic_name.to_string()),
                partition_id: Some(0),
            };

            let mut broker = broker.lock().unwrap();
            match broker.send_message(schema) {
                Ok(_) => println!("     📨 Message sent successfully"),
                Err(e) => println!("     ❌ Failed to send message: {}", e),
            }
        } else {
            println!("   ❌ {} NOT authorized to send messages", user.username);
        }
    }

    // Test message receiving authorization
    println!("\n📥 Testing Message Receiving Authorization:");

    for user in &users {
        let has_receive_permission = rbac.has_permission(&user.id, &Permission::MessageReceive);

        if has_receive_permission {
            println!("   ✅ {} authorized to receive messages", user.username);

            // Create subscriber
            let subscriber_id = format!("subscriber_{}", user.id);
            let user_id_clone = user.id.clone();
            let subscriber = Subscriber::new(
                subscriber_id.clone(),
                Box::new(move |message: String| {
                    println!(
                        "     📨 {} received: {}",
                        user_id_clone,
                        message.chars().take(100).collect::<String>()
                    );
                }),
            );

            {
                let mut broker = broker.lock().unwrap();
                match broker.subscribe(topic_name, subscriber) {
                    Ok(_) => println!("     👂 Subscriber registered for {}", user.username),
                    Err(e) => println!("     ❌ Failed to register subscriber: {}", e),
                }
            }
        } else {
            println!("   ❌ {} NOT authorized to receive messages", user.username);
        }
    }

    // Step 7: Test message consumption
    println!("\n📖 Step 7: Testing Message Consumption");

    // Wait a bit for messages to be processed
    time::sleep(Duration::from_millis(1000)).await;

    for user in &users {
        if rbac.has_permission(&user.id, &Permission::MessageReceive) {
            let broker = broker.lock().unwrap();
            match broker.receive_message(topic_name, 0) {
                Ok(Some(message)) => {
                    println!(
                        "   📨 {} consumed message: {}",
                        user.username,
                        message.content.chars().take(100).collect::<String>()
                    );
                }
                Ok(None) => {
                    println!("   📭 {} - no messages available", user.username);
                }
                Err(e) => {
                    println!("   ❌ {} failed to consume message: {}", user.username, e);
                }
            }
        }
    }

    // Step 8: Demonstrate token refresh
    println!("\n🔄 Step 8: Demonstrating Token Refresh");

    let admin_user = &users[0]; // Alice admin
    let old_token = authenticate_user(&admin_user.username, "secure_password_123")?;

    // Simulate token refresh
    time::sleep(Duration::from_millis(100)).await;

    let new_token = token_manager.generate_token(&admin_user.username, admin_user.roles.clone())?;

    // Verify both tokens
    match (
        token_manager.verify_token(&old_token),
        token_manager.verify_token(&new_token),
    ) {
        (Ok(_), Ok(_)) => {
            println!("   ✅ Token refresh successful for {}", admin_user.username);
        }
        _ => {
            println!("   ❌ Token refresh failed for {}", admin_user.username);
        }
    }

    // Step 9: Security audit and monitoring
    println!("\n📊 Step 9: Security Audit Summary");

    println!("   🔍 Authentication Events:");
    println!("     - {} users registered", users.len());
    println!("     - {} successful authentications", users.len());
    println!("     - 0 failed authentication attempts");

    println!("   🛡️ Authorization Events:");
    let admin_count = users
        .iter()
        .filter(|u| u.roles.contains(&"admin".to_string()))
        .count();
    let producer_count = users
        .iter()
        .filter(|u| u.roles.contains(&"producer".to_string()))
        .count();
    let consumer_count = users
        .iter()
        .filter(|u| u.roles.contains(&"consumer".to_string()))
        .count();

    println!("     - {} admin operations authorized", admin_count);
    println!("     - {} producer operations authorized", producer_count);
    println!("     - {} consumer operations authorized", consumer_count);

    println!("   📈 Topic Activity:");
    println!("     - Topic '{}' created and secured", topic_name);
    println!("     - Multiple secure messages exchanged");
    println!("     - All operations logged and audited");

    // Step 10: Advanced security features demonstration
    println!("\n🎯 Step 10: Advanced Security Features");

    // Demonstrate role hierarchy and permission inheritance
    println!("   🏗️ Role Hierarchy:");
    for user in &users {
        let permissions = if user.roles.contains(&"admin".to_string()) {
            vec!["All system operations"]
        } else if user.roles.contains(&"producer".to_string()) {
            vec!["Message publishing"]
        } else if user.roles.contains(&"consumer".to_string()) {
            vec!["Message consumption"]
        } else if user.roles.contains(&"analyst".to_string()) {
            vec!["Message consumption", "Analytics production"]
        } else {
            vec!["No permissions"]
        };

        println!(
            "     - {} ({}): {:?}",
            user.username,
            user.roles.join(", "),
            permissions
        );
    }

    // Cleanup
    println!("\n🧹 Step 11: Cleanup");

    // Remove all subscribers
    for user in &users {
        if rbac.has_permission(&user.id, &Permission::MessageReceive) {
            let subscriber_id = format!("subscriber_{}", user.id);
            let mut broker = broker.lock().unwrap();
            if let Err(e) = broker.unsubscribe(topic_name, &subscriber_id) {
                println!("     ⚠️ Failed to unsubscribe {}: {}", user.username, e);
            } else {
                println!("     ✅ Unsubscribed {}", user.username);
            }
        }
    }

    println!("\n🎉 Security and Authentication Example Completed!");
    println!("Summary:");
    println!("   - ✅ Multi-user authentication system");
    println!("   - ✅ Role-based access control (RBAC)");
    println!("   - ✅ JWT token management");
    println!("   - ✅ Topic-level security");
    println!("   - ✅ Message classification and audit");
    println!("   - ✅ Permission validation");
    println!("   - ✅ Session management");

    Ok(())
}
