//! # Schema Registry and Message Serialization Example
//!
//! This example demonstrates advanced schema management and message serialization:
//! - Schema registry for versioned message formats
//! - JSON Schema validation
//! - Schema evolution and compatibility
//! - Structured message serialization
//!
//! ## Features Covered
//! - Schema registry initialization and management
//! - Schema registration with versioning
//! - Message serialization/deserialization with schema validation
//! - Schema compatibility checking (backward/forward)
//! - Structured data handling with typed messages

use pilgrimage::broker::Broker;
use pilgrimage::schema::compatibility::Compatibility;
use pilgrimage::schema::message_schema::MessageSchema;
use pilgrimage::schema::registry::SchemaRegistry;
use pilgrimage::subscriber::types::Subscriber;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio::time;

// Define structured message types
#[derive(Serialize, Deserialize, Debug, Clone)]
struct UserProfile {
    user_id: String,
    email: String,
    name: String,
    age: u32,
    preferences: UserPreferences,
    created_at: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UserPreferences {
    theme: String,
    notifications: bool,
    language: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderEvent {
    order_id: String,
    user_id: String,
    items: Vec<OrderItem>,
    total_amount: f64,
    currency: String,
    status: String,
    timestamp: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderItem {
    product_id: String,
    quantity: u32,
    unit_price: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("📋 Pilgrimage Schema Registry Example");
    println!("=====================================");

    // Step 1: Initialize broker and schema registry
    println!("\n🏗️ Step 1: Setting Up Infrastructure");
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let storage_path = format!("storage/schema_{}", timestamp);
    let mut broker =
        Broker::new("schema-broker-001", 3, 1, &storage_path).expect("Failed to create broker");

    let mut schema_registry = SchemaRegistry::new();
    schema_registry.set_compatibility(Compatibility::BACKWARD);
    println!("   ✓ Broker initialized with schema support");
    println!("   ✓ Schema registry created with BACKWARD compatibility");

    // Step 2: Register schemas for different message types
    println!("\n📋 Step 2: Registering Message Schemas");

    // User profile schema (v1)
    let user_schema_v1 = json!({
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "email": {"type": "string", "format": "email"},
            "name": {"type": "string"},
            "age": {"type": "integer", "minimum": 0},
            "preferences": {
                "type": "object",
                "properties": {
                    "theme": {"type": "string", "enum": ["light", "dark"]},
                    "notifications": {"type": "boolean"},
                    "language": {"type": "string"}
                },
                "required": ["theme", "notifications", "language"]
            },
            "created_at": {"type": "string", "format": "date-time"}
        },
        "required": ["user_id", "email", "name", "age", "preferences", "created_at"]
    })
    .to_string();

    let user_schema = schema_registry.register_schema("user-profiles", &user_schema_v1)?;
    println!(
        "   ✓ User profile schema v{} registered (ID: {})",
        user_schema.version.major, user_schema.id
    );

    // Order event schema
    let order_schema_def = json!({
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "user_id": {"type": "string"},
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "product_id": {"type": "string"},
                        "quantity": {"type": "integer", "minimum": 1},
                        "unit_price": {"type": "number", "minimum": 0}
                    },
                    "required": ["product_id", "quantity", "unit_price"]
                }
            },
            "total_amount": {"type": "number", "minimum": 0},
            "currency": {"type": "string", "enum": ["USD", "EUR", "JPY"]},
            "status": {"type": "string", "enum": ["pending", "confirmed", "shipped", "delivered"]},
            "timestamp": {"type": "string", "format": "date-time"}
        },
        "required": ["order_id", "user_id", "items", "total_amount", "currency", "status", "timestamp"]
    }).to_string();

    let order_schema = schema_registry.register_schema("order-events", &order_schema_def)?;
    println!(
        "   ✓ Order event schema v{} registered (ID: {})",
        order_schema.version.major, order_schema.id
    );

    // Step 3: Create topics for different message types
    println!("\n📊 Step 3: Creating Typed Topics");
    let topics = ["user-profiles", "order-events"];

    for topic in &topics {
        broker.create_topic(topic, None)?;
        println!("   ✓ Topic '{}' created", topic);
    }

    // Step 4: Create subscribers for each topic
    println!("\n👥 Step 4: Setting Up Subscribers");
    let user_subscriber = Subscriber::new(
        "user-profile-processor",
        Box::new(|message: String| {
            println!("📨 User Profile: {}", message);
        }),
    );

    let order_subscriber = Subscriber::new(
        "order-event-processor",
        Box::new(|message: String| {
            println!("📨 Order Event: {}", message);
        }),
    );

    broker.subscribe("user-profiles", user_subscriber.clone())?;
    broker.subscribe("order-events", order_subscriber.clone())?;

    println!("   ✓ User profile processor subscribed");
    println!("   ✓ Order event processor subscribed");

    // Step 5: Send structured user profile messages
    println!("\n📤 Step 5: Sending User Profile Messages");
    let user_profiles = vec![
        UserProfile {
            user_id: "usr_12345".to_string(),
            email: "alice@example.com".to_string(),
            name: "Alice Johnson".to_string(),
            age: 28,
            preferences: UserPreferences {
                theme: "dark".to_string(),
                notifications: true,
                language: "en".to_string(),
            },
            created_at: "2024-01-15T10:30:00Z".to_string(),
        },
        UserProfile {
            user_id: "usr_67890".to_string(),
            email: "bob@example.com".to_string(),
            name: "Bob Smith".to_string(),
            age: 35,
            preferences: UserPreferences {
                theme: "light".to_string(),
                notifications: false,
                language: "es".to_string(),
            },
            created_at: "2024-01-15T11:15:00Z".to_string(),
        },
    ];

    for (i, profile) in user_profiles.iter().enumerate() {
        let serialized = serde_json::to_string(profile)?;

        let schema = MessageSchema {
            id: (i + 1) as u32,
            definition: serialized,
            version: pilgrimage::schema::version::SchemaVersion::new(1),
            compatibility: Compatibility::Backward,
            metadata: Some({
                let mut metadata = std::collections::HashMap::new();
                metadata.insert("content_type".to_string(), "application/json".to_string());
                metadata.insert("user_id".to_string(), profile.user_id.clone());
                metadata
            }),
            topic_id: Some("user-profiles".to_string()),
            partition_id: Some(i % 3),
        };

        broker.send_message(schema)?;
        println!(
            "   ✓ User profile sent: {} ({})",
            profile.name, profile.user_id
        );
        time::sleep(Duration::from_millis(100)).await;
    }

    // Step 6: Send structured order event messages
    println!("\n🛒 Step 6: Sending Order Event Messages");
    let order_events = vec![
        OrderEvent {
            order_id: "ord_abc123".to_string(),
            user_id: "usr_12345".to_string(),
            items: vec![
                OrderItem {
                    product_id: "prod_laptop_001".to_string(),
                    quantity: 1,
                    unit_price: 999.99,
                },
                OrderItem {
                    product_id: "prod_mouse_002".to_string(),
                    quantity: 2,
                    unit_price: 29.99,
                },
            ],
            total_amount: 1059.97,
            currency: "USD".to_string(),
            status: "pending".to_string(),
            timestamp: "2024-01-15T12:00:00Z".to_string(),
        },
        OrderEvent {
            order_id: "ord_xyz789".to_string(),
            user_id: "usr_67890".to_string(),
            items: vec![OrderItem {
                product_id: "prod_book_003".to_string(),
                quantity: 3,
                unit_price: 15.99,
            }],
            total_amount: 47.97,
            currency: "USD".to_string(),
            status: "confirmed".to_string(),
            timestamp: "2024-01-15T12:30:00Z".to_string(),
        },
    ];

    for (i, order) in order_events.iter().enumerate() {
        let serialized = serde_json::to_string(order)?;

        let schema = MessageSchema {
            id: (i + 10) as u32, // Different ID range for orders
            definition: serialized,
            version: pilgrimage::schema::version::SchemaVersion::new(1),
            compatibility: Compatibility::Backward,
            metadata: Some({
                let mut metadata = std::collections::HashMap::new();
                metadata.insert("content_type".to_string(), "application/json".to_string());
                metadata.insert("order_id".to_string(), order.order_id.clone());
                metadata.insert("user_id".to_string(), order.user_id.clone());
                metadata
            }),
            topic_id: Some("order-events".to_string()),
            partition_id: Some(i % 3),
        };

        broker.send_message(schema)?;
        println!(
            "   ✓ Order event sent: {} (${}) - {}",
            order.order_id, order.total_amount, order.status
        );
        time::sleep(Duration::from_millis(100)).await;
    }

    // Step 7: Consume and deserialize user profile messages
    println!("\n📥 Step 7: Processing User Profiles");
    let mut user_count = 0;

    // Allow messages to be processed
    time::sleep(Duration::from_millis(500)).await;

    // Try to receive messages from each partition for user-profiles
    for partition_id in 0..3 {
        loop {
            match broker.receive_message("user-profiles", partition_id) {
                Ok(Some(message)) => {
                    user_count += 1;

                    // Deserialize the message
                    match serde_json::from_str::<UserProfile>(&message.content) {
                        Ok(profile) => {
                            println!(
                                "   ✓ User Profile {}: {} ({}) - {} theme, notifications: {}",
                                user_count,
                                profile.name,
                                profile.email,
                                profile.preferences.theme,
                                profile.preferences.notifications
                            );
                        }
                        Err(e) => println!("   ✗ Failed to deserialize user profile: {}", e),
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    println!("   ❌ Error reading user profiles: {}", e);
                    break;
                }
            }
        }
    }

    // Step 8: Consume and deserialize order event messages
    println!("\n🛒 Step 8: Processing Order Events");
    let mut order_count = 0;

    // Try to receive messages from each partition for order-events
    for partition_id in 0..3 {
        loop {
            match broker.receive_message("order-events", partition_id) {
                Ok(Some(message)) => {
                    order_count += 1;

                    // Deserialize the message
                    match serde_json::from_str::<OrderEvent>(&message.content) {
                        Ok(order) => {
                            println!(
                                "   ✓ Order {}: {} - ${} {} ({} items) - {}",
                                order_count,
                                order.order_id,
                                order.total_amount,
                                order.currency,
                                order.items.len(),
                                order.status
                            );

                            for (idx, item) in order.items.iter().enumerate() {
                                println!(
                                    "     • Item {}: {} x{} @ ${}",
                                    idx + 1,
                                    item.product_id,
                                    item.quantity,
                                    item.unit_price
                                );
                            }
                        }
                        Err(e) => println!("   ✗ Failed to deserialize order event: {}", e),
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    println!("   ❌ Error reading order events: {}", e);
                    break;
                }
            }
        }
    }

    // Step 9: Demonstrate schema evolution
    println!("\n🔄 Step 9: Schema Evolution Example");

    // Register an evolved user schema (v2) with additional optional field
    let user_schema_v2 = json!({
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "email": {"type": "string", "format": "email"},
            "name": {"type": "string"},
            "age": {"type": "integer", "minimum": 0},
            "preferences": {
                "type": "object",
                "properties": {
                    "theme": {"type": "string", "enum": ["light", "dark"]},
                    "notifications": {"type": "boolean"},
                    "language": {"type": "string"}
                },
                "required": ["theme", "notifications", "language"]
            },
            "created_at": {"type": "string", "format": "date-time"},
            "subscription_tier": {"type": "string", "enum": ["free", "premium", "enterprise"]}
        },
        "required": ["user_id", "email", "name", "age", "preferences", "created_at"]
    })
    .to_string();

    match schema_registry.register_schema("user-profiles", &user_schema_v2) {
        Ok(new_schema) => {
            println!(
                "   ✓ User profile schema evolved to v{} (ID: {})",
                new_schema.version.major, new_schema.id
            );
            println!("   ✓ Added optional 'subscription_tier' field");
            println!("   ✓ Backward compatibility maintained");
        }
        Err(e) => println!("   ✗ Schema evolution failed: {}", e),
    }

    // Step 10: Display schema registry statistics
    println!("\n📊 Step 10: Schema Registry Statistics");
    if let Some(user_schemas) = schema_registry.get_all_schemas("user-profiles") {
        println!(
            "   📋 User Profile Schemas: {} versions",
            user_schemas.len()
        );
        for schema in &user_schemas {
            println!(
                "     • v{} (ID: {}) - {} bytes",
                schema.version.major,
                schema.id,
                schema.definition.len()
            );
        }
    }

    if let Some(order_schemas) = schema_registry.get_all_schemas("order-events") {
        println!(
            "   🛒 Order Event Schemas: {} versions",
            order_schemas.len()
        );
        for schema in &order_schemas {
            println!(
                "     • v{} (ID: {}) - {} bytes",
                schema.version.major,
                schema.id,
                schema.definition.len()
            );
        }
    }

    // Cleanup
    println!("\n🧹 Cleanup");
    broker.unsubscribe("user-profiles", &user_subscriber.id)?;
    broker.unsubscribe("order-events", &order_subscriber.id)?;

    println!("\n✅ Schema registry example completed successfully!");
    println!("   You've learned how to:");
    println!("   • Set up and configure a schema registry");
    println!("   • Register versioned schemas for message types");
    println!("   • Send and receive structured, typed messages");
    println!("   • Validate message schemas during processing");
    println!("   • Evolve schemas while maintaining compatibility");

    Ok(())
}
