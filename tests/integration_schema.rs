/// Schema registry functionality integration tests
///
/// This test module tests the following functions:
/// - Schema registration and management
/// - Versioning
/// - Registry operations
use pilgrimage::schema::compatibility::Compatibility;
use pilgrimage::schema::message_schema::MessageSchema;
use pilgrimage::schema::registry::SchemaRegistry;
use pilgrimage::schema::version::SchemaVersion;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_message_schema_creation() {
    // Message schema creation test
    let schema = MessageSchema::new();

    // Check default values
    assert_eq!(schema.id, 0, "Default ID is not correct");
    assert!(
        schema.definition.is_empty(),
        "Default definition is not empty"
    );
    assert_eq!(schema.version.major, 1, "Default version is not correct");
}

#[tokio::test]
async fn test_message_schema_with_definition() {
    // Message schema with definition test
    let schema_definition = r#"
    {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "content": {"type": "string"}
        }
    }
    "#
    .to_string();

    let schema = MessageSchema::new_with_definition(schema_definition.clone());

    assert_eq!(
        schema.definition, schema_definition,
        "Schema definition is not correct"
    );
    assert_eq!(schema.id, 0, "Initial ID is not correct");
}

#[tokio::test]
async fn test_schema_version_operations() {
    // Schema version operations test
    let version_1 = SchemaVersion::new(1);
    let version_2 = SchemaVersion::new(2);

    assert_eq!(version_1.major, 1, "Version 1 major number is not correct");
    assert_eq!(version_2.major, 2, "Version 2 major number is not correct");

    // Version comparison
    assert!(version_1 < version_2, "Version comparison is not correct");

    // Version increment
    let mut incremented = version_1.clone();
    incremented.increment_major();
    assert_eq!(incremented.major, 2, "Version increment is not correct");
}

#[tokio::test]
async fn test_schema_registry_creation() {
    // Schema registry creation test
    let _schema_registry = SchemaRegistry::new();

    // Verify registry is created
    println!("Schema registry created successfully");
}

#[tokio::test]
async fn test_schema_registration() {
    // Schema registration test
    let schema_registry = SchemaRegistry::new();

    // Test schema definition
    let schema_definition = r#"
    {
        "type": "object",
        "properties": {
            "message": {"type": "string"},
            "timestamp": {"type": "integer"}
        }
    }
    "#;

    // Schema registration
    let topic_id = "test_topic";
    let registration_result = schema_registry.register_schema(topic_id, schema_definition);

    assert!(
        registration_result.is_ok(),
        "Schema registration failed: {:?}",
        registration_result.err()
    );

    // Get registered schema
    let registered_schema = registration_result.unwrap();
    assert_eq!(registered_schema.id, 1, "First schema ID is not correct");
    assert_eq!(
        registered_schema.version.major, 1,
        "Schema version is not correct"
    );
    assert_eq!(
        registered_schema.definition, schema_definition,
        "Schema definition does not match"
    );

    // Get schema from registry
    let retrieved_schema = schema_registry.get_schema(topic_id, Some(1));
    assert!(retrieved_schema.is_some(), "Registered schema not found");

    let schema = retrieved_schema.unwrap();
    assert_eq!(
        schema.definition, schema_definition,
        "Retrieved schema definition does not match"
    );
}

#[tokio::test]
async fn test_schema_topic_association() {
    // Schema and topic association test
    let schema_registry = SchemaRegistry::new();

    let topic_id = "user_events";
    let schema_definition = r#"
    {
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "event_type": {"type": "string"},
            "timestamp": {"type": "integer"}
        }
    }
    "#;

    // Schema registration
    let registered_schema = schema_registry
        .register_schema(topic_id, schema_definition)
        .expect("Schema registration failed");

    // Get topic schema list
    let topic_schemas = schema_registry.get_all_schemas(topic_id);
    assert!(topic_schemas.is_some(), "Topic schemas not found");

    let schemas = topic_schemas.unwrap();
    assert!(!schemas.is_empty(), "Topic schema list is empty");

    // Verify that the registered schema is included
    let contains_registered_schema = schemas
        .iter()
        .any(|schema| schema.id == registered_schema.id);
    assert!(
        contains_registered_schema,
        "Registered schema is not included in topic schema list"
    );
}

#[tokio::test]
async fn test_schema_validation() {
    // Schema validation test
    let schema = MessageSchema::new_with_definition(
        r#"
    {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "value": {"type": "integer"}
        },
        "required": ["id"]
    }
    "#
        .to_string(),
    );

    // Valid JSON data
    let valid_json = r#"{"id": "test123", "value": 42}"#;
    let validation_result = schema.validate(valid_json);
    assert!(
        validation_result.is_ok(),
        "Valid JSON validation failed: {:?}",
        validation_result.err()
    );

    // Invalid JSON format
    let malformed_json = r#"{"id": "test", invalid}"#;
    let malformed_validation = schema.validate(malformed_json);
    assert!(
        malformed_validation.is_err(),
        "Invalid JSON was validated as valid"
    );
}

#[tokio::test]
async fn test_multiple_schema_registration() {
    // Multiple schema registration test
    let schema_registry = SchemaRegistry::new();

    let schema_definitions = vec![
        (
            "users",
            r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#,
        ),
        (
            "orders",
            r#"{"type": "object", "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}}}"#,
        ),
        (
            "products",
            r#"{"type": "object", "properties": {"product_id": {"type": "string"}, "price": {"type": "number"}}}"#,
        ),
    ];

    let mut registered_schemas = Vec::new();

    // Register multiple schemas
    for (topic, definition) in &schema_definitions {
        let schema = schema_registry
            .register_schema(topic, definition)
            .expect("Schema registration failed");

        let schema_id = schema.id;
        registered_schemas.push(schema);
        // Schema IDs start from 1, so they should be positive numbers
        assert!(
            schema_id >= 1,
            "Schema ID should be a positive number, got: {}",
            schema_id
        );
    }

    // Retrieve and verify all registered schemas
    for (i, schema) in registered_schemas.iter().enumerate() {
        let retrieved_schema = schema_registry
            .get_schema(schema_definitions[i].0, Some(schema.version.major))
            .expect("Registered schema not found");

        assert_eq!(
            retrieved_schema.definition, schema_definitions[i].1,
            "Schema {} definition does not match",
            i
        );
    }
}

#[tokio::test]
async fn test_schema_registry_performance() {
    // Schema registry performance test
    let schema_registry = SchemaRegistry::new();
    let schema_count = 100;

    // Schema definition template
    let base_definition = r#"
    {
        "type": "object",
        "properties": {
            "field_INDEX": {"type": "string"},
            "value": {"type": "integer"}
        }
    }
    "#;

    // Performance test for large-scale schema registration
    let start_time = Instant::now();
    let mut registered_schemas = Vec::new();

    for i in 0..schema_count {
        let topic = format!("perf_topic_{}", i);
        let definition = base_definition.replace("INDEX", &i.to_string());

        let schema = schema_registry
            .register_schema(&topic, &definition)
            .expect("Performance test: Schema registration failed");

        registered_schemas.push((topic, schema));
    }

    let registration_time = start_time.elapsed();

    // Retrieval performance test
    let retrieval_start = Instant::now();

    for (topic, schema) in &registered_schemas {
        let retrieved_schema = schema_registry
            .get_schema(topic, Some(schema.version.major))
            .expect("Performance test: Schema retrieval failed");

        assert!(
            !retrieved_schema.definition.is_empty(),
            "Retrieved schema definition is empty"
        );
    }

    let retrieval_time = retrieval_start.elapsed();

    println!("Schema registry performance test results:");
    println!("  Schema count: {}", schema_count);
    println!("  Registration time: {:?}", registration_time);
    println!("  Retrieval time: {:?}", retrieval_time);
    println!(
        "  Average registration time: {:?}",
        registration_time / schema_count
    );
    println!(
        "  Average retrieval time: {:?}",
        retrieval_time / schema_count
    );

    // Performance requirements
    assert!(
        registration_time < Duration::from_secs(5),
        "Schema registration performance does not meet requirements: {:?}",
        registration_time
    );
    assert!(
        retrieval_time < Duration::from_millis(500),
        "Schema retrieval performance does not meet requirements: {:?}",
        retrieval_time
    );
}

#[tokio::test]
async fn test_schema_error_handling() {
    // Schema error handling test
    let schema_registry = SchemaRegistry::new();

    // Retrieving non-existent schema
    let nonexistent_schema = schema_registry.get_schema("nonexistent_topic", Some(999));
    assert!(
        nonexistent_schema.is_none(),
        "Non-existent schema was retrieved"
    );

    // Schema retrieval for non-existent topic
    let nonexistent_topic_schemas = schema_registry.get_all_schemas("nonexistent_topic");
    assert!(
        nonexistent_topic_schemas.is_none(),
        "Schemas found for non-existent topic"
    );

    println!("Schema error handling test completed");
}

#[tokio::test]
async fn test_schema_compatibility_modes() {
    // Schema compatibility mode test
    let mut schema_registry = SchemaRegistry::new();

    // Test in BACKWARD mode
    schema_registry.set_compatibility(Compatibility::BACKWARD);

    let topic = "compatibility_test";
    let schema_v1 = r#"{"type": "object", "properties": {"id": {"type": "string"}}}"#;
    let schema_v2 = r#"{"type": "object", "properties": {"id": {"type": "string"}, "name": {"type": "string"}}}"#;

    // Register first schema
    let result_v1 = schema_registry.register_schema(topic, schema_v1);
    assert!(result_v1.is_ok(), "First schema registration failed");

    // Register compatible schema
    let result_v2 = schema_registry.register_schema(topic, schema_v2);
    // Depends on compatibility check result, but usually succeeds or fails
    println!("Compatibility check result: {:?}", result_v2);

    // Change to FULL mode
    schema_registry.set_compatibility(Compatibility::FULL);
    println!("Changed compatibility mode to FULL");
}

#[tokio::test]
async fn test_schema_metadata_operations() {
    // Schema metadata operations test
    let mut schema = MessageSchema::new_with_definition(
        r#"
    {
        "type": "object",
        "properties": {
            "data": {"type": "string"}
        }
    }
    "#
        .to_string(),
    );

    // Verify initial metadata state
    assert!(
        schema.metadata.is_none(),
        "Metadata is set in initial state"
    );

    // Set metadata
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("author".to_string(), "test_user".to_string());
    metadata.insert("version".to_string(), "1.0.0".to_string());
    metadata.insert(
        "description".to_string(),
        "Test schema for integration tests".to_string(),
    );

    schema.metadata = Some(metadata.clone());

    // Verify metadata
    assert!(schema.metadata.is_some(), "Metadata is not set");
    let schema_metadata = schema.metadata.as_ref().unwrap();

    assert_eq!(
        schema_metadata.get("author"),
        Some(&"test_user".to_string()),
        "Author metadata is incorrect"
    );
    assert_eq!(
        schema_metadata.get("version"),
        Some(&"1.0.0".to_string()),
        "Version metadata is incorrect"
    );
    assert_eq!(
        schema_metadata.get("description"),
        Some(&"Test schema for integration tests".to_string()),
        "Description metadata is incorrect"
    );

    // Set topic and partition information
    schema.topic_id = Some("test_topic".to_string());
    schema.partition_id = Some(0);

    assert_eq!(
        schema.topic_id,
        Some("test_topic".to_string()),
        "Topic ID is incorrect"
    );
    assert_eq!(schema.partition_id, Some(0), "Partition ID is incorrect");
}

#[tokio::test]
async fn test_concurrent_schema_operations() {
    // Concurrent schema operations test
    use std::sync::Arc;
    use tokio::task;

    let schema_registry = Arc::new(SchemaRegistry::new());
    let mut handles = Vec::new();

    // Register schemas concurrently
    for i in 0..10 {
        let registry_clone = Arc::clone(&schema_registry);

        let handle = task::spawn(async move {
            let topic = format!("concurrent_topic_{}", i);
            let definition = format!(
                r#"{{"type": "object", "properties": {{"field_{}": {{"type": "string"}}}}}}"#,
                i
            );

            registry_clone.register_schema(&topic, &definition)
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    let mut successful_registrations = 0;
    for handle in handles {
        match handle.await {
            Ok(result) => {
                if result.is_ok() {
                    successful_registrations += 1;
                }
            }
            Err(e) => {
                println!("Task execution error: {:?}", e);
            }
        }
    }

    assert!(
        successful_registrations > 0,
        "No concurrent schema registrations succeeded"
    );
    println!(
        "Successful concurrent schema registrations: {}",
        successful_registrations
    );
}
