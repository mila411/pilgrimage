use crate::broker::Broker;
use crate::message::Message;
use crate::message::MessageMetadata;
use crate::schema::message_schema::MessageSchema;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn test_message_send_receive() {
    let dir = tempdir().unwrap();
    let storage_path = dir.path().to_str().unwrap();
    let broker = Broker::new("test-broker", 3, 2, storage_path);

    // Send a test message
    let content = "Hello, World!";
    let metadata = MessageMetadata {
        id: uuid::Uuid::new_v4().to_string(),
        content: content.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        topic_id: Some("test-topic".to_string()),
        partition_id: Some(0),
        schema: None,
    };

    broker.send_message(metadata).unwrap();

    // Receive the message
    let received = broker.receive_message("test-topic", 0).unwrap();
    assert_eq!(received.content, content);
}

#[tokio::test]
async fn test_message_with_schema() {
    let dir = tempdir().unwrap();
    let storage_path = dir.path().to_str().unwrap();
    let broker = Broker::new("test-broker", 3, 2, storage_path);

    // Create a schema
    let mut schema = MessageSchema::new();
    schema.definition = r#"{"type":"object","properties":{"name":{"type":"string"}}}"#.to_string();

    // Send a test message with schema
    let content = r#"{"name":"test"}"#;
    let metadata = MessageMetadata {
        id: uuid::Uuid::new_v4().to_string(),
        content: content.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        topic_id: Some("test-topic".to_string()),
        partition_id: Some(0),
        schema: Some(schema),
    };

    broker.send_message(metadata).unwrap();

    // Receive the message
    let received = broker.receive_message("test-topic", 0).unwrap();
    assert_eq!(received.content, content);
}

#[tokio::test]
async fn test_message_acknowledgment() {
    let dir = tempdir().unwrap();
    let storage_path = dir.path().to_str().unwrap();
    let broker = Broker::new("test-broker", 3, 2, storage_path);

    // Create and send a message
    let message = Message::new("Test message".to_string())
        .with_topic("test-topic".to_string())
        .with_partition(0);

    let timeout = Duration::from_secs(5);
    let ack = broker.send_message_with_ack(message.clone(), timeout).await;

    assert!(ack.is_ok());
    let ack = ack.unwrap();
    assert_eq!(ack.message_id, message.id);
}
