use pilgrimage::broker::Broker;
use pilgrimage::schema::message_schema::MessageSchema;

#[test]
fn test_simple_send_receive() {
    // Create a unique storage path for this test
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let storage_path = format!("test_storage_{}", timestamp);

    let mut broker = Broker::new("test-broker", 4, 2, &storage_path);

    // Create Topic
    broker
        .create_topic("test-topic", None)
        .expect("Topic creation failed");

    // Send a message
    let message = MessageSchema::new()
        .with_content("test message".to_string())
        .with_topic("test-topic".to_string())
        .with_partition(0);

    println!("Sending message...");
    broker
        .send_message(message)
        .expect("Failed to send message");

    println!("Receiving messages...");
    match broker.receive_message("test-topic", 0) {
        Ok(Some(message)) => {
            println!("Message received.: {}", message.content);
            assert!(message.content.contains("test message"));
        }
        Ok(None) => {
            panic!("Message not found");
        }
        Err(e) => {
            panic!("Error in receiving message: {:?}", e);
        }
    }

    // Attempt to receive again (should not get the same message)
    println!("Receiving message again...");
    match broker.receive_message("test-topic", 0) {
        Ok(Some(_)) => {
            panic!("Already consumed message retrieved again");
        }
        Ok(None) => {
            println!("As expected, messages already consumed were not retrieved");
        }
        Err(e) => {
            panic!("Error in receiving message: {:?}", e);
        }
    }

    // Clean up test file
    let _ = std::fs::remove_file(&storage_path);
}
