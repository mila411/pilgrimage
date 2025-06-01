use pilgrimage::broker::{Broker, MessageSchema};
use tokio::time::{Duration, sleep};

// Auxiliary functions to display test results
fn display_test_result(test_name: &str, result: Result<(), String>) {
    match result {
        Ok(_) => println!("✅ test success: {}", test_name),
        Err(e) => println!("❌ test failure: {} - error: {}", test_name, e),
    }
}

// Testing of basic messaging functionality
async fn test_basic_messaging(broker: &mut Broker) -> Result<(), String> {
    println!("\n📝 Testing of basic messaging functionality begins....");

    let topic = "test-topic";
    // Delete and recreate existing topics
    let _ = broker.delete_topic(topic);
    broker.create_topic(topic, None)?;
    println!("Topic '{}' has been created.", topic);

    // Sending a message
    let message = MessageSchema::new()
        .with_content("test message".to_string())
        .with_topic(topic.to_string())
        .with_partition(0);

    broker.send_message(message)?;
    println!("Message sent.");

    // Wait a moment and wait for the message to be processed.
    sleep(Duration::from_millis(100)).await;

    // Attempt to receive a message
    if let Ok(Some(received)) = broker.receive_message(topic, 0) {
        println!("Message received: {}", received.content);
        Ok(())
    } else {
        Err("Message not received".to_string())
    }
}

// Transaction functionality testing
async fn test_transactions(broker: &mut Broker) -> Result<(), String> {
    println!("\n💼 Transaction functionality testing begins....");

    let topic = "test-topic";
    // Delete and recreate existing topics
    let _ = broker.delete_topic(topic);
    broker.create_topic(topic, None)?;
    println!("Topic '{}' has been created.", topic);

    broker.begin_transaction();
    println!("Transaction initiated.");

    // Sending a message
    let message1 = MessageSchema::new()
        .with_content("Transaction message 1".to_string())
        .with_topic("test-topic".to_string())
        .with_partition(0);

    let message2 = MessageSchema::new()
        .with_content("Transaction Message 2".to_string())
        .with_topic("test-topic".to_string())
        .with_partition(0);

    broker.send_message(message1)?;
    broker.send_message(message2)?;
    println!("Two messages were sent within the transaction");

    // Commit Transaction
    broker.commit_transaction()?;
    println!("Transaction committed.");

    // Check that messages are saved correctly
    if let Ok(Some(received1)) = broker.receive_message("test-topic", 0) {
        println!("Received 1st message: {}", received1.content);
        if let Ok(Some(received2)) = broker.receive_message("test-topic", 0) {
            println!("Second message received: {}", received2.content);
            Ok(())
        } else {
            Err("Second message not received".to_string())
        }
    } else {
        Err("The first message was not received.".to_string())
    }
}

// Testing the rollback function
async fn test_rollback(broker: &mut Broker) -> Result<(), String> {
    println!("\n🔄 Started testing rollback function...");

    let topic = "test-topic";
    // Delete and recreate existing topics
    let _ = broker.delete_topic(topic);
    broker.create_topic(topic, None)?;
    println!("Topic '{}' has been created.", topic);

    // Send a normal message first
    let normal_message = MessageSchema::new()
        .with_content("Normal message".to_string())
        .with_topic(topic.to_string())
        .with_partition(0);

    broker.send_message(normal_message)?;

    // Start transaction
    broker.begin_transaction();
    println!("Transaction initiated.");

    // Send messages within a transaction
    let message = MessageSchema::new()
        .with_content("Message to be rolled back".to_string())
        .with_topic(topic.to_string())
        .with_partition(0);

    broker.send_message(message)?;
    println!("Message sent within transaction");

    // Transaction rollback
    broker.rollback_transaction();
    println!("Transaction rolled back.");

    // Ensure that only normal messages remain
    if let Ok(Some(received)) = broker.receive_message(topic, 0) {
        if received.content == "Normal message" {
            println!("Only non-transactional messages remain");
            Ok(())
        } else {
            Err("Unexpected messages remain.".to_string())
        }
    } else {
        Err("Message not found".to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Begin comprehensive testing of brokers...\n");

    // Broker Initialization
    let mut broker = Broker::new("test-broker", 2, 1, "test_storage/broker.log");
    println!("Broker initialized.");

    // Test execution and result collection for each function
    let test_results = vec![
        ("Basic Messaging", test_basic_messaging(&mut broker).await),
        ("transaction", test_transactions(&mut broker).await),
        ("rollback", test_rollback(&mut broker).await),
    ];

    // Display of test results
    println!("\n📊 Test Result Summary:");
    for (name, result) in test_results {
        display_test_result(name, result);
    }

    Ok(())
}
