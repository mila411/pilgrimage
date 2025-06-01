use pilgrimage::broker::{Broker, MessageSchema};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// message receiving handler
fn start_message_receiver(broker: Arc<Mutex<Broker>>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            if let Ok(broker) = broker.lock() {
                if let Ok(Some(message)) = broker.receive_message("test_topic", 0) {
                    println!("Received: content={}", message.content);
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    })
}

// batch transaction processing
fn process_batch_transaction(broker: &Mutex<Broker>, messages: Vec<String>) -> Result<(), String> {
    let mut broker = broker.lock().map_err(|e| e.to_string())?;

    // Create Topic
    let _ = broker.delete_topic("test_topic");
    broker.create_topic("test_topic", None)?;

    // transaction initiation
    broker.begin_transaction();
    println!("Transaction start: {} messages", messages.len());

    // Send all messages
    for (index, content) in messages.into_iter().enumerate() {
        let message = MessageSchema::new()
            .with_content(content.clone())
            .with_topic("test_topic".to_string())
            .with_partition(0);

        println!("Message {}: content={}", index + 1, content);

        if let Err(e) = broker.send_message(message) {
            // Rollback in case of error
            broker.rollback_transaction();
            return Err(format!("Transmission error (message {}): {}", index + 1, e));
        }
    }

    // Commit Transaction
    if let Err(e) = broker.commit_transaction() {
        return Err(format!("Commit error: {}", e));
    }

    println!("transaction complete");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Broker Initialization
    let broker = Arc::new(Mutex::new(Broker::new(
        "broker1",
        3,
        2,
        "storage/batch_broker.log",
    )));

    // Start message receiving thread
    let _receiver = start_message_receiver(Arc::clone(&broker));

    // Message preparation for batch processing
    let messages = vec![
        "Order no: 001, Item: apple, Qty: 5".to_string(),
        "Order no: 002, Item: mandarin oranges, Qty: 3".to_string(),
        "Order no: 003, Item: bananas, Qty: 2".to_string(),
    ];

    // Execute batch transaction processing
    match process_batch_transaction(&broker, messages) {
        Ok(_) => println!("Batch processing completed successfully"),
        Err(e) => eprintln!("batch processing error: {}", e),
    }

    // Wait for receiving thread to complete processing
    thread::sleep(Duration::from_secs(2));
    Ok(())
}
