use pilgrimage::broker::{Broker, MessageSchema};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// Extract message reception processing as a separate function
fn start_message_receiver(broker: Arc<Mutex<Broker>>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            if let Ok(broker) = broker.lock() {
                if let Ok(Some(message)) = broker.receive_message("test_topic", 0) {
                    println!("Received: Contents={}", message.content);
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    })
}

// Extract transaction processing as a separate function
fn process_transaction(broker: &Mutex<Broker>, content: String) -> Result<(), String> {
    let mut broker = broker.lock().map_err(|e| e.to_string())?;

    // Create Topic
    let _ = broker.delete_topic("test_topic");
    broker.create_topic("test_topic", None)?;

    broker.begin_transaction();

    // Creating MessageSchema
    let message = MessageSchema::new()
        .with_content(content.clone())
        .with_topic("test_topic".to_string())
        .with_partition(0);

    println!("Send: Contents={}", content);

    // Transaction processing
    if let Err(e) = broker.send_message(message) {
        broker.rollback_transaction();
        return Err(format!("transmission error: {}", e));
    }

    if let Err(e) = broker.commit_transaction() {
        return Err(format!("commit error: {}", e));
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Broker Initialization
    let broker = Arc::new(Mutex::new(Broker::new(
        "broker1",
        3,
        2,
        "storage/improved_broker.log",
    )));

    // Start message receiving thread
    let _receiver = start_message_receiver(Arc::clone(&broker)); // Creating and Sending Messages
    let content = "Hello world!".to_string();

    // Transaction processing execution
    if let Err(e) = process_transaction(&broker, content) {
        eprintln!("transaction processing error: {}", e);
    }

    // Wait for receiving thread to complete processing
    thread::sleep(Duration::from_secs(1));
    Ok(())
}
