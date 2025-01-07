use pilgrimage::broker::Broker;
use pilgrimage::message::message::Message;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs")));

    // Receiving thread
    let broker_clone = Arc::clone(&broker);
    let _subscriber = thread::spawn(move || {
        loop {
            if let Ok(broker) = broker_clone.lock() {
                if let Some(message) = broker.receive_message() {
                    println!("Received: ID={}, Content={}", message.id, message.content);
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Transaction Processing
    {
        if let Ok(broker) = broker.lock() {
            broker.begin_transaction();

            let message = Message::new("Hello, world!".to_string());
            println!("Send: ID={}, Content={}", message.id, message.content);

            if let Err(e) = broker.send_message(message) {
                broker.rollback_transaction();
                println!("Transmission error: {}", e);
                return Ok(());
            }

            if let Err(e) = broker.commit_transaction() {
                println!("Commit error: {}", e);
                return Ok(());
            }
        }
    }

    thread::sleep(Duration::from_secs(1));
    Ok(())
}
