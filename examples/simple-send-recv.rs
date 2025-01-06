use pilgrimage::broker::{Broker, Message};
use std::fs;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    // Create the necessary directories and files
    let log_dir = "data/broker1/logs";
    let log_file = "data/broker1/logs/messages.log";
    let processed_message_ids_file = "data/broker1/logs/processed_message_ids.txt";

    fs::create_dir_all(log_dir).expect("Failed to create directory");
    fs::File::create(log_file).expect("Failed to create log file.");
    fs::File::create(processed_message_ids_file)
        .expect("Failed to create processed_message_ids.txt");

    // Checking the existence of a file
    if !std::path::Path::new(log_file).exists() {
        eprintln!("The log file does not exist.: {}", log_file);
        return;
    }
    if !std::path::Path::new(processed_message_ids_file).exists() {
        eprintln!(
            "processed_message_ids.txt does not exist: {}",
            processed_message_ids_file
        );
        return;
    }

    // Broker initialization
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, log_file).await));

    // Send a message
    let broker_producer = Arc::clone(&broker);
    let handle = tokio::spawn(async move {
        let broker_lock = broker_producer.lock().await;

        // Start transaction
        let mut transaction = broker_lock.begin_transaction();

        // Message generation
        let message_content = "Hello, Pilgrimage!".to_string();
        let message = Message::new(message_content.clone());

        // Send a message (send without waiting for ACK)
        match broker_lock
            .send_message_transaction(&mut transaction, message)
            .await
        {
            Ok(_) => {
                if let Err(e) = transaction.commit() {
                    eprintln!("Transaction commit failed.: {:?}", e);
                } else {
                    println!("Message sent.: {}", message_content);
                }
            }
            Err(e) => eprintln!("Failed to send message: {:?}", e),
        }
    });

    // Waiting for the transmission task to finish
    handle.await.unwrap();
}
