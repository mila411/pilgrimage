use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct MessageProcessingState {
    processed_at: DateTime<Utc>,
    consumer_id: String,
    status: ProcessingStatus,
}

#[derive(Debug, Serialize, Deserialize)]
enum ProcessingStatus {
    Processing,
    Completed,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProcessedState {
    processed_messages: HashMap<String, MessageProcessingState>,
}

impl ProcessedState {
    fn load(path: &str) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap_or_else(|_| File::create(path).unwrap());
        let reader = BufReader::new(file);
        serde_json::from_reader(reader).unwrap_or_else(|_| ProcessedState {
            processed_messages: HashMap::new(),
        })
    }

    fn save(&self, path: &str) {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .unwrap();
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, self).unwrap();
    }

    fn is_processed(&self, message_id: &Uuid) -> bool {
        self.processed_messages
            .contains_key(&message_id.to_string())
    }

    fn mark_processing(&mut self, message_id: Uuid, consumer_id: String) {
        let key = message_id.to_string();
        let state = MessageProcessingState {
            processed_at: Utc::now(),
            consumer_id,
            status: ProcessingStatus::Processing,
        };
        self.processed_messages.insert(key, state);
    }

    fn mark_completed(&mut self, message_id: Uuid) {
        let key = message_id.to_string();
        if let Some(state) = self.processed_messages.get_mut(&key) {
            state.status = ProcessingStatus::Completed;
        }
    }
}

#[derive(Debug, Clone)]
struct Message {
    id: Uuid,
    content: String,
}

impl Message {
    fn new(content: String) -> Self {
        Message {
            id: Uuid::new_v4(),
            content,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state_path = "processed_messages.json";
    let mut state = ProcessedState::load(state_path);

    for i in 0..5 {
        let message = Message::new(format!("Message {}", i));
        let message_id = message.id;

        // Determine if it has already been processed
        if state.is_processed(&message_id) {
            println!(
                "Skip: Messages that have already been processed ID={}",
                message_id
            );
            continue;
        }

        state.mark_processing(message_id, format!("consumer{}", i % 3 + 1));
        println!("Send: ID={}, Content={}", message_id, message.content);

        let msg_clone = message.clone();
        let handle = tokio::spawn(async move {
            // Simulate message processing
            sleep(std::time::Duration::from_secs(1)).await;
            println!("ACK: Received for ID={}", msg_clone.id);
        });

        handle.await?;
        state.mark_completed(message_id);
        println!("ACK: Sent for ID={}", message_id);
    }

    // Checking for commutativity
    for i in 0..5 {
        let message = Message::new(format!("Message {}", i));
        let message_id = message.id;

        if state.is_processed(&message_id) {
            println!(
                "Checking for idempotence: The message has not been reprocessed. ID={}",
                message_id
            );
        } else {
            println!(
                "Checking for idempotence: The message is being reprocessed. ID={}",
                message_id
            );
        }
    }

    state.save(state_path);
    Ok(())
}
