use chrono::{DateTime, Utc};
use pilgrimage::message::message::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    time::Duration,
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
    fn new() -> Self {
        Self {
            processed_messages: HashMap::new(),
        }
    }

    fn load(path: &str) -> Self {
        File::open(path)
            .ok()
            .and_then(|f| {
                let reader = BufReader::new(f);
                serde_json::from_reader(reader).ok()
            })
            .unwrap_or_else(Self::new)
    }

    fn save(&self, path: &str) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }

    // Set the argument to &Uuid and convert it to a String internally
    fn is_processed(&self, message_id: &Uuid) -> bool {
        let key = message_id.to_string();
        self.processed_messages
            .get(&key)
            .map(|state| matches!(state.status, ProcessingStatus::Completed))
            .unwrap_or(false)
    }

    fn mark_processing(&mut self, message_id: Uuid, consumer_id: String) {
        let key = message_id.to_string();
        self.processed_messages.insert(key, MessageProcessingState {
            processed_at: Utc::now(),
            consumer_id,
            status: ProcessingStatus::Processing,
        });
    }

    fn mark_completed(&mut self, message_id: Uuid) {
        let key = message_id.to_string();
        if let Some(state) = self.processed_messages.get_mut(&key) {
            state.status = ProcessingStatus::Completed;
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
            sleep(Duration::from_millis(500 * (i + 1) as u64)).await;
            msg_clone.id
        });

        let processed_id = handle.await?;
        state.mark_completed(processed_id);
        state.save(state_path)?;
    }

    Ok(())
}
