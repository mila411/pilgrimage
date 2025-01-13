use chrono::Utc;
use pilgrimage::broker::Broker;
use pilgrimage::message::ack::{AckStatus, MessageAck};
use pilgrimage::message::message::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    time::Duration,
};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct ProcessedState {
    processed_ids: HashSet<Uuid>,
}

impl ProcessedState {
    fn new() -> Self {
        Self {
            processed_ids: HashSet::new(),
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state_path = "processed_messages.json";
    let mut state = ProcessedState::load(state_path);

    let mut broker = Broker::new("broker1", 3, 2, "logs");
    broker.create_topic("test_topic", None)?;

    let mut handles = vec![];

    for i in 0..5 {
        let message = Message::new(format!("Message {}", i));
        let message_id = message.id;

        if state.processed_ids.contains(&message_id) {
            println!(
                "Skip: Messages that have already been processed ID={}",
                message_id
            );
            continue;
        }

        println!("Send: ID={}, Content={}", message_id, message.content);
        let msg_clone = message.clone();

        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(500 * (i + 1) as u64)).await;

            let ack = MessageAck::new(
                msg_clone.id,
                Utc::now(),
                AckStatus::Processed,
                format!("consumer{}", i % 3 + 1),
                0,
            );
            println!("ACK issued: {:?}", ack);

            msg_clone.id
        });
        handles.push(handle);
    }

    for handle in handles {
        let processed_id = handle.await?;
        state.processed_ids.insert(processed_id);
    }

    state.save(state_path)?;
    println!("The processed message has been saved.: {}", state_path);

    Ok(())
}
