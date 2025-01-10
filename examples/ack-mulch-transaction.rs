use chrono::Utc;
use pilgrimage::broker::Broker;
use pilgrimage::message::ack::{AckStatus, MessageAck};
use pilgrimage::message::message::Message;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = Broker::new("broker1", 3, 2, "logs");
    broker.create_topic("test_topic", None)?;

    let mut handles = vec![];

    // Send multiple messages
    for i in 0..5 {
        let message = Message::new(format!("Message {}", i));
        println!("Send: ID={}, Content={}", message.id, message.content);

        let msg_clone = message.clone();
        let handle = tokio::spawn(async move {
            // Set a different delay for each message
            sleep(Duration::from_millis(500 * (i + 1) as u64)).await;

            let ack = MessageAck::new(
                msg_clone.id,
                Utc::now(),
                AckStatus::Processed,
                format!("consumer{}", i % 3 + 1),
                0,
            );
            println!("ACK issued: {:?}", ack);
        });
        handles.push(handle);
    }

    // Waiting for all ACKs to be completed
    for handle in handles {
        handle.await?;
    }

    Ok(())
}
