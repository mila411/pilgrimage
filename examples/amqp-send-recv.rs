use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use std::{error::Error, time::Duration};
use tokio::{signal, time::timeout};

const MESSAGE_COUNT: usize = 5;

async fn publish_message(channel: &Channel, message: &str) -> Result<(), Box<dyn Error>> {
    channel
        .basic_publish(
            "",
            "hello",
            BasicPublishOptions::default(),
            message.as_bytes(),
            BasicProperties::default(),
        )
        .await?
        .await?;
    println!("Sent '{}'", message);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    let _queue = channel
        .queue_declare(
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    // Send multiple messages
    for i in 0..MESSAGE_COUNT {
        let message = format!("Message {}", i + 1);
        publish_message(&channel, &message).await?;
    }

    let mut consumer = channel
        .basic_consume(
            "hello",
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("Waiting for {} messages...", MESSAGE_COUNT);
    let mut received_count = 0;

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("Received Ctrl+C, shutting down...");
                break;
            }
            message = timeout(Duration::from_secs(5), consumer.next()) => {
                match message {
                    Ok(Some(delivery_result)) => {
                        if let Ok(delivery) = delivery_result {
                            if let Ok(message) = std::str::from_utf8(&delivery.data) {
                                println!("Received message: {}", message);
                                delivery.ack(BasicAckOptions::default()).await?;
                                received_count += 1;
                                if received_count >= MESSAGE_COUNT {
                                    println!("All messages received. Shutting down...");
                                    break;
                                }
                            }
                        }
                    },
                    Ok(None) => break,
                    Err(_) => continue,
                }
            }
        }
    }

    Ok(())
}
