use chrono::Utc;
use pilgrimage::broker::Broker;
use pilgrimage::message::ack::{AckStatus, MessageAck};
use pilgrimage::message::message::Message;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::new("broker1", 3, 2, "logs");

    // メッセージ送信とACK待機
    let message = Message::new("Hello with ACK".to_string());
    println!("送信: ID={}, 内容={}", message.id, message.content);

    let broker_clone = broker.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        let ack = MessageAck::new(
            message.id,
            Utc::now(),
            AckStatus::Processed,
            "test_topic".to_string(),
            0,
        );
        broker_clone.receive_ack(ack);
    });

    match broker
        .send_message_with_ack(message, Duration::from_secs(5))
        .await
    {
        Ok(ack) => println!("ACK受信: {:?}", ack),
        Err(e) => println!("エラー: {}", e),
    }

    Ok(())
}
