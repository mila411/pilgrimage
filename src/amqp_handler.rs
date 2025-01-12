use std::time::Duration;

use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
        QueuePurgeOptions,
    },
    types::FieldTable,
};
use tokio::time::timeout;

#[derive(Clone)]
pub struct AmqpConnection {
    channel: Channel,
    queue_name: String,
}

impl AmqpConnection {
    pub async fn new(addr: &str, queue_name: &str) -> lapin::Result<Self> {
        let conn = Connection::connect(addr, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            channel,
            queue_name: queue_name.to_string(),
        })
    }

    pub async fn send_message(&self, message: &str) -> lapin::Result<()> {
        self.channel
            .basic_publish(
                "",
                &self.queue_name,
                BasicPublishOptions::default(),
                message.as_bytes(),
                BasicProperties::default(),
            )
            .await?
            .await?;
        Ok(())
    }

    pub async fn receive_message(&self) -> lapin::Result<String> {
        let mut consumer = self
            .channel
            .basic_consume(
                &self.queue_name,
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        if let Some(delivery_result) = consumer.next().await {
            match delivery_result {
                Ok(delivery) => {
                    let message = match std::str::from_utf8(&delivery.data) {
                        Ok(s) => s.to_string(),
                        Err(_) => return Err(lapin::Error::InvalidChannel(0)),
                    };
                    delivery
                        .ack(BasicAckOptions::default())
                        .await
                        .map_err(lapin::Error::from)?;
                    Ok(message)
                }
                Err(error) => Err(error),
            }
        } else {
            Err(lapin::Error::InvalidChannel(0))
        }
    }

    pub async fn receive_message_with_tag(&self, consumer_tag: &str) -> lapin::Result<String> {
        let mut consumer = self
            .channel
            .basic_consume(
                &self.queue_name,
                consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        match timeout(Duration::from_secs(5), consumer.next()).await {
            Ok(Some(delivery_result)) => match delivery_result {
                Ok(delivery) => {
                    let message = match std::str::from_utf8(&delivery.data) {
                        Ok(s) => s.to_string(),
                        Err(_) => return Err(lapin::Error::InvalidChannel(0)),
                    };
                    delivery.ack(BasicAckOptions::default()).await?;
                    Ok(message)
                }
                Err(e) => Err(e),
            },
            Ok(None) => Err(lapin::Error::InvalidChannel(0)),
            Err(_) => Err(lapin::Error::InvalidChannel(0)),
        }
    }

    pub async fn purge_queue(&self) -> lapin::Result<()> {
        self.channel
            .queue_purge(&self.queue_name, QueuePurgeOptions::default())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use uuid::Uuid;

    fn generate_unique_queue_name(base: &str) -> String {
        format!("{}_{}", base, Uuid::new_v4())
    }

    #[tokio::test]
    async fn test_message_send_receive() -> Result<(), Box<dyn Error>> {
        let queue_name = generate_unique_queue_name("test_send_receive");
        let amqp = AmqpConnection::new("amqp://127.0.0.1:5672/%2f", &queue_name).await?;
        amqp.purge_queue().await?;

        amqp.send_message("Hello, Test!").await?;

        let receive_amqp = amqp.clone();
        let received = receive_amqp.receive_message().await?;
        assert_eq!(received, "Hello, Test!");

        Ok(())
    }
}
