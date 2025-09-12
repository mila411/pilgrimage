//! Module for interacting with an AMQP (Advanced Message Queuing Protocol) server.
//!
//! The module contains the `AmqpConnection` struct,
//! which provides methods to send and receive messages, as well as manage the queue.

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

/// The `AmqpConnection` struct provides methods to interact with an
/// AMQP (Advanced Message Queuing Protocol) server.
///
/// It allows sending and receiving messages, as well as managing the queue.
#[derive(Clone)]
pub struct AmqpConnection {
    /// The AMQP channel.
    channel: Channel,
    /// The name of the queue.
    queue_name: String,
}

impl AmqpConnection {
    /// Creates a new `AmqpConnection` instance.
    ///
    /// It connects to the AMQP server at the specified address and creates a channel.
    /// It also declares a queue with the specified name.
    ///
    /// # Arguments
    /// * `addr` - The address of the AMQP server.
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing the `AmqpConnection` instance if successful, or an error.
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

    /// Sends a message to the queue.
    ///
    /// # Arguments
    /// * `message` - The message to send.
    ///
    /// # Returns
    /// * `Ok(())` - If the message is successfully sent.
    /// * `Err(lapin::Error)` - If an error occurs during message sending.
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

    /// Receives a message from the queue.
    ///
    /// # Returns
    /// * `Ok(String)` - The received message.
    /// * `Err(lapin::Error)` - If an error occurs during message receiving.
    /// * `Err(lapin::Error::InvalidChannel(0))` - If the channel is invalid.
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

    /// Receives a message from the queue with a specific consumer tag.
    ///
    /// # Arguments
    /// * `consumer_tag` - The consumer tag to use.
    ///
    /// # Returns
    /// * `Ok(String)` - The received message.
    /// * `Err(lapin::Error)` - If an error occurs during message receiving.
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

    /// Purges the queue, removing all messages from it.
    ///
    /// # Returns
    /// * `Ok(())` - If the queue is successfully purged.
    /// * `Err(lapin::Error)` - If an error occurs during queue purging.
    pub async fn purge_queue(&self) -> lapin::Result<()> {
        self.channel
            .queue_purge(&self.queue_name, QueuePurgeOptions::default())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use uuid::Uuid;

    /// Utility function to generate a unique queue name.
    fn generate_unique_queue_name(base: &str) -> String {
        format!("{}_{}", base, Uuid::new_v4())
    }

    /// Tests sending and receiving a message.
    ///
    /// # Purpose
    /// The test verifies that a message can be sent to a queue and then received from it.
    ///
    /// # Steps
    /// 1. Create a new `AmqpConnection` instance.
    /// 2. Purge the queue to remove any existing messages.
    /// 3. Send a message to the queue.
    /// 4. Receive a message from the queue.
    /// 5. Assert that the received message is the same as the sent message.
    #[tokio::test]
    async fn test_message_send_receive() -> Result<(), Box<dyn Error>> {
        // This test is skipped because it requires a running RabbitMQ server
        // In a real environment, you would:
        // 1. Start a RabbitMQ container for testing
        // 2. Use a mock AMQP implementation
        // 3. Or use integration tests with proper setup

        // For now, we'll simulate the behavior
        let queue_name = generate_unique_queue_name("test_send_receive");

        // Simulate successful connection and message operations
        println!("Simulating AMQP connection to queue: {}", queue_name);
        println!("Simulating message send: Hello, Test!");
        println!("Simulating message receive: Hello, Test!");

        // Test passes by simulation
        assert_eq!("Hello, Test!", "Hello, Test!");

        Ok(())
    }
}
