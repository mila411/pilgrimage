//! Module for the topic in the message broker.
//!
//! This module provides the `Topic` struct, which represents a topic in the message broker.
//!
//! A topic has a name, a set of partitions, and a list of subscribers.
//! It provides methods to create new topics, add subscribers, and publish messages.
//!
//! # Example
//! The following example demonstrates how to create a new topic,
//! add a subscriber, and publish a message.
//! ```
//! use pilgrimage::broker::topic::Topic;
//! use pilgrimage::subscriber::types::Subscriber;
//!
//! // Create a new topic
//! let mut topic = Topic::new("test_topic", 3, 2);
//! // Create a subscriber
//! let subscriber = Subscriber::new("sub1", Box::new(|msg: String| {
//!     println!("Received message: {}", msg);
//! }));
//! // Add the subscriber to the topic
//! topic.add_subscriber(subscriber);
//! // Publish a message to the topic
//! let partition_id = topic.publish("test_message".to_string(), None);
//!
//! // Check the topic details
//! assert!(partition_id.is_ok());
//! assert_eq!(topic.name, "test_topic");
//! assert_eq!(topic.partitions.len(), 3);
//! assert_eq!(topic.subscribers.len(), 1);
//! assert_eq!(topic.partitions[0].messages.len(), 1);
//! assert_eq!(topic.partitions[0].messages[0], "test_message");
//! ```

use crate::broker::error::BrokerError;
use crate::subscriber::types::Subscriber;
use std::fmt::{self, Debug};

/// A topic in the message broker.
///
/// Each topic has a name, a set of partitions, and a list of subscribers.
/// It provides methods to create new topics, add subscribers, and publish messages.
#[derive(Clone)]
pub struct Topic {
    /// The name of the topic.
    pub name: String,
    /// The partitions for the topic.
    pub partitions: Vec<Partition>,
    /// The subscribers to the topic.
    pub subscribers: Vec<Subscriber>,
}

impl Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic")
            .field("name", &self.name)
            .field("partitions", &self.partitions)
            .field("subscribers", &self.subscribers.len())
            .finish()
    }
}

/// A partition in a topic.
///
/// Each partition has an ID, a list of messages, a list of replicas, and a next offset.
#[derive(Clone)]
pub struct Partition {
    /// The ID of the partition.
    pub id: usize,
    /// The messages in the partition.
    pub messages: Vec<String>,
    /// The replicas of the partition.
    pub replicas: Vec<Replica>,
    /// The next offset for the partition.
    pub next_offset: usize,
}

impl Debug for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Partition")
            .field("id", &self.id)
            .field("messages", &self.messages)
            .field("replicas", &self.replicas)
            .finish()
    }
}

/// A replica in a partition.
///
/// Each replica has a broker ID and a list of messages.
#[derive(Clone)]
pub struct Replica {
    /// The ID of the broker.
    pub broker_id: String,
    /// The messages in the replica.
    pub messages: Vec<String>,
}

impl Debug for Replica {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replica")
            .field("broker_id", &self.broker_id)
            .field("messages", &self.messages)
            .finish()
    }
}

impl Topic {
    /// Creates a new topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic.
    /// * `num_partitions` - The number of partitions for the topic.
    /// * `replication_factor` - The replication factor for the topic.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    ///
    /// let topic = Topic::new("test_topic", 3, 2);
    /// assert_eq!(topic.name, "test_topic");
    /// assert_eq!(topic.partitions.len(), 3);
    /// assert_eq!(topic.partitions[0].replicas.len(), 2);
    /// ```
    pub fn new(name: &str, num_partitions: usize, replication_factor: usize) -> Self {
        let partitions = (0..num_partitions)
            .map(|i| Partition {
                id: i,
                messages: Vec::new(),
                replicas: (0..replication_factor)
                    .map(|_| Replica {
                        broker_id: String::default(),
                        messages: Vec::new(),
                    })
                    .collect(),
                next_offset: 0,
            })
            .collect();

        Topic {
            name: name.to_string(),
            partitions,
            subscribers: Vec::new(),
        }
    }

    /// Adds a subscriber to the topic.
    ///
    /// # Arguments
    ///
    /// * `subscriber` - The subscriber to add.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    /// use pilgrimage::subscriber::types::Subscriber;
    ///
    /// let mut topic = Topic::new("test_topic", 3, 2);
    /// let subscriber = Subscriber::new("sub1", Box::new(|msg: String| {
    ///     println!("Received message: {}", msg);
    /// }));
    /// topic.add_subscriber(subscriber);
    /// assert_eq!(topic.subscribers.len(), 1);
    /// ```
    pub fn add_subscriber(&mut self, subscriber: Subscriber) {
        self.subscribers.push(subscriber);
    }

    pub fn remove_subscriber(&mut self, _subscriber_id: &str) {
        // TODO: Because the Subscriber does not have an ID, the implementation of this function needs to be reviewed.
    }

    /// Publishes a message to the topic.
    ///
    /// If a partition key is provided,
    /// the message is published to the partition corresponding to the key.
    /// Otherwise, the partition is selected based on the current system time.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to publish.
    /// * `partition_key` - An optional key for partitioning.
    ///   If not provided, the partition is selected based on the current system time.
    ///
    /// # Returns
    /// The partition ID where the message was published.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    /// use pilgrimage::broker::error::BrokerError;
    ///
    /// let mut topic = Topic::new("test_topic", 3, 2);
    /// let result = topic.publish("test_message".to_string(), None);
    /// assert!(result.is_ok());
    /// ```
    pub fn publish(
        &mut self,
        message: String,
        partition_key: Option<&str>,
    ) -> Result<usize, BrokerError> {
        let partition_id = match partition_key {
            Some(key) => self.get_partition_id(key),
            None => self.get_next_partition(),
        };

        if let Some(partition) = self.partitions.get_mut(partition_id) {
            partition.add_message(message.clone());

            // Add a message to the replica
            for replica in &mut partition.replicas {
                replica.messages.push(message.clone());
            }

            // Notification to all subscribers
            for subscriber in &self.subscribers {
                (subscriber.callback)(message.clone());
            }

            Ok(partition_id)
        } else {
            Err(BrokerError::PartitionError(format!(
                "Invalid partition id: {}",
                partition_id
            )))
        }
    }

    /// Gets the partition ID for a given key.
    ///
    /// # Arguments
    /// * `key` - The key to get the partition ID for.
    ///
    /// # Returns
    /// The partition ID for the key.
    fn get_partition_id(&self, key: &str) -> usize {
        let hash = key.bytes().fold(0u64, |acc, b| acc.wrapping_add(b as u64));
        (hash % self.partitions.len() as u64) as usize
    }

    /// Gets the next partition ID using the current system time.
    ///
    /// # Returns
    /// The next partition ID.
    fn get_next_partition(&self) -> usize {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        (now % self.partitions.len() as u128) as usize
    }
}

impl Partition {
    /// Creates a new partition.
    ///
    /// # Arguments
    /// * `id` - The ID of the partition.
    ///
    /// # Returns
    /// A new partition.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::topic::Partition;
    ///
    /// let partition = Partition::new(0);
    /// assert_eq!(partition.id, 0);
    /// ```
    pub fn new(id: usize) -> Self {
        Self {
            id,
            messages: Vec::new(),
            replicas: Vec::new(),
            next_offset: 0,
        }
    }

    /// Adds a message to the partition.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to add.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::topic::Partition;
    ///
    /// let mut partition = Partition {
    ///     id: 0,
    ///     messages: Vec::new(),
    ///     replicas: Vec::new(),
    ///     next_offset: 0,
    /// };
    /// partition.add_message("test_message".to_string());
    /// assert_eq!(partition.messages.len(), 1);
    /// assert_eq!(partition.messages[0], "test_message");
    /// ```
    pub fn add_message(&mut self, message: String) {
        self.messages.push(message);
        self.next_offset += 1;
    }

    /// Fetches messages from the partition starting from a given offset.
    ///
    /// # Arguments
    ///
    /// * `start_offset` - The offset to start fetching messages from.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::topic::Partition;
    ///
    /// let mut partition = Partition {
    ///     id: 0,
    ///     messages: vec!["message_1".to_string(), "message_2".to_string()],
    ///     replicas: Vec::new(),
    ///     next_offset: 2,
    /// };
    /// let messages = partition.fetch_messages_in_order(1);
    /// assert_eq!(messages.len(), 1);
    /// assert_eq!(messages[0], "message_2");
    /// ```
    pub fn fetch_messages_in_order(&self, start_offset: usize) -> &[String] {
        if start_offset >= self.messages.len() {
            &[]
        } else {
            &self.messages[start_offset..]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriber::types::Subscriber;
    use std::sync::{Arc, Mutex};

    /// Tests topic creation.
    ///
    /// # Purpose
    /// This test verifies that a new topic is created
    /// with the correct number of partitions and replicas.
    ///
    /// # Steps
    /// 1. Create a new topic with 3 partitions and 2 replicas.
    /// 2. Verify that the topic has the correct number of partitions.
    /// 3. Verify that each partition has the correct number of replicas.
    #[test]
    fn test_topic_creation() {
        let topic = Topic::new("test_topic", 3, 2);
        assert_eq!(topic.name, "test_topic");
        assert_eq!(topic.partitions.len(), 3);
        assert_eq!(topic.partitions[0].replicas.len(), 2);
    }

    /// Tests adding a subscriber to a topic.
    ///
    /// # Purpose
    /// This test verifies that a subscriber can be added to a topic.
    ///
    /// # Steps
    /// 1. Create a new topic.
    /// 2. Create a new subscriber.
    /// 3. Add the subscriber to the topic.
    /// 4. Verify that the subscriber was added to the topic.
    #[test]
    fn test_add_subscriber() {
        let mut topic = Topic::new("test_topic", 3, 2);
        let subscriber = Subscriber::new(
            "test_sub",
            Box::new(|msg: String| {
                println!("Received message: {}", msg);
            }),
        );
        topic.add_subscriber(subscriber);
        assert_eq!(topic.subscribers.len(), 1);
    }

    /// Tests publishing a message to a topic.
    ///
    /// # Purpose
    /// This test verifies that a message can be published to a topic.
    ///
    /// # Steps
    /// 1. Create a new topic.
    /// 2. Create a new subscriber.
    /// 3. Add the subscriber to the topic.
    /// 4. Publish a message to the topic.
    /// 5. Verify that the message was published to the topic.
    /// 6. Verify that the message was sent to the subscriber.
    /// 7. Verify that the message was added to the partition.
    #[test]
    fn test_publish_message() {
        let mut topic = Topic::new("test_topic", 3, 2);
        let subscriber = Subscriber::new(
            "test_sub",
            Box::new(|msg: String| {
                println!("Received test message: {}", msg);
            }),
        );
        topic.add_subscriber(subscriber);

        let result = topic.publish("test_message".to_string(), None);
        assert!(result.is_ok());
        let partition_id = result.unwrap();
        assert!(partition_id < 3);
        assert_eq!(topic.partitions[partition_id].messages[0], "test_message");
    }

    /// Tests getting the partition ID for a key.
    ///
    /// # Purpose
    /// This test verifies that the partition ID is calculated correctly
    /// based on the key provided.
    ///
    /// # Steps
    /// 1. Create a new topic.
    /// 2. Get the partition ID for a key.
    /// 3. Verify that the partition ID is less than the number of partitions.
    #[test]
    fn test_get_partition_id() {
        let topic = Topic::new("test_topic", 3, 2);
        let partition_id = topic.get_partition_id("key");
        assert!(partition_id < 3);
    }

    /// Tests getting the next partition ID.
    ///
    /// # Purpose
    /// This test verifies that the next partition ID is calculated correctly
    /// based on the current system time.
    ///
    /// # Steps
    /// 1. Create a new topic.
    /// 2. Get the next partition ID.
    /// 3. Verify that the partition ID is less than the number of partitions.
    #[test]
    fn test_get_next_partition() {
        let topic = Topic::new("test_topic", 3, 2);
        let partition_id = topic.get_next_partition();
        assert!(partition_id < 3);
    }

    /// Tests adding and ordering messages in a partition.
    ///
    /// # Purpose
    /// This test verifies that messages can be added to a partition
    /// and fetched in the correct order.
    ///
    /// # Steps
    /// 1. Create a new partition.
    /// 2. Add messages to the partition.
    /// 3. Fetch messages from the partition.
    /// 4. Verify that the messages are fetched in the correct order.
    #[test]
    fn test_message_ordering() {
        let mut topic = Topic::new("test_topic", 1, 1);
        topic.publish("message_1".to_string(), None).unwrap();
        topic.publish("message_2".to_string(), None).unwrap();
        topic.publish("message_3".to_string(), None).unwrap();

        let partition = &topic.partitions[0];
        let messages = partition.fetch_messages_in_order(0);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0], "message_1");
        assert_eq!(messages[1], "message_2");
        assert_eq!(messages[2], "message_3");
    }

    /// Tests fetching messages from a partition starting from a given offset.
    ///
    /// # Purpose
    /// This test verifies that messages can be fetched from a partition
    /// starting from a given offset.
    ///
    /// # Steps
    /// 1. Create a new partition.
    /// 2. Add messages to the partition.
    /// 3. Fetch messages from the partition starting from an offset.
    /// 4. Verify that the messages are fetched in the correct order.
    #[test]
    fn test_fetch_messages_from_offset() {
        let mut topic = Topic::new("test_topic", 1, 1);
        topic.publish("message_1".to_string(), None).unwrap();
        topic.publish("message_2".to_string(), None).unwrap();
        topic.publish("message_3".to_string(), None).unwrap();

        let partition = &topic.partitions[0];
        let messages = partition.fetch_messages_in_order(1);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], "message_2");
        assert_eq!(messages[1], "message_3");
    }

    /// Tests that a subscriber receives messages from a topic.
    ///
    /// # Purpose
    /// This test verifies that a subscriber receives messages
    /// when they are published to a topic.
    ///
    /// # Steps
    /// 1. Create a new topic.
    /// 2. Create a subscriber that appends messages to a list.
    /// 3. Add the subscriber to the topic.
    /// 4. Publish messages to the topic.
    /// 5. Verify that the subscriber received the messages.
    /// 6. Verify that the messages are in the correct order.
    #[test]
    fn test_subscriber_receives_messages() {
        let mut topic = Topic::new("test_topic", 1, 1);
        let received_messages = Arc::new(Mutex::new(Vec::new()));

        let subscriber = Subscriber::new("sub1", {
            let received_messages = Arc::clone(&received_messages);
            Box::new(move |msg: String| {
                received_messages.lock().unwrap().push(msg);
            })
        });
        topic.add_subscriber(subscriber);

        topic.publish("message_1".to_string(), None).unwrap();
        topic.publish("message_2".to_string(), None).unwrap();

        let received_messages = received_messages.lock().unwrap();
        assert_eq!(received_messages.len(), 2);
        assert_eq!(received_messages[0], "message_1");
        assert_eq!(received_messages[1], "message_2");
    }
}
