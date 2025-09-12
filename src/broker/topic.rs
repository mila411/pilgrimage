//! Module for message topics
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
//! ```

use crate::subscriber::types::Subscriber;
use std::fmt;

/// Represents a message topic in the broker
#[derive(Clone)]
pub struct Topic {
    /// The name of the topic
    pub name: String,
    /// The number of partitions
    pub num_partitions: usize,
    /// The replication factor
    pub replication_factor: usize,
    /// The list of subscribers
    pub subscribers: Vec<Subscriber>,
}

impl fmt::Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic")
            .field("name", &self.name)
            .field("num_partitions", &self.num_partitions)
            .field("replication_factor", &self.replication_factor)
            .field("subscribers_count", &self.subscribers.len())
            .finish()
    }
}

impl Topic {
    /// Creates a new topic with the given name, number of partitions, and replication factor
    ///
    /// # Arguments
    /// * `name` - The name of the topic
    /// * `num_partitions` - The number of partitions
    /// * `replication_factor` - The replication factor
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    ///
    /// let topic = Topic::new("test_topic", 3, 2);
    /// assert_eq!(topic.name, "test_topic");
    /// assert_eq!(topic.num_partitions, 3);
    /// assert_eq!(topic.replication_factor, 2);
    /// ```
    pub fn new(name: &str, num_partitions: usize, replication_factor: usize) -> Self {
        Self {
            name: name.to_string(),
            num_partitions,
            replication_factor,
            subscribers: Vec::new(),
        }
    }

    /// Adds a subscriber to the topic
    ///
    /// # Arguments
    /// * `subscriber` - The subscriber to add
    ///
    /// # Example
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

    /// Removes a subscriber from the topic
    ///
    /// # Arguments
    /// * `id` - The ID of the subscriber to remove
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    /// use pilgrimage::subscriber::types::Subscriber;
    ///
    /// let mut topic = Topic::new("test_topic", 3, 2);
    /// let subscriber = Subscriber::new("sub1", Box::new(|msg: String| {
    ///    println!("Received message: {}", msg);
    /// }));
    /// topic.add_subscriber(subscriber);
    /// topic.remove_subscriber("sub1");
    /// assert_eq!(topic.subscribers.len(), 0);
    /// ```
    pub fn remove_subscriber(&mut self, id: &str) -> Option<Subscriber> {
        if let Some(pos) = self.subscribers.iter().position(|s| s.id == id) {
            Some(self.subscribers.remove(pos))
        } else {
            None
        }
    }

    /// Get the number of subscribers
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    /// use pilgrimage::subscriber::types::Subscriber;
    ///
    /// let mut topic = Topic::new("test_topic", 3, 2);
    /// let subscriber = Subscriber::new("sub1", Box::new(|msg: String| {
    ///   println!("Received message: {}", msg);
    /// }));
    /// topic.add_subscriber(subscriber);
    /// assert_eq!(topic.subscriber_count(), 1);
    /// ```
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Calculates the partition for a message based on a key
    ///
    /// # Arguments
    /// * `key` - The key to use for partitioning
    ///
    /// # Returns
    /// The partition number
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    ///
    /// let topic = Topic::new("test_topic", 3, 2);
    /// let partition = topic.get_partition_for_key("test_key");
    /// assert!(partition < topic.num_partitions);
    /// ```
    pub fn get_partition_for_key(&self, key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(key, &mut hasher);
        let hash = std::hash::Hasher::finish(&hasher);
        (hash % self.num_partitions as u64) as usize
    }

    /// Gets the next partition in a round-robin fashion
    ///
    /// # Arguments
    /// * `last_partition` - The last partition used
    ///
    /// # Returns
    /// The next partition to use
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::topic::Topic;
    ///
    /// let topic = Topic::new("test_topic", 3, 2);
    /// let partition = topic.get_next_partition(0);
    /// assert_eq!(partition, 1);
    /// ```
    pub fn get_next_partition(&self, last_partition: usize) -> usize {
        (last_partition + 1) % self.num_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_creation() {
        let topic = Topic::new("test_topic", 3, 2);
        assert_eq!(topic.name, "test_topic");
        assert_eq!(topic.num_partitions, 3);
        assert_eq!(topic.replication_factor, 2);
        assert_eq!(topic.subscribers.len(), 0);
    }

    #[test]
    fn test_add_subscriber() {
        let mut topic = Topic::new("test_topic", 3, 2);
        let subscriber = Subscriber::new("sub1", Box::new(|_msg: String| {}));
        topic.add_subscriber(subscriber);
        assert_eq!(topic.subscribers.len(), 1);
    }

    #[test]
    fn test_remove_subscriber() {
        let mut topic = Topic::new("test_topic", 3, 2);
        let subscriber = Subscriber::new("sub1", Box::new(|_msg: String| {}));
        topic.add_subscriber(subscriber);
        let removed = topic.remove_subscriber("sub1");
        assert!(removed.is_some());
        assert_eq!(topic.subscribers.len(), 0);
    }

    #[test]
    fn test_get_partition_for_key() {
        let topic = Topic::new("test_topic", 3, 2);
        let partition = topic.get_partition_for_key("test_key");
        assert!(partition < topic.num_partitions);
    }

    #[test]
    fn test_get_next_partition() {
        let topic = Topic::new("test_topic", 3, 2);
        assert_eq!(topic.get_next_partition(0), 1);
        assert_eq!(topic.get_next_partition(1), 2);
        assert_eq!(topic.get_next_partition(2), 0);
    }
}
