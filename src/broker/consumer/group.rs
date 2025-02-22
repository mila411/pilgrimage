//! Module containing the consumer group implementation.
//!
//! The [`ConsumerGroup`] struct represents a consumer group that is used to distribute
//! the partitions of a topic among multiple consumers.
//!
//! The consumer group is responsible for managing the members of the group and
//! assigning partitions to each member.
//!
//! The implementation includes methods to add members to the group, rebalance partitions,
//! and deliver messages to all members.
//!
//! # Example
//! ```rust
//! use pilgrimage::broker::consumer::group::ConsumerGroup;
//! use pilgrimage::subscriber::types::Subscriber;
//! use tokio::runtime::Runtime;
//!
//! // Create a new consumer group with the ID "group1"
//! let mut group = ConsumerGroup::new("group1");
//!
//! // Add a member to the group with the ID "consumer1"
//! let subscriber = Subscriber::new("consumer1", Box::new(|msg: String| {
//!     println!("Received message: {}", msg);
//! }));
//! group.add_member("consumer1", subscriber);
//!
//! // Deliver a message to all members of the group
//! // and check if the message was delivered successfully
//! let rt = Runtime::new().unwrap();
//! rt.block_on(async {
//!     let result = group.deliver_message("A beautiful message").await;
//!     assert_eq!(result, Ok(()));
//! });
//! ```

use crate::subscriber::types::Subscriber;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Represents a consumer group.
///
/// A consumer group is a collection of consumers
/// that work together to consume messages from a topic.
///
/// Like Kafka, Pilgrimage uses the concept of consumer groups
/// to distribute the partitions of a topic among multiple consumers.
/// Each consumer in the group is assigned a subset of the partitions.
/// This allows multiple consumers to work together to consume messages
/// from a topic in parallel.
///
/// For example, consider a topic with 10 partitions and a consumer group
/// with 3 consumers. The partitions of the topic will be distributed
/// among the 3 consumers as follows:
/// * Consumer 1: Partitions 0, 3, 6, 9
/// * Consumer 2: Partitions 1, 4, 7
/// * Consumer 3: Partitions 2, 5, 8
///
/// # Key points
/// * Each consumer group has a unique ID that is used to identify the group.
/// * The group members are identified by their consumer IDs.
/// * The partitions of the topic are distributed among the members of the group.
pub struct ConsumerGroup {
    /// The ID of the consumer group. It is used to uniquely identify the group.
    pub group_id: String,
    /// The members of the consumer group. Each member is identified by a unique consumer ID.
    pub members: Arc<Mutex<HashMap<String, GroupMember>>>,
    /// The partition assignments for the members.
    /// It maps each member to the list of partitions assigned to it.
    pub assignments: Arc<Mutex<HashMap<String, Vec<usize>>>>,
}

/// Represents a member of a consumer group.
pub struct GroupMember {
    /// The [`Subscriber`] associated with the member.
    pub subscriber: Subscriber,
}

impl ConsumerGroup {
    /// Creates a new consumer group.
    ///
    /// Uses the given ID to uniquely identify the group,
    /// and initializes the members and assignments with empty maps.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The ID of the consumer group.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::consumer::group::ConsumerGroup;
    ///
    /// let group = ConsumerGroup::new("group1");
    /// assert_eq!(group.group_id, "group1");
    /// ```
    pub fn new(group_id: &str) -> Self {
        ConsumerGroup {
            group_id: group_id.to_string(),
            members: Arc::new(Mutex::new(HashMap::new())),
            assignments: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adds a member to the consumer group.
    ///
    /// Associates the given subscriber with the consumer ID
    /// and adds it to the group's members.
    /// After adding the member, rebalances the partitions among the members.
    ///
    /// # Arguments
    ///
    /// * `consumer_id` - The ID of the consumer.
    /// * `subscriber` - The subscriber to add.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::consumer::group::ConsumerGroup;
    /// use pilgrimage::subscriber::types::Subscriber;
    ///
    /// let group = ConsumerGroup::new("group1");
    /// let subscriber = Subscriber::new("consumer1", Box::new(|msg: String| {
    ///     println!("Received message: {}", msg);
    /// }));
    /// group.add_member("consumer1", subscriber);
    ///
    /// // Check if the member was added successfully
    /// let members = group.members.lock().unwrap();
    /// assert_eq!(members.len(), 1);
    /// assert_eq!(members.get("consumer1").is_some(), true);
    /// ```
    pub fn add_member(&self, consumer_id: &str, subscriber: Subscriber) {
        let mut members = self.members.lock().unwrap();
        // if the consumer ID already exists, remove the existing subscriber first and then add the new one
        if members.contains_key(consumer_id) {
            members.remove(consumer_id);
        }
        members.insert(consumer_id.to_string(), GroupMember { subscriber });
        drop(members); // Unlock after adding members
        self.rebalance_partitions();
    }

    /// Rebalances the partitions among the members of the consumer group.
    ///
    /// Assigns partitions to the members in a round-robin fashion.
    /// Therefore, each member gets an equal number of partitions.
    ///
    /// The method assumes that the number of partitions is fixed at 10.
    ///
    /// For example, if there are 10 partitions and 3 members,
    /// the partitions will be distributed as follows:
    /// * Member 1: Partitions 0, 3, 6, 9
    /// * Member 2: Partitions 1, 4, 7
    /// * Member 3: Partitions 2, 5, 8
    ///
    /// Remarks:
    /// * The partition assignments are stored in the `assignments` field of the consumer group.
    /// * The existing assignments are cleared before rebalancing.
    /// * If there are no members in the group, no partitions are assigned.
    fn rebalance_partitions(&self) {
        let members = self.members.lock().unwrap();
        let member_ids: Vec<_> = members.keys().cloned().collect();
        drop(members); // Unlock after obtaining the member list

        let mut assignments = self.assignments.lock().unwrap();
        assignments.clear();

        if member_ids.is_empty() {
            return;
        }

        let total_partitions = 10;
        let num_members = member_ids.len();

        for partition_id in 0..total_partitions {
            let idx = partition_id % num_members;
            let member_id = &member_ids[idx];
            assignments
                .entry(member_id.clone())
                .or_default()
                .push(partition_id);
        }
    }

    /// Delivers a message to all members of the consumer group.
    ///
    /// The message is delivered to each member's subscriber callback.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to deliver.
    ///
    /// # Returns
    ///
    /// * `Result<(), String>` - `Ok(())` if the message was delivered successfully,
    ///   or an error message otherwise.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::consumer::group::ConsumerGroup;
    /// use pilgrimage::subscriber::types::Subscriber;
    /// use tokio::runtime::Runtime;
    ///
    /// let mut group = ConsumerGroup::new("group1");
    /// let subscriber1 = Subscriber::new("consumer1", Box::new(|msg: String| {
    ///     println!("Consumer 1 received message: {}", msg);
    /// }));
    /// group.add_member("consumer1", subscriber1);
    ///
    /// // Send a message to all members of the group and check if it was delivered successfully
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let result = group.deliver_message("A beautiful message").await;
    ///     assert_eq!(result, Ok(()));
    /// });
    pub async fn deliver_message(&mut self, message: &str) -> Result<(), String> {
        if let Ok(members) = self.members.lock() {
            for member in members.values() {
                (member.subscriber.callback)(message.to_string());
            }
            Ok(())
        } else {
            Err("Failed to acquire lock on members".to_string())
        }
    }
}
