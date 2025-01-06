use crate::subscriber::types::Subscriber;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Represents a consumer group.
pub struct ConsumerGroup {
    pub group_id: String,
    pub members: Arc<Mutex<HashMap<String, GroupMember>>>,
    pub assignments: Arc<Mutex<HashMap<String, Vec<usize>>>>,
}

/// Represents a member of a consumer group.
pub struct GroupMember {
    pub subscriber: Subscriber,
}

impl ConsumerGroup {
    /// Creates a new consumer group.
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
    /// ```
    pub fn add_member(&self, consumer_id: &str, subscriber: Subscriber) {
        let mut members = self.members.lock().unwrap();
        members.insert(consumer_id.to_string(), GroupMember { subscriber });
        drop(members); // Unlock after adding members
        self.rebalance_partitions();
    }

    /// Rebalances the partitions among the members of the consumer group.
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
}
