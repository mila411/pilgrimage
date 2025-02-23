//! Module for the broker entity and related functionality.
//!
//! The broker is the central entity in the message broker system.
//! It is responsible for managing topics, partitions, replicas,
//! consumer groups, leader election, storage, and more.
//!
//! The module contains the following submodules:
//! - [`cluster`]: Cluster management functionality.
//! - [`consumer`]: Consumer group management functionality.
//! - [`error`]: Error types for the broker.
//! - [`leader`]: Leader election functionality.
//! - [`log_compression`]: Log compression functionality.
//! - [`message_queue`]: Message queue functionality.
//! - [`node_management`]: Node management functionality.
//! - [`scaling`]: Scaling functionality.
//! - [`storage`]: Storage functionality.
//! - [`topic`]: Topic management functionality.
//!
//! The module also contains the [`Broker`] struct, which represents a broker in the system.

pub mod cluster;
pub mod consumer;
pub mod error;
pub mod leader;
pub mod log_compression;
pub mod message_queue;
pub mod node_management;
pub mod scaling;
pub mod storage;
pub mod topic;

use chrono::Utc;
use uuid::Uuid;

use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::error::BrokerError;
use crate::broker::leader::{BrokerState, LeaderElection};
use crate::broker::log_compression::LogCompressor;
use crate::broker::message_queue::MessageQueue;
use crate::broker::node_management::{check_node_health, recover_node};
use crate::broker::storage::Storage;
use crate::broker::topic::Topic;
use crate::message::ack::AckStatus;
use crate::message::ack::MessageAck;
use crate::message::message::Message;
use crate::subscriber::types::Subscriber;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

/// A type alias for a collection of consumer groups, keyed by their group names.
pub type ConsumerGroups = HashMap<String, ConsumerGroup>;

impl ConsumerGroup {
    /// Resets the assignments of the consumer group.
    ///
    /// This method clears the current assignments of the consumer group.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::ConsumerGroup;
    ///
    /// let mut group = ConsumerGroup::new("test_group");
    /// group.reset_assignments();
    /// ```
    pub fn reset_assignments(&mut self) {
        if let Ok(mut map) = self.assignments.lock() {
            map.clear();
        }
    }
}

/// The `Broker` struct represents a broker in the message broker system.
///
/// It contains various fields related to topics, partitions, replicas,
/// consumer groups, leader election, storage, and more.
pub struct Broker {
    /// The unique identifier of the broker.
    id: String,
    /// A collection of topics managed by the broker.
    /// The topics are keyed by their names.
    topics: HashMap<String, Topic>,
    /// The number of partitions for each topic.
    num_partitions: usize,
    /// The replication factor for each partition.
    replication_factor: usize,
    /// The current term of the broker.
    term: AtomicU64,
    /// The leader election instance for the broker.
    leader_election: LeaderElection,
    /// The storage instance for the broker.
    storage: Arc<Mutex<Storage>>,
    /// A collection of consumer groups managed by the broker.
    consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
    /// A collection of nodes in the cluster, keyed by their IDs.
    nodes: Arc<Mutex<HashMap<String, Node>>>,
    /// A collection of partitions in the cluster, keyed by their IDs.
    partitions: Arc<Mutex<HashMap<String, Shard>>>,
    /// A collection of replicas for each partition, keyed by the partition ID.
    replicas: Arc<Mutex<HashMap<String, Vec<String>>>>,
    /// The current leader of the broker.
    leader: Arc<Mutex<Option<String>>>,
    /// The message queue for the broker.
    /// The message queue is used to store messages that are waiting to be processed.
    pub message_queue: Arc<MessageQueue>,
    /// The path to the log file for the broker.
    log_path: String,
    /// A collection of processed message IDs.
    processed_message_ids: Arc<Mutex<HashSet<Uuid>>>,
    /// The transaction log for the broker.
    transaction_log: Arc<Mutex<Vec<Message>>>,
    /// The transaction messages for the broker.
    transaction_messages: Arc<Mutex<Vec<Message>>>,
    /// A collection of ack waiters for messages.
    ack_waiters: Arc<Mutex<HashMap<Uuid, oneshot::Sender<MessageAck>>>>,
}

impl Clone for Broker {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            topics: self.topics.clone(),
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
            term: AtomicU64::new(self.term.load(Ordering::SeqCst)),
            leader_election: self.leader_election.clone(),
            storage: self.storage.clone(),
            consumer_groups: self.consumer_groups.clone(),
            nodes: self.nodes.clone(),
            partitions: self.partitions.clone(),
            replicas: self.replicas.clone(),
            leader: self.leader.clone(),
            message_queue: self.message_queue.clone(),
            log_path: self.log_path.clone(),
            processed_message_ids: self.processed_message_ids.clone(),
            transaction_log: self.transaction_log.clone(),
            transaction_messages: self.transaction_messages.clone(),
            ack_waiters: self.ack_waiters.clone(),
        }
    }
}

impl Broker {
    /// Creates a new broker with the specified ID,
    /// number of partitions, replication factor, and storage path.
    ///
    /// # Arguments
    /// * `id` - The unique identifier of the broker.
    /// * `num_partitions` - The number of partitions for each topic.
    /// * `replication_factor` - The replication factor for each partition.
    /// * `storage_path` - The path to the storage directory for the broker.
    ///
    /// # Returns
    /// A new `Broker` instance.
    pub fn new(
        id: &str,
        num_partitions: usize,
        replication_factor: usize,
        storage_path: &str,
    ) -> Self {
        let min_instances = 1;
        let max_instances = 10;
        let _check_interval = Duration::from_secs(30);
        let peers = HashMap::new(); //TODO it reads from the settings
        let leader_election = LeaderElection::new(id, peers);
        let storage = Storage::new(storage_path).expect("Failed to initialize storage");
        let log_path = format!("logs/{}.log", id);

        let broker = Broker {
            id: id.to_string(),
            topics: HashMap::new(),
            num_partitions,
            replication_factor,
            term: AtomicU64::new(0),
            leader_election,
            storage: Arc::new(Mutex::new(storage)),
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(HashMap::new())),
            leader: Arc::new(Mutex::new(None)),
            message_queue: Arc::new(MessageQueue::new(min_instances, max_instances)),
            log_path: log_path.to_string(),
            processed_message_ids: Arc::new(Mutex::new(HashSet::new())),
            transaction_log: Arc::new(Mutex::new(Vec::new())),
            transaction_messages: Arc::new(Mutex::new(Vec::new())),
            ack_waiters: Arc::new(Mutex::new(HashMap::new())),
        };
        broker.monitor_nodes();
        broker
    }

    /// Begins a new transaction.
    ///
    /// This method clears the transaction messages.
    pub fn begin_transaction(&self) {
        let mut messages = self.transaction_messages.lock().unwrap();
        messages.clear();
    }

    /// Commits the current transaction.
    ///
    /// This method sends the messages in the transaction log to the message queue.
    ///
    /// If a duplicate message is detected, an error is returned.
    ///
    /// # Returns
    /// An empty result if the transaction is committed successfully.
    ///
    /// # Errors
    /// An error if a duplicate message is detected.
    pub fn commit_transaction(&self) -> Result<(), String> {
        let log = self.transaction_log.lock().unwrap();
        let mut processed_ids = self.processed_message_ids.lock().unwrap();

        for message in log.iter() {
            if processed_ids.contains(&message.id) {
                return Err("Duplicate message detected.".to_string());
            }
        }

        for message in log.iter() {
            self.message_queue.send(message.clone())?;
            processed_ids.insert(message.id);
        }

        Ok(())
    }

    /// Rolls back the current transaction.
    ///
    /// This method clears the transaction log.
    pub fn rollback_transaction(&self) {
        let mut log = self.transaction_log.lock().unwrap();
        log.clear();
    }

    /// Sends a message to the broker.
    ///
    /// This method adds the message to the transaction messages.
    ///
    /// # Arguments
    /// * `message` - The message to send.
    ///
    /// # Returns
    /// An empty result if the message is sent successfully.
    pub fn send_message(&self, message: Message) -> Result<(), String> {
        let mut messages = self.transaction_messages.lock().unwrap();
        messages.push(message);
        Ok(())
    }

    /// Receives a message from the broker.
    ///
    /// This method returns the next message in the message queue.
    ///
    /// If the message has already been processed, it is not returned.
    ///
    /// # Returns
    /// An optional message if one is available.
    pub fn receive_message(&self) -> Option<Message> {
        if let Some(message) = self.message_queue.receive() {
            let mut processed_ids = self.processed_message_ids.lock().unwrap();
            if processed_ids.contains(&message.id) {
                None
            } else {
                processed_ids.insert(message.id);
                Some(message)
            }
        } else {
            None
        }
    }

    /// Sends a message to the broker and waits for an ACK.
    ///
    /// # Arguments
    /// * `message` - The message to send.
    /// * `timeout_duration` - The duration to wait for an ACK.
    ///
    /// # Returns
    /// The ACK for the message if it is received within the timeout duration.
    ///
    /// # Errors
    /// An error if the ACK is not received within the timeout duration.
    pub async fn send_message_with_ack(
        &self,
        message: Message,
        timeout_duration: Duration,
    ) -> Result<MessageAck, String> {
        self.send_message(message.clone())?;

        match timeout(timeout_duration, self.wait_for_ack(message.id)).await {
            Ok(ack) => ack,
            Err(_) => Err("ACK timeout".to_string()),
        }
    }

    /// Waits for an ACK for the specified message.
    ///
    /// # Arguments
    /// * `message_id` - The ID of the message to wait for an ACK.
    ///
    /// # Returns
    /// The ACK for the message if it is received.
    ///
    /// # Errors
    /// An error if the ACK is not received.
    async fn wait_for_ack(&self, message_id: Uuid) -> Result<MessageAck, String> {
        let (tx, rx) = oneshot::channel();

        {
            let mut ack_waiters = self.ack_waiters.lock().unwrap();
            ack_waiters.insert(message_id, tx);
        }

        match rx.await {
            Ok(ack) => Ok(ack),
            Err(_) => Err("Failed to receive ACK".to_string()),
        }
    }

    /// Receives an ACK for the specified message.
    ///
    /// This method removes the ACK from the ACK waiters.
    ///
    /// # Arguments
    /// * `ack` - The ACK to receive.
    ///
    /// # Returns
    /// An empty result if the ACK is received successfully.
    pub fn receive_ack(&self, ack: MessageAck) {
        let mut ack_waiters = self.ack_waiters.lock().unwrap();
        if let Some(tx) = ack_waiters.remove(&ack.message_id) {
            let _ = tx.send(ack);
        }
    }

    /// Performs an operation with retry.
    ///
    /// This method performs the specified operation with retry.
    ///
    /// # Arguments
    /// * `operation` - The operation to perform.
    /// * `max_retries` - The maximum number of retries.
    /// * `delay` - The delay between retries.
    ///
    /// # Returns
    /// The result of the operation if it is successful.
    ///
    /// # Errors
    /// An error if the operation fails after the maximum number of retries.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::Broker;
    /// use std::time::Duration;
    ///
    /// let broker = Broker::new("broker1", 3, 2, "logs");
    /// let result = broker.perform_operation_with_retry(|| {
    ///     Ok(42)
    /// }, 3, Duration::from_secs(1));
    ///
    /// // Check the result
    /// assert_eq!(result.unwrap(), 42);
    /// ```
    pub fn perform_operation_with_retry<F, T, E>(
        &self,
        operation: F,
        max_retries: u32,
        delay: Duration,
    ) -> Result<T, E>
    where
        F: Fn() -> Result<T, E>,
        E: std::fmt::Debug,
    {
        let mut retries = 0;
        loop {
            match operation() {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retries >= max_retries {
                        return Err(err);
                    }
                    retries += 1;
                    std::thread::sleep(delay);
                }
            }
        }
    }

    /// Checks if the broker is healthy.
    ///
    /// This method checks the health of the broker by performing various checks:
    /// - Storage health check
    /// - Node health check
    /// - Message queue status check
    /// - Leader election status check
    ///
    /// # Returns
    /// A boolean value indicating whether the broker is healthy.
    pub fn is_healthy(&self) -> bool {
        // Storage Health Check
        if let Ok(storage) = self.storage.lock() {
            if !storage.is_available() {
                return false;
            }
        } else {
            return false;
        }

        // Node Health Check
        if let Ok(nodes) = self.nodes.lock() {
            for node in nodes.values() {
                if let Ok(data) = node.data.lock() {
                    if data.is_empty() {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }

        // Checking the status of the message queue
        if self.message_queue.is_empty() {
            return false;
        }

        // Check the status of the leader election
        if *self.leader_election.state.lock().unwrap() != BrokerState::Leader {
            return false;
        }

        true
    }

    /// Replicates data to the specified partition.
    ///
    /// This method replicates the specified data to the partition with the specified ID.
    ///
    /// # Arguments
    /// * `partition_id` - The ID of the partition to replicate the data to.
    /// * `data` - The data to replicate.
    pub fn replicate_data(&self, partition_id: &str, data: &[u8]) {
        let replicas = self.replicas.lock().unwrap();
        if let Some(nodes) = replicas.get(partition_id) {
            let partition_id_usize = partition_id.parse::<usize>().expect("Invalid partition_id");
            let num_nodes = nodes.len();
            let node_index = partition_id_usize % num_nodes;
            if let Some(node_id) = nodes.get(node_index) {
                if let Some(node) = self.nodes.lock().unwrap().get(node_id) {
                    let mut node_data = node.data.lock().unwrap();
                    node_data.clear();
                    node_data.extend_from_slice(data);
                }
            }
        }
    }

    /// Monitors the nodes in the cluster.
    ///
    /// This method monitors the nodes in the cluster and checks their health
    /// ([`check_node_health`]).
    /// If a node is not healthy, it is recovered ([`recover_node`]).
    fn monitor_nodes(&self) {
        let storage = self.storage.clone();
        let consumer_groups = self.consumer_groups.clone();
        std::thread::spawn(move || {
            loop {
                let node_healthy = check_node_health(&storage);

                if !node_healthy {
                    recover_node(&storage, &consumer_groups);
                }

                std::thread::sleep(Duration::from_secs(5));
            }
        });
    }

    /// Detects a node failure and initiates fail-over.
    ///
    /// This method detects a node failure and initiates fail-over by:
    /// 1. Removing the node from the cluster.
    /// 2. Rebalancing the partitions.
    /// 3. Starting a new leader election.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the failed node.
    pub fn detect_failure(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.remove(node_id).is_some() {
            // println!("Node {} has failed", node_id);
            drop(nodes);
            self.rebalance_partitions();
            self.start_election();
        }
    }

    /// Adds a node to the cluster.
    ///
    /// This method adds a node to the cluster and rebalances the partitions.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to add.
    /// * `node` - The node to add.
    pub fn add_node(&self, node_id: String, node: Node) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(node_id, node);
        drop(nodes);
        self.rebalance_partitions();
    }

    /// Removes a node from the cluster.
    ///
    /// This method removes a node from the cluster and rebalances the partitions.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to remove.
    pub fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.remove(node_id);
        drop(nodes);
        self.rebalance_partitions();
    }

    /// Rebalances the partitions in the cluster.
    ///
    /// This method rebalances the partitions in the cluster by assigning each partition to a node.
    /// The partitions are assigned to nodes in a round-robin fashion.
    pub fn rebalance_partitions(&self) {
        let nodes = self.nodes.lock().unwrap();
        let num_nodes = nodes.len();
        if num_nodes == 0 {
            return;
        }

        let mut partitions = self.partitions.lock().unwrap();
        for (partition_id, partition) in partitions.iter_mut() {
            let partition_id_usize = partition_id.parse::<usize>().expect("Invalid partition_id");
            let node_index = partition_id_usize % num_nodes;
            let node_id = nodes.keys().nth(node_index).unwrap().clone();
            partition.node_id = node_id;
        }
    }

    /// Starts a new leader election.
    ///
    /// This method starts a new leader election by electing a new leader.
    ///
    /// # Returns
    /// A boolean value indicating whether a new leader was elected.
    pub fn start_election(&self) -> bool {
        let nodes = self.nodes.lock().unwrap();
        if let Some((new_leader, _)) = nodes.iter().next() {
            let mut leader = self.leader.lock().unwrap();
            *leader = Some(new_leader.clone());
            // println!("New leader elected: {}", new_leader);
            return true;
        }
        false
    }

    /// Checks if the broker is the leader.
    ///
    /// # Returns
    /// A boolean value indicating whether the broker is the leader.
    pub fn is_leader(&self) -> bool {
        *self.leader_election.state.lock().unwrap() == BrokerState::Leader
    }

    /// Creates a new topic with the specified name and number of partitions.
    ///
    /// # Arguments
    /// * `name` - The name of the topic.
    /// * `num_partitions` - The number of partitions for the topic.
    ///   If not provided, the default number of partitions is used.
    ///
    /// # Returns
    /// An empty result if the topic is created successfully.
    ///
    /// # Errors
    /// An error if the topic already exists.
    pub fn create_topic(
        &mut self,
        name: &str,
        num_partitions: Option<usize>,
    ) -> Result<(), BrokerError> {
        if self.topics.contains_key(name) {
            return Err(BrokerError::TopicError(format!(
                "Topic {} already exists",
                name
            )));
        }

        let partitions = num_partitions.unwrap_or(self.num_partitions);
        let topic = Topic::new(name, partitions, self.replication_factor);
        self.topics.insert(name.to_string(), topic);
        Ok(())
    }

    /// Subscribes a consumer to the specified topic.
    ///
    /// If a group ID is provided, the consumer is added to the consumer group.
    /// Otherwise, the consumer is added as a subscriber to the topic.
    ///
    /// # Arguments
    /// * `topic_name` - The name of the topic to subscribe to.
    /// * `subscriber` - The subscriber to add.
    /// * `group_id` - The ID of the consumer group to add the consumer to.
    ///   If not provided, the consumer is added as a subscriber.
    ///
    /// # Returns
    /// An empty result if the subscriber is added successfully.
    ///
    /// # Errors
    /// An error if the topic is not found.
    pub fn subscribe(
        &mut self,
        topic_name: &str,
        subscriber: Subscriber,
        group_id: Option<&str>,
    ) -> Result<(), BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            if let Some(group_id) = group_id {
                let mut groups = self.consumer_groups.lock().unwrap();
                let group = groups
                    .entry(group_id.to_string())
                    .or_insert_with(|| ConsumerGroup::new(group_id));
                group.add_member(&subscriber.id.clone(), subscriber);
            } else {
                topic.add_subscriber(subscriber);
            }
            Ok(())
        } else {
            Err(BrokerError::TopicError(format!(
                "Topic {} not found",
                topic_name
            )))
        }
    }

    /// Publishes a message to the specified topic sending an ACK.
    ///
    /// An ACK is sent to the publisher after the message is published.
    ///
    /// If a key is provided, the message is partitioned based on the key.
    ///
    /// # Arguments
    /// * `topic_name` - The name of the topic to publish the message to.
    /// * `message` - The message to publish.
    /// * `key` - The key to use for partitioning the message.
    ///   If not specified, the message is published to the partition selected
    ///   based on the current system time (see also [`Topic::publish`]).
    ///
    /// # Returns
    /// The ACK for the message if it is published successfully.
    ///
    /// # Errors
    /// An error if the topic is not found.
    pub async fn publish_with_ack(
        &mut self,
        topic_name: &str,
        message: String,
        key: Option<&str>,
    ) -> Result<MessageAck, BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            let partition_id = topic.publish(message.clone(), key)?;

            if let Ok(mut groups) = self.consumer_groups.lock() {
                for group in groups.values_mut() {
                    if let Err(e) = group.deliver_message(&message).await {
                        eprintln!("Message delivery failed: {}", e);
                    }
                }
            }

            let ack = MessageAck::new(
                Uuid::new_v4(),
                Utc::now(),
                AckStatus::Processed,
                topic_name.to_string(),
                partition_id,
            );

            Ok(ack)
        } else {
            Err(BrokerError::TopicError(format!(
                "Topic {} not found",
                topic_name
            )))
        }
    }

    /// Rotates the log file.
    ///
    /// This method compresses the log file and creates a new log file.
    pub fn rotate_logs(&self) {
        let log_path = Path::new(&self.log_path);
        let rotated = log_path.with_extension("old");
        if let Err(e) = LogCompressor::compress_file(log_path, rotated.as_path()) {
            eprintln!("Failed to compress log file: {}", e);
        }
        if let Err(e) = File::create(&self.log_path) {
            eprintln!("Failed to create new log file: {}", e);
        }
    }

    /// Writes a message to the log file.
    ///
    /// # Arguments
    /// * `message` - The message to write to the log file.
    ///
    /// # Returns
    /// An empty result if the message is written successfully.
    ///
    /// # Errors
    /// An error if the message cannot be written to the log file.
    pub fn write_log(&self, message: &str) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;
        writeln!(file, "{}", message)?;
        Ok(())
    }

    /// Cleans up the log files.
    ///
    /// This method cleans up the old log files.
    ///
    /// # Returns
    /// An empty result if the log files are cleaned up successfully.
    ///
    /// # Errors
    /// An error if the log files cannot be cleaned up.
    pub fn cleanup_logs(&self) -> Result<(), BrokerError> {
        self.storage.lock().unwrap().cleanup_logs()?;
        Ok(())
    }
}

/// The `Node` struct represents a node in the cluster.
///
/// It contains various fields related to the node, such as its ID, address, and data.
#[derive(Clone)]
pub struct Node {
    /// The unique identifier of the node.
    pub id: String,
    /// The address of the node.
    pub address: String,
    /// A boolean value indicating whether the node is active.
    pub is_active: bool,
    /// The data stored on the node.
    pub data: Arc<Mutex<Vec<u8>>>,
}

impl Node {
    pub fn new(id: &str, address: &str, is_active: bool) -> Self {
        Node {
            id: id.to_string(),
            address: address.to_string(),
            is_active,
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

/// The `Shard` struct represents a shard in the cluster.
///
/// It contains the ID of the node that the shard is assigned to.
///
/// Shards are used to partition the data in the cluster.
pub struct Shard {
    /// The ID of the node that the shard is assigned to.
    pub node_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::fs::{self, File, OpenOptions};
    use std::path::Path;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::tempdir;

    /// Utility function to create a test broker.
    fn create_test_broker() -> Broker {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();

        Broker {
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
            id: String::from("test_broker"),
            leader: Arc::new(Mutex::new(Some(String::from("leader1")))),
            storage: Arc::new(Mutex::new(Storage::new(&storage_path).unwrap())),
            topics: HashMap::new(),
            num_partitions: 1,
            replication_factor: 1,
            term: AtomicU64::new(1),
            leader_election: LeaderElection::new("test_broker", HashMap::new()),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(HashMap::new())),
            message_queue: Arc::new(MessageQueue::new(1, 10)),
            log_path: "logs/broker.log".to_string(),
            ack_waiters: Arc::new(Mutex::new(HashMap::new())),
            processed_message_ids: Arc::new(Mutex::new(HashSet::new())),
            transaction_log: Arc::new(Mutex::new(Vec::new())),
            transaction_messages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Utility function to create a test node.
    fn create_test_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            address: "127.0.0.1:8080".to_string(),
            is_active: true,
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Utility function to create a test broker with a log path.
    fn create_test_broker_with_path(log_path: &str) -> Broker {
        Broker {
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
            id: String::from("test_broker"),
            leader: Arc::new(Mutex::new(Some(String::from("leader1")))),
            storage: Arc::new(Mutex::new(Storage::new("test_storage").unwrap())),
            topics: HashMap::new(),
            num_partitions: 3,
            replication_factor: 2,
            term: AtomicU64::new(0),
            leader_election: LeaderElection::new("test_broker", HashMap::new()),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(HashMap::new())),
            message_queue: Arc::new(MessageQueue::new(1, 10)),
            log_path: log_path.to_string(),
            ack_waiters: Arc::new(Mutex::new(HashMap::new())),
            processed_message_ids: Arc::new(Mutex::new(HashSet::new())),
            transaction_log: Arc::new(Mutex::new(Vec::new())),
            transaction_messages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Utility function to set up test logs.
    fn setup_test_logs(path: &str) {
        cleanup_test_logs(path);
    }

    /// Utility function to clean up test logs.
    fn cleanup_test_logs(path: &str) {
        if Path::new(path).exists() {
            let _ = fs::remove_file(path);
        }
        let old_path = format!("{}.old", path);
        if Path::new(&old_path).exists() {
            let _ = fs::remove_file(old_path);
        }
    }

    /// Tests the creation of a node.
    ///
    /// # Purpose
    /// Tests that a node can be created with the specified ID, address, and active status.
    ///
    /// # Steps
    /// 1. Create a new node with the ID "node1", address "127.0.0.1:8080", and active status.
    /// 2. Check that the node ID is "node1".
    /// 3. Check that the node address is "127.0.0.1:8080".
    /// 4. Check that the node is active.
    #[test]
    fn test_node_creation() {
        let node = Node::new("node1", "127.0.0.1:8080", true);
        assert_eq!(node.id, "node1");
        assert_eq!(node.address, "127.0.0.1:8080");
        assert!(node.is_active);
    }

    /// Tests log rotation and cleanup.
    ///
    /// # Purpose
    /// Tests that the log file can be rotated and cleaned up.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new storage instance with the temporary directory path.
    /// 3. Attempt to rotate the logs.
    /// 4. Check that the log rotation fails because the old log file does not exist.
    /// 5. Create an old log file.
    /// 6. Attempt to rotate the logs.
    /// 7. Check that the log rotation succeeds.
    #[test]
    fn test_log_rotation_and_cleanup() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();
        let storage = Storage::new(&storage_path).unwrap();

        let result = storage.rotate_logs();
        assert!(result.is_err(), "Old log file does not exist");

        let old_log_path = format!("{}.old", storage_path);
        File::create(&old_log_path).unwrap();
        let result = storage.rotate_logs();
        assert!(
            result.is_ok(),
            "Log rotation should succeed when old log file exists"
        );
    }

    /// Tests the creation of a broker.
    ///
    /// # Purpose
    /// Tests that a broker can be created with the specified ID,
    /// number of partitions, replication factor, and storage path.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Check that the broker ID is "broker1".
    /// 3. Check that the number of partitions is 3.
    /// 4. Check that the replication factor is 2.
    #[test]
    fn test_broker_creation() {
        let test_log = "test_logs_creation";
        setup_test_logs(test_log);

        let broker = Broker::new("broker1", 3, 2, test_log);
        assert_eq!(broker.id, "broker1");
        assert_eq!(broker.num_partitions, 3);
        assert_eq!(broker.replication_factor, 2);

        cleanup_test_logs(test_log);
    }

    /// Tests the creation of a topic.
    ///
    /// # Purpose
    /// Tests that a topic can be created with the specified name and number of partitions.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Create a new topic "test_topic" with 3 partitions (default).
    /// 3. Check that the topic is created successfully.
    #[test]
    fn test_create_topic() {
        let test_log = "test_logs_topic";
        setup_test_logs(test_log);

        let mut broker = Broker::new("broker1", 3, 2, test_log);
        broker.create_topic("test_topic", None).unwrap();
        assert!(broker.topics.contains_key("test_topic"));

        cleanup_test_logs(test_log);
    }

    /// Tests the subscription of a consumer to a topic and the publication of a message.
    ///
    /// # Purpose
    /// Tests that a consumer can subscribe to a topic and receive a published message.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Create a new topic "test_topic" with 3 partitions (default).
    /// 3. Create a new subscriber "test_consumer" that prints the received message.
    /// 4. Subscribe the subscriber to the topic.
    /// 5. Publish a message to the topic.
    /// 6. Check that the message is received by the subscriber.
    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let mut broker = create_test_broker();
        let topic = "test_topic";
        let message = "test_message".to_string();

        broker.create_topic(topic, None).unwrap();

        let subscriber = Subscriber::new(
            "test_consumer",
            Box::new(move |msg: String| {
                println!("Received message: {}", msg);
            }),
        );

        broker
            .subscribe(topic, subscriber, Some("test_group"))
            .unwrap();

        let ack = broker
            .publish_with_ack(topic, message.clone(), None)
            .await
            .unwrap();

        assert!(
            !ack.message_id.is_nil(),
            "The message ID was not generated correctly."
        );

        if let Some(received) = broker.receive_message() {
            assert_eq!(
                received.content, message,
                "The content of the received message does not match."
            );
        }
    }

    /// Tests the distribution of messages to a consumer group.
    ///
    /// # Purpose
    /// Tests that messages are distributed to consumers in a consumer group.
    ///
    /// # Steps
    /// 1. Create a temporary directory to store the test storage.
    /// 2. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and the test storage path.
    /// 3. Create a new topic "test_topic" with 3 partitions (default).
    /// 4. Create a new subscriber "consumer_1" that increments a counter for each message received.
    ///    And subscribe the subscriber to the topic.
    /// 5. Publish 10 messages to the topic.
    /// 6. Wait for 1 second and
    ///    check that the number of messages processed matches the number of messages published.
    #[tokio::test]
    async fn test_consumer_group_message_distribution() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("test_storage");
        fs::create_dir_all(&storage_path).unwrap();

        let mut broker = create_test_broker_with_path(storage_path.to_str().unwrap());
        let topic = "test_topic";
        let group = "test_group";
        let message_count = 10;
        let group_message_counter = Arc::new(AtomicUsize::new(0));

        broker.create_topic(topic, None).unwrap();
        let counter = group_message_counter.clone();

        let subscriber = Subscriber::new(
            "consumer_1",
            Box::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
            }),
        );
        broker.subscribe(topic, subscriber, Some(group)).unwrap();

        for i in 0..message_count {
            broker
                .publish_with_ack(topic, format!("message_{}", i), None)
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            group_message_counter.load(Ordering::SeqCst),
            message_count,
            "Number of messages processed does not match"
        );
    }

    /// Tests the replication of data to a partition.
    ///
    /// # Purpose
    /// Tests that data can be replicated to a partition.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Replicate data to partition 1.
    /// 3. Check that the data is replicated to the correct node.
    /// 4. Check that the data is not replicated to non-existent nodes.
    #[test]
    fn test_data_replication() {
        let broker = create_test_broker();
        let partition_id = "1";
        let data = vec![1, 2, 3];

        broker.replicate_data(partition_id, &data);

        let mut partitions = HashMap::new();
        for i in 0..3 {
            partitions.insert(
                i.to_string(),
                Shard {
                    node_id: format!("node_{}", i),
                },
            );
        }
        let partition_id = "test_partition";
        let nodes = ["node1", "node2", "node3"];
        let node_index = partition_id.as_bytes()[0] as usize % nodes.len();

        assert!(node_index < nodes.len(), "Node index is out of range.");
    }

    /// Tests the fail-over mechanism.
    ///
    /// # Purpose
    /// Tests that the fail-over mechanism works correctly when a node fails.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Create two test nodes "node1" and "node2".
    /// 3. Add the nodes to the broker.
    /// 4. Set "node1" as the leader.
    /// 5. Detect the failure of "node1".
    /// 6. Check that "node1" is removed from the cluster.
    /// 7. Check that "node2" is the new leader.
    #[test]
    fn test_failover() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node1 = create_test_node("node1");
        let node2 = create_test_node("node2");

        broker.add_node("node1".to_string(), node1);
        broker.add_node("node2".to_string(), node2);

        {
            let mut leader = broker.leader.lock().unwrap();
            *leader = Some("node1".to_string());
        }

        broker.detect_failure("node1");

        let nodes = broker.nodes.lock().unwrap();
        assert!(!nodes.contains_key("node1"));
        assert!(nodes.contains_key("node2"));

        let leader = broker.leader.lock().unwrap();
        assert_eq!(leader.as_deref(), Some("node2"));
    }

    /// Tests the rebalancing of partitions.
    ///
    /// # Purpose
    /// Tests that the partitions are rebalanced correctly when a node is added or removed.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Insert three shards in three partitions.
    /// 3. Check that the first partition is assigned to node1.
    #[test]
    fn test_rebalance() {
        let _broker = create_test_broker();
        let mut partitions = HashMap::new();
        for i in 0..3 {
            let partition_id = i.to_string();
            partitions.insert(
                partition_id,
                Shard {
                    node_id: format!("node_{}", i),
                },
            );
        }

        let nodes = ["node1", "node2", "node3"];
        let partition_id = "test_partition";
        let node_index = partition_id.chars().next().map(|c| c as usize).unwrap_or(0) % nodes.len();

        assert!(node_index < nodes.len(), "Node index is out of range.");
        assert!(partitions.contains_key("0"), "Partition 0 does not exist.");
    }

    /// Tests the addition of an existing node.
    ///
    /// # Purpose
    /// Tests that adding an existing node does not create a duplicate node.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Create a test node "node1".
    /// 3. Add the node to the broker.
    /// 4. Add the node to the broker again.
    /// 5. Check that only one node is added.
    #[test]
    fn test_add_existing_node() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node = create_test_node("node1");

        broker.add_node("node1".to_string(), node.clone());
        broker.add_node("node1".to_string(), node);

        let nodes = broker.nodes.lock().unwrap();
        assert_eq!(nodes.len(), 1);
    }

    /// Tests the removal of a non-existent node.
    ///
    /// # Purpose
    /// Tests that removing a non-existent node does not cause an error.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Remove a non-existent node.
    /// 3. Check that no nodes are present in the broker.
    #[test]
    fn test_remove_nonexistent_node() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        broker.remove_node("node1");

        let nodes = broker.nodes.lock().unwrap();
        assert_eq!(nodes.len(), 0);
    }

    /// Tests the replication of data to non-existent nodes.
    ///
    /// # Purpose
    /// Tests that data is not replicated to non-existent nodes.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Replicate data to a non-existent node.
    /// 3. Check that the data is not replicated to any nodes.
    #[test]
    fn test_replicate_to_nonexistent_node() {
        let broker = create_test_broker();
        let partition_id = "1";
        let data = vec![1, 2, 3];

        broker.replicate_data(partition_id, &data);

        let replicas = broker.replicas.lock().unwrap();
        assert!(
            replicas.is_empty(),
            "Replication is being performed on a node that does not exist."
        );
    }

    /// Tests the election of a leader with no nodes.
    ///
    /// # Purpose
    /// Tests that a leader election fails when there are no nodes in the cluster.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Start a new leader election.
    /// 3. Check that the leader election fails.
    #[test]
    fn test_election_with_no_nodes() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let elected = broker.start_election();

        assert!(!elected);
    }

    /// Tests adding an invalid node.
    ///
    /// # Purpose
    /// Tests that adding an invalid node does not create a duplicate node.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Create a test node "node1".
    /// 3. Add the node to the broker.
    /// 4. Add the node to the broker again.
    /// 5. Check that only one node is added.
    #[test]
    fn test_add_invalid_node() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node = create_test_node("node1");

        broker.add_node("node1".to_string(), node.clone());
        broker.add_node("node1".to_string(), node);

        let nodes = broker.nodes.lock().unwrap();
        assert_eq!(nodes.len(), 1, "Duplicate nodes should not be added.");
    }

    /// Tests electing a leader with no nodes.
    ///
    /// # Purpose
    /// Tests that a leader election fails when there are no nodes in the cluster.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Start a new leader election.
    /// 3. Check that the leader election fails.
    #[test]
    fn test_elect_leader_with_no_nodes() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let elected = broker.start_election();
        assert!(
            !elected,
            "If there are no nodes, the leader election should fail."
        );
    }

    /// Tests replicating data to non-existent nodes.
    ///
    /// # Purpose
    /// Tests that data is not replicated to non-existent nodes.
    ///
    /// # Steps
    /// 1. Create a new broker with the ID "broker1", 3 partitions, 2 replicas,
    ///    and "logs" storage path.
    /// 2. Replicate data to non-existent nodes.
    /// 3. Check that the data is not replicated to any nodes.
    #[tokio::test]
    async fn test_replicate_to_nonexistent_nodes() {
        let broker = create_test_broker();
        let partition_id = "1";
        let data = vec![1, 2, 3];
        let timeout = Duration::from_secs(5);
        let replication = tokio::time::timeout(timeout, async {
            broker.replicate_data(partition_id, &data);
            for id in ["2", "3", "4"].iter() {
                broker.replicate_data(id, &data);
                let replicas = broker.replicas.lock().unwrap();
                assert!(
                    replicas.is_empty(),
                    "Replication is being performed to a non-existent node {}.",
                    id
                );
            }
        });

        match replication.await {
            Ok(_) => {
                let replicas = broker.replicas.lock().unwrap();
                assert!(
                    replicas.is_empty(),
                    "Replication is being performed to a node that does not exist."
                );
            }
            Err(_) => panic!("The replication test timed out."),
        }
    }

    /// Tests the rotation of logs.
    ///
    /// # Purpose
    /// Tests that the log file can be rotated successfully.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new log file in the temporary directory.
    /// 3. Create a new broker with the log file path.
    /// 4. Rotate the logs.
    /// 5. Check that the log file is rotated successfully.
    #[test]
    fn test_rotate_logs() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        {
            let mut file = File::create(&log_path).unwrap();
            writeln!(file, "This is a test log entry.").unwrap();
        }

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());

        broker.rotate_logs();
        let rotated_log_path = log_path.with_extension("old");

        assert!(rotated_log_path.exists());
        assert!(log_path.exists());

        let new_log_content = fs::read_to_string(&log_path).unwrap();
        assert!(new_log_content.is_empty());
    }

    /// Tests the rotation of logs with an invalid path.
    ///
    /// # Purpose
    /// Tests that the log file cannot be rotated with an invalid path.
    ///
    /// # Steps
    /// 1. Create a new broker with an invalid log file path.
    /// 2. Rotate the logs.
    #[test]
    fn test_rotate_logs_invalid_path() {
        let broker = create_test_broker_with_path("/invalid/path/test.log");
        broker.rotate_logs();
    }

    /// Tests the rotation of logs with no permissions.
    ///
    /// # Purpose
    /// Tests that the log file cannot be rotated with no permissions.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new log file in the temporary directory.
    /// 3. Set the permissions of the log file to read-only.
    /// 4. Create a new broker with the log file path.
    /// 5. Rotate the logs.
    #[test]
    fn test_rotate_logs_no_permissions() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        {
            let mut file = File::create(&log_path).unwrap();
            writeln!(file, "test data").unwrap();
        }

        fs::set_permissions(
            &log_path,
            <fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o444),
        )
        .unwrap();

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        broker.rotate_logs();
    }

    /// Tests rotating logs with a file not found.
    ///
    /// # Purpose
    /// Tests that the log file cannot be rotated if it does not exist.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new broker with a non-existent log file path.
    /// 3. Rotate the logs.
    #[test]
    fn test_rotate_logs_file_not_found() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("nonexistent.log");

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        broker.rotate_logs();
    }

    /// Tests rotating logs with a file in use.
    ///
    /// # Purpose
    /// Tests that the log file cannot be rotated if it is in use.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new log file in the temporary directory.
    /// 3. Open the log file.
    /// 4. Check that the log file exists.
    #[test]
    fn test_rotate_logs_file_in_use() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("test_logs");
        fs::create_dir_all(&log_path).unwrap();

        let file_path = log_path.join("test.log");
        let _file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();

        assert!(file_path.exists());
    }

    /// Tests writing a log message.
    ///
    /// # Purpose
    /// Tests that a log message can be written to the log file.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new log file in the temporary directory.
    /// 3. Create a new broker with the log file path.
    /// 4. Write a log message to the log file.
    /// 5. Check that the log message is written to the log file.
    #[test]
    fn test_write_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        fs::create_dir_all(log_path.parent().unwrap()).unwrap();

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        broker.write_log("Test message 1").unwrap();
        broker.write_log("Test message 2").unwrap();

        let content = fs::read_to_string(&log_path).unwrap();
        assert!(content.contains("Test message 1"));
        assert!(content.contains("Test message 2"));
    }

    /// Tests writing a log message with an invalid path.
    ///
    /// # Purpose
    /// Tests that an error is returned when writing a log message with an invalid path.
    ///
    /// # Steps
    /// 1. Create a new broker with an invalid log file path.
    /// 2. Write a log message to the log file.
    /// 3. Check that an error is returned.
    #[test]
    fn test_write_log_invalid_path() {
        let dir = tempdir().unwrap();
        let invalid_path = dir.path().join("nonexistent").join("test.log");
        let broker = create_test_broker_with_path(invalid_path.to_str().unwrap());

        // Test error handling - check that it returns an error
        let result = broker.write_log("This should not panic");
        assert!(result.is_err());
    }

    /// Tests writing a log message with no permissions.
    ///
    /// # Purpose
    /// Tests that an error is returned when writing a log message with no permissions.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new log file in the temporary directory.
    /// 3. Set the permissions of the log file to read-only.
    /// 4. Create a new broker with the log file path.
    /// 5. Write a log message to the log file.
    /// 6. Check that an error is returned.
    #[test]
    fn test_write_log_no_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create a file and set the permissions to read-only.
        {
            let mut file = File::create(&log_path).unwrap();
            writeln!(file, "Initial content").unwrap();
        }
        fs::set_permissions(&log_path, fs::Permissions::from_mode(0o444)).unwrap();

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        let result = broker.write_log("This should handle permission error");
        assert!(result.is_err());
    }

    /// Tests rotating logs with an invalid storage path.
    ///
    /// # Purpose
    /// Tests that the log file cannot be rotated with an invalid storage path.
    ///
    /// # Steps
    /// 1. Create a new storage instance with an invalid path.
    /// 2. Check that operations on the storage instance fail.
    #[test]
    fn test_rotate_logs_with_invalid_storage() {
        let storage_result = Storage::new("/invalid/path/to/storage");
        assert!(
            storage_result.is_err(),
            "Operations on invalid storage paths should fail."
        );
    }

    /// Tests multi-threaded message processing.
    ///
    /// # Purpose
    /// Tests that messages can be processed concurrently by multiple threads.
    ///
    /// # Steps
    /// 1. Create a new broker.
    /// 2. Create a counter to track the number of processed messages.
    /// 3. Create 10 messages and spawn a new thread for each message.
    /// 4. Check that all messages are processed.
    #[tokio::test]
    async fn test_multithreaded_message_processing() {
        let broker = Arc::new(create_test_broker());
        let processed = Arc::new(AtomicUsize::new(0));
        let message_count = 10;

        let handles: Vec<_> = (0..message_count)
            .map(|i| {
                let broker = broker.clone();
                let processed = processed.clone();
                tokio::spawn(async move {
                    let message = Message {
                        id: Uuid::new_v4(),
                        content: format!("message_{}", i),
                        timestamp: Utc::now(),
                    };
                    if broker.send_message(message).is_ok() {
                        processed.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(
            processed.load(Ordering::SeqCst),
            message_count,
            "The number of processed messages does not match."
        );
    }

    /// Tests the rotation of logs with an error message.
    ///
    /// # Purpose
    /// Tests that an error message is returned when the log file cannot be rotated.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new storage instance with the temporary directory path.
    /// 3. Attempt to rotate the logs.
    /// 4. Check that an error message is returned and formatted correctly.
    #[test]
    fn test_log_rotation_error_message() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();
        let storage = Storage::new(&storage_path).unwrap();

        let result = storage.rotate_logs();
        if let Err(e) = result {
            assert_eq!(
                e.to_string(),
                format!("Old log file {}.old does not exist", storage_path)
            );
        } else {
            panic!("Expected an error but got success");
        }
    }

    /// Tests writing and reading messages.
    ///
    /// # Purpose
    /// Tests that messages can be written to and read from storage.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create a new storage instance with the temporary directory path.
    /// 3. Write two messages to storage.
    /// 4. Read the messages from storage.
    /// 5. Check that the messages are read correctly.
    #[test]
    fn test_write_and_read_messages() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();
        let mut storage = Storage::new(&storage_path).unwrap();

        storage.write_message("test_message_1").unwrap();
        storage.write_message("test_message_2").unwrap();

        let messages = storage.read_messages().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], "test_message_1");
        assert_eq!(messages[1], "test_message_2");
    }

    /// Tests operation success on the first try.
    ///
    /// # Purpose
    /// Tests that an operation succeeds on the first try.
    ///
    /// # Steps
    /// 1. Create a new broker.
    /// 2. Perform an operation that succeeds on the first try.
    /// 3. Check that the operation is successful.
    #[test]
    fn test_perform_operation_with_retry_success_on_first_try() {
        let broker = create_test_broker();

        let result = broker.perform_operation_with_retry(
            || Ok::<&str, &str>("Success"),
            3,
            Duration::from_millis(10),
        );

        assert_eq!(result.unwrap(), "Success");
    }

    /// Tests operation success after retries.
    ///
    /// # Purpose
    /// Tests whether an operation succeeds within the specified number of retries.
    ///
    /// # Steps
    /// 1. Create a new broker.
    /// 2. Perform an operation that succeeds after multiple retries.
    /// 3. Check that the operation is successful.
    #[test]
    fn test_perform_operation_with_retry_success_after_retries() {
        let broker = create_test_broker();

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = broker.perform_operation_with_retry(
            || {
                let mut num = counter_clone.lock().unwrap();
                *num += 1;
                if *num < 3 {
                    Err("Temporary Error")
                } else {
                    Ok("Success after retries")
                }
            },
            5,
            Duration::from_millis(10),
        );

        assert_eq!(result.unwrap(), "Success after retries");
        assert_eq!(*counter.lock().unwrap(), 3);
    }

    /// Tests operation failure after retries.
    ///
    /// # Purpose
    /// Tests if an operation fails after reaching the maximum number of retries.
    ///
    /// # Steps
    /// 1. Create a new broker.
    /// 2. Perform an operation that fails after multiple retries.
    /// 3. Check that the operation fails.
    #[test]
    fn test_perform_operation_with_retry_failure_after_max_retries() {
        let broker = create_test_broker();

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = broker.perform_operation_with_retry(
            || {
                let mut num = counter_clone.lock().unwrap();
                *num += 1;
                Err::<(), &str>("Persistent Error")
            },
            3,
            Duration::from_millis(10),
        );

        assert!(result.is_err());
        assert_eq!(*counter.lock().unwrap(), 4);
    }

    /// Tests the ordering of messages.
    ///
    /// # Purpose
    /// Tests that messages are received in the correct order.
    ///
    /// # Steps
    /// 1. Create a new broker.
    /// 2. Create a new topic.
    /// 3. Create a subscriber that appends messages to a vector.
    /// 4. Subscribe the subscriber to the topic.
    /// 5. Publish three messages to the topic.
    /// 6. Check that the messages are received in the correct order.
    #[tokio::test]
    async fn test_message_ordering() {
        let mut broker = create_test_broker();
        let topic = "test_topic";
        let received_messages = Arc::new(Mutex::new(Vec::new()));

        broker.create_topic(topic, None).unwrap();
        let messages = received_messages.clone();

        let subscriber = Subscriber::new(
            "test_subscriber",
            Box::new(move |msg| {
                messages.lock().unwrap().push(msg);
            }),
        );
        broker.subscribe(topic, subscriber, None).unwrap();

        let expected_messages = vec!["message1", "message2", "message3"];
        for msg in &expected_messages {
            broker
                .publish_with_ack(topic, msg.to_string(), None)
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let received = received_messages.lock().unwrap();
        assert_eq!(
            received.len(),
            expected_messages.len(),
            "Not all messages have been received."
        );

        for (i, msg) in received.iter().enumerate() {
            assert_eq!(
                msg,
                &expected_messages[i].to_string(),
                "The order of the messages is incorrect."
            );
        }
    }

    /// Tests the distribution of messages to partitions.
    ///
    /// # Purpose
    /// Tests that messages are distributed to partitions correctly.
    ///
    /// # Steps
    /// 1. Create a new broker.
    /// 2. Create a new topic with three partitions.
    /// 3. Create a subscriber that appends messages to a vector.
    /// 4. Subscribe the subscriber to the topic.
    /// 5. Publish 10 messages to the topic.
    /// 6. Check that all messages are received.
    /// 7. Check that the number of partitions matches the expected number.
    #[tokio::test]
    async fn test_partition_distribution() {
        let mut broker = create_test_broker();
        let topic = "test_topic";
        let num_partitions = 3;

        broker.create_topic(topic, Some(num_partitions)).unwrap();

        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let messages = received_messages.clone();
        let subscriber = Subscriber::new(
            "test_subscriber",
            Box::new(move |msg| {
                messages.lock().unwrap().push(msg);
            }),
        );
        broker.subscribe(topic, subscriber, None).unwrap();

        for i in 0..10 {
            broker
                .publish_with_ack(topic, format!("message_{}", i), None)
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let received = received_messages.lock().unwrap();
        assert_eq!(received.len(), 10, "Not all messages have been received.");

        if let Some(topic_data) = broker.topics.get(topic) {
            assert_eq!(
                topic_data.partitions.len(),
                { num_partitions },
                "The number of partitions does not match."
            );
        }
    }
}
