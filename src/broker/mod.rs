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

use uuid::Uuid;

use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::error::BrokerError;
use crate::broker::leader::{BrokerState, LeaderElection};
use crate::broker::log_compression::LogCompressor;
use crate::broker::message_queue::MessageQueue;
use crate::broker::node_management::{check_node_health, recover_node};
use crate::broker::storage::Storage;
use crate::broker::topic::Topic;
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

pub type ConsumerGroups = HashMap<String, ConsumerGroup>;

impl ConsumerGroup {
    pub fn reset_assignments(&mut self) {
        if let Ok(mut map) = self.assignments.lock() {
            map.clear();
        }
    }
}

pub struct Broker {
    id: String,
    topics: HashMap<String, Topic>,
    num_partitions: usize,
    replication_factor: usize,
    term: AtomicU64,
    leader_election: LeaderElection,
    storage: Arc<Mutex<Storage>>,
    consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
    nodes: Arc<Mutex<HashMap<String, Node>>>,
    partitions: Arc<Mutex<HashMap<String, Partition>>>,
    replicas: Arc<Mutex<HashMap<String, Vec<String>>>>,
    leader: Arc<Mutex<Option<String>>>,
    pub message_queue: Arc<MessageQueue>,
    log_path: String,
    processed_message_ids: Arc<Mutex<HashSet<Uuid>>>,
    transaction_log: Arc<Mutex<Vec<Message>>>,
    transaction_messages: Arc<Mutex<Vec<Message>>>,
}

impl Broker {
    /// Creates a new broker instance.
    ///
    /// # Arguments
    ///
    /// * `id` - A string slice that holds the ID of the broker.
    /// * `num_partitions` - The number of partitions for the broker.
    /// * `replication_factor` - The replication factor for the broker.
    /// * `storage_path` - The path to the storage file.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    ///
    /// let broker = Broker::new("broker1", 3, 2, "logs");
    /// assert_eq!(broker.id, "broker1");
    /// assert_eq!(broker.num_partitions, 3);
    /// assert_eq!(broker.replication_factor, 2);
    /// ```
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
        };
        broker.monitor_nodes();
        broker
    }

    pub fn begin_transaction(&self) {
        let mut messages = self.transaction_messages.lock().unwrap();
        messages.clear();
    }

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

    pub fn rollback_transaction(&self) {
        let mut log = self.transaction_log.lock().unwrap();
        log.clear();
    }

    pub fn send_message(&self, message: Message) -> Result<(), String> {
        let mut messages = self.transaction_messages.lock().unwrap();
        messages.push(message);
        Ok(())
    }

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

    pub fn detect_failure(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.remove(node_id).is_some() {
            // println!("Node {} has failed", node_id);
            drop(nodes);
            self.rebalance_partitions();
            self.start_election();
        }
    }

    pub fn add_node(&self, node_id: String, node: Node) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(node_id, node);
        drop(nodes);
        self.rebalance_partitions();
    }

    pub fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.remove(node_id);
        drop(nodes);
        self.rebalance_partitions();
    }

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

    pub fn is_leader(&self) -> bool {
        *self.leader_election.state.lock().unwrap() == BrokerState::Leader
    }

    /// Creates a new topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic.
    /// * `num_partitions` - The number of partitions for the topic.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// assert!(broker.topics.contains_key("test_topic"));
    /// ```
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

    /// Subscribes a subscriber to a topic.
    ///
    /// # Arguments
    ///
    /// * `topic_name` - The name of the topic.
    /// * `subscriber` - The subscriber to add.
    /// * `group_id` - Optional consumer group ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    /// use pilgrimage::subscriber::types::Subscriber;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// let subscriber = Subscriber::new(
    ///     "consumer1",
    ///     Box::new(|msg: String| {
    ///         println!("Received message: {}", msg);
    ///     }),
    /// );
    /// broker.subscribe("test_topic", subscriber, None).unwrap();
    /// ```
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

    /// Publishes a message to a topic with acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `topic_name` - The name of the topic.
    /// * `message` - The message to publish.
    /// * `key` - An optional key for partitioning.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// let ack = broker.publish_with_ack("test_topic", "test_message".to_string(), None).unwrap();
    /// assert_eq!(ack.topic, "test_topic");
    /// ```
    pub fn publish_with_ack(
        &mut self,
        topic_name: &str,
        message: String,
        key: Option<&str>,
    ) -> Result<MessageAck, BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            let partition_id = topic.publish(message.clone(), key)?;
            let ack = MessageAck::new(
                self.term.fetch_add(1, Ordering::SeqCst),
                topic_name,
                partition_id,
            );

            self.storage.lock().unwrap().write_message(&message)?;

            // Message delivery to consumer groups
            let message_clone = message.clone();
            let consumer_groups = self.consumer_groups.clone();
            std::thread::spawn(move || {
                if let Ok(groups) = consumer_groups.lock() {
                    for group in groups.values() {
                        if let (Ok(assignments), Ok(members)) =
                            (group.assignments.lock(), group.members.lock())
                        {
                            for (cons_id, parts) in assignments.iter() {
                                if parts.contains(&partition_id) {
                                    if let Some(member) = members.get(cons_id) {
                                        (member.subscriber.callback)(message_clone.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            });

            Ok(ack)
        } else {
            Err(BrokerError::TopicError(format!(
                "Topic {} not found",
                topic_name
            )))
        }
    }

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

    pub fn write_log(&self, message: &str) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;
        writeln!(file, "{}", message)?;
        Ok(())
    }

    pub fn cleanup_logs(&self) -> Result<(), BrokerError> {
        self.storage.lock().unwrap().cleanup_logs()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Node {
    pub id: String,
    pub address: String,
    pub is_active: bool,
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

pub struct Partition {
    pub node_id: String,
}
