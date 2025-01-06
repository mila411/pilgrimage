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
pub mod transaction;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::error::BrokerError;
use crate::broker::leader::{BrokerState, LeaderElection};
use crate::broker::log_compression::LogCompressor;
use crate::broker::message_queue::MessageQueue;
use crate::broker::node_management::{check_node_health, recover_node};
use crate::broker::storage::Storage;
use crate::broker::topic::Topic;
pub use crate::broker::transaction::Transaction;
pub use crate::message::ack::Message;
use crate::message::ack::MessageAck;
use crate::subscriber::types::Subscriber;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
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

#[derive(Debug)]
pub enum BrokerStatus {
    Running,
    Stopped,
    Error(String),
}

pub struct Broker {
    pub id: String,
    pub topics: HashMap<String, Topic>,
    pub num_partitions: usize,
    pub replication_factor: usize,
    pub term: AtomicU64,
    pub leader_election: LeaderElection,
    pub storage: Arc<Mutex<Storage>>,
    pub consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
    pub nodes: Arc<Mutex<HashMap<String, Node>>>,
    partitions: Arc<Mutex<HashMap<usize, Partition>>>,
    pub replicas: Arc<Mutex<HashMap<String, Vec<String>>>>,
    pub leader: Arc<Mutex<Option<String>>>,
    pub message_queue: Arc<MessageQueue>,
    log_path: String,
    pub processed_message_ids: Arc<Mutex<HashSet<Uuid>>>,
    pub ack_port: u16, // Move outside the mutable lock
    ack_listener: Option<TcpListener>,
}

impl Broker {
    pub async fn new(
        id: &str,
        num_partitions: usize,
        replication_factor: usize,
        storage_path: &str,
    ) -> Self {
        let min_instances = 1;
        let max_instances = 10;
        let check_interval = Duration::from_secs(30);
        let peers = HashMap::new(); //TODO it reads from the settings
        let leader_election = LeaderElection::new(id, peers);
        let storage = Storage::new(storage_path).expect("Failed to initialize storage");
        let log_path = format!("logs/{}.log", id);

        let ack_port = 8083;
        let ack_listener = match TcpListener::bind(format!("127.0.0.1:{}", ack_port)).await {
            Ok(listener) => Some(listener),
            Err(e) => {
                println!("Failed to bind ACK listener: {}", e);
                None
            }
        };

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
            message_queue: Arc::new(MessageQueue::new(
                min_instances,
                max_instances,
                check_interval,
            )),
            log_path: log_path.to_string(),
            processed_message_ids: Arc::new(Mutex::new(HashSet::new())),
            ack_port: ack_port,
            ack_listener,
        };
        broker.monitor_nodes();
        broker
    }

    pub fn get_status(&self) -> BrokerStatus {
        BrokerStatus::Running
    }

    pub async fn send_message_transaction(
        &self,
        transaction: &mut Transaction,
        message: Message,
    ) -> Result<(), BrokerError> {
        if self.process_message(&message).await? {
            transaction.add_message(message);
            Ok(())
        } else {
            Err(BrokerError::TransactionError)
        }
    }

    pub async fn process_message(&self, message: &Message) -> io::Result<bool> {
        let message_id = message.id.to_string(); // Convert Uuid to String

        // Load a message ID that has already been processed.
        let processed_message_ids = self.load_processed_message_ids()?;

        // Check if the message has already been processed.
        if processed_message_ids.contains(&message_id) {
            println!("メッセージは既に処理済みです: {}", message_id);
            return Ok(false);
        }

        // Save processed message ID
        self.save_processed_message_id(&message_id)?;

        Ok(true)
    }

    pub async fn send_and_process(&self, message: Message) -> Result<(), BrokerError> {
        let mut transaction = self.begin_transaction();
        self.send_message_transaction(&mut transaction, message)
            .await?;
        let _ = transaction.commit();
        Ok(())
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::new(Arc::clone(&self.storage))
    }

    pub fn perform_operation_with_retry<F, T, E>(
        &self,
        operation: F,
        max_retries: u32,
        delay: Duration,
    ) -> Result<T, E>
    where
        F: Fn() -> Result<T, E>,
    {
        let mut attempts = 0;
        loop {
            match operation() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempts >= max_retries {
                        return Err(e);
                    }
                    attempts += 1;
                    println!("Operation failed. Retry {} times...", attempts);
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

    /// Send a message and wait for an ACK.
    pub async fn send_message(&mut self, message: String) -> Result<(), String> {
        // ACK listener initialization confirmed
        self.init_ack_listener().await?;

        let max_retries = 3;
        let mut attempts = 0;

        loop {
            if attempts >= max_retries {
                return Err(
                    "The maximum number of retries has been reached. The ACK cannot be received."
                        .to_string(),
                );
            }

            println!("Message sending attempt: {}/{}", attempts + 1, max_retries);
            match TcpStream::connect("127.0.0.1:8081").await {
                Ok(mut stream) => {
                    if let Err(e) = stream.write_all(message.as_bytes()).await {
                        println!("Message sending error: {}", e);
                        attempts += 1;
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    println!(
                        "The message was sent to the receiver on port 8081.: {}",
                        message
                    );
                }
                Err(e) => {
                    println!("Could not connect to the receiving side.: {}", e);
                    attempts += 1;
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            }

            println!("Waiting for ACK on port {}.", self.ack_port);
            match self.receive_ack().await {
                Ok(_) => {
                    println!("ACK received. Message sent successfully.");
                    return Ok(());
                }
                Err(e) => {
                    println!("ACK reception error: {}", e);
                    attempts += 1;
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            }
        }
    }

    /// Initializes the ACK listener.
    async fn init_ack_listener(&mut self) -> Result<(), String> {
        if self.ack_listener.is_none() {
            let addr = format!("127.0.0.1:{}", self.ack_port);
            match TcpListener::bind(&addr).await {
                Ok(listener) => {
                    println!("The ACK listener has been bound with {}.", addr);
                    self.ack_listener = Some(listener);
                    Ok(())
                }
                Err(e) => Err(format!("Failed to bind ACK listener: {}", e)),
            }
        } else {
            Ok(())
        }
    }

    /// ACK is received. It is ensured that ACK is received with retries.
    async fn receive_ack(&self) -> Result<(), String> {
        if let Some(listener) = &self.ack_listener {
            println!("Waiting for ACK on port {}.", self.ack_port);
            let max_retries = 5;
            let mut attempts = 0;

            loop {
                if attempts >= max_retries {
                    return Err("The maximum number of retries has been reached. The ACK cannot be received.".to_string());
                }

                println!("ACK reception attempt: {}/{}", attempts + 1, max_retries);
                match timeout(Duration::from_secs(5), listener.accept()).await {
                    Ok(Ok((mut socket, _))) => {
                        let mut buf = [0; 1024];
                        match timeout(Duration::from_secs(5), socket.read(&mut buf)).await {
                            Ok(Ok(n)) => {
                                let ack = String::from_utf8_lossy(&buf[..n]).trim().to_string();
                                println!("ACK received: {}", ack);
                                if ack == "ACK" {
                                    println!("A valid ACK was received.");
                                    return Ok(());
                                } else {
                                    println!("Invalid ACK received: {}", ack);
                                    attempts += 1;
                                    continue;
                                }
                            }
                            Ok(Err(e)) => {
                                println!("ACK read error: {}", e);
                                attempts += 1;
                                continue;
                            }
                            Err(_) => {
                                println!("ACK read timeout");
                                attempts += 1;
                                continue;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        println!("ACK acceptance error: {}", e);
                        attempts += 1;
                        continue;
                    }
                    Err(_) => {
                        println!("ACK acceptance timeout");
                        attempts += 1;
                        continue;
                    }
                }
            }
        } else {
            Err("ACK listener is not initialized.".to_string())
        }
    }

    pub fn receive_message(&self) -> Option<String> {
        Some(self.message_queue.receive())
    }

    fn save_processed_message_id(&self, message_id: &str) -> io::Result<()> {
        let path = "processed_message_ids.txt";
        if !Path::new(path).exists() {
            File::create(path)?;
        }
        let mut file = OpenOptions::new().append(true).create(true).open(path)?;
        writeln!(file, "{}", message_id)?;
        Ok(())
    }

    fn load_processed_message_ids(&self) -> io::Result<Vec<String>> {
        let path = "processed_message_ids.txt";
        if !Path::new(path).exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut message_ids = Vec::new();
        for line in reader.lines() {
            message_ids.push(line?);
        }
        Ok(message_ids)
    }

    pub fn is_message_processed(&self, message_id: &Uuid) -> bool {
        let messages = self
            .storage
            .lock()
            .unwrap()
            .load_messages()
            .unwrap_or_default();
        messages.iter().any(|msg| &msg.id == message_id)
    }

    pub fn save_message(&self, message: &Message) -> io::Result<()> {
        self.storage.lock().unwrap().save_message(message)
    }

    pub fn replicate_data(&self, partition_id: usize, data: &[u8]) {
        let replicas = self.replicas.lock().unwrap();
        if let Some(nodes) = replicas.get(&partition_id.to_string()) {
            for node_id in nodes {
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
            let node_index = partition_id % num_nodes;
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
