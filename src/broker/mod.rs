use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use uuid::Uuid;

use crate::message::Message;
pub use crate::schema::message_schema::MessageSchema;
use crate::subscriber::types::Subscriber;

mod config;
use self::storage::Storage;
use self::topic::Topic;
pub use config::TopicConfig;

// Module declarations
pub mod cluster;
pub mod consumer;
pub mod error;
pub mod leader;
pub mod log_compression;
pub mod message_queue;
pub mod metrics;
pub mod node;
pub mod node_management;
pub mod replication;
pub mod scaling;
pub mod storage;
pub mod topic;

/// Message broker for handling pub/sub communications
#[derive(Clone)]
pub struct Broker {
    pub id: String,
    pub num_partitions: usize,
    pub replication_factor: usize,
    pub storage: Arc<Mutex<Storage>>,
    pub topics: Arc<Mutex<HashMap<String, Topic>>>,
    transaction_in_progress: Arc<Mutex<bool>>,
    transaction_messages: Arc<Mutex<Vec<MessageSchema>>>,
}

impl Broker {
    /// Creates a new broker instance
    pub fn new(
        id: &str,
        num_partitions: usize,
        replication_factor: usize,
        storage_path: &str,
    ) -> Self {
        let storage = Arc::new(Mutex::new(
            Storage::new(PathBuf::from(storage_path)).expect("Failed to initialize storage"),
        ));

        Self {
            id: id.to_string(),
            num_partitions,
            replication_factor,
            storage,
            topics: Arc::new(Mutex::new(HashMap::new())),
            transaction_in_progress: Arc::new(Mutex::new(false)),
            transaction_messages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Send a message
    pub fn send_message(&mut self, schema: MessageSchema) -> Result<(), String> {
        // If in transaction, add message to buffer
        {
            let in_progress = self
                .transaction_in_progress
                .lock()
                .map_err(|e| e.to_string())?;

            if *in_progress {
                self.transaction_messages
                    .lock()
                    .map_err(|e| e.to_string())?
                    .push(schema);
                return Ok(());
            }
        }

        // Get the topic ID of the message
        let topic_id = schema
            .topic_id
            .as_ref()
            .ok_or_else(|| "Topic ID is not specified".to_string())?;

        // Confirmation of the existence of the topic
        let mut topics = self.topics.lock().map_err(|e| e.to_string())?;
        let topic = topics
            .get_mut(topic_id)
            .ok_or_else(|| format!("Topic {} does not exist", topic_id))?;

        // Send message to topic and partition
        let partition_id = schema.partition_id.unwrap_or(0);
        let msg = Message {
            id: Uuid::new_v4(),
            content: schema.definition.clone(),
            timestamp: Utc::now(),
            topic_id: topic_id.clone(),
            partition_id,
            schema: Some(schema),
        };

        // Add message to topic queue and persist
        let mut storage = self.storage.lock().map_err(|e| e.to_string())?;
        if let Err(e) = storage.write_message(&msg) {
            return Err(format!("Message persistence failed.: {}", e));
        }

        // Notify subscribers
        for subscriber in &topic.subscribers {
            (subscriber.callback)(msg.content.clone());
        }

        Ok(())
    }

    /// Receives a message from a specific topic and partition
    pub fn receive_message(
        &self,
        topic_id: &str,
        partition_id: usize,
    ) -> Result<Option<Message>, String> {
        let topics = self.topics.lock().map_err(|e| e.to_string())?;

        // Confirmation of the existence of the topic
        let topic = topics
            .get(topic_id)
            .ok_or_else(|| format!("Topic {} does not exist", topic_id))?;

        // Verification of partitions
        if partition_id >= topic.num_partitions {
            return Err(format!(
                "Partition ID {} is invalid. Topic {} has {} partitions",
                partition_id, topic_id, topic.num_partitions
            ));
        }

        // Drop the topics lock before acquiring the storage lock to avoid potential deadlock
        drop(topics);

        // Load messages from storage
        let mut storage = self.storage.lock().map_err(|e| e.to_string())?;
        let messages = storage
            .read_messages(topic_id, partition_id)
            .map_err(|e| e.to_string())?;

        // Get the first message in FIFO order
        if !messages.is_empty() {
            let message = messages[0].clone();
            // Mark the message as consumed
            storage.consume_message(message.id);
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    /// Creates a new topic with configuration
    pub fn create_topic(&mut self, name: &str, config: Option<TopicConfig>) -> Result<(), String> {
        let mut topics = self.topics.lock().map_err(|e| e.to_string())?;
        if topics.contains_key(name) {
            return Err(format!("Topic {} already exists", name));
        }

        let conf = config.unwrap_or_else(|| TopicConfig {
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
        });

        let topic = Topic {
            name: name.to_string(),
            num_partitions: conf.num_partitions,
            replication_factor: conf.replication_factor,
            subscribers: Vec::new(),
        };

        topics.insert(name.to_string(), topic);
        Ok(())
    }

    /// Subscribe to a topic
    pub fn subscribe(&mut self, topic_id: &str, subscriber: Subscriber) -> Result<(), String> {
        let mut topics = self.topics.lock().map_err(|e| e.to_string())?;

        let topic = topics
            .get_mut(topic_id)
            .ok_or_else(|| format!("Topic {} does not exist", topic_id))?;

        // Check to see if subscribers already exist
        if topic.subscribers.iter().any(|s| s.id == subscriber.id) {
            return Err(format!("Subscriber {} already exists", subscriber.id));
        }

        topic.subscribers.push(subscriber);
        Ok(())
    }

    /// Unsubscribe from a topic
    pub fn unsubscribe(&mut self, topic_id: &str, subscriber_id: &str) -> Result<(), String> {
        let mut topics = self.topics.lock().map_err(|e| e.to_string())?;

        let topic = topics
            .get_mut(topic_id)
            .ok_or_else(|| format!("Topic {} does not exist", topic_id))?;

        let index = topic
            .subscribers
            .iter()
            .position(|s| s.id == subscriber_id)
            .ok_or_else(|| format!("Subscriber {} does not exist", subscriber_id))?;

        topic.subscribers.remove(index);
        Ok(())
    }

    /// Delete a topic
    pub fn delete_topic(&mut self, name: &str) -> Result<(), String> {
        let mut topics = self.topics.lock().map_err(|e| e.to_string())?;
        if !topics.contains_key(name) {
            return Err(format!("Topic {} does not exist", name));
        }

        topics.remove(name);
        Ok(())
    }

    /// List all topics
    pub fn list_topics(&self) -> Result<Vec<String>, String> {
        let topics = self.topics.lock().map_err(|e| e.to_string())?;
        Ok(topics.keys().cloned().collect())
    }

    /// Get topic details
    pub fn get_topic(&self, name: &str) -> Result<Option<Topic>, String> {
        let topics = self.topics.lock().map_err(|e| e.to_string())?;
        Ok(topics.get(name).cloned())
    }

    /// Starts a transaction
    pub fn begin_transaction(&mut self) {
        *self
            .transaction_in_progress
            .lock()
            .expect("Failed to acquire transaction lock") = true;
        self.transaction_messages
            .lock()
            .expect("Failed to acquire message lock")
            .clear();
    }

    /// Commit transaction
    pub fn commit_transaction(&mut self) -> Result<(), String> {
        if !*self
            .transaction_in_progress
            .lock()
            .expect("Failed to acquire transaction lock")
        {
            return Err("No active transactions".to_string());
        }

        let messages = self
            .transaction_messages
            .lock()
            .expect("Failed to acquire message lock")
            .clone();
        for message in messages {
            self.send_message(message)?;
        }

        *self
            .transaction_in_progress
            .lock()
            .expect("Failed to acquire transaction lock") = false;
        self.transaction_messages
            .lock()
            .expect("Failed to acquire message lock")
            .clear();
        Ok(())
    }

    /// Rolls back a transaction
    pub fn rollback_transaction(&mut self) {
        *self
            .transaction_in_progress
            .lock()
            .expect("Failed to acquire transaction lock") = false;
        self.transaction_messages
            .lock()
            .expect("Failed to acquire message lock")
            .clear();
    }

    /// Checks the health of the broker
    ///
    /// Storage and topic maps are considered healthy if they are accessible
    pub fn is_healthy(&self) -> bool {
        // Check if storage lock can be obtained
        if self.storage.lock().is_err() {
            return false;
        }

        // Check if you can get a lock on the topic map
        if self.topics.lock().is_err() {
            return false;
        }

        true
    }

    /// Add a new node to the broker
    pub fn add_node(&mut self, node_id: String, node: node::Node) -> Result<(), String> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| format!("Failed to lock storage: {}", e))?;

        storage
            .add_node(node_id, node)
            .map_err(|e| format!("Failed to add node: {}", e))?;

        Ok(())
    }
}
