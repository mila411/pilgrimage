//! Module for message queue management.
//!
//! This module provides a thread-safe queue for message processing,
//! with support for auto-scaling and duplicate detection.
//!
//! # Example
//! The following example demonstrates how to create a new message queue,
//! add a message to the queue, and check if the message was successfully added.
//! ```
//! use pilgrimage::broker::message_queue::MessageQueue;
//! use pilgrimage::message::message::Message;
//! use std::thread;
//!
//! // Create a new message queue with 1 minimum instance and 10 maximum instances
//! let message_queue = MessageQueue::new(1, 10, "test_storage").unwrap();
//!
//! // Create a new message
//! let message = Message::new("Hello, world!".to_string())
//!     .with_topic("test_topic".to_string()).with_partition(0);
//!
//! // Add the message to the queue using send method
//! let result = message_queue.send(message);
//! assert!(result.is_ok());
//! ```

use crate::broker::metrics::SystemMetrics;
use crate::broker::scaling::{ScalingConfig, ScalingManager};
use crate::broker::storage::Storage;
use crate::message::message::Message;
use lazy_static::lazy_static;
use prometheus::{Counter, register_counter};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use uuid::Uuid;

lazy_static! {
    /// Prometheus counter for the total number of messages received by the broker.
    static ref MESSAGES_RECEIVED: Counter = register_counter!(
        "pilgrimage_messages_received_total",
        "Total number of messages received"
    )
    .unwrap();
}

/// The `MessageQueue` struct provides a thread-safe queue for message processing,
/// with support for auto-scaling and duplicate detection.
///
/// This struct is designed to handle high-throughput message processing,
/// with mechanisms to automatically scale the number of processing instances
/// based on the current load.
pub struct MessageQueue {
    /// The queue of messages to be processed.
    queue: Mutex<VecDeque<Message>>,
    /// The set of message IDs that have already been processed.
    processed_ids: Mutex<HashSet<Uuid>>,
    /// The auto-scaler for the message queue.
    auto_scaler: ScalingManager,
    /// The condition variable for the message queue.
    condvar: Condvar,
    /// The storage backend for persisting messages.
    storage: Arc<Mutex<Storage>>,
    /// The configuration for auto-scaling
    config: ScalingConfig,
    /// This allows for processing messages in specific partitions.
    partition_queues: Mutex<HashMap<(String, usize), VecDeque<Message>>>,
}

impl MessageQueue {
    /// Creates a new `MessageQueue` instance with the given scaling parameters.
    ///
    /// # Arguments
    /// * `min_instances` - The minimum number of processing instances.
    /// * `max_instances` - The maximum number of processing instances.
    /// * `storage_path` - The file path for storing messages.
    ///
    /// # Returns
    /// A new `MessageQueue` instance with the given scaling parameters.
    pub fn new(
        min_instances: usize,
        max_instances: usize,
        storage_path: &str,
    ) -> std::io::Result<Self> {
        let config = ScalingConfig {
            min_nodes: min_instances,
            max_nodes: max_instances,
            ..Default::default()
        };

        Ok(Self {
            queue: Mutex::new(VecDeque::new()),
            processed_ids: Mutex::new(HashSet::new()),
            auto_scaler: {
                let nodes = Arc::new(Mutex::new(HashMap::new()));
                ScalingManager::new(nodes, Some(config.clone()))
            },
            condvar: Condvar::new(),
            storage: Arc::new(Mutex::new(Storage::new(storage_path.into())?)),
            config,
            partition_queues: Mutex::new(HashMap::new()),
        })
    }

    /// Add a message to the queue and notifies a waiting consumer, if any.
    ///
    /// If the queue length exceeds the high watermark, the auto-scaler scales up.
    ///
    /// # Arguments
    /// * `message` - The message to add to the queue.
    ///
    /// # Returns
    /// An empty `Result` if the message was successfully added to the queue,
    /// or an error message if the message could not be added.
    ///
    /// # Errors
    /// If the message could not be added to the queue.
    /// This can happen if the queue is poisoned or if the auto-scaler fails to scale up.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::message_queue::MessageQueue;
    /// use pilgrimage::message::message::Message;
    /// use uuid::Uuid;
    /// use tokio::runtime::Runtime;
    ///
    /// // Create a new message queue with 1 minimum instance and 10 maximum instances
    /// let message_queue = MessageQueue::new(1, 10, "test_storage").unwrap();
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string())
    ///     .with_topic("test_topic".to_string()).with_partition(0);
    ///
    /// // Add the message to the queue
    /// let rt = Runtime::new().unwrap();
    /// let result = rt.block_on(message_queue.push(message));
    ///
    /// // Check if the message was successfully added to the queue
    /// assert!(result.is_ok());
    /// ```
    pub async fn push(&self, message: Message) -> Result<(), String> {
        let mut queue = self.queue.lock().map_err(|e| e.to_string())?;
        queue.push_back(message);
        self.condvar.notify_one();
        MESSAGES_RECEIVED.inc();

        if queue.len() > 100 {
            // 仮の閾値、必要に応じて調整
            let metrics = SystemMetrics::new(
                0.8, // 高負荷を示す値
                0.7,
                queue.len() as f32,
            );
            self.auto_scaler
                .handle_metrics_update("message_queue", metrics)
                .await;
            self.auto_scaler.check_and_scale().await;
        }
        Ok(())
    }

    /// Remove and return the first message from the queue, blocking if the queue is empty.
    ///
    /// If the queue length falls below the low watermark, the auto-scaler scales down.
    ///
    /// # Returns
    /// The first message from the queue, or `None` if the queue is empty.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::message_queue::MessageQueue;
    /// use pilgrimage::message::message::Message;
    /// use uuid::Uuid;
    /// use tokio::runtime::Runtime;
    ///
    /// // Create a new message queue with 1 minimum instance and 10 maximum instances
    /// let message_queue = MessageQueue::new(1, 10, "test_storage").unwrap();
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string())
    ///     .with_topic("test_topic".to_string()).with_partition(0);
    ///
    /// // Add the message to the queue using send method
    /// let _ = message_queue.send(message);
    ///
    /// // Pop the first message from the queue
    /// let rt = Runtime::new().unwrap();
    /// let result = rt.block_on(message_queue.pop());
    ///
    /// // Check if the message was successfully popped from the queue
    /// assert!(result.is_some());
    /// ```
    pub async fn pop(&self) -> Option<Message> {
        let mut queue = self.queue.lock().ok()?;
        while queue.is_empty() {
            queue = self.condvar.wait(queue).ok()?;
        }
        let message = queue.pop_front();

        if queue.len() < 50 {
            // Tentative thresholds, adjust as needed
            let metrics = SystemMetrics::new(
                0.3, // Value indicating low load
                0.4,
                queue.len() as f32,
            );
            let _ = self
                .auto_scaler
                .handle_metrics_update("message_queue", metrics)
                .await;
        }
        message
    }

    /// Check if a message with the given UUID has already been processed.
    ///
    /// # Arguments
    /// * `id` - The UUID of the message to check.
    ///
    /// # Returns
    /// `true` if the message has already been processed, `false` otherwise.
    pub fn is_processed(&self, id: &Uuid) -> bool {
        self.processed_ids
            .lock()
            .map(|ids| ids.contains(id))
            .unwrap_or(false)
    }

    /// Send a message to the queue if it has not been processed before.
    ///
    /// This method checks for duplicate messages and ensures that each message
    /// is processed only once.
    ///
    /// # Arguments
    /// * `message` - The message to send to the queue.
    ///
    /// # Returns
    /// An empty `Result` if the message was successfully sent to the queue,
    /// or an error message if a duplicate message was detected or an error occurred.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::message_queue::MessageQueue;
    /// use pilgrimage::message::message::Message;
    ///
    /// // Create a new message queue with 1 minimum instance and 10 maximum instances
    /// let message_queue = MessageQueue::new(1, 10, "test_storage").unwrap();
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string())
    ///     .with_topic("test_topic".to_string()).with_partition(0);
    ///
    /// // Send the message to the queue
    /// let result = message_queue.send(message);
    ///
    /// // Check if the message was successfully sent to the queue
    /// assert!(result.is_ok());
    /// ```
    pub fn send(&self, message: Message) -> Result<(), String> {
        let mut processed_ids = self.processed_ids.lock().unwrap();
        if processed_ids.contains(&message.id) {
            return Err("Duplicate message detected.".to_string());
        }

        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message.clone());
        processed_ids.insert(message.id);
        MESSAGES_RECEIVED.inc();
        self.condvar.notify_one();
        Ok(())
    }

    /// Receive a message from the queue without blocking.
    ///
    /// # Returns
    /// The first message from the queue, or `None` if the queue is empty.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::message_queue::MessageQueue;
    /// use pilgrimage::message::message::Message;
    ///
    /// // Create a new message queue with 1 minimum instance and 10 maximum instances
    /// let message_queue = MessageQueue::new(1, 10, "test_storage").unwrap();
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string())
    ///     .with_topic("test_topic".to_string()).with_partition(0);
    ///
    /// // Send the message to the queue
    /// let _ = message_queue.send(message);
    ///
    /// // Receive the first message from the queue
    /// let result = message_queue.receive();
    ///
    /// // Check if the message was successfully received from the queue
    /// assert!(result.is_some());
    /// ```
    pub fn receive(&self) -> Option<Message> {
        if let Ok(mut queue) = self.queue.lock() {
            queue.pop_front()
        } else {
            None
        }
    }

    /// Check if the queue is empty.
    ///
    /// # Returns
    /// `true` if the queue is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        let queue = self.queue.lock().unwrap();
        queue.is_empty()
    }

    /// Send a message to a specific partition if it has not been processed before.
    ///
    /// This method checks for duplicate messages and ensures that each message
    /// is processed only once. It also persists the message to disk.
    ///
    /// # Arguments
    /// * `message` - The message to send to the partition.
    /// * `partition_id` - The ID of the partition to send the message to.
    ///
    /// # Returns
    /// An empty `Result` if the message was successfully sent to the partition,
    /// or an error message if a duplicate message was detected, an error occurred,
    /// or the message could not be sent to the partition.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::message_queue::MessageQueue;
    /// use pilgrimage::message::message::Message;
    ///
    /// // Create a new message queue with 1 minimum instance and 10 maximum instances
    /// let message_queue = MessageQueue::new(1, 10, "test_storage").unwrap();
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, partitioned world!".to_string())
    ///     .with_topic("test_topic".to_string()).with_partition(1);
    ///
    /// // Send the message to partition 1
    /// let result = message_queue.send_to_partition(message);
    ///
    /// // Check if the message was successfully sent to the partition
    /// assert!(result.is_ok());
    /// ```
    pub fn send_to_partition(&self, message: Message) -> Result<(), String> {
        let mut processed_ids = self.processed_ids.lock().unwrap();
        if processed_ids.contains(&message.id) {
            return Err("Duplicate message detected.".to_string());
        }

        // Message Persistence
        if let Err(e) = self.storage.lock().unwrap().write_message(&message) {
            return Err(format!("Failed to persist message: {}", e));
        }

        let mut partition_queues = self.partition_queues.lock().unwrap();
        let queue = partition_queues
            .entry((message.topic_id.clone(), message.partition_id))
            .or_insert_with(VecDeque::new);
        queue.push_back(message.clone());

        processed_ids.insert(message.id);
        MESSAGES_RECEIVED.inc();
        self.condvar.notify_one();
        Ok(())
    }

    /// Get and remove the first message from the specified partition.
    pub fn get_from_partition(&self, topic_id: &str, partition_id: usize) -> Option<Message> {
        let mut partition_queues = self.partition_queues.lock().ok()?;
        if let Some(queue) = partition_queues.get_mut(&(topic_id.to_string(), partition_id)) {
            queue.pop_front()
        } else {
            None
        }
    }
}
