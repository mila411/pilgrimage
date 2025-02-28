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
//! let message_queue = MessageQueue::new(1, 10);
//!
//! // Create a new message
//! let message = Message::new("Hello, world!".to_string());
//!
//! // Add the message to the queue
//! let result = message_queue.push(message);
//! assert!(result.is_ok());
//! ```

use crate::broker::scaling::AutoScaler;
use crate::message::message::Message;
use lazy_static::lazy_static;
use prometheus::{Counter, register_counter};
use std::collections::{HashSet, VecDeque};
use std::sync::{Condvar, Mutex};
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
    /// This is a FIFO queue that is shared between all processing instances.
    queue: Mutex<VecDeque<Message>>,
    /// The set of message IDs that have already been processed.
    /// This is used to detect and prevent duplicate messages.
    processed_ids: Mutex<HashSet<Uuid>>,
    /// The auto-scaler for the message queue.
    /// This is used to automatically scale the number of processing instances.
    auto_scaler: AutoScaler,
    /// The condition variable for the message queue.
    /// This is used to notify processing instances when new messages are available.
    condvar: Condvar,
}

impl MessageQueue {
    /// Creates a new `MessageQueue` instance with the given scaling parameters.
    ///
    /// # Arguments
    /// * `min_instances` - The minimum number of processing instances.
    /// * `max_instances` - The maximum number of processing instances.
    ///
    /// # Returns
    /// A new `MessageQueue` instance with the given scaling parameters.
    pub fn new(min_instances: usize, max_instances: usize) -> Self {
        MessageQueue {
            queue: Mutex::new(VecDeque::new()),
            processed_ids: Mutex::new(HashSet::new()),
            auto_scaler: AutoScaler::new(min_instances, max_instances),
            condvar: Condvar::new(),
        }
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
    ///
    /// // Create a new message queue with 1 minimum instance and 10 maximum instances
    /// let message_queue = MessageQueue::new(1, 10);
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string());
    ///
    /// // Add the message to the queue
    /// let result = message_queue.push(message);
    ///
    /// // Check if the message was successfully added to the queue
    /// assert!(result.is_ok());
    pub fn push(&self, message: Message) -> Result<(), String> {
        let mut queue = self.queue.lock().map_err(|e| e.to_string())?;
        queue.push_back(message);
        self.condvar.notify_one();
        MESSAGES_RECEIVED.inc();

        if queue.len() > self.auto_scaler.high_watermark() {
            self.auto_scaler.scale_up().map_err(|e| e.to_string())?;
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
    ///
    /// // Create a new message queue with 1 minimum instance and 10 maximum instances
    /// let message_queue = MessageQueue::new(1, 10);
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string());
    ///
    /// // Add the message to the queue
    /// let _ = message_queue.push(message);
    ///
    /// // Pop the first message from the queue
    /// let result = message_queue.pop();
    ///
    /// // Check if the message was successfully popped from the queue
    /// assert!(result.is_some());
    /// ```
    pub fn pop(&self) -> Option<Message> {
        let mut queue = self.queue.lock().ok()?;
        while queue.is_empty() {
            queue = self.condvar.wait(queue).ok()?;
        }
        let message = queue.pop_front();

        if queue.len() < self.auto_scaler.low_watermark() {
            let _ = self.auto_scaler.scale_down();
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
    /// let message_queue = MessageQueue::new(1, 10);
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string());
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
    /// let message_queue = MessageQueue::new(1, 10);
    ///
    /// // Create a new message
    /// let message = Message::new("Hello, world!".to_string());
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
}
