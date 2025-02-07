//! Module for message acknowledgments.
//!
//! This module provides the [`MessageAck`] struct and associated methods to handle message
//! acknowledgments.
//!
//! The [`MessageAck`] struct is used to acknowledge the receipt or processing status of a message.
//! This includes metadata such as the message ID, timestamp, topic, and partition. It also defines
//! the various statuses an acknowledgment can have.
//!
//! # Example
//! The following example demonstrates how to create a new acknowledgment for a message:
//! ```rust
//! use chrono::{DateTime, Utc};
//! use uuid::Uuid;
//! use pilgrimage::message::ack::{AckStatus, MessageAck};
//!
//! // Create a new acknowledgment for a message
//! let message_id = Uuid::new_v4();
//! let timestamp = Utc::now();
//! let status = AckStatus::Received;
//! let topic = String::from("test-topic");
//! let partition = 0;
//! let ack = MessageAck::new(
//!     message_id, timestamp, status, topic.as_str().parse().unwrap(), partition
//! );
//!
//! // Check the message ID and topic of the acknowledgment
//! assert_eq!(ack.message_id, message_id);
//! assert_eq!(ack.topic, topic);
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an acknowledgment for a message.
///
/// The `MessageAck` struct is used to acknowledge the receipt or processing status
/// of a message, along with additional metadata such as the topic and partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAck {
    /// The unique identifier of the message (UUID).
    pub message_id: Uuid,
    /// The timestamp when the acknowledgment was created (UTC).
    pub timestamp: DateTime<Utc>,
    /// The status of the acknowledgment.
    pub status: AckStatus,
    /// The topic of the message.
    pub topic: String,
    /// The partition of the message.
    pub partition: usize,
}

/// Represents the acknowledgment status.
///
/// The `AckStatus` enum defines the possible states of an acknowledgment,
/// such as [`AckStatus::Received`], [`AckStatus::Processed`], or [`AckStatus::Failed`]
/// with an error message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AckStatus {
    /// The message was received successfully.
    Received,
    /// The message was processed successfully.
    Processed,
    /// The message processing failed with an error message.
    Failed(String),
}

impl MessageAck {
    /// Creates a new `MessageAck` instance.
    ///
    /// This method initializes a new acknowledgment with the given message ID, timestamp,
    /// acknowledgment status, topic, and partition.
    ///
    /// # Parameters
    /// * `message_id` - The unique identifier of the message.
    /// * `timestamp` - The timestamp when the acknowledgment was created.
    /// * `status` - The status of the acknowledgment.
    /// * `topic` - The topic of the message.
    /// * `partition` - The partition of the message.
    ///
    /// # Returns
    /// A new `MessageAck` instance with the provided data.
    ///
    /// # Example
    /// ```rust
    /// use chrono::{DateTime, Utc};
    /// use uuid::Uuid;
    /// use pilgrimage::message::ack::{AckStatus, MessageAck};
    ///
    /// // Create a new acknowledgment for a message
    /// let message_id = Uuid::new_v4();
    /// let timestamp = Utc::now();
    /// let status = AckStatus::Received;
    /// let topic = String::from("test-topic");
    /// let partition = 0;
    /// let ack = MessageAck::new(
    ///     message_id, timestamp, status, topic.as_str().parse().unwrap(), partition
    /// );
    ///
    /// // Check the message ID and topic of the acknowledgment
    /// assert_eq!(ack.message_id, message_id);
    /// assert_eq!(ack.topic, topic);
    /// ```
    pub fn new(
        message_id: Uuid,
        timestamp: DateTime<Utc>,
        status: AckStatus,
        topic: String,
        partition: usize,
    ) -> Self {
        Self {
            message_id,
            timestamp,
            status,
            topic,
            partition,
        }
    }
}
