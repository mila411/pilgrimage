//! Module for the [`Message`] struct.
//!
//! This module provides the [`Message`] struct and associated method to create and manage messages.
//!
//! The [`Message`] struct encapsulates the ID, content, and timestamp of a message.
//! It includes implementations for creating a new message, converting to and from a string,
//! and displaying the message in a human-readable format.
//!
//! # Example
//! ```
//! use pilgrimage::message::message::Message;
//! use chrono::Utc;
//!
//! // Create a new message with content "Hello, world!"
//! let message = Message::new(String::from("Hello, world!"));
//!
//! // Check the content of the message
//! assert_eq!(message.content, "Hello, world!");
//! ```

use crate::schema::message_schema::MessageSchema;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents a message with an ID, content, and timestamp.
///
/// The `Message` struct is used to encapsulate a message with a unique identifier (`id`),
/// text content (`content`), and a time (`timestamp`) when the message was created.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// The unique identifier of the message (UUID).
    pub id: Uuid,
    /// The content of the message.
    pub content: String,
    /// The timestamp when the message was created (UTC).
    pub timestamp: DateTime<Utc>,
    /// The ID of the topic this message belongs to.
    #[serde(default)]
    pub topic_id: String,
    /// The ID of the partition this message belongs to.
    #[serde(default)]
    pub partition_id: usize,
    /// The schema of the message, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<MessageSchema>,
}

impl Message {
    /// Creates a new `Message` with the given content.
    ///
    /// This method generates a unique ID and timestamp for the message.
    ///
    /// ## Example
    ///
    /// ```
    /// use pilgrimage::message::message::Message;
    ///
    /// // Create a new message with content "Hello, world!"
    /// let message = Message::new(String::from("Hello, world!"));
    ///
    /// // Check the content of the message
    /// assert_eq!(message.content, "Hello, world!");
    /// ```
    pub fn new(content: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            content,
            timestamp: Utc::now(),
            topic_id: String::new(),
            partition_id: 0,
            schema: None,
        }
    }

    /// Creates a new `Message` with the given content and schema.
    ///
    /// This method generates a unique ID and timestamp for the message.
    ///
    /// ## Example
    ///
    /// ```
    /// use pilgrimage::message::message::Message;
    /// use pilgrimage::schema::message_schema::MessageSchema;
    ///
    /// // Create a schema for the message
    /// let schema = MessageSchema::new();
    ///
    /// // Create a new message with content "Hello, world!" and the given schema
    /// let message = Message::new_with_schema(String::from("Hello, world!"), schema);
    ///
    /// // Check the content and schema of the message
    /// assert_eq!(message.content, "Hello, world!");
    /// ```
    pub fn new_with_schema(content: String, schema: MessageSchema) -> Self {
        Self {
            id: Uuid::new_v4(),
            content,
            timestamp: Utc::now(),
            topic_id: String::new(),
            partition_id: 0,
            schema: Some(schema),
        }
    }

    /// Sets the topic ID for this message.
    ///
    /// ## Example
    ///
    /// ```
    /// use pilgrimage::message::message::Message;
    ///
    /// // Create a new message
    /// let message = Message::new(String::from("Hello, world!"));
    ///
    /// // Set the topic ID for the message
    /// let message = message.with_topic(String::from("topic1"));
    ///
    /// // Check the topic ID of the message
    /// assert_eq!(message.topic_id, "topic1");
    /// ```
    pub fn with_topic(mut self, topic_id: String) -> Self {
        self.topic_id = topic_id;
        self
    }

    /// Sets the partition ID for this message.
    ///
    /// ## Example
    ///
    /// ```
    /// use pilgrimage::message::message::Message;
    ///
    /// // Create a new message
    /// let message = Message::new(String::from("Hello, world!"));
    ///
    /// // Set the partition ID for the message
    /// let message = message.with_partition(1);
    ///
    /// // Check the partition ID of the message
    /// assert_eq!(message.partition_id, 1);
    /// ```
    pub fn with_partition(mut self, partition_id: usize) -> Self {
        self.partition_id = partition_id;
        self
    }
}

impl From<String> for Message {
    /// Converts a [`String`] into a [`Message`].
    ///
    /// This `From` implementation allows creating a [`Message`] directly from a string.
    fn from(content: String) -> Self {
        Message::new(content)
    }
}

impl From<Message> for String {
    /// Converts a [`Message`] back into a [`String`].
    ///
    /// This `From` implementation allows obtaining the content of a [`Message`] as a string.
    fn from(message: Message) -> Self {
        message.content
    }
}

impl std::fmt::Display for Message {
    /// Formats the [`Message`] for display purposes.
    ///
    /// This method returns a string representation of the message in the format:
    /// ```text
    /// Message[<id>] <content> (at <timestamp>)
    /// ```
    /// Where:
    /// - `id` is the unique identifier of the message.
    /// - `content` is the text content of the message.
    /// - `timestamp` is the time when the message was created.
    ///
    /// # Parameters
    /// * `f`: A mutable reference to a [`std::fmt::Formatter`] instance.
    ///
    /// # Returns
    /// A [`std::fmt::Result`] indicating the success or failure of the operation.
    ///
    /// # Example
    /// ```
    /// use pilgrimage::message::message::Message;
    /// use chrono::Utc;
    /// use uuid::Uuid;
    ///
    /// // Create a new message with content "Hello, world!"
    /// let message = Message::new(String::from("Hello, world!"));
    ///
    /// // Format the message for display
    /// let formatted = format!("{}", message);
    ///
    /// // Check the formatted message
    /// assert_eq!(
    ///     formatted,
    ///     format!("Message[{}] {} (at {})", message.id, message.content, message.timestamp)
    /// );
    /// ```
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Message[{}] {} (at {})",
            self.id, self.content, self.timestamp
        )
    }
}

impl From<Message> for crate::message::metadata::MessageMetadata {
    fn from(msg: Message) -> Self {
        Self {
            id: msg.id.to_string(),
            content: msg.content.clone(),
            timestamp: msg.timestamp.to_rfc3339(),
            topic_id: Some(msg.topic_id),
            partition_id: Some(msg.partition_id),
            schema: msg.schema,
        }
    }
}
