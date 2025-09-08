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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::compatibility::Compatibility;
    use crate::schema::version::SchemaVersion;

    #[test]
    fn test_message_new() {
        let content = "Test message content".to_string();
        let message = Message::new(content.clone());

        assert_eq!(message.content, content);
        assert!(!message.id.to_string().is_empty());
        assert_eq!(message.topic_id, String::new());
        assert_eq!(message.partition_id, 0);
        assert!(message.schema.is_none());

        // Verify timestamp is recent (within last minute)
        let now = Utc::now();
        let diff = now.signed_duration_since(message.timestamp);
        assert!(diff.num_seconds() < 60);
    }

    #[test]
    fn test_message_new_with_schema() {
        let content = "Test message with schema".to_string();
        let schema = MessageSchema {
            id: 123,
            definition: "test_schema_definition".to_string(),
            version: SchemaVersion::new(2),
            compatibility: Compatibility::Forward,
            metadata: None,
            topic_id: Some("test_topic".to_string()),
            partition_id: Some(1),
        };

        let message = Message::new_with_schema(content.clone(), schema.clone());

        assert_eq!(message.content, content);
        assert!(message.schema.is_some());
        let msg_schema = message.schema.unwrap();
        assert_eq!(msg_schema.id, 123);
        assert_eq!(msg_schema.definition, "test_schema_definition");
        assert_eq!(msg_schema.version.major, 2);
    }

    #[test]
    fn test_message_with_topic() {
        let message = Message::new("test".to_string()).with_topic("my_topic".to_string());

        assert_eq!(message.topic_id, "my_topic");
        assert_eq!(message.partition_id, 0);
    }

    #[test]
    fn test_message_with_partition() {
        let message = Message::new("test".to_string()).with_partition(5);

        assert_eq!(message.partition_id, 5);
        assert_eq!(message.topic_id, String::new());
    }

    #[test]
    fn test_message_chaining() {
        let message = Message::new("test".to_string())
            .with_topic("my_topic".to_string())
            .with_partition(3);

        assert_eq!(message.content, "test");
        assert_eq!(message.topic_id, "my_topic");
        assert_eq!(message.partition_id, 3);
    }

    #[test]
    fn test_message_from_string() {
        let content = "Test content from string".to_string();
        let message: Message = content.clone().into();

        assert_eq!(message.content, content);
        assert!(!message.id.to_string().is_empty());
    }

    #[test]
    fn test_message_to_string() {
        let content = "Test content to string".to_string();
        let message = Message::new(content.clone());
        let result: String = message.into();

        assert_eq!(result, content);
    }

    #[test]
    fn test_message_display() {
        let content = "Display test".to_string();
        let message = Message::new(content.clone());
        let display_str = format!("{}", message);

        assert!(display_str.contains(&message.id.to_string()));
        assert!(display_str.contains(&content));
        assert!(display_str.contains("Message["));
        assert!(display_str.contains("(at"));
    }

    #[test]
    fn test_message_clone() {
        let original = Message::new("Clone test".to_string())
            .with_topic("clone_topic".to_string())
            .with_partition(2);

        let cloned = original.clone();

        assert_eq!(original.id, cloned.id);
        assert_eq!(original.content, cloned.content);
        assert_eq!(original.topic_id, cloned.topic_id);
        assert_eq!(original.partition_id, cloned.partition_id);
        assert_eq!(original.timestamp, cloned.timestamp);
    }

    #[test]
    fn test_message_serialization() {
        let message = Message::new("Serialization test".to_string())
            .with_topic("serial_topic".to_string())
            .with_partition(1);

        // Test JSON serialization
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains("\"content\":\"Serialization test\""));
        assert!(json.contains("\"topic_id\":\"serial_topic\""));
        assert!(json.contains("\"partition_id\":1"));

        // Test JSON deserialization
        let deserialized: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.content, message.content);
        assert_eq!(deserialized.topic_id, message.topic_id);
        assert_eq!(deserialized.partition_id, message.partition_id);
        assert_eq!(deserialized.id, message.id);
    }

    #[test]
    fn test_message_metadata_conversion() {
        let message = Message::new("Metadata test".to_string())
            .with_topic("meta_topic".to_string())
            .with_partition(4);

        let metadata: crate::message::metadata::MessageMetadata = message.clone().into();

        assert_eq!(metadata.id, message.id.to_string());
        assert_eq!(metadata.content, message.content);
        assert_eq!(metadata.topic_id, Some(message.topic_id));
        assert_eq!(metadata.partition_id, Some(message.partition_id));
        assert_eq!(metadata.timestamp, message.timestamp.to_rfc3339());
    }

    #[test]
    fn test_message_debug() {
        let message = Message::new("Debug test".to_string());
        let debug_str = format!("{:?}", message);

        assert!(debug_str.contains("Message"));
        assert!(debug_str.contains("Debug test"));
    }

    #[test]
    fn test_message_equality() {
        let message1 = Message::new("Test".to_string());
        let mut message2 = message1.clone();

        // Messages should be equal when cloned
        assert_eq!(message1.id, message2.id);
        assert_eq!(message1.content, message2.content);

        // Changing content should make them different
        message2.content = "Different content".to_string();
        assert_ne!(message1.content, message2.content);
    }

    #[test]
    fn test_message_empty_content() {
        let message = Message::new(String::new());

        assert_eq!(message.content, "");
        assert!(!message.id.to_string().is_empty());
    }

    #[test]
    fn test_message_unicode_content() {
        let unicode_content = "Hello world 🌍 émojis ñ".to_string();
        let message = Message::new(unicode_content.clone());

        assert_eq!(message.content, unicode_content);

        // Test serialization with unicode
        let json = serde_json::to_string(&message).unwrap();
        let deserialized: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.content, unicode_content);
    }

    #[test]
    fn test_message_large_content() {
        let large_content = "x".repeat(10000);
        let message = Message::new(large_content.clone());

        assert_eq!(message.content, large_content);
        assert_eq!(message.content.len(), 10000);
    }
}
