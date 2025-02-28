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
    pub timestamp: chrono::DateTime<chrono::Utc>,
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
            timestamp: chrono::Utc::now(),
        }
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
