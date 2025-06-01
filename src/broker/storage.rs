//! Module for handling file I/O operations for the broker.
//!
//! The `Storage` struct provides methods to write messages to a file, read messages from a file,
//! rotate logs, and clean up old logs.
//!
//! # Examples
//! The following example demonstrates
//! how to create a new storage instance and write a message to it:
//! ```
//! use pilgrimage::broker::storage::Storage;
//! use pilgrimage::message::Message;
//! use std::path::PathBuf;
//!
//! // Create a new storage instance
//! let mut storage = Storage::new(PathBuf::from("general_test_log")).unwrap();
//!
//! // Create a message
//! let message = Message::new("test_message".to_string())
//!     .with_topic("test_topic".to_string())
//!     .with_partition(0);
//!
//! // Write a message to the storage
//! storage.write_message(&message).unwrap();
//!
//! // Read all messages from the storage
//! let messages = storage.read_messages("test_topic", 0).unwrap();
//! ```

use crate::broker::node::Node;
use crate::message::message::Message;
use serde_json;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// The `Storage` struct handles file I/O operations for the broker.
///
/// It provides methods to write messages to a file, read messages from a file,
/// rotate logs, and clean up old logs.
/// This is useful for persisting logs or any sequential data that needs to be stored reliably.
#[derive(Debug)]
pub struct Storage {
    /// The file writer. This is a buffered writer that writes to the storage file.
    pub file: File,
    pub path: PathBuf,
    nodes: Arc<Mutex<HashMap<String, Node>>>,
    /// Track consumed message IDs to avoid duplicate consumption
    consumed_messages: HashSet<Uuid>,
}

impl Storage {
    /// Creates a new storage instance.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the storage file.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    /// use std::path::PathBuf;
    ///
    /// let storage = Storage::new(PathBuf::from("test_logs")).unwrap();
    /// ```
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            file,
            path,
            nodes: Arc::new(Mutex::new(HashMap::new())),
            consumed_messages: HashSet::new(),
        })
    }

    /// Writes a message to the storage.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to write.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    /// use pilgrimage::message::Message;
    /// use std::path::PathBuf;
    ///
    /// let mut storage = Storage::new(PathBuf::from("test_logs")).unwrap();
    /// let message = Message::new("test_message".to_string())
    ///     .with_topic("test_topic".to_string())
    ///     .with_partition(0);
    /// storage.write_message(&message).unwrap();
    /// ```
    pub fn write_message(&mut self, message: &Message) -> io::Result<()> {
        let json = serde_json::to_string(message)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.file.write_all(json.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        Ok(())
    }

    /// Reads all messages from the storage.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    /// use std::path::PathBuf;
    ///
    /// let mut storage = Storage::new(PathBuf::from("test_logs")).unwrap();
    /// let messages = storage.read_messages("test_topic", 0).unwrap();
    /// ```
    pub fn read_messages(&mut self, topic: &str, partition: usize) -> io::Result<Vec<Message>> {
        let mut messages = Vec::new();

        // Open file with read-only access
        let read_file = File::open(&self.path)?;
        let reader = BufReader::new(read_file);

        for line in reader.lines() {
            let line = line?;
            if let Ok(message) = serde_json::from_str::<Message>(&line) {
                if message.topic_id == topic && message.partition_id == partition {
                    // Only include messages that haven't been consumed
                    if !self.consumed_messages.contains(&message.id) {
                        messages.push(message);
                    }
                }
            }
        }

        Ok(messages)
    }

    /// Rotates the logs by processing the old log file.
    ///
    /// It checks for the existence of an old log file and processes it.
    ///
    /// # Returns
    /// * `Ok(())` if the log rotation is successful.
    /// * An `io::Error` if the old log file does not exist or an error occurs.
    ///
    /// # Errors
    /// * If the old log file does not exist.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    /// use std::path::PathBuf;
    ///
    /// // Create a new storage instance
    /// let mut storage = Storage::new(PathBuf::from("test_rotate_logs")).unwrap();
    /// // Rotate the logs
    /// let result = storage.rotate_logs();
    ///
    /// // Log rotation should succeed
    /// assert!(result.is_ok());
    /// ```
    pub fn rotate_logs(&mut self) -> io::Result<()> {
        let backup_path = self.path.with_extension("old");
        std::fs::rename(&self.path, &backup_path)?;

        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&self.path)?;

        Ok(())
    }

    /// Check if storage is available.
    ///
    /// # Returns
    /// * `true` - If storage is available
    /// * `false` - If storage is not available
    pub fn is_available(&self) -> bool {
        self.file.metadata().is_ok()
    }

    /// Reinitialize storage.
    ///
    /// This closes the current file and creates a new file.
    ///
    /// # Returns
    /// * `Ok(())` - If reinitialization is successful
    /// * `Err(io::Error)` - If an error occurs
    pub fn reinitialize(&mut self) -> io::Result<()> {
        let _ = &self.file;
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        Ok(())
    }

    /// Add a new node to storage
    pub fn add_node(&self, node_id: String, node: Node) -> Result<(), String> {
        let mut nodes = self
            .nodes
            .lock()
            .map_err(|e| format!("Failed to lock nodes: {}", e))?;

        nodes.insert(node_id, node);
        Ok(())
    }

    /// Obtains the node with the specified ID
    pub fn get_node(&self, node_id: &str) -> Result<Option<Node>, String> {
        let nodes = self
            .nodes
            .lock()
            .map_err(|e| format!("Failed to lock nodes: {}", e))?;

        Ok(nodes.get(node_id).cloned())
    }

    /// Marks a message as consumed to avoid duplicate processing
    ///
    /// # Arguments
    /// * `message_id` - The UUID of the message to mark as consumed
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    /// use std::path::PathBuf;
    /// use uuid::Uuid;
    ///
    /// let mut storage = Storage::new(PathBuf::from("test_logs")).unwrap();
    /// let message_id = Uuid::new_v4();
    /// storage.consume_message(message_id);
    /// ```
    pub fn consume_message(&mut self, message_id: Uuid) {
        self.consumed_messages.insert(message_id);
    }

    /// Checks if a message has already been consumed
    ///
    /// # Arguments
    /// * `message_id` - The UUID of the message to check
    ///
    /// # Returns
    /// * `true` if the message has been consumed, `false` otherwise
    pub fn is_message_consumed(&self, message_id: &Uuid) -> bool {
        self.consumed_messages.contains(message_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_storage_write_read() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut storage = Storage::new(file_path).unwrap();

        let message = Message::new("Test message".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();

        let messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "Test message");
        assert_eq!(messages[0].topic_id, "test_topic");
        assert_eq!(messages[0].partition_id, 0);
    }

    #[test]
    fn test_storage_rotation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut storage = Storage::new(file_path.clone()).unwrap();

        let message = Message::new("Test message".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();
        storage.rotate_logs().unwrap();

        assert!(file_path.exists());
        assert!(file_path.with_extension("old").exists());
    }

    #[test]
    fn test_storage_is_available() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let storage = Storage::new(file_path.clone()).unwrap();

        assert!(storage.is_available());
    }

    #[test]
    fn test_storage_reinitialize() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut storage = Storage::new(file_path.clone()).unwrap();

        let message = Message::new("Test message".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();
        storage.reinitialize().unwrap();

        let messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_message_consumption() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut storage = Storage::new(file_path).unwrap();

        let message = Message::new("Test message".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();

        // Consume the message
        storage.consume_message(message.id);

        // Try to read the message again
        let messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages.len(), 0);
    }
}
