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
//! let mut storage = Storage::new(PathBuf::from("test_storage/general_test_log")).unwrap();
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
    /// let storage = Storage::new(PathBuf::from("test_storage/test_logs")).unwrap();
    /// ```
    pub fn new(path: PathBuf) -> io::Result<Self> {
        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                // Only propagate error if it's not "already exists"
                if e.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(e);
                }
            }
        }

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
    /// let mut storage = Storage::new(PathBuf::from("test_storage/test_logs")).unwrap();
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
    /// let mut storage = Storage::new(PathBuf::from("test_storage/test_logs")).unwrap();
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
    /// let mut storage = Storage::new(PathBuf::from("test_storage/test_rotate_logs")).unwrap();
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
    /// let mut storage = Storage::new(PathBuf::from("test_storage/test_logs")).unwrap();
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

    /// Write replication data to storage
    ///
    /// This method persists replicated data from other nodes to ensure
    /// data consistency across the distributed system.
    ///
    /// # Arguments
    /// * `topic` - The topic to write data to
    /// * `data` - The data to replicate
    /// * `source_node` - The node that originated this data
    ///
    /// # Returns
    /// * `Ok(())` if the data was successfully written
    /// * `Err(io::Error)` if there was an error writing to storage
    pub fn write_replication_data(
        &mut self,
        topic: &str,
        data: &[u8],
        source_node: &str,
    ) -> io::Result<()> {
        // Create a replication entry
        let replication_entry = ReplicationEntry {
            id: Uuid::new_v4(),
            topic: topic.to_string(),
            data: data.to_vec(),
            source_node: source_node.to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let json = serde_json::to_string(&replication_entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.file.write_all(b"REPL:")?;
        self.file.write_all(json.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;

        log::info!(
            "Wrote replication data for topic {} from node {}",
            topic,
            source_node
        );
        Ok(())
    }

    /// Read replication data from storage
    ///
    /// # Arguments
    /// * `topic` - The topic to read replication data from
    ///
    /// # Returns
    /// * `Ok(Vec<ReplicationEntry>)` containing all replication entries for the topic
    /// * `Err(io::Error)` if there was an error reading from storage
    pub fn read_replication_data(&self, topic: &str) -> io::Result<Vec<ReplicationEntry>> {
        let mut entries = Vec::new();
        let read_file = File::open(&self.path)?;
        let reader = BufReader::new(read_file);

        for line in reader.lines() {
            let line = line?;
            if line.starts_with("REPL:") {
                let json_data = &line[5..]; // Remove "REPL:" prefix
                if let Ok(entry) = serde_json::from_str::<ReplicationEntry>(json_data) {
                    if entry.topic == topic {
                        entries.push(entry);
                    }
                }
            }
        }

        Ok(entries)
    }

    /// Get storage statistics
    ///
    /// # Returns
    /// * `StorageStats` containing information about the storage
    pub fn get_stats(&self) -> io::Result<StorageStats> {
        let metadata = self.file.metadata()?;
        let read_file = File::open(&self.path)?;
        let reader = BufReader::new(read_file);

        let mut message_count = 0;
        let mut replication_count = 0;

        for line in reader.lines() {
            let line = line?;
            if line.starts_with("REPL:") {
                replication_count += 1;
            } else if !line.trim().is_empty() {
                message_count += 1;
            }
        }

        Ok(StorageStats {
            file_size: metadata.len(),
            message_count,
            replication_count,
            consumed_count: self.consumed_messages.len(),
        })
    }

    /// Compact storage by removing consumed messages
    ///
    /// This method creates a new storage file without consumed messages
    /// to reduce disk usage and improve performance.
    ///
    /// # Returns
    /// * `Ok(usize)` containing the number of messages removed
    /// * `Err(io::Error)` if there was an error during compaction
    pub fn compact(&mut self) -> io::Result<usize> {
        let temp_path = self.path.with_extension("tmp");
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        let read_file = File::open(&self.path)?;
        let reader = BufReader::new(read_file);

        let mut removed_count = 0;
        let mut kept_count = 0;

        for line in reader.lines() {
            let line = line?;
            let mut should_keep = true;

            // Check if this is a message that has been consumed
            if !line.starts_with("REPL:") && !line.trim().is_empty() {
                if let Ok(message) = serde_json::from_str::<Message>(&line) {
                    if self.consumed_messages.contains(&message.id) {
                        should_keep = false;
                        removed_count += 1;
                    }
                }
            }

            if should_keep {
                temp_file.write_all(line.as_bytes())?;
                temp_file.write_all(b"\n")?;
                kept_count += 1;
            }
        }

        temp_file.flush()?;
        drop(temp_file);

        // Replace the original file with the compacted version
        std::fs::rename(temp_path, &self.path)?;

        // Reopen the file
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&self.path)?;

        log::info!(
            "Storage compaction completed: removed {} messages, kept {}",
            removed_count,
            kept_count
        );
        Ok(removed_count)
    }
}

/// Represents a replication entry in storage
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicationEntry {
    pub id: Uuid,
    pub topic: String,
    pub data: Vec<u8>,
    pub source_node: String,
    pub timestamp: u64,
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub file_size: u64,
    pub message_count: usize,
    pub replication_count: usize,
    pub consumed_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
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
    fn test_storage_multiple_writes() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("multi_write.log");
        let mut storage = Storage::new(file_path).unwrap();

        // Write multiple messages
        for i in 0..10 {
            let message = Message::new(format!("Message {}", i))
                .with_topic("test_topic".to_string())
                .with_partition(0);
            storage.write_message(&message).unwrap();
        }

        let messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages.len(), 10);

        for (i, message) in messages.iter().enumerate() {
            assert_eq!(message.content, format!("Message {}", i));
        }
    }

    #[test]
    fn test_storage_different_topics_and_partitions() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("topics_partitions.log");
        let mut storage = Storage::new(file_path).unwrap();

        // Write messages to different topics and partitions
        let msg1 = Message::new("Topic A Message".to_string())
            .with_topic("topic_a".to_string())
            .with_partition(0);
        let msg2 = Message::new("Topic B Message".to_string())
            .with_topic("topic_b".to_string())
            .with_partition(1);
        let msg3 = Message::new("Topic A Partition 1".to_string())
            .with_topic("topic_a".to_string())
            .with_partition(1);

        storage.write_message(&msg1).unwrap();
        storage.write_message(&msg2).unwrap();
        storage.write_message(&msg3).unwrap();

        // Read topic_a partition 0
        let topic_a_p0 = storage.read_messages("topic_a", 0).unwrap();
        assert_eq!(topic_a_p0.len(), 1);
        assert_eq!(topic_a_p0[0].content, "Topic A Message");

        // Read topic_a partition 1
        let topic_a_p1 = storage.read_messages("topic_a", 1).unwrap();
        assert_eq!(topic_a_p1.len(), 1);
        assert_eq!(topic_a_p1[0].content, "Topic A Partition 1");

        // Read topic_b partition 1
        let topic_b_p1 = storage.read_messages("topic_b", 1).unwrap();
        assert_eq!(topic_b_p1.len(), 1);
        assert_eq!(topic_b_p1[0].content, "Topic B Message");

        // Read non-existent topic
        let nonexistent = storage.read_messages("nonexistent", 0).unwrap();
        assert_eq!(nonexistent.len(), 0);
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

        // New file should be empty
        let messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_storage_rotation_preserves_backup() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("rotate_backup.log");
        let mut storage = Storage::new(file_path.clone()).unwrap();

        // Write initial message
        let message1 = Message::new("Before rotation".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);
        storage.write_message(&message1).unwrap();

        storage.rotate_logs().unwrap();

        // Write message after rotation
        let message2 = Message::new("After rotation".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);
        storage.write_message(&message2).unwrap();

        // Check backup file contains original message
        let backup_path = file_path.with_extension("old");
        assert!(backup_path.exists());

        let backup_content = fs::read_to_string(&backup_path).unwrap();
        assert!(backup_content.contains("Before rotation"));

        // Check new file contains new message
        let new_messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(new_messages.len(), 1);
        assert_eq!(new_messages[0].content, "After rotation");
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
        let file_path = dir.path().join("reinit.log");
        let mut storage = Storage::new(file_path.clone()).unwrap();

        let message = Message::new("Test message".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();

        // Verify message was written
        let messages_before = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages_before.len(), 1);

        storage.reinitialize().unwrap();

        // After reinitialization, file should be empty
        let messages_after = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages_after.len(), 0);
    }

    #[test]
    fn test_message_consumption() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("consume.log");
        let mut storage = Storage::new(file_path).unwrap();

        let message = Message::new("Test message".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();

        // Initially message should be available
        let messages_before = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages_before.len(), 1);

        // Consume the message
        storage.consume_message(message.id);

        // After consumption, message should not be available
        let messages_after = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages_after.len(), 0);
    }

    #[test]
    fn test_multiple_message_consumption() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("multi_consume.log");
        let mut storage = Storage::new(file_path).unwrap();

        let mut message_ids = Vec::new();

        // Write multiple messages
        for i in 0..5 {
            let message = Message::new(format!("Message {}", i))
                .with_topic("test_topic".to_string())
                .with_partition(0);
            message_ids.push(message.id);
            storage.write_message(&message).unwrap();
        }

        // Consume some messages
        storage.consume_message(message_ids[1]);
        storage.consume_message(message_ids[3]);

        // Should only see unconsumed messages
        let messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages.len(), 3);

        let contents: Vec<&str> = messages.iter().map(|m| m.content.as_str()).collect();
        assert!(contents.contains(&"Message 0"));
        assert!(contents.contains(&"Message 2"));
        assert!(contents.contains(&"Message 4"));
        assert!(!contents.contains(&"Message 1"));
        assert!(!contents.contains(&"Message 3"));
    }

    #[test]
    fn test_storage_with_invalid_json() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("invalid_json.log");

        // Write some invalid JSON directly to file
        fs::write(&file_path, "invalid json line\n{\"valid\": \"json\"}\n").unwrap();

        let mut storage = Storage::new(file_path).unwrap();

        // Should handle invalid JSON gracefully
        let messages = storage.read_messages("any_topic", 0).unwrap();
        assert_eq!(messages.len(), 0); // No valid messages should be found
    }

    #[test]
    fn test_storage_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty.log");
        let mut storage = Storage::new(file_path).unwrap();

        let messages = storage.read_messages("any_topic", 0).unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_storage_large_messages() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("large_messages.log");
        let mut storage = Storage::new(file_path).unwrap();

        // Create a large message
        let large_content = "x".repeat(10000);
        let message = Message::new(large_content.clone())
            .with_topic("large_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();

        let messages = storage.read_messages("large_topic", 0).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, large_content);
        assert_eq!(messages[0].content.len(), 10000);
    }

    #[test]
    fn test_storage_unicode_content() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("unicode.log");
        let mut storage = Storage::new(file_path).unwrap();

        let unicode_content = "Hello world 🌍 émojis ñ";
        let message = Message::new(unicode_content.to_string())
            .with_topic("unicode_topic".to_string())
            .with_partition(0);

        storage.write_message(&message).unwrap();

        let messages = storage.read_messages("unicode_topic", 0).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, unicode_content);
    }

    #[test]
    fn test_storage_concurrent_access_simulation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("concurrent.log");
        let mut storage = Storage::new(file_path).unwrap();

        // Simulate concurrent writes by writing messages rapidly
        for i in 0..100 {
            let message = Message::new(format!("Concurrent message {}", i))
                .with_topic("concurrent_topic".to_string())
                .with_partition(i % 3); // Use different partitions
            storage.write_message(&message).unwrap();
        }

        // Verify all messages for each partition
        for partition in 0..3 {
            let messages = storage
                .read_messages("concurrent_topic", partition)
                .unwrap();
            // Each partition should have approximately 33-34 messages
            assert!(messages.len() >= 30 && messages.len() <= 40);
        }
    }

    #[test]
    fn test_storage_stats() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("stats.log");
        let mut storage = Storage::new(file_path).unwrap();

        // Write some messages
        for i in 0..5 {
            let message = Message::new(format!("Stats message {}", i))
                .with_topic("stats_topic".to_string())
                .with_partition(0);
            storage.write_message(&message).unwrap();
        }

        let stats = storage.get_stats().unwrap();
        assert!(stats.file_size > 0);
        assert_eq!(stats.message_count, 5);
    }

    #[test]
    fn test_storage_node_operations() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nodes.log");
        let storage = Storage::new(file_path).unwrap();

        // Test adding a node
        let node = Node::new("test_node", "127.0.0.1:8080", true);
        storage
            .add_node("test_node".to_string(), node.clone())
            .unwrap();

        // Test getting the node
        let retrieved_node = storage.get_node("test_node").unwrap();
        assert!(retrieved_node.is_some());
        assert_eq!(retrieved_node.unwrap().id, "test_node");

        // Test that we can retrieve the node we just added
        let second_retrieval = storage.get_node("test_node").unwrap();
        assert!(second_retrieval.is_some());
    }

    #[test]
    fn test_storage_read_write_after_rotation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("rotate_read_write.log");
        let mut storage = Storage::new(file_path).unwrap();

        // Write initial message
        let msg1 = Message::new("Before rotation".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);
        storage.write_message(&msg1).unwrap();

        // Rotate
        storage.rotate_logs().unwrap();

        // Write and read after rotation
        let msg2 = Message::new("After rotation".to_string())
            .with_topic("test_topic".to_string())
            .with_partition(0);
        storage.write_message(&msg2).unwrap();

        let messages = storage.read_messages("test_topic", 0).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "After rotation");
    }

    #[test]
    fn test_storage_error_handling() {
        // Test with invalid path (trying to create storage in a directory that doesn't exist and can't be created)
        let invalid_path = std::path::PathBuf::from("/root/invalid/path/test.log");

        // This might succeed on some systems, so we just ensure it doesn't panic
        let _result = Storage::new(invalid_path);
        // The actual behavior depends on system permissions
    }

    #[test]
    fn test_storage_debug_and_clone_behavior() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("debug.log");
        let storage = Storage::new(file_path).unwrap();

        // Test debug formatting
        let debug_str = format!("{:?}", storage);
        assert!(debug_str.contains("Storage"));
    }
}
