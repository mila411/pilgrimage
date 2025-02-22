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
//!
//! // Create a new storage instance
//! let mut storage = Storage::new("general_test_log").unwrap();
//!
//! // Write a message to the storage
//! storage.write_message("test_message").unwrap();
//!
//! // Check if the message was written successfully
//! let messages = storage.read_messages().unwrap();
//! assert_eq!(messages, vec!["test_message"]);
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

/// The `Storage` struct handles file I/O operations for the broker.
///
/// It provides methods to write messages to a file, read messages from a file,
/// rotate logs, and clean up old logs.
/// This is useful for persisting logs or any sequential data that needs to be stored reliably.
#[derive(Debug)]
pub struct Storage {
    /// The file writer. This is a buffered writer that writes to the storage file.
    file: BufWriter<File>,
    /// The path to the storage file.
    path: String,
    /// A flag indicating whether the storage is available.
    pub available: bool,
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
    ///
    /// let storage = Storage::new("test_logs").unwrap();
    /// ```
    pub fn new(path: &str) -> io::Result<Self> {
        // Checking for the existence of the parent directory and creating it
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!(
                            "Failed to create parent directory {}: {}",
                            parent.display(),
                            e
                        ),
                    )
                })?;
            }
        }

        // File open
        let file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| {
                    io::Error::new(e.kind(), format!("Failed to open file {}: {}", path, e))
                })?,
        );

        Ok(Storage {
            file,
            path: path.to_string(),
            available: true,
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
    ///
    /// let mut storage = Storage::new("test_logs").unwrap();
    /// storage.write_message("test_message").unwrap();
    /// ```
    pub fn write_message(&mut self, message: &str) -> io::Result<()> {
        self.file.write_all(message.as_bytes())?;
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
    ///
    /// let storage = Storage::new("test_logs").unwrap();
    /// let messages = storage.read_messages().unwrap();
    /// ```
    pub fn read_messages(&self) -> io::Result<Vec<String>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut messages = Vec::new();
        for line in reader.lines() {
            messages.push(line?);
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
    ///
    /// // Create a new storage instance
    /// let storage = Storage::new("test_rotate_logs").unwrap();
    /// // Rotate the logs
    /// let result = storage.rotate_logs();
    ///
    /// // Assert that the log rotation failed because the old log file does not exist
    /// assert!(result.is_err());
    /// ```
    pub fn rotate_logs(&self) -> io::Result<()> {
        // Log rotation process
        let old_log_path = format!("{}.old", self.path);
        if !Path::new(&old_log_path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Old log file {} does not exist", old_log_path),
            ));
        }

        // Implemented log rotation processing
        Ok(())
    }

    /// Cleans up the old log file.
    ///
    /// This method deletes the old log file if it exists.
    ///
    /// # Returns
    /// * `Ok(())` if the old log file is successfully deleted.
    /// * An `io::Error` if the old log file does not exist or an error occurs.
    ///
    /// # Errors
    /// * If the old log file does not exist.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    ///
    /// // Create a new storage instance
    /// let storage = Storage::new("test_cleanup_logs").unwrap();
    /// // Clean up the old log file
    /// let result = storage.cleanup_logs();
    ///
    /// // Assert that the cleanup failed because the old log file does not exist
    /// assert!(result.is_err());
    /// ```
    pub fn cleanup_logs(&self) -> io::Result<()> {
        let old_path = format!("{}.old", self.path);
        if Path::new(&old_path).exists() {
            std::fs::remove_file(&old_path)?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Old log file does not exist",
            ));
        }
        Ok(())
    }

    /// Checks if the storage is available.
    ///
    /// # Returns
    /// * `true` if the storage is available.
    /// * `false` if the storage is not available.
    pub fn is_available(&self) -> bool {
        self.available
    }

    /// Reinitialize the storage by recreating the storage file.
    ///
    /// This method creates a new storage file and writes an initialization message to it.
    ///
    /// # Returns
    /// * `Ok(())` if the storage is successfully reinitialized.
    /// * An error message if the storage cannot be reinitialized.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    ///
    /// // Create a new storage instance
    /// let mut storage = Storage::new("test_reinitialize").unwrap();
    /// // Reinitialize the storage
    /// let result = storage.reinitialize();
    ///
    /// // Assert that the storage is successfully reinitialized
    /// assert!(result.is_ok());
    /// ```
    pub fn reinitialize(&mut self) -> Result<(), String> {
        let file = File::create(&self.path).map_err(|e| e.to_string())?;
        self.file = BufWriter::new(file);
        writeln!(self.file, "Initialized").map_err(|e| e.to_string())?;
        self.available = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Tests writing a message to an invalid path.
    ///
    /// # Purpose
    /// This test verifies that writing a message to an invalid path returns an error.
    ///
    /// # Steps
    /// 1. Create a new storage instance with an invalid path.
    /// 2. Verify that the result is an error.
    #[test]
    fn test_write_message_to_invalid_path() {
        let invalid_path = "/invalid_path/test_logs";
        let result = Storage::new(invalid_path);
        assert!(result.is_err());
    }

    /// Tests reading messages from an invalid path.
    ///
    /// # Purpose
    /// This test verifies that reading messages from an invalid path returns an error.
    ///
    /// # Steps
    /// 1. Create a new filepath for the test log.
    /// 2. Verify that the filepath does not exist.
    /// 3. Create a new storage instance with the test log filepath.
    /// 4. Remove the test log file.
    /// 5. Attempt to read messages from the storage.
    /// 6. Verify that the result is an error.
    #[test]
    fn test_read_message_from_nonexistent_file() {
        let test_log = format!("/tmp/test_log_{}", std::process::id());

        assert!(!Path::new(&test_log).exists());

        let storage = Storage::new(&test_log).unwrap();
        std::fs::remove_file(&test_log).unwrap_or(());

        let result = storage.read_messages();
        assert!(
            result.is_err(),
            "Loading a non-existent file should return an error"
        );
    }

    /// Tests rotating logs with an invalid path.
    ///
    /// # Purpose
    /// This test verifies that rotating logs with an invalid path returns an error.
    ///
    /// # Steps
    /// 1. Create a new storage instance with an invalid path.
    /// 2. Rotate the logs.
    /// 3. Verify that the result is an error.
    #[test]
    fn test_rotate_logs_with_invalid_path() {
        let invalid_path = "/invalid_path/test_logs";
        let storage = Storage::new(invalid_path).unwrap_or_else(|_| Storage {
            path: invalid_path.to_string(),
            file: BufWriter::new(File::create("/dev/null").unwrap()),
            available: false,
        });
        let result = storage.rotate_logs();
        assert!(result.is_err());
    }

    /// Tests cleaning up logs with an invalid path.
    ///
    /// # Purpose
    /// This test verifies that cleaning up logs with an invalid path returns an error.
    ///
    /// # Steps
    /// 1. Create a new filepath for the test log.
    /// 2. Verify that the filepath does not exist.
    /// 3. Create a new storage instance with the test log filepath.
    /// 4. Attempt to clean up the logs.
    /// 5. Verify that the result is an error.
    #[test]
    fn test_cleanup_logs_with_invalid_path() {
        let invalid_path = format!("/tmp/nonexistent_{}/test.log", std::process::id());
        assert!(!Path::new(&invalid_path).exists());

        let storage = Storage {
            path: invalid_path,
            file: BufWriter::new(File::create("/dev/null").unwrap()),
            available: false,
        };

        let result = storage.cleanup_logs();
        assert!(
            result.is_err(),
            "Deleting a file that does not exist should return an error"
        );
    }
}
