use crate::message::ack::Message;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::SystemTime;

use uuid::Uuid;
#[derive(Debug)]
pub struct Storage {
    file: BufWriter<File>,
    path: String,
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

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub fn reinitialize(&mut self) -> Result<(), String> {
        let file = File::create(&self.path).map_err(|e| e.to_string())?;
        self.file = BufWriter::new(file);
        writeln!(self.file, "Initialized").map_err(|e| e.to_string())?;
        self.available = true;
        Ok(())
    }

    pub fn save_message(&mut self, message: &Message) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open("messages.txt")?;
        writeln!(
            file,
            "{},{},{}",
            message.id,
            message.content,
            message
                .timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        )?;
        Ok(())
    }

    pub fn load_messages(&self) -> io::Result<Vec<Message>> {
        let file = File::open("messages.txt")?;
        let reader = BufReader::new(file);
        let mut messages = Vec::new();
        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() == 3 {
                let id = Uuid::parse_str(parts[0]).unwrap();
                let content = parts[1].to_string();
                let timestamp = SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs(parts[2].parse().unwrap());
                messages.push(Message {
                    id,
                    content,
                    timestamp,
                });
            }
        }
        Ok(messages)
    }

    pub fn is_message_processed(&self, message_id: &Uuid) -> bool {
        let messages = self.load_messages().unwrap_or_default();
        messages.iter().any(|msg| &msg.id == message_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_write_message_to_invalid_path() {
        let invalid_path = "/invalid_path/test_logs";
        let result = Storage::new(invalid_path);
        assert!(result.is_err());
    }

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
