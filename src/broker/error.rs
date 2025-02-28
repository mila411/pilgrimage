//! Module for handling errors in the broker system.
//!
//! This module provides an enum for categorizing and handling errors that may
//! arise within the broker system, such as issues with topics, partitions,
//! acknowledgments, I/O operations, and scaling.
//!
//! # Examples
//! The following example demonstrates how to create a new `BrokerError` and
//! convert it to a string:
//! ```
//! use pilgrimage::broker::error::BrokerError;
//!
//! let error = BrokerError::TopicError("topic not found".to_string());
//! assert_eq!(format!("{}", error), "Topic error: topic not found");
//! ```

use std::error::Error;
use std::fmt;

/// Represents different types of errors that can occur in the broker.
///
/// This enum is used to categorize and handle errors that may arise within
/// the broker system, such as issues with topics, partitions, acknowledgments,
/// I/O operations, and scaling.
#[derive(Debug)]
pub enum BrokerError {
    /// Represents an error related to a specific topic.
    ///
    /// This error is used when a topic is not found or is invalid.
    TopicError(String),
    /// Represents an error related to a specific partition.
    ///
    /// This error is used when a partition is not found or is invalid.
    PartitionError(String),
    /// Represents an error related to acknowledgments.
    ///
    /// This error is used when an acknowledgment fails or is invalid.
    AckError(String),
    /// Represents an error related to I/O operations.
    ///
    /// This error is used when an I/O operation fails.
    IoError(std::io::Error),
    /// Represents an error related to scaling operations.
    ///
    /// This error is used when a scaling operation fails.
    ///
    /// This could be due to a variety of reasons, such as a lack of resources,
    /// a failure to communicate with the scaling system, or an invalid scaling
    /// operation.
    ScalingError(String),
}

impl From<std::io::Error> for BrokerError {
    /// Converts a standard I/O error into a [`BrokerError`].
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::error::BrokerError;
    /// use std::io;
    ///
    /// let io_error = io::Error::new(io::ErrorKind::Other, "an I/O error");
    /// let broker_error: BrokerError = io_error.into();
    /// if let BrokerError::IoError(err) = broker_error {
    ///     assert_eq!(err.to_string(), "an I/O error");
    /// }
    /// ```
    fn from(error: std::io::Error) -> Self {
        BrokerError::IoError(error)
    }
}

impl fmt::Display for BrokerError {
    /// Formats the BrokerError for display purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::error::BrokerError;
    ///
    /// let error = BrokerError::TopicError("topic not found".to_string());
    /// assert_eq!(format!("{}", error), "Topic error: topic not found");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BrokerError::TopicError(msg) => write!(f, "Topic error: {}", msg),
            BrokerError::PartitionError(msg) => write!(f, "Partition error: {}", msg),
            BrokerError::AckError(msg) => write!(f, "Acknowledgment error: {}", msg),
            BrokerError::IoError(err) => write!(f, "IO error: {}", err),
            BrokerError::ScalingError(msg) => write!(f, "Scaling Error: {}", msg),
        }
    }
}

impl Error for BrokerError {
    /// Returns the source of the error, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::error::BrokerError;
    /// use std::error::Error;
    /// use std::io;
    ///
    /// let io_error = io::Error::new(io::ErrorKind::Other, "an I/O error");
    /// let broker_error: BrokerError = io_error.into();
    /// assert!(broker_error.source().is_some());
    /// ```
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BrokerError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    /// Tests the display format for the [`BrokerError::TopicError`] enum.
    ///
    /// # Purpose
    /// This test verifies that the display format for the [`BrokerError::TopicError`]
    /// enum is correct.
    ///
    /// # Steps
    /// 1. Create a [`BrokerError::TopicError`] variant with a message.
    /// 2. Verify that the display format is correct.
    #[test]
    fn test_topic_error() {
        let error = BrokerError::TopicError("Test topic error".to_string());
        assert_eq!(format!("{}", error), "Topic error: Test topic error");
    }

    /// Tests the display format for the [`BrokerError::PartitionError`] enum.
    ///
    /// # Purpose
    /// This test verifies that the display format for the [`BrokerError::PartitionError`]
    /// enum is correct.
    ///
    /// # Steps
    /// 1. Create a [`BrokerError::PartitionError`] variant with a message.
    /// 2. Verify that the display format is correct.
    #[test]
    fn test_partition_error() {
        let error = BrokerError::PartitionError("Test partition error".to_string());
        assert_eq!(
            format!("{}", error),
            "Partition error: Test partition error"
        );
    }

    /// Tests the display format for the [`BrokerError::AckError`] enum.
    ///
    /// # Purpose
    /// This test verifies that the display format for the [`BrokerError::AckError`]
    ///
    /// # Steps
    /// 1. Create a [`BrokerError::AckError`] variant with a message.
    /// 2. Verify that the display format is correct.
    #[test]
    fn test_ack_error() {
        let error = BrokerError::AckError("Test ack error".to_string());
        assert_eq!(format!("{}", error), "Acknowledgment error: Test ack error");
    }

    /// Tests the display format for the [`BrokerError::IoError`] enum.
    ///
    /// # Purpose
    /// This test verifies that the display format for the [`BrokerError::IoError`]
    /// enum is correct.
    ///
    /// # Steps
    /// 1. Create a [`BrokerError::IoError`] variant with a message.
    /// 2. Verify that the display format is correct.
    #[test]
    fn test_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "Test IO error");
        let error = BrokerError::IoError(io_error);
        assert_eq!(format!("{}", error), "IO error: Test IO error");
    }

    /// Tests the form conversion from an I/O error to a [`BrokerError`].
    ///
    /// # Purpose
    /// This test verifies that an I/O error can be converted into a [`BrokerError`].
    ///
    /// # Steps
    /// 1. Create an I/O error.
    /// 2. Convert the I/O error into a [`BrokerError`].
    /// 3. Verify that the converted error is correct.
    #[test]
    fn test_from_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "Test IO error");
        let error: BrokerError = io_error.into();
        assert_eq!(format!("{}", error), "IO error: Test IO error");
    }
}
