//! Module that defines the subscriber struct and its implementation.
//!
//! The [`Subscriber`] struct is used to manage subscribers with unique identifiers (`id`)
//! and callback functions (`callback`). When a message is published to the topic a subscriber
//! is subscribed to, the callback function is executed.
//!
//! The subscriber struct is implemented to be thread-safe by wrapping the callback function
//! in an [`Arc`] and implementing the [`Send`] and [`Sync`] traits.
//!
//! # Example
//! ```rust
//! use pilgrimage::subscriber::types::Subscriber;
//!
//! // Create a new subscriber with the id "example-id" and
//! // a callback function that prints the message
//! let subscriber = Subscriber::new("example-id", Box::new(|message| {
//!     println!("Received message: {}", message);
//! }));
//!
//! // Check if the subscriber was created successfully
//! assert_eq!(subscriber.id, "example-id");
//! ```

use std::fmt::{self, Debug};
use std::sync::Arc;

/// Represents a subscriber to a topic.
///
/// A subscriber is a struct that contains an id and a callback function.
/// * The `id` is used to identify the subscriber;
/// * The `callback` function is called when a message is published to the topic
///   the subscriber is subscribed to.
///
/// The `callback` function takes a string as an argument, which is the message that was published.
///
/// # Under the hood
/// The `callback` function is wrapped in an [`Arc`] to allow the subscriber to be cloned
/// and shared between threads safely.
///
/// Also, data races are prevented by implementing the [`Send`] and [`Sync`] traits.
///
/// The subscriber struct implements:
/// * The [`Clone`] trait to allow it to be cloned.
/// * The [`Debug`] trait is implemented to allow the subscriber
///   to be printed for debugging purposes.
pub struct Subscriber {
    /// Unique identifier for the subscriber.
    pub id: String,
    /// Callback function to be executed when a message is published.
    pub callback: Arc<Box<dyn Fn(String) + Send + Sync>>,
}

impl Clone for Subscriber {
    /// Clones the subscriber struct.
    ///
    /// This method makes a deep copy of the [`Subscriber`]'s ID and
    /// a clone of the callback function.
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            callback: Arc::clone(&self.callback),
        }
    }
}

impl Debug for Subscriber {
    /// Formats the subscriber struct for debugging purposes.
    ///
    /// This method returns a string representation of the subscriber struct:
    /// ```text
    /// Subscriber {
    ///     id: "example-id",
    /// }
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriber").field("id", &self.id).finish()
    }
}

impl Subscriber {
    /// Creates a new subscriber with the given id and callback function.
    ///
    /// The callback function is wrapped in an [`Arc`] to guarantee thread safety.
    ///
    /// # Arguments
    /// * `id` - A unique identifier for the subscriber.
    /// * `callback` - A callback function to be executed when a message is published.
    ///   The callback function is executed when a message is published to the topic
    ///   the subscriber is subscribed to.
    ///   It takes a string as an argument, which is the message that was published.
    ///
    /// # Returns
    /// A new subscriber with the given id and callback function.
    ///
    /// # Example
    /// ```rust
    /// use pilgrimage::subscriber::types::Subscriber;
    ///
    /// // Create a new subscriber with the id "example-id" and
    /// // a callback function that prints the message
    /// let subscriber = Subscriber::new("example-id", Box::new(|message| {
    ///     println!("Received message: {}", message);
    /// }));
    ///
    /// // Check if the subscriber was created successfully
    /// assert_eq!(subscriber.id, "example-id");
    /// ```
    pub fn new<S: Into<String>>(id: S, callback: Box<dyn Fn(String) + Send + Sync>) -> Self {
        Self {
            id: id.into(),
            callback: Arc::new(callback),
        }
    }

    /// Notify the subscriber with a message
    pub fn notify(&self, message: &str) -> Result<(), String> {
        (self.callback)(message.to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the [`Subscriber::new`] method.
    ///
    /// # Purpose
    /// The test checks if the subscriber is created successfully.
    ///
    /// # Steps
    /// 1. Create a new subscriber with the id `test-id` and a callback function.
    /// 2. Check if the subscriber was created successfully.
    #[test]
    fn test_subscriber_new() {
        let message_received = Arc::new(std::sync::Mutex::new(false));
        let message_clone = message_received.clone();

        let callback = Box::new(move |_| {
            let mut received = message_clone.lock().unwrap();
            *received = true;
        });

        let subscriber = Subscriber::new("test-id", callback);
        assert_eq!(subscriber.id, "test-id");
    }

    /// Tests the [`Subscriber::clone`] method.
    ///
    /// # Purpose
    /// The test checks if the subscriber is cloned successfully.
    ///
    /// # Steps
    /// 1. Create a new subscriber with the id `test-id` and a callback function.
    /// 2. Clone the subscriber.
    /// 3. Check if the subscriber was cloned successfully.
    #[test]
    fn test_subscriber_clone() {
        let subscriber = Subscriber::new("test-id", Box::new(|_| {}));
        let cloned = subscriber.clone();

        assert_eq!(subscriber.id, cloned.id);
    }
}
