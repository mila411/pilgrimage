//! Module for user authentication.
//!
//! This module provides functionality for authenticating users based on username and password.
//!
//! The [`Authenticator`] trait defines the interface for authenticating users.
//!
//! The [`BasicAuthenticator`] struct is a simple implementation
//! of the [`Authenticator`] trait that uses a HashMap for storing credentials.
//! The [`BasicAuthenticator`] struct also provides a method for adding new users to the
//! authenticator.
//!
//! # Examples
//! ```rust
//! use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
//!
//! // Create a new BasicAuthenticator instance
//! let mut authenticator = BasicAuthenticator::new();
//!
//! // Add some users
//! authenticator.add_user("user1", "password");
//! authenticator.add_user("user2", "password");
//!
//! // Authenticate users
//! assert!(authenticator.authenticate("user1", "password").unwrap());
//! assert!(!authenticator.authenticate("user1", "wrong_password").unwrap());
//! assert!(!authenticator.authenticate("user3", "password").unwrap());
//! ```

use std::collections::HashMap;
use std::error::Error;

/// A trait for authenticating users based on username and password.
pub trait Authenticator {
    /// Authenticates a user with the given username and password.
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `password`: A string slice representing the password.
    ///
    /// # Returns
    /// * `Result<bool, Box<dyn Error>>`: A result indicating whether the authentication
    ///   was successful (`true` for success, `false` for failure), or an error if an error occurs.
    fn authenticate(&self, username: &str, password: &str) -> Result<bool, Box<dyn Error>>;
}

/// A struct representing a basic authenticator that uses a HashMap for storing credentials.
pub struct BasicAuthenticator {
    /// A HashMap containing the username-password pairs.
    credentials: std::collections::HashMap<String, String>,
}

impl BasicAuthenticator {
    /// Creates a new instance of `BasicAuthenticator`. It simply initializes the credential store.
    ///
    /// # Returns
    /// * A new `BasicAuthenticator` instance with an empty credentials store.
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
        }
    }

    /// Adds a new user with the given username and password to the authenticator.
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `password`: A string slice representing the password.
    pub fn add_user(&mut self, username: &str, password: &str) {
        self.credentials
            .insert(username.to_string(), password.to_string());
    }
}

impl Default for BasicAuthenticator {
    /// Creates a default instance of `BasicAuthenticator`.
    ///
    /// # Returns
    /// * A new `BasicAuthenticator` instance with an empty credentials store.
    fn default() -> Self {
        Self::new()
    }
}

impl Authenticator for BasicAuthenticator {
    /// Authenticates a user with the given username and password.
    ///
    /// This method checks if the given username exists in the credentials store
    /// and if the password matches.
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `password`: A string slice representing the password.
    ///
    /// # Returns
    /// * `Result<bool, Box<dyn Error>>`: A result indicating whether the authentication
    ///   was successful (`true` for success, `false` for failure), or an error if an error occurs.
    fn authenticate(&self, username: &str, password: &str) -> Result<bool, Box<dyn Error>> {
        Ok(self
            .credentials
            .get(username)
            .is_some_and(|stored_password| stored_password == password))
    }
}
