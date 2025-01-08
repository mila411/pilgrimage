//! # pilgrimage
//!
//! This is a Rust implementation of a distributed messaging system.
//! It uses a simple design inspired by [Apache Kafka][Kafka].
//! It simply records messages to local files.
//!
//! Current Pilgrimage supports **At-least-once**.
//!
//!
//! ## Security
//!
//! When using Pilgramage as a Crate, client authentication is implemented,
//! but at present, authentication is not implemented for message sending
//! and receiving from the CLI and web client.
//! You can find a sample of authentication with Crate
//! [examples/auth-example.rs][auth-example.rs], [examples/auth-send-recv.rs][auth-send-recv.rs].
//!
//!
//! ## Features
//!
//! - Topic-based pub/sub model
//! - Scalability through partitioning
//! - Persistent messages (log file based)
//! - Leader/Follower Replication
//! - Fault Detection and Automatic Recovery
//! - Delivery guaranteed by acknowledgement (ACK)
//! - Fully implemented leader selection mechanism
//! - Partition Replication
//! - Persistent messages
//! - Schema Registry for managing message schemas and ensuring compatibility
//! - Automatic Scaling
//! - Broker Clustering
//! - Message processing in parallel
//! - Authentication and Authorization Mechanisms
//! - Data Encryption
//! - CLI based console
//! - WEB based console
//!
//!
//! [Kafka]: https://kafka.apache.org/
//! [auth-example.rs]: https://github.com/mila411/pilgrimage/blob/main/examples/auth-example.rs
//! [auth-send-recv.rs]: https://github.com/mila411/pilgrimage/blob/main/examples/auth-send-recv.rs

// These options control how the docs look at a crate level.
// See https://doc.rust-lang.org/rustdoc/write-documentation/the-doc-attribute.html
// for more information.
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/mila411/pilgrimage/refs/heads/main/.github/images/logo.png"
)]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/mila411/pilgrimage/refs/heads/main/.github/images/logo.png"
)]

/// Trigger for CI testing line
pub mod auth;
pub mod broker;
pub mod crypto;
pub mod message;
pub mod schema;
pub mod subscriber;

pub use broker::Broker;
pub use broker::error::BrokerError;
pub use broker::topic::Topic;
pub use message::ack::MessageAck;
pub use subscriber::types::Subscriber;

pub mod prelude {
    pub use crate::broker::Broker;
    pub use crate::broker::error::BrokerError;
    pub use crate::broker::topic::Topic;
    pub use crate::message::ack::MessageAck;
    pub use crate::subscriber::types::Subscriber;
}
