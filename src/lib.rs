#![allow(clippy::needless_doctest_main)]
//! [![github]](https://github.com/mila411/pilgrimage)&ensp;
//! [![crates-io]](https://crates.io/crates/pilgrimage)&ensp;
//! [![docs-rs]](https://docs.rs/pilgrimage/latest/pilgrimage/)
//!
//! [github]: https://img.shields.io/badge/github-black?style=for-the-badge&logo=github&labelColor=555555
//! [crates-io]: https://img.shields.io/badge/crates.io-fc8d62?style=for-the-badge&logo=rust&labelColor=555555
//! [docs-rs]: https://img.shields.io/badge/docs.rs-66c2a5?style=for-the-badge&logo=docs.rs&labelColor=555555
//!
//! # pilgrimage
//!
//! Pilgrimage is a Rust implementation of a distributed messaging system inspired
//! by [Apache Kafka][Kafka].
//! It records messages to local files and supports **At-least-once**
//! and **Exactly-once** delivery semantics.
//!
//! ## Installation
//!
//! To use Pilgrimage, add the following to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! pilgrimage = "0.12"
//! ```
//!
//! ## Security
//!
//! When using Pilgramage as a Crate, client authentication is implemented,
//! but at present, authentication is not implemented for message sending
//! and receiving from the CLI and web client.
//! You can find a sample of authentication with Crate
//! [examples/auth-example.rs][auth-example.rs], [examples/auth-send-recv.rs][auth-send-recv.rs].
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
//! ## Basic Usage
//!
//! ```rust
//! use pilgrimage::broker::{Broker, TopicConfig};
//! use pilgrimage::schema::{registry::SchemaRegistry, message_schema::MessageSchema};
//! use std::sync::{Arc, Mutex};
//! use std::thread;
//! use std::time::Duration;
//!
//! fn main() {
//!     // Creating a schema registry
//!     let schema_registry = SchemaRegistry::new();
//!     let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
//!     schema_registry
//!         .register_schema("test_topic", schema_def)
//!         .unwrap();
//!
//!     // Broker Creation
//!     let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs").expect("Failed to create broker")));
//!
//!     // Creating a topic
//!     {
//!         let mut broker = broker.lock().unwrap();
//!         let config = TopicConfig {
//!             num_partitions: 1,
//!             replication_factor: 1,
//!         };
//!         broker.create_topic("test_topic", Some(config)).unwrap();
//!     }
//!
//!     // Receiving thread
//!     let broker_clone = Arc::clone(&broker);
//!     let receiver = thread::spawn(move || {
//!         for _ in 0..5 {
//!             let broker = broker_clone.lock().unwrap();
//!             if let Ok(Some(message)) = broker.receive_message("test_topic", 0) {
//!                 println!("Received: {}", message.content);
//!             }
//!             drop(broker);
//!             thread::sleep(Duration::from_millis(100));
//!         }
//!     });
//!
//!     // Sender processing
//!     for i in 1..=5 {
//!         let message = MessageSchema::new()
//!             .with_content(format!("Message {}", i))
//!             .with_topic("test_topic".to_string())
//!             .with_partition(0);
//!         {
//!             let mut broker = broker.lock().unwrap();
//!             broker.send_message(message).unwrap();
//!             println!("Sent message {}", i);
//!         }
//!         thread::sleep(Duration::from_millis(100));
//!     }
//!
//!     // Waiting for the end of the incoming thread
//!     receiver.join().unwrap();
//! }
//! ```
//!
//! ### Dependency
//!
//! Rust `1.51.0` or later is required to use Pilgrimage.
//!
//! ### Functionality Implemented
//!
//! - **Message Queue**: Efficient message queue implementation using `Mutex` and `VecDeque`.
//! - **Broker**: Core broker functionality including message handling, node management,
//!   and leader election.
//! - **Consumer Groups**: Support for consumer groups to allow multiple consumers
//!   to read from the same topic.
//! - **Leader Election**: Mechanism for electing a leader among brokers
//!   to manage partitions and replication.
//! - **Storage**: Persistent storage of messages using local files.
//! - **Replication**: Replication of messages across multiple brokers for fault tolerance.
//! - **Schema Registry**: Management of message schemas to ensure compatibility
//!   between producers and consumers.
//! - **Benchmarking**: Comprehensive benchmarking tests to measure performance
//!   of various components.
//! - **Automatic Scaling:** Automatically scale the number of instances based on load.
//! - **Log Compressions:** Compress and optimize logs.
//!
//!
//! ### Examples
//!
//! To execute a basic example, use the following command:
//!
//! ```bash
//! cargo run --example ack-mulch-transaction
//! cargo run --example ack-send-recv
//! cargo run --example auth-example
//! cargo run --example auth-send-recv
//! cargo run --example idempotency
//! cargo run --example persistant-ack
//! cargo run --example simple-send-recv
//! cargo run --example thread-send-recv
//! cargo run --example transaction-send-recv
//! ```
//!
//! ### Bench
//!
//! If the allocated memory is small, it may fail.
//!
//! ```sh
//! Gnuplot not found, using plotters backend
//! send_message            time:   [849.75 ns 851.33 ns 853.18 ns]
//!                         change: [-23.318% -20.131% -18.021%] (p = 0.00 < 0.05)
//!                         Performance has improved.
//! Found 1 outliers among 100 measurements (1.00%)
//!   1 (1.00%) high mild
//!
//! send_benchmark_message  time:   [850.69 ns 852.08 ns 853.67 ns]
//!                         change: [-33.444% -26.669% -22.423%] (p = 0.00 < 0.05)
//!                         Performance has improved.
//! Found 2 outliers among 100 measurements (2.00%)
//!   2 (2.00%) high mild
//! ```
//!
//! Use the following command to run the benchmark:
//! ```bash
//! cargo bench
//! ```
//!
//! ## CLI Features
//!
//! Pilgrimage offers a comprehensive Command-Line Interface (CLI)
//! to manage and interact with your message brokers efficiently.
//! Below are the available commands along with their descriptions and usage examples.
//!
//! ### start
//!
//! **Description:**
//! Starts the broker with the specified configurations.
//!
//! **Usage:**
//!
//! ```bash
//! pilgrimage start --id <BROKER_ID> --partitions <NUMBER_OF_PARTITIONS> --replication <REPLICATION_FACTOR> --storage <STORAGE_PATH> [--test-mode]
//! ```
//!
//! **Options:**
//!
//! - `--id`, `-i` (required): Sets the broker ID.
//! - `--partitions`, `-p` (required): Sets the number of partitions.
//! - `--replication`, `-r` (required): Sets the replication factor.
//! - `--storage`, `-s` (required): Sets the storage path.
//! - `--test-mode`: Runs the broker in test mode, which breaks out of the main loop quickly
//!   for testing purposes.
//!
//! **Example:**
//!
//! ```bash
//! pilgrimage start --id broker1 --partitions 3 --replication 2 --storage /data/broker1 --test-mode
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ### stop
//!
//! **Description:**
//! Stops the specified broker.
//!
//! **Usage**
//!
//! `pilgrimage stop --id <BROKER_ID>`
//!
//! **Options:**
//!
//! - `--id`, `-i` (required): Sets the broker ID.
//!
//! **Example**
//!
//! ```bash
//! pilgrimage stop --id broker1
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ### send
//!
//! **Description:**
//!
//! Sends a message to the specified broker.
//!
//! **Usage**
//!
//! `pilgrimage send <BROKER_ID> <MESSAGE>`
//!
//! **Arguments:**
//!
//! - `<BROKER_ID>` (required): The ID of the broker to send the message to.
//! - `<MESSAGE>` (required): The message content to send.
//!
//! **Example**
//!
//! ```bash
//! pilgrimage send broker1 "Hello, World!"
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ### consume
//!
//! **Description:**
//!
//! Consumes messages from the specified broker.
//!
//! **Usage**
//!
//! `pilgrimage consume <BROKER_ID>`
//!
//! **Arguments:**
//!
//! - `<BROKER_ID>` (required): The ID of the broker to consume messages from.
//!
//! **Example:**
//!
//! ```bash
//! pilgrimage consume broker1
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ### status
//!
//! **Description:**
//!
//! Checks the status of the specified broker.
//!
//! **Usage:**
//!
//! `pilgrimage status --id <BROKER_ID>`
//!
//! **Options:**
//! - `--id`, `-i` (required): Sets the broker ID.
//!
//! **Example:**
//!
//! ```bash
//! pilgrimage status --id broker1
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ### Additional Information
//!
//! - **Help Command:**
//!   To view all available commands and options, use the `help` command:
//!
//! ```bash
//! pilgrimage help
//! ```
//!
//! - **Version Information:**
//!   To check the current version of Pilgrimage, use:
//!
//! ```bash
//! pilgrimage --version
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ### Running the CLI
//!
//! To start the web server:
//!
//! ```bash
//! cargo run --bin pilgrimage
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ## Web Console API
//!
//! Pilgrimage provides a REST API for managing brokers through HTTP requests.
//! The server runs on `http://localhost:8080` by default.
//!
//! ### Available Endpoints
//!
//! #### Start Broker
//!
//! Starts a new broker instance.
//!
//! **Endpoint:** `POST /start`
//! **Request:**
//!
//! ```json
//! {
//!     "id": "broker1",
//!     "partitions": 3,
//!     "replication": 2,
//!     "storage": "/tmp/broker1"
//! }
//! ```
//!
//! **Example:**
//!
//! ```sh
//! curl -X POST http://localhost:8080/start \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "id": "broker1",
//!     "partitions": 3,
//!     "replication": 2,
//!     "storage": "/tmp/broker1"
//!   }'
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! #### Stop Broker
//!
//! Stops a running broker instance.
//!
//! **Endpoint**: `POST /stop`
//! **Request:**
//!
//! ```json
//! {
//!     "id": "broker1"
//! }
//! ```
//!
//! **Example:**
//!
//! ```sh
//! curl -X POST http://localhost:8080/stop \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "id": "broker1"
//!   }'
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! #### Send Message
//!
//! Sends a message to the broker.
//!
//! **Endpoint**: `POST /send`
//! **Request:**
//!
//! ```json
//! {
//!     "id": "broker1",
//!     "message": "Hello, World!"
//! }
//! ```
//!
//! **Example:**
//!
//! ```sh
//! curl -X POST http://localhost:8080/send \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "id": "broker1",
//!     "message": "Hello, World!"
//!   }'
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! #### Consume Messages
//!
//! Consumes messages from the broker.
//!
//! **Endpoint**: `POST /consume`
//!
//! **Request:**
//!
//! ```sh
//! {
//!     "id": "broker1"
//! }
//! ```
//!
//! **Example:**
//!
//! ```sh
//! curl -X POST http://localhost:8080/consume \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "id": "broker1"
//!   }'
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! #### Check Status
//!
//! Checks the status of the broker.
//!
//! **Endpoint**: `POST /status`
//!
//! `Request:`
//!
//! ```sh
//! {
//!     "id": "broker1"
//! }
//! ```
//!
//! **Example:**
//!
//! ```sh
//! curl -X POST http://localhost:8080/status \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "id": "broker1"
//!   }'
//! ```
//!
//! ------------------------------------------------------------------------------------------------
//!
//! ### Running the Web Server
//!
//! To start the web server:
//!
//! ```sh
//! cargo run --bin web
//! ```
//!
//! The server will be available at `http://localhost:8080`.
//!
//! ## Version increment on release
//!
//! - The commit message is parsed and the version of either major, minor or patch is incremented.
//! - The version of Cargo.toml is updated.
//! - The updated Cargo.toml is committed and a new tag is created.
//! - The changes and tag are pushed to the remote repository.
//!
//! The version is automatically incremented based on the commit message.
//! Here, we treat `feat` as minor, `fix` as patch, and `BREAKING CHANGE` as major.
//!
//! ### License
//!
//! MIT
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

pub mod amqp_handler;
pub mod auth;
pub mod broker;
pub mod config;
pub mod crypto;
pub mod message;
pub mod monitoring;
pub mod network;
pub mod schema;
pub mod security;
pub mod subscriber;
pub mod testing;
pub mod web_console;

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
