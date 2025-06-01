//! Module for handling messages and acknowledgements.
//!
//! This module provides the core components for handling messages and their acknowledgements.
//!
//! It includes the following submodules:
//! * [`message`]: Defines the [`message::Message`] struct,
//!   representing individual messages with an ID, content, and timestamp.
//!   This module provides methods to create and manage messages.
//! * [`ack`]: Defines the [`ack::MessageAck`] struct,
//!   used to acknowledge the receipt or processing status of messages.
//!   This module includes metadata such as message ID, timestamp, topic, and partition,
//!   as well as various acknowledgement statuses.

pub mod ack;
pub mod message;
pub mod metadata;

pub use self::ack::{MessageAck, AckStatus};
pub use self::message::Message;
pub use self::metadata::MessageMetadata;
