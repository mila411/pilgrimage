//! Module for schema related functionality.
//!
//! This module contains the following submodules:
//! * [`compatibility`] - Module for schema compatibility checks.
//! * [`registry`] - Module for schema registry functionality.
//! * [`version`] - Module for schema versioning.

pub mod compatibility;
pub mod message_schema;
pub mod registry;
pub mod version;

pub use self::message_schema::MessageSchema;
pub use self::registry::Schema;
