//! Module for schema related functionality.
//!
//! This module contains the following submodules:
//! * [`compatibility`] - Module for schema compatibility checks.
//! * [`registry`] - Module for schema registry functionality.
//! * [`version`] - Module for schema versioning.
//!
//! # Example
//! The following example demonstrates how to:
//! 1. Create a new schema;
//! 2. Register it in a schema registry;
//! 3. Check the compatibility between two schemas.
//! ```
//! use pilgrimage::schema::registry::{SchemaRegistry, Schema};
//! use pilgrimage::schema::compatibility::Compatibility;
//! use pilgrimage::schema::version::SchemaVersion;
//!
//! // Create a new schema registry
//! let registry = SchemaRegistry::new();
//!
//! // Create a new schema
//! let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
//! let schema = Schema {
//!     id: 1,
//!     version: SchemaVersion::new(1),
//!     definition: schema_def.to_string(),
//! };
//!
//! // Register the schema in the schema registry
//! let result = registry.register_schema("test_topic", schema_def);
//! assert!(result.is_ok());
//!
//! // Get the registered schema
//! let registered_schema = registry.get_schema("test_topic", Some(1));
//! assert!(registered_schema.is_some());
//!
//! // Check the compatibility between the new schema and the registered schema
//! let compatibility = Compatibility::BACKWARD;
//! assert!(compatibility.check(&schema, &registered_schema.unwrap()));
//! ```

pub mod compatibility;
pub mod registry;
pub mod version;
