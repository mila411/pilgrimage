//! Module containing the compatibility check for schema evolution.
//!
//! The compatibility check ensures that new schemas can read data written by old schemas.
//!
//! # Example
//! The following example demonstrates how to check the compatibility between two schemas.
//! ```
//! use pilgrimage::schema::compatibility::Compatibility;
//! use pilgrimage::schema::version::SchemaVersion;
//! use pilgrimage::schema::registry::Schema;
//!
//! // Get an old schema (we are using a dummy schema for demonstration purposes)
//! let old_schema = Schema {
//!     id: 1,
//!     version: SchemaVersion::new(1),
//!     definition: r#"{"type":"record","fields":[{"name":"id","type":"string"}]}"#.to_string(),
//! };
//!
//! // Get a new schema (we are using a dummy schema for demonstration purposes)
//! let new_schema = Schema {
//!    id: 2,
//!   version: SchemaVersion::new(2),
//!     definition: r#"{"type":"record","fields":[{"name":"id","type":"string"},{"name":"value","type":"string","default":""}]}"#.to_string(),
//! };
//!
//! // Check the compatibility between the new schema and the old schema
//! let compatibility = Compatibility::BACKWARD;
//! assert!(compatibility.check(&new_schema, &old_schema));
//! ```

use crate::schema::registry::Schema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Enum representing the compatibility modes for schema evolution.
///
/// # Variants
///
/// * `BACKWARD` - Ensures that new schemas can read data written by old schemas.
/// * `FORWARD` - Ensures that old schemas can read data written by new schemas.
/// * `FULL` - Ensures both backward and forward compatibility.
/// * `NONE` - No compatibility checks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Compatibility {
    Backward,
    Forward,
    Full,
    None,
}

impl Compatibility {
    /// Backward compatibility mode (new schema can read old data)
    pub const BACKWARD: Self = Self::Backward;
    /// Forward compatibility mode (old schema can read new data)
    pub const FORWARD: Self = Self::Forward;
    /// Full compatibility mode (guaranteed bi-directional compatibility)
    pub const FULL: Self = Self::Full;
    /// No compatibility check
    pub const NONE: Self = Self::None;

    /// Checks the compatibility between a new schema and an old schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if the schemas are compatible according to the specified mode.
    /// * `false` otherwise.
    pub fn check(&self, new_schema: &Schema, old_schema: &Schema) -> bool {
        match self {
            Compatibility::Backward => Self::check_backward(new_schema, old_schema),
            Compatibility::Forward => Self::check_forward(new_schema, old_schema),
            Compatibility::Full => {
                Self::check_backward(new_schema, old_schema)
                    && Self::check_forward(new_schema, old_schema)
            }
            Compatibility::None => true,
        }
    }

    /// Checks backward compatibility between a new schema and an old schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if the new schema is backward compatible with the old schema.
    /// * `false` otherwise.
    fn check_backward(new_schema: &Schema, old_schema: &Schema) -> bool {
        if let (Ok(new_json), Ok(old_json)) = (
            serde_json::from_str::<Value>(&new_schema.definition),
            serde_json::from_str::<Value>(&old_schema.definition),
        ) {
            // The new schema must contain all the fields of the old schema.
            Self::contains_all_required_fields(&new_json, &old_json)
        } else {
            false
        }
    }

    /// Checks forward compatibility between a new schema and an old schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if the new schema is forward compatible with the old schema.
    /// * `false` otherwise.
    fn check_forward(new_schema: &Schema, old_schema: &Schema) -> bool {
        if let (Ok(new_json), Ok(old_json)) = (
            serde_json::from_str::<Value>(&new_schema.definition),
            serde_json::from_str::<Value>(&old_schema.definition),
        ) {
            // New fields must be optional
            Self::new_fields_are_optional(&new_json, &old_json)
        } else {
            false
        }
    }

    /// Ensures that all required fields in the old schema are present in the new schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if all required fields are present.
    /// * `false` otherwise.
    fn contains_all_required_fields(new_schema: &Value, old_schema: &Value) -> bool {
        if let (Some(new_fields), Some(old_fields)) = (
            new_schema.get("fields").and_then(Value::as_array),
            old_schema.get("fields").and_then(Value::as_array),
        ) {
            old_fields.iter().all(|old_field| {
                let old_name = old_field.get("name").and_then(Value::as_str);
                new_fields.iter().any(|new_field| {
                    let new_name = new_field.get("name").and_then(Value::as_str);
                    old_name == new_name
                })
            })
        } else {
            true // If there is no field, it is determined to be compatible.
        }
    }

    /// Ensures that new fields in the new schema are optional.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if new fields are optional.
    /// * `false` otherwise.
    fn new_fields_are_optional(new_schema: &Value, old_schema: &Value) -> bool {
        if let (Some(new_fields), Some(old_fields)) = (
            new_schema.get("fields").and_then(Value::as_array),
            old_schema.get("fields").and_then(Value::as_array),
        ) {
            new_fields.iter().all(|new_field| {
                let new_name = new_field.get("name").and_then(Value::as_str);
                old_fields.iter().any(|old_field| {
                    let old_name = old_field.get("name").and_then(Value::as_str);
                    new_name == old_name || new_field.get("default").is_some()
                })
            })
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests backward compatibility.
    ///
    /// # Purpose
    /// This test ensures that the new schema is backward compatible with the old schema.
    ///
    /// # Steps
    /// 1. Create an old schema with a single field.
    /// 2. Create a new schema with the same field and an additional field.
    /// 3. Check if the new schema is backward compatible with the old schema.
    #[test]
    fn test_backward_compatibility() {
        let old_schema = Schema {
            id: 1,
            version: crate::schema::version::SchemaVersion::new(1),
            definition: r#"{"type":"record","fields":[{"name":"id","type":"string"}]}"#.to_string(),
        };

        let new_schema = Schema {
            id: 2,
            version: crate::schema::version::SchemaVersion::new(2),
            definition: r#"{"type":"record","fields":[{"name":"id","type":"string"},{"name":"value","type":"string","default":""}]}"#.to_string(),
        };

        let compatibility = Compatibility::BACKWARD;
        assert!(compatibility.check(&new_schema, &old_schema));
    }
}
