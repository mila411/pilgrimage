use crate::schema::compatibility::Compatibility;
use crate::schema::registry::Schema;
use crate::schema::version::SchemaVersion;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSchema {
    pub id: u32,
    pub definition: String,
    pub version: SchemaVersion,
    pub compatibility: Compatibility,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<usize>,
}

impl MessageSchema {
    pub fn new() -> Self {
        MessageSchema {
            id: 0,
            definition: String::new(),
            version: SchemaVersion::new(1),
            compatibility: Compatibility::Backward,
            metadata: None,
            topic_id: None,
            partition_id: None,
        }
    }

    pub fn new_with_definition(definition: String) -> Self {
        MessageSchema {
            id: 0,
            definition,
            version: SchemaVersion::new(1),
            compatibility: Compatibility::Backward,
            metadata: None,
            topic_id: None,
            partition_id: None,
        }
    }

    pub fn new_with_schema(schema: Schema) -> Self {
        MessageSchema {
            id: schema.id,
            definition: schema.definition,
            version: schema.version,
            compatibility: Compatibility::Backward,
            metadata: None,
            topic_id: None,
            partition_id: None,
        }
    }

    /// Verify that messages conform to the schema
    pub fn validate(&self, message: &str) -> Result<(), String> {
        let value = serde_json::from_str::<Value>(message)
            .map_err(|e| format!("Message is not valid JSON: {}", e))?;

        let schema = serde_json::from_str::<Value>(&self.definition)
            .map_err(|e| format!("Schema is not valid JSON: {}", e))?;

        if self.validate_against_schema(&value, &schema) {
            Ok(())
        } else {
            Err("Message does not conform to schema".to_string())
        }
    }

    fn validate_against_schema(&self, value: &Value, schema: &Value) -> bool {
        match (schema, value) {
            (Value::Object(schema_obj), Value::Object(value_obj)) => {
                if let Some(Value::Array(required)) = schema_obj.get("required") {
                    for field in required {
                        if let Value::String(field_name) = field {
                            if !value_obj.contains_key(field_name) {
                                return false;
                            }
                        }
                    }
                }

                if let Some(Value::Object(properties)) = schema_obj.get("properties") {
                    for (key, schema_type) in properties {
                        if let Some(value) = value_obj.get(key) {
                            if !self.validate_type(value, schema_type) {
                                return false;
                            }
                        }
                    }
                }
                true
            }
            _ => false,
        }
    }

    fn validate_type(&self, value: &Value, schema_type: &Value) -> bool {
        match schema_type {
            Value::Object(type_obj) => {
                if let Some(Value::String(type_name)) = type_obj.get("type") {
                    match type_name.as_str() {
                        "string" => matches!(value, Value::String(_)),
                        "number" | "integer" => matches!(value, Value::Number(_)), // Support both number and integer
                        "boolean" => matches!(value, Value::Bool(_)),
                        "null" => matches!(value, Value::Null),
                        "array" => {
                            if let Value::Array(items) = value {
                                if let Some(item_schema) = type_obj.get("items") {
                                    items
                                        .iter()
                                        .all(|item| self.validate_type(item, item_schema))
                                } else {
                                    true
                                }
                            } else {
                                false
                            }
                        }
                        "object" => {
                            if let Value::Object(_) = value {
                                self.validate_against_schema(value, schema_type)
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn with_content(mut self, content: String) -> Self {
        self.definition = content;
        self
    }

    pub fn with_topic(mut self, topic: String) -> Self {
        self.topic_id = Some(topic);
        self
    }

    pub fn with_partition(mut self, partition: usize) -> Self {
        self.partition_id = Some(partition);
        self
    }

    pub fn update_version(&mut self, version: SchemaVersion) {
        self.version = version;
    }

    pub fn set_compatibility(&mut self, compatibility: Compatibility) {
        self.compatibility = compatibility;
    }

    pub fn add_metadata(&mut self, key: String, value: String) {
        let metadata = self.metadata.get_or_insert_with(HashMap::new);
        metadata.insert(key, value);
    }

    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.as_ref().and_then(|m| m.get(key))
    }
}

impl From<Schema> for MessageSchema {
    fn from(schema: Schema) -> Self {
        MessageSchema {
            id: schema.id,
            definition: schema.definition,
            version: schema.version,
            compatibility: Compatibility::Backward,
            metadata: None,
            topic_id: None,
            partition_id: None,
        }
    }
}

impl From<MessageSchema> for Schema {
    fn from(schema: MessageSchema) -> Self {
        Schema {
            id: schema.id,
            definition: schema.definition,
            version: schema.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_message_schema_new() {
        let schema = MessageSchema::new();

        assert_eq!(schema.id, 0);
        assert_eq!(schema.definition, String::new());
        assert_eq!(schema.version.major, 1);
        assert_eq!(schema.compatibility, Compatibility::Backward);
        assert!(schema.metadata.is_none());
        assert!(schema.topic_id.is_none());
        assert!(schema.partition_id.is_none());
    }

    #[test]
    fn test_message_schema_new_with_definition() {
        let definition =
            r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#.to_string();
        let schema = MessageSchema::new_with_definition(definition.clone());

        assert_eq!(schema.definition, definition);
        assert_eq!(schema.id, 0);
        assert_eq!(schema.version.major, 1);
    }

    #[test]
    fn test_message_schema_new_with_schema() {
        let base_schema = Schema {
            id: 42,
            definition: "test_definition".to_string(),
            version: SchemaVersion::new(3),
        };

        let message_schema = MessageSchema::new_with_schema(base_schema.clone());

        assert_eq!(message_schema.id, 42);
        assert_eq!(message_schema.definition, "test_definition");
        assert_eq!(message_schema.version.major, 3);
        assert_eq!(message_schema.compatibility, Compatibility::Backward);
    }

    #[test]
    fn test_validate_valid_json_message() {
        let schema_def = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        })
        .to_string();

        let schema = MessageSchema::new_with_definition(schema_def);

        let valid_message = r#"{"name": "John", "age": 30}"#;
        assert!(schema.validate(valid_message).is_ok());

        let valid_message_without_optional = r#"{"name": "Jane"}"#;
        assert!(schema.validate(valid_message_without_optional).is_ok());
    }

    #[test]
    fn test_validate_invalid_json_message() {
        let schema_def = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        })
        .to_string();

        let schema = MessageSchema::new_with_definition(schema_def);

        // Missing required field
        let invalid_message = r#"{"age": 30}"#;
        assert!(schema.validate(invalid_message).is_err());

        // Wrong type
        let wrong_type_message = r#"{"name": 123}"#;
        assert!(schema.validate(wrong_type_message).is_err());

        // Invalid JSON
        let malformed_json = r#"{"name": "John", invalid}"#;
        assert!(schema.validate(malformed_json).is_err());
    }

    #[test]
    fn test_validate_different_types() {
        let schema_def = json!({
            "type": "object",
            "properties": {
                "string_field": {"type": "string"},
                "number_field": {"type": "number"},
                "integer_field": {"type": "integer"},
                "boolean_field": {"type": "boolean"},
                "null_field": {"type": "null"}
            }
        })
        .to_string();

        let schema = MessageSchema::new_with_definition(schema_def);

        let valid_message = json!({
            "string_field": "test",
            "number_field": 3.14,
            "integer_field": 42,
            "boolean_field": true,
            "null_field": null
        })
        .to_string();

        assert!(schema.validate(&valid_message).is_ok());
    }

    #[test]
    fn test_validate_array_type() {
        let schema_def = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        })
        .to_string();

        let schema = MessageSchema::new_with_definition(schema_def);

        let valid_message = json!({
            "items": ["item1", "item2", "item3"]
        })
        .to_string();

        assert!(schema.validate(&valid_message).is_ok());

        // Invalid array with wrong item types
        let invalid_message = json!({
            "items": ["item1", 123, "item3"]
        })
        .to_string();

        assert!(schema.validate(&invalid_message).is_err());
    }

    #[test]
    fn test_validate_nested_object() {
        let schema_def = json!({
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"}
                    },
                    "required": ["name"]
                }
            }
        })
        .to_string();

        let schema = MessageSchema::new_with_definition(schema_def);

        let valid_message = json!({
            "user": {
                "name": "John",
                "email": "john@example.com"
            }
        })
        .to_string();

        assert!(schema.validate(&valid_message).is_ok());

        let invalid_message = json!({
            "user": {
                "email": "john@example.com"
            }
        })
        .to_string();

        assert!(schema.validate(&invalid_message).is_err());
    }

    #[test]
    fn test_with_content() {
        let mut schema = MessageSchema::new();
        schema = schema.with_content("new_content".to_string());

        assert_eq!(schema.definition, "new_content");
    }

    #[test]
    fn test_with_topic() {
        let mut schema = MessageSchema::new();
        schema = schema.with_topic("test_topic".to_string());

        assert_eq!(schema.topic_id, Some("test_topic".to_string()));
    }

    #[test]
    fn test_with_partition() {
        let mut schema = MessageSchema::new();
        schema = schema.with_partition(5);

        assert_eq!(schema.partition_id, Some(5));
    }

    #[test]
    fn test_update_version() {
        let mut schema = MessageSchema::new();
        let new_version = SchemaVersion::new(10);

        schema.update_version(new_version.clone());

        assert_eq!(schema.version.major, 10);
    }

    #[test]
    fn test_set_compatibility() {
        let mut schema = MessageSchema::new();

        schema.set_compatibility(Compatibility::Forward);
        assert_eq!(schema.compatibility, Compatibility::Forward);

        schema.set_compatibility(Compatibility::Full);
        assert_eq!(schema.compatibility, Compatibility::Full);
    }

    #[test]
    fn test_metadata_operations() {
        let mut schema = MessageSchema::new();

        // Add metadata
        schema.add_metadata("author".to_string(), "test_user".to_string());
        schema.add_metadata("version".to_string(), "1.0".to_string());

        // Get metadata
        assert_eq!(
            schema.get_metadata("author"),
            Some(&"test_user".to_string())
        );
        assert_eq!(schema.get_metadata("version"), Some(&"1.0".to_string()));
        assert_eq!(schema.get_metadata("nonexistent"), None);

        // Verify metadata structure
        assert!(schema.metadata.is_some());
        let metadata = schema.metadata.as_ref().unwrap();
        assert_eq!(metadata.len(), 2);
    }

    #[test]
    fn test_schema_conversion_from_schema() {
        let base_schema = Schema {
            id: 99,
            definition: "conversion_test".to_string(),
            version: SchemaVersion::new(7),
        };

        let message_schema: MessageSchema = base_schema.clone().into();

        assert_eq!(message_schema.id, 99);
        assert_eq!(message_schema.definition, "conversion_test");
        assert_eq!(message_schema.version.major, 7);
        assert_eq!(message_schema.compatibility, Compatibility::Backward);
    }

    #[test]
    fn test_schema_conversion_to_schema() {
        let message_schema = MessageSchema {
            id: 88,
            definition: "to_schema_test".to_string(),
            version: SchemaVersion::new(5),
            compatibility: Compatibility::Forward,
            metadata: None,
            topic_id: Some("test".to_string()),
            partition_id: Some(1),
        };

        let base_schema: Schema = message_schema.into();

        assert_eq!(base_schema.id, 88);
        assert_eq!(base_schema.definition, "to_schema_test");
        assert_eq!(base_schema.version.major, 5);
    }

    #[test]
    fn test_schema_serialization() {
        let mut schema = MessageSchema::new_with_definition("test_def".to_string());
        schema.add_metadata("key".to_string(), "value".to_string());
        schema = schema.with_topic("topic1".to_string()).with_partition(2);

        let json = serde_json::to_string(&schema).unwrap();
        let deserialized: MessageSchema = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.definition, "test_def");
        assert_eq!(deserialized.topic_id, Some("topic1".to_string()));
        assert_eq!(deserialized.partition_id, Some(2));
        assert_eq!(deserialized.get_metadata("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_invalid_schema_definition() {
        let invalid_schema_def = "not valid json";
        let schema = MessageSchema::new_with_definition(invalid_schema_def.to_string());

        let test_message = r#"{"test": "value"}"#;
        let result = schema.validate(test_message);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Schema is not valid JSON"));
    }

    #[test]
    fn test_validate_with_empty_schema() {
        let schema = MessageSchema::new(); // Empty definition

        let test_message = r#"{"test": "value"}"#;
        let result = schema.validate(test_message);

        assert!(result.is_err());
    }

    #[test]
    fn test_clone_and_debug() {
        let schema = MessageSchema::new_with_definition("clone_test".to_string());
        let cloned = schema.clone();

        assert_eq!(schema.definition, cloned.definition);

        // Test debug format
        let debug_str = format!("{:?}", schema);
        assert!(debug_str.contains("MessageSchema"));
        assert!(debug_str.contains("clone_test"));
    }
}
