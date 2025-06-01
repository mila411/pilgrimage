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
                        "number" => matches!(value, Value::Number(_)),
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
