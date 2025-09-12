use crate::schema::message_schema::MessageSchema;
use serde::{Deserialize, Serialize};

/// Represents metadata for a message in the broker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    /// The unique identifier of the message
    pub id: String,
    /// The content of the message
    pub content: String,
    /// The timestamp when the message was created
    pub timestamp: String,
    /// The topic this message belongs to
    pub topic_id: Option<String>,
    /// The partition this message belongs to
    pub partition_id: Option<usize>,
    /// The schema of the message, if any
    pub schema: Option<MessageSchema>,
}

impl MessageMetadata {
    /// Creates a new instance of MessageMetadata
    pub fn new(id: String, content: String, timestamp: String) -> Self {
        Self {
            id,
            content,
            timestamp,
            topic_id: None,
            partition_id: None,
            schema: None,
        }
    }

    /// Sets the topic ID for this metadata
    pub fn with_topic(mut self, topic_id: String) -> Self {
        self.topic_id = Some(topic_id);
        self
    }

    /// Sets the partition ID for this metadata
    pub fn with_partition(mut self, partition_id: usize) -> Self {
        self.partition_id = Some(partition_id);
        self
    }

    /// Sets the schema for this metadata
    pub fn with_schema(mut self, schema: MessageSchema) -> Self {
        self.schema = Some(schema);
        self
    }
}
