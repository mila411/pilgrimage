use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAck {
    pub message_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub status: AckStatus,
    pub topic: String,
    pub partition: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AckStatus {
    Received,
    Processed,
    Failed(String),
}

impl MessageAck {
    pub fn new(
        message_id: Uuid,
        timestamp: DateTime<Utc>,
        status: AckStatus,
        topic: String,
        partition: usize,
    ) -> Self {
        Self {
            message_id,
            timestamp,
            status,
            topic,
            partition,
        }
    }
}
