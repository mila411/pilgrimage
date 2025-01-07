use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub content: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Message {
    pub fn new(content: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            content,
            timestamp: chrono::Utc::now(),
        }
    }
}

impl From<String> for Message {
    fn from(content: String) -> Self {
        Message::new(content)
    }
}

impl From<Message> for String {
    fn from(message: Message) -> Self {
        message.content
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Message[{}] {} (at {})",
            self.id, self.content, self.timestamp
        )
    }
}
