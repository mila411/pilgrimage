use crate::broker::error::BrokerError;
use crate::broker::storage::Storage;
use crate::message::ack::Message;
use std::sync::{Arc, Mutex};

pub struct Transaction {
    storage: Arc<Mutex<Storage>>,
    messages: Vec<Message>,
}

impl Transaction {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Self {
            storage,
            messages: Vec::new(),
        }
    }

    pub fn add_message(&mut self, message: Message) {
        self.messages.push(message);
    }

    pub fn commit(&self) -> Result<(), BrokerError> {
        let mut storage = self.storage.lock().unwrap();
        for message in &self.messages {
            if !storage.is_message_processed(&message.id) {
                storage.save_message(message)?;
            }
        }
        Ok(())
    }

    pub fn rollback(&self) -> Result<(), BrokerError> {
        // TODO: Implement rollback (if necessary)
        Ok(())
    }
}
