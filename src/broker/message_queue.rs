use crate::broker::scaling::AutoScaler;
use crate::message::message::Message;
use lazy_static::lazy_static;
use prometheus::{Counter, register_counter};
use std::collections::{HashSet, VecDeque};
use std::sync::{Condvar, Mutex};
use uuid::Uuid;

lazy_static! {
    static ref MESSAGES_RECEIVED: Counter = register_counter!(
        "pilgrimage_messages_received_total",
        "Total number of messages received"
    )
    .unwrap();
}

pub struct MessageQueue {
    queue: Mutex<VecDeque<Message>>,
    processed_ids: Mutex<HashSet<Uuid>>,
    auto_scaler: AutoScaler,
    condvar: Condvar,
}

impl MessageQueue {
    pub fn new(min_instances: usize, max_instances: usize) -> Self {
        MessageQueue {
            queue: Mutex::new(VecDeque::new()),
            processed_ids: Mutex::new(HashSet::new()),
            auto_scaler: AutoScaler::new(min_instances, max_instances),
            condvar: Condvar::new(),
        }
    }

    pub fn send(&self, message: Message) -> Result<(), String> {
        let mut processed_ids = self.processed_ids.lock().unwrap();
        if processed_ids.contains(&message.id) {
            return Err("Duplicate message detected.".to_string());
        }

        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message.clone());
        processed_ids.insert(message.id);
        MESSAGES_RECEIVED.inc();
        Ok(())
    }

    pub fn receive(&self) -> Option<Message> {
        if let Ok(mut queue) = self.queue.lock() {
            queue.pop_front()
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        let queue = self.queue.lock().unwrap();
        queue.is_empty()
    }
}
