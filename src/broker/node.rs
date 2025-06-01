use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub address: String,
    pub data: Arc<Mutex<Vec<u8>>>,
    pub last_heartbeat: Arc<Mutex<Instant>>,
    pub is_active: Arc<Mutex<bool>>,
}

impl Node {
    pub fn new(id: &str, address: &str, is_active: bool) -> Self {
        Self {
            id: id.to_string(),
            address: address.to_string(),
            data: Arc::new(Mutex::new(Vec::new())),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            is_active: Arc::new(Mutex::new(is_active)),
        }
    }

    pub fn create_test_node(id: &str) -> Self {
        Self::new(id, "127.0.0.1:8080", true)
    }

    pub fn store_data(&self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut node_data = self.data.lock().map_err(|e| e.to_string())?;
        node_data.clear();
        node_data.extend_from_slice(data);
        Ok(())
    }

    pub fn update_heartbeat(&self) {
        if let Ok(mut last_heartbeat) = self.last_heartbeat.lock() {
            *last_heartbeat = Instant::now();
        }
    }

    pub fn is_alive(&self) -> bool {
        if let Ok(is_active) = self.is_active.lock() {
            return *is_active;
        }
        false
    }

    pub fn set_active(&self, active: bool) {
        if let Ok(mut is_active) = self.is_active.lock() {
            *is_active = active;
        }
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }
}
