//! Module for leader election and broker leader state.
//!
//! This module contains the implementation of leader election and broker leader state.

pub mod election;
pub mod heartbeat;
pub mod state;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Re-export types from submodules
pub use election::LeaderElection as Election;
pub use state::BrokerState;

#[derive(Clone)]
pub struct LeaderElection {
    pub node_id: String,
    pub state: Arc<Mutex<BrokerState>>,
    pub peers: Arc<Mutex<HashMap<String, String>>>,
    pub terms: Arc<Mutex<HashMap<String, u64>>>,
    pub current_term: Arc<Mutex<u64>>,
}

impl LeaderElection {
    pub fn new(node_id: &str, peers: HashMap<String, String>) -> Self {
        Self {
            node_id: node_id.to_string(),
            state: Arc::new(Mutex::new(BrokerState::Follower)),
            peers: Arc::new(Mutex::new(peers)),
            terms: Arc::new(Mutex::new(HashMap::new())),
            current_term: Arc::new(Mutex::new(0)),
        }
    }

    pub fn get_term(&self, node_id: &str) -> u64 {
        self.terms
            .lock()
            .unwrap()
            .get(node_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn increment_term(&self, node_id: &str) {
        let mut terms = self.terms.lock().unwrap();
        let term = terms.entry(node_id.to_string()).or_insert(0);
        *term += 1;
    }

    pub fn update_term(&self, node_id: &str, term: u64) {
        let mut terms = self.terms.lock().unwrap();
        terms.insert(node_id.to_string(), term);
    }
}
