use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};

use crate::broker::Broker;

pub struct Cluster {
    brokers: Arc<Mutex<HashMap<String, Arc<Mutex<Broker>>>>>,
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new()
    }
}

impl Cluster {
    pub fn new() -> Self {
        Cluster {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn monitor_cluster(&self) {
        loop {
            println!("Monitoring cluster...");
            tokio::time::sleep(Duration::from_secs(10)).await;
            // TODO: Add monitoring logic here
        }
    }

    pub async fn add_broker(&self, broker_id: String, broker: Arc<Mutex<Broker>>) {
        let mut brokers = self.brokers.lock().await;
        brokers.insert(broker_id, broker);
    }

    pub async fn remove_broker(&self, broker_id: &str) {
        let mut brokers = self.brokers.lock().await;
        brokers.remove(broker_id);
    }

    pub async fn get_broker(&self, broker_id: &str) -> Option<Arc<Mutex<Broker>>> {
        let brokers = self.brokers.lock().await;
        brokers.get(broker_id).cloned()
    }

    pub async fn list_brokers(&self) -> Vec<String> {
        let brokers = self.brokers.lock().await;
        brokers.keys().cloned().collect()
    }

    pub async fn perform_maintenance(&self) {
        loop {
            sleep(Duration::from_secs(60)).await;
            let brokers = self.brokers.lock().await;
            for id in brokers.keys() {
                println!("Performing maintenance on broker: {}", id);
                // TODO: Add maintenance processing here
            }
        }
    }
}
