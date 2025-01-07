use crate::broker::Broker;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct Cluster {
    brokers: Arc<Mutex<HashMap<String, Arc<Broker>>>>,
}

impl Cluster {
    pub fn new() -> Self {
        Cluster {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_broker(&self, broker_id: String, broker: Arc<Broker>) {
        let mut brokers = self.brokers.lock().unwrap();
        brokers.insert(broker_id, broker);
    }

    pub fn remove_broker(&self, broker_id: &str) {
        let mut brokers = self.brokers.lock().unwrap();
        brokers.remove(broker_id);
    }

    pub fn get_broker(&self, broker_id: &str) -> Option<Arc<Broker>> {
        let brokers = self.brokers.lock().unwrap();
        brokers.get(broker_id).cloned()
    }

    pub fn monitor_cluster(&self) {
        let brokers = self.brokers.clone();
        std::thread::spawn(move || {
            loop {
                let mut brokers_to_remove = Vec::new();
                {
                    let brokers = brokers.lock().unwrap();
                    for (broker_id, broker) in brokers.iter() {
                        if !broker.is_healthy() {
                            println!("Broker {} is not healthy", broker_id);
                            brokers_to_remove.push(broker_id.clone());
                        }
                    }
                }

                // Delete the broker from the cluster
                if !brokers_to_remove.is_empty() {
                    let mut brokers = brokers.lock().unwrap();
                    for broker_id in brokers_to_remove {
                        brokers.remove(&broker_id);
                        println!("Broker {} has been removed from the cluster", broker_id);
                    }
                }

                std::thread::sleep(Duration::from_secs(10));
            }
        });
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::Broker;
    use crate::broker::Node;
    use crate::broker::leader::BrokerState;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cluster_initialization() {
        let cluster = Cluster::default();

        let broker1 = Arc::new(Broker::new("broker1", 3, 2, "logs"));
        let broker2 = Arc::new(Broker::new("broker2", 3, 2, "logs"));

        cluster.add_broker("broker1".to_string(), broker1.clone());
        cluster.add_broker("broker2".to_string(), broker2.clone());

        cluster.monitor_cluster();

        assert!(cluster.get_broker("broker1").is_some());
        assert!(cluster.get_broker("broker2").is_some());
    }

    #[test]
    fn test_broker_removal_on_unhealthy() {
        let cluster = Cluster::default();

        let broker1 = Arc::new(Broker::new("broker1", 3, 2, "logs"));
        let broker2 = Arc::new(Broker::new("broker2", 3, 2, "logs"));

        cluster.add_broker("broker1".to_string(), broker1.clone());
        cluster.add_broker("broker2".to_string(), broker2.clone());

        // Simulate broker1 becoming unhealthy
        {
            let mut storage = broker1.storage.lock().unwrap();
            storage.available = false;
        }

        // Make broker2 healthy
        {
            // Add node data
            let mut nodes = broker2.nodes.lock().unwrap();
            let node = Node {
                address: "127.0.0.1".to_string(),
                id: "node_id".to_string(),
                is_active: true,
                data: Arc::new(Mutex::new(vec![1, 2, 3])),
            };
            nodes.insert("test_node".to_string(), node);

            // Set as leader
            let mut state = broker2.leader_election.state.lock().unwrap();
            *state = BrokerState::Leader;

            // Add message to queue
            let message_content = "test_message".to_string();
            let message = crate::message::message::Message::new(message_content);
            broker2.message_queue.send(message);
        }

        cluster.monitor_cluster();

        // Wait for the monitor to detect and remove the unhealthy broker
        thread::sleep(Duration::from_secs(15));

        assert!(cluster.get_broker("broker1").is_none());
        assert!(cluster.get_broker("broker2").is_some());
    }
}
