//! Module for managing a cluster of brokers.
//!
//! This module provides the [`Cluster`] struct,
//! which is responsible for managing the brokers in the cluster.
//!
//! The cluster is a collection of [`Broker`] instances that are responsible for
//! handling messages and managing partitions.
//!
//! # Example
//! The following example shows how to create a new cluster,
//! add a broker to it, and monitor the cluster (which automatically removes unhealthy brokers):
//! ```rust
//! use std::thread;
//! use std::sync::Arc;
//! use std::time::Duration;
//! use pilgrimage::broker::Broker;
//! use pilgrimage::broker::cluster::Cluster;
//!
//! // Create a new cluster
//! let cluster = Cluster::new();
//!
//! // Create a new broker
//! let broker = Arc::new(Broker::new(
//!     "broker1", // broker ID
//!     3,         // number of partitions
//!     2,         // replication factor
//!     "logs"     // storage path
//! ));
//!
//! // Add the broker to the cluster
//! cluster.add_broker("broker1".to_string(), broker);
//!
//! // Check if the broker exists in the cluster
//! let broker_exists = cluster.get_broker("broker1");
//! assert_eq!(broker_exists.is_some(), true);
//!
//! // Monitor the cluster (runs in background)
//! cluster.monitor_cluster();
//!
//! // Since the broker is healthy, it should still exist after monitoring
//! let broker_still_exists = cluster.get_broker("broker1");
//! assert_eq!(broker_still_exists.is_some(), true);
//! ```

use crate::broker::Broker;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A `Cluster` is a collection of [`Broker`] instances.
///
/// It is responsible for managing the brokers in the cluster, adding and removing brokers, and
/// monitoring the health of the brokers.
pub struct Cluster {
    /// A map of broker IDs to broker instances.
    ///
    /// The broker ID is a unique identifier for each broker in the cluster.
    brokers: Arc<Mutex<HashMap<String, Arc<Broker>>>>,
}

impl Cluster {
    /// Create a new `Cluster` instance.
    pub fn new() -> Self {
        Cluster {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a broker to the cluster.
    ///
    /// # Arguments
    /// * `broker_id` - The unique identifier for the broker.
    /// * `broker` - The broker instance to add to the cluster.
    ///   To allow sharing between threads, the broker must be wrapped in an [`Arc`].
    ///
    /// # Example
    /// ```rust
    /// use std::sync::Arc;
    /// use pilgrimage::broker::Broker;
    /// use pilgrimage::broker::cluster::Cluster;
    ///
    /// // Create a new cluster
    /// let cluster = Cluster::new();
    /// // Create a new broker
    /// let broker = Arc::new(Broker::new(
    ///     "broker1", // broker ID
    ///     3,         // number of partitions
    ///     2,         // replication factor
    ///     "logs"     // storage path
    /// ));
    /// // Add the broker to the cluster
    /// cluster.add_broker("broker1".to_string(), broker);
    ///
    /// // Check if the broker was added to the cluster
    /// let broker_added = cluster.get_broker("broker1");
    /// assert_eq!(broker_added.is_some(), true);
    /// ```
    pub fn add_broker(&self, broker_id: String, broker: Arc<Broker>) {
        let mut brokers = self.brokers.lock().unwrap();
        brokers.insert(broker_id, broker);
    }

    /// Remove a broker from the cluster.
    ///
    /// # Arguments
    /// * `broker_id` - The unique identifier for the broker to remove.
    ///
    /// # Example
    /// ```rust
    /// use std::sync::Arc;
    /// use pilgrimage::broker::Broker;
    /// use pilgrimage::broker::cluster::Cluster;
    ///
    /// // Create a new cluster
    /// let cluster = Cluster::new();
    /// // Create a new broker
    /// let broker = Arc::new(Broker::new(
    ///     "broker1", // broker ID
    ///     3,         // number of partitions
    ///     2,         // replication factor
    ///     "logs"     // storage path
    /// ));
    /// // Add the broker to the cluster
    /// cluster.add_broker("broker1".to_string(), broker);
    ///
    /// // Remove the broker from the cluster
    /// cluster.remove_broker("broker1");
    ///
    /// // Check if the broker was removed from the cluster
    /// let broker_removed = cluster.get_broker("broker1");
    /// assert_eq!(broker_removed.is_none(), true);
    /// ```
    pub fn remove_broker(&self, broker_id: &str) {
        let mut brokers = self.brokers.lock().unwrap();
        brokers.remove(broker_id);
    }

    /// Get a broker from the cluster by its ID.
    ///
    /// # Arguments
    /// * `broker_id` - The unique identifier for the broker to get.
    ///
    /// # Returns
    /// An [`Option`] containing an [`Arc`] reference to the broker if it exists in the cluster.
    ///
    /// The broker instance returned is a clone of the broker in the cluster.
    ///
    /// # Example
    /// ```rust
    /// use std::sync::Arc;
    /// use pilgrimage::broker::Broker;
    /// use pilgrimage::broker::cluster::Cluster;
    ///
    /// // Create a new cluster
    /// let cluster = Cluster::new();
    /// // Create a new broker
    /// let broker = Arc::new(Broker::new(
    ///     "broker1", // broker ID
    ///     3,         // number of partitions
    ///     2,         // replication factor
    ///     "logs"     // storage path
    /// ));
    /// // Add the broker to the cluster
    /// cluster.add_broker("broker1".to_string(), broker);
    ///
    /// // Get the broker from the cluster
    /// let broker = cluster.get_broker("broker1");
    /// assert_eq!(broker.is_some(), true);
    ///
    /// // But if the broker does not exist in the cluster, it will return None
    /// let broker = cluster.get_broker("broker2");
    /// assert_eq!(broker.is_none(), true);
    /// ```
    pub fn get_broker(&self, broker_id: &str) -> Option<Arc<Broker>> {
        let brokers = self.brokers.lock().unwrap();
        brokers.get(broker_id).cloned()
    }

    /// Monitor the health of the brokers in the cluster.
    ///
    /// This method spawns a new thread that periodically checks the health of the brokers in the
    /// cluster. If a broker is not healthy, it is removed from the cluster.
    ///
    /// The health check interval is 10 seconds.
    ///
    /// # Example
    /// ```rust
    /// use pilgrimage::broker::cluster::Cluster;
    /// use std::sync::Arc;
    /// use std::thread;
    /// use std::time::Duration;
    /// use pilgrimage::broker::Broker;
    ///
    /// // Create a new cluster
    /// let cluster = Cluster::new();
    ///
    /// // Create a new broker
    /// let broker = Arc::new(Broker::new(
    ///     "broker1", // broker ID
    ///     3,         // number of partitions
    ///     2,         // replication factor
    ///     "logs"     // storage path
    /// ));
    ///
    /// // Add the broker to the cluster
    /// cluster.add_broker("broker1".to_string(), broker);
    ///
    /// // Monitor the cluster (runs in background)
    /// cluster.monitor_cluster();
    ///
    /// // Since the broker is healthy, it should still exist after monitoring
    /// let broker_still_exists = cluster.get_broker("broker1");
    /// assert_eq!(broker_still_exists.is_some(), true);
    /// ```
    pub fn monitor_cluster(&self) {
        let brokers = self.brokers.clone();
        std::thread::spawn(move || {
            loop {
                let mut brokers_to_remove = Vec::new();
                {
                    let brokers = brokers.lock().unwrap();
                    for (broker_id, broker) in brokers.iter() {
                        if !broker.is_healthy() {
                            brokers_to_remove.push(broker_id.clone());
                        }
                    }
                }

                // Delete the broker from the cluster
                if !brokers_to_remove.is_empty() {
                    let mut brokers = brokers.lock().unwrap();
                    for broker_id in brokers_to_remove {
                        brokers.remove(&broker_id);
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
