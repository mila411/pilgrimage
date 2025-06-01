use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;

use crate::broker::node::Node;

#[derive(Debug, Clone)]
pub struct ReplicationState {
    last_replicated: Instant,
    status: ReplicationStatus,
    replicas: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum ReplicationStatus {
    InProgress,
    Complete,
    Failed(String),
}

pub struct ReplicationManager {
    states: Arc<Mutex<HashMap<String, ReplicationState>>>,
    nodes: Arc<Mutex<HashMap<String, Arc<Node>>>>,
    replication_factor: usize,
    tx: mpsc::Sender<ReplicationEvent>,
    rx: mpsc::Receiver<ReplicationEvent>,
}

#[derive(Debug)]
pub enum ReplicationEvent {
    DataUpdate {
        partition_id: String,
        data: Vec<u8>,
    },
    StateUpdate {
        partition_id: String,
        status: ReplicationStatus,
    },
    NodeFailure {
        node_id: String,
    },
}

impl ReplicationManager {
    pub fn new(nodes: Arc<Mutex<HashMap<String, Arc<Node>>>>, replication_factor: usize) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            states: Arc::new(Mutex::new(HashMap::new())),
            nodes,
            replication_factor,
            tx,
            rx,
        }
    }

    pub async fn send_data_update(
        &self,
        partition_id: String,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.tx
            .send(ReplicationEvent::DataUpdate { partition_id, data })
            .await
            .map_err(|e| format!("Failed to send replication event: {}", e))
    }

    pub async fn send_state_update(
        &self,
        partition_id: String,
        status: ReplicationStatus,
    ) -> Result<(), String> {
        self.tx
            .send(ReplicationEvent::StateUpdate {
                partition_id,
                status,
            })
            .await
            .map_err(|e| format!("Failed to send status update event: {}", e))
    }

    pub async fn notify_node_failure(&self, node_id: String) -> Result<(), String> {
        self.tx
            .send(ReplicationEvent::NodeFailure { node_id })
            .await
            .map_err(|e| format!("Failed to send node failure event: {}", e))
    }

    pub async fn start(&mut self) {
        info!("Start Replication Manager");
        while let Some(event) = self.rx.recv().await {
            match event {
                ReplicationEvent::DataUpdate { partition_id, data } => {
                    self.handle_data_update(&partition_id, &data).await;
                }
                ReplicationEvent::StateUpdate {
                    partition_id,
                    status,
                } => {
                    self.update_replication_state(&partition_id, status);
                }
                ReplicationEvent::NodeFailure { node_id } => {
                    self.handle_node_failure(&node_id).await;
                }
            }
        }
    }

    async fn handle_data_update(&self, partition_id: &str, data: &[u8]) {
        let nodes = self.nodes.lock().unwrap();
        let mut states = self.states.lock().unwrap();

        let state = states
            .entry(partition_id.to_string())
            .or_insert_with(|| ReplicationState {
                last_replicated: Instant::now(),
                status: ReplicationStatus::InProgress,
                replicas: Vec::new(),
            });

        let healthy_nodes: Vec<_> = nodes.iter().filter(|(_, node)| node.is_alive()).collect();

        if healthy_nodes.len() < self.replication_factor {
            warn!(
                "Not enough nodes available. Required: {}, Current: {}",
                self.replication_factor,
                healthy_nodes.len()
            );
        }

        for (node_id, node) in healthy_nodes.iter().take(self.replication_factor) {
            match node.store_data(data) {
                Ok(_) => {
                    if !state.replicas.contains(node_id) {
                        state.replicas.push(node_id.to_string());
                    }
                    debug!("Successful replication to node {}", node_id);
                }
                Err(e) => {
                    error!("Replication failure to node {}: {}", node_id, e);
                    state.status = ReplicationStatus::Failed(format!(
                        "Replication failure to node {}",
                        node_id
                    ));
                }
            }
        }

        if state.replicas.len() >= self.replication_factor {
            state.status = ReplicationStatus::Complete;
            state.last_replicated = Instant::now();
        }
    }

    fn update_replication_state(&self, partition_id: &str, status: ReplicationStatus) {
        let mut states = self.states.lock().unwrap();
        if let Some(state) = states.get_mut(partition_id) {
            state.status = status;
            state.last_replicated = Instant::now();
        }
    }

    async fn handle_node_failure(&self, failed_node_id: &str) {
        info!("Failure of node {} is being handled", failed_node_id);
        let mut states = self.states.lock().unwrap();

        for (partition_id, state) in states.iter_mut() {
            if state.replicas.contains(&failed_node_id.to_string()) {
                // Find alternative node and re-replicate
                self.re_replicate(partition_id).await;
            }
        }
    }

    async fn re_replicate(&self, partition_id: &str) {
        debug!("Start re-replication of partition {}", partition_id);
        // Implementation: Retrieve data from healthy nodes and replicate to new nodes
    }
}
