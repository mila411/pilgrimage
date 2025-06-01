//! Module for managing auto-scaling of broker nodes based on load.
//!
//! This module provides a ScalingManager that dynamically manages the number of
//! nodes in the cluster based on load and resource usage.

use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::broker::metrics::SystemMetrics;
use crate::broker::node::Node;

#[derive(Debug, Clone)]
pub struct ScalingConfig {
    pub min_nodes: usize,
    pub max_nodes: usize,
    pub scale_up_threshold: f32,
    pub scale_down_threshold: f32,
    pub cooldown_period: Duration,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            min_nodes: 1,
            max_nodes: 10,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            cooldown_period: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Error)]
pub enum ScalingError {
    #[error("scaling execution error: {0}")]
    ExecutionError(String),
    #[error("Invalid setting: {0}")]
    InvalidConfig(String),
}

#[derive(Debug)]
pub enum ScalingEvent {
    MetricsUpdate {
        node_id: String,
        metrics: SystemMetrics,
    },
    ScaleUp {
        reason: String,
    },
    ScaleDown {
        node_id: String,
    },
}

pub struct ScalingManager {
    nodes: Arc<Mutex<HashMap<String, Arc<Node>>>>,
    metrics: Arc<Mutex<HashMap<String, SystemMetrics>>>,
    config: ScalingConfig,
    last_scaling_action: Arc<Mutex<Instant>>,
    tx: mpsc::Sender<ScalingEvent>,
    rx: mpsc::Receiver<ScalingEvent>,
}

impl ScalingManager {
    pub fn new(
        nodes: Arc<Mutex<HashMap<String, Arc<Node>>>>,
        config: Option<ScalingConfig>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            nodes,
            metrics: Arc::new(Mutex::new(HashMap::new())),
            config: config.unwrap_or_default(),
            last_scaling_action: Arc::new(Mutex::new(Instant::now())),
            tx,
            rx,
        }
    }

    pub async fn start(&mut self) {
        info!("Start Scaling Manager");
        while let Some(event) = self.rx.recv().await {
            match event {
                ScalingEvent::MetricsUpdate { node_id, metrics } => {
                    self.handle_metrics_update(&node_id, metrics).await;
                }
                ScalingEvent::ScaleUp { reason } => {
                    self.handle_scale_up(&reason).await;
                }
                ScalingEvent::ScaleDown { node_id } => {
                    self.handle_scale_down(&node_id).await;
                }
            }
        }
    }

    pub async fn handle_metrics_update(&self, node_id: &str, metrics: SystemMetrics) {
        let mut metrics_map = self.metrics.lock().unwrap();
        metrics_map.insert(node_id.to_string(), metrics);

        if self.can_scale() {
            self.check_and_scale().await;
        }
    }

    pub async fn check_and_scale(&self) {
        let metrics = self.metrics.lock().unwrap();
        let nodes = self.nodes.lock().unwrap();

        let avg_cpu = self.calculate_average_cpu_usage(&metrics);

        if avg_cpu > self.config.scale_up_threshold && nodes.len() < self.config.max_nodes {
            let reason = format!(
                "Average CPU utilization exceeds {}% (currently: {:.1}%)",
                self.config.scale_up_threshold * 100.0,
                avg_cpu * 100.0
            );
            drop(nodes);
            self.request_scale_up(reason).await.ok();
        } else if avg_cpu < self.config.scale_down_threshold && nodes.len() > self.config.min_nodes
        {
            if let Some(node_id) = self.find_least_loaded_node(&metrics) {
                drop(nodes);
                self.request_scale_down(node_id).await.ok();
            }
        }
    }

    async fn handle_scale_up(&self, reason: &str) {
        info!("Perform scale-up: {}", reason);

        let node_id = format!("node_{}", Instant::now().elapsed().as_millis());
        let new_node = Arc::new(Node::new(&node_id, "127.0.0.1:8080", true));

        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(node_id.clone(), new_node);

        self.update_last_scaling_time();
        debug!("New node {} added", node_id);
    }

    async fn handle_scale_down(&self, node_id: &str) {
        info!("Perform scale-down of node {}", node_id);

        let mut nodes = self.nodes.lock().unwrap();
        if nodes.len() <= self.config.min_nodes {
            warn!("Cancel scale-down because the minimum number of nodes has been reached");
            return;
        }

        if let Some(node) = nodes.get(node_id) {
            node.set_active(false);
            nodes.remove(node_id);
            self.update_last_scaling_time();
            debug!("Deleted node {}", node_id);
        }
    }

    fn can_scale(&self) -> bool {
        let last_scaling = self.last_scaling_action.lock().unwrap();
        last_scaling.elapsed() >= self.config.cooldown_period
    }

    fn update_last_scaling_time(&self) {
        if let Ok(mut last_scaling) = self.last_scaling_action.lock() {
            *last_scaling = Instant::now();
        }
    }

    fn calculate_average_cpu_usage(&self, metrics: &HashMap<String, SystemMetrics>) -> f32 {
        if metrics.is_empty() {
            return 0.0;
        }

        let total_cpu = metrics.values().map(|m| m.cpu_usage).sum::<f32>();

        total_cpu / metrics.len() as f32
    }

    fn find_least_loaded_node(&self, metrics: &HashMap<String, SystemMetrics>) -> Option<String> {
        metrics
            .iter()
            .min_by(|a, b| a.1.cpu_usage.partial_cmp(&b.1.cpu_usage).unwrap())
            .map(|(id, _)| id.clone())
    }

    async fn request_scale_up(&self, reason: String) -> Result<(), ScalingError> {
        match self.tx.clone().try_send(ScalingEvent::ScaleUp { reason }) {
            Ok(_) => {
                debug!("Scale-up request sent.");
                Ok(())
            }
            Err(e) => {
                error!("Failed to send scale-up request: {}", e);
                Err(ScalingError::ExecutionError(e.to_string()))
            }
        }
    }

    async fn request_scale_down(&self, node_id: String) -> Result<(), ScalingError> {
        match self
            .tx
            .clone()
            .try_send(ScalingEvent::ScaleDown { node_id })
        {
            Ok(_) => {
                debug!("Scale-down request sent.");
                Ok(())
            }
            Err(e) => {
                error!("Failed to send scale-down request: {}", e);
                Err(ScalingError::ExecutionError(e.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    async fn setup_test_scaling_manager() -> ScalingManager {
        let nodes = Arc::new(Mutex::new(HashMap::new()));
        let config = ScalingConfig {
            min_nodes: 1,
            max_nodes: 5,
            scale_up_threshold: 0.7,
            scale_down_threshold: 0.3,
            cooldown_period: Duration::from_millis(100),
        };

        ScalingManager::new(nodes, Some(config))
    }

    #[tokio::test]
    async fn test_scaling_up() {
        let manager = setup_test_scaling_manager().await;
        let metrics = SystemMetrics::new(0.9, 0.8, 900.0);

        let initial_count = {
            let nodes = manager.nodes.lock().unwrap();
            nodes.len()
        };

        manager.handle_metrics_update("test_node", metrics).await;
        manager.handle_scale_up("テスト用高負荷").await;
        let nodes = manager.nodes.lock().unwrap();
        assert!(nodes.len() > initial_count, "ノード数が増加していません");
    }

    #[tokio::test]
    async fn test_scaling_down() {
        let manager = setup_test_scaling_manager().await;
        let node1 = Arc::new(Node::new("node1", "127.0.0.1:8080", true));
        let node2 = Arc::new(Node::new("node2", "127.0.0.1:8081", true));

        {
            let mut nodes = manager.nodes.lock().unwrap();
            nodes.insert("node1".to_string(), node1);
            nodes.insert("node2".to_string(), node2);
        }

        let initial_count = {
            let nodes = manager.nodes.lock().unwrap();
            nodes.len()
        };

        manager.handle_scale_down("node1").await;
        let nodes = manager.nodes.lock().unwrap();
        assert!(
            nodes.len() < initial_count,
            "Number of nodes not decreasing: current {}, initial {}",
            nodes.len(),
            initial_count
        );
        assert_eq!(
            nodes.len(),
            1,
            "Expected number of nodes is 1, but actually it is {}",
            nodes.len()
        );
    }

    #[tokio::test]
    async fn test_metrics_update() {
        let manager = setup_test_scaling_manager().await;
        let metrics = SystemMetrics::new(0.5, 0.6, 500.0);

        manager.handle_metrics_update("test_node", metrics).await;

        let stored_metrics = manager.metrics.lock().unwrap();
        assert!(
            stored_metrics.contains_key("test_node"),
            "Metrics not saved"
        );
        assert_eq!(stored_metrics.get("test_node").unwrap().cpu_usage, 0.5);
    }

    #[tokio::test]
    async fn test_cooldown_period() {
        let manager = setup_test_scaling_manager().await;

        sleep(Duration::from_millis(150)).await;
        assert!(manager.can_scale());

        manager.update_last_scaling_time();
        assert!(!manager.can_scale());

        sleep(Duration::from_millis(150)).await;
        assert!(manager.can_scale());
    }
}
