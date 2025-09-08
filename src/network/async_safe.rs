use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, RwLock, Semaphore};
use tokio::time::timeout;

/// Async-safe connection manager
#[derive(Debug)]
pub struct AsyncSafeConnectionManager<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    connections: DashMap<K, V>,
    active_connections: AsyncMutex<u64>,
    max_connections: u64,
}

/// Async-safe resource pool
#[derive(Debug)]
pub struct AsyncSafeResourcePool<T>
where
    T: Clone + Send + Sync + 'static,
{
    resources: Arc<RwLock<Vec<T>>>,
    available: Arc<Semaphore>,
    max_size: usize,
    current_size: Arc<AsyncMutex<usize>>,
}

/// Thread-safe metrics collector
#[derive(Debug, Clone)]
pub struct AsyncSafeMetrics {
    counters: Arc<DashMap<String, u64>>,
    gauges: Arc<DashMap<String, f64>>,
    last_updated: Arc<RwLock<std::time::Instant>>,
}

/// Connection management errors
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    #[error("Connection limit exceeded: {current}/{max}")]
    ConnectionLimitExceeded { current: u64, max: u64 },

    #[error("Resource pool exhausted")]
    ResourcePoolExhausted,

    #[error("Timeout waiting for resource: {timeout:?}")]
    ResourceTimeout { timeout: Duration },

    #[error("Resource allocation failed")]
    AllocationFailed,
}

pub type ConcurrencyResult<T> = Result<T, ConcurrencyError>;

impl<K, V> AsyncSafeConnectionManager<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new connection manager
    pub fn new(max_connections: u64) -> Self {
        Self {
            connections: DashMap::new(),
            active_connections: AsyncMutex::new(0),
            max_connections,
        }
    }

    /// Add Connection (Non-Blocking)
    pub async fn add_connection(&self, key: K, value: V) -> ConcurrencyResult<()> {
        // Check connection limit before adding
        let current_count = self.connections.len() as u64;
        if current_count >= self.max_connections {
            return Err(ConcurrencyError::ConnectionLimitExceeded {
                current: current_count,
                max: self.max_connections,
            });
        }

        // Secure Insertion Using DashMap
        self.connections.insert(key, value);

        // Update Active Connection Count
        let mut active = self.active_connections.lock().await;
        *active += 1;

        Ok(())
    }

    /// Get Connection
    pub fn get_connection(&self, key: &K) -> Option<V> {
        self.connections.get(key).map(|entry| entry.clone())
    }

    /// Remove Connection
    pub async fn remove_connection(&self, key: &K) -> Option<V> {
        let result = self.connections.remove(key).map(|(_, v)| v);

        if result.is_some() {
            let mut active = self.active_connections.lock().await;
            *active = active.saturating_sub(1);
        }

        result
    }

    /// Get All Connections
    pub fn get_all_connections(&self) -> Vec<(K, V)> {
        self.connections
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Get Active Connection Count
    pub async fn active_count(&self) -> u64 {
        *self.active_connections.lock().await
    }

    /// Check Connection Limit
    pub async fn is_connection_limit_reached(&self) -> bool {
        *self.active_connections.lock().await >= self.max_connections
    }
}

impl<T> AsyncSafeResourcePool<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Create a new resource pool
    pub fn new(max_size: usize) -> Self {
        Self {
            resources: Arc::new(RwLock::new(Vec::with_capacity(max_size))),
            available: Arc::new(Semaphore::new(0)),
            max_size,
            current_size: Arc::new(AsyncMutex::new(0)),
        }
    }

    /// Add Resource
    pub async fn add_resource(&self, resource: T) -> ConcurrencyResult<()> {
        let mut current = self.current_size.lock().await;

        if *current >= self.max_size {
            return Err(ConcurrencyError::ResourcePoolExhausted);
        }

        let mut resources = self.resources.write().await;
        resources.push(resource);
        *current += 1;

        // Add a permit to the semaphore
        self.available.add_permits(1);

        Ok(())
    }

    /// Acquire Resource (With Timeout)
    pub async fn acquire_resource(&self, timeout_duration: Duration) -> ConcurrencyResult<T> {
        // Check resource availability with semaphore
        let _permit = timeout(timeout_duration, self.available.acquire())
            .await
            .map_err(|_| ConcurrencyError::ResourceTimeout {
                timeout: timeout_duration,
            })?
            .map_err(|_| ConcurrencyError::AllocationFailed)?;

        // Acquire Resource
        let mut resources = self.resources.write().await;
        resources
            .pop()
            .ok_or(ConcurrencyError::ResourcePoolExhausted)
    }

    /// Release Resource
    pub async fn release_resource(&self, resource: T) -> ConcurrencyResult<()> {
        let mut resources = self.resources.write().await;

        if resources.len() >= self.max_size {
            return Err(ConcurrencyError::ResourcePoolExhausted);
        }

        resources.push(resource);
        self.available.add_permits(1);

        Ok(())
    }

    /// Get Available Resource Count
    pub async fn available_count(&self) -> usize {
        self.resources.read().await.len()
    }

    /// Get Current Pool Size
    pub async fn current_size(&self) -> usize {
        *self.current_size.lock().await
    }
}

impl AsyncSafeMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            gauges: Arc::new(DashMap::new()),
            last_updated: Arc::new(RwLock::new(std::time::Instant::now())),
        }
    }

    /// Increment Counter
    pub fn increment_counter(&self, key: &str, value: u64) {
        let mut counter = self.counters.entry(key.to_string()).or_insert(0);
        *counter += value;
    }

    /// Set Gauge
    pub async fn set_gauge(&self, key: &str, value: f64) {
        self.gauges.insert(key.to_string(), value);

        // Update Last Updated Time
        let mut last_updated = self.last_updated.write().await;
        *last_updated = std::time::Instant::now();
    }

    /// Get Counter Value
    pub fn get_counter(&self, key: &str) -> Option<u64> {
        self.counters.get(key).map(|entry| *entry)
    }

    /// Get Gauge Value
    pub fn get_gauge(&self, key: &str) -> Option<f64> {
        self.gauges.get(key).map(|entry| *entry)
    }

    /// Get All Metrics
    pub fn get_all_metrics(&self) -> (Vec<(String, u64)>, Vec<(String, f64)>) {
        let counters = self
            .counters
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();

        let gauges = self
            .gauges
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();

        (counters, gauges)
    }

    /// Reset Metrics
    pub async fn reset_metrics(&self) {
        self.counters.clear();
        self.gauges.clear();

        let mut last_updated = self.last_updated.write().await;
        *last_updated = std::time::Instant::now();
    }

    /// Get Last Updated Time
    pub async fn last_updated(&self) -> std::time::Instant {
        *self.last_updated.read().await
    }
}

impl Default for AsyncSafeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_connection_manager() {
        let manager = AsyncSafeConnectionManager::new(2);

        // Add connections
        assert!(manager.add_connection("node1", "connection1").await.is_ok());
        assert!(manager.add_connection("node2", "connection2").await.is_ok());

        // Connections exceeding limit are rejected
        assert!(
            manager
                .add_connection("node3", "connection3")
                .await
                .is_err()
        );

        // Get Connection
        assert_eq!(manager.get_connection(&"node1"), Some("connection1"));
        assert_eq!(manager.get_connection(&"node3"), None);

        // Check Active Connection Count
        assert_eq!(manager.active_count().await, 2);
    }

    #[tokio::test]
    async fn test_resource_pool() {
        let pool = AsyncSafeResourcePool::new(2);

        // Add Resources
        assert!(pool.add_resource("resource1").await.is_ok());
        assert!(pool.add_resource("resource2").await.is_ok());

        // Adding when pool is full fails
        assert!(pool.add_resource("resource3").await.is_err());

        // Get Resource
        let resource = pool.acquire_resource(Duration::from_millis(100)).await;
        assert!(resource.is_ok());

        // Check Available Resource Count
        assert_eq!(pool.available_count().await, 1);
    }

    #[tokio::test]
    async fn test_metrics() {
        let metrics = AsyncSafeMetrics::new();

        // Increment Counter
        metrics.increment_counter("requests", 1);
        metrics.increment_counter("requests", 5);
        assert_eq!(metrics.get_counter("requests"), Some(6));

        // Set Gauge
        metrics.set_gauge("cpu_usage", 75.5).await;
        assert_eq!(metrics.get_gauge("cpu_usage"), Some(75.5));

        // Get All Metrics
        let (counters, gauges) = metrics.get_all_metrics();
        assert_eq!(counters.len(), 1);
        assert_eq!(gauges.len(), 1);
    }
}
