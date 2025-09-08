//! Flow control and backpressure handling for network operations
//!
//! This module implements production-grade flow control mechanisms to prevent
//! overwhelming receivers and ensure system stability under high load.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore, watch};
use tokio::time::{interval, sleep};

use crate::network::error::{NetworkError, NetworkResult};

/// Flow control configuration
#[derive(Debug, Clone)]
pub struct FlowControlConfig {
    /// Maximum number of outstanding messages per connection
    pub max_outstanding_messages: usize,
    /// Maximum send buffer size in bytes
    pub max_send_buffer_size: usize,
    /// Backpressure threshold (0.0 to 1.0)
    pub backpressure_threshold: f64,
    /// Window size for sliding window flow control
    pub window_size: usize,
    /// Window update threshold
    pub window_update_threshold: usize,
    /// Maximum message rate (messages per second)
    pub max_message_rate: f64,
    /// Rate limiting window duration
    pub rate_window: Duration,
    /// Memory pressure threshold in bytes
    pub memory_pressure_threshold: usize,
}

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            max_outstanding_messages: 1000,
            max_send_buffer_size: 1024 * 1024, // 1MB
            backpressure_threshold: 0.8,
            window_size: 65536, // 64KB
            window_update_threshold: 32768, // 32KB
            max_message_rate: 1000.0, // 1000 messages per second
            rate_window: Duration::from_secs(1),
            memory_pressure_threshold: 100 * 1024 * 1024, // 100MB
        }
    }
}

/// Flow control window for sliding window protocol
#[derive(Debug)]
pub struct FlowWindow {
    /// Current window size
    size: AtomicUsize,
    /// Used window space
    used: AtomicUsize,
    /// Sequence number for messages
    sequence: AtomicU64,
    /// Last acknowledged sequence number
    last_ack: AtomicU64,
    /// Window update threshold
    update_threshold: usize,
}

impl FlowWindow {
    /// Create a new flow window
    pub fn new(size: usize, update_threshold: usize) -> Self {
        Self {
            size: AtomicUsize::new(size),
            used: AtomicUsize::new(0),
            sequence: AtomicU64::new(0),
            last_ack: AtomicU64::new(0),
            update_threshold,
        }
    }

    /// Check if window has space for a message of given size
    pub fn has_space(&self, message_size: usize) -> bool {
        let current_size = self.size.load(Ordering::Acquire);
        let current_used = self.used.load(Ordering::Acquire);

        current_used + message_size <= current_size
    }

    /// Reserve space in the window
    pub fn reserve(&self, message_size: usize) -> NetworkResult<u64> {
        let current_size = self.size.load(Ordering::Acquire);
        let current_used = self.used.load(Ordering::Acquire);

        if current_used + message_size > current_size {
            return Err(NetworkError::ResourceExhausted(
                "Flow control window full".to_string()
            ));
        }

        // Atomically update used space
        let prev_used = self.used.fetch_add(message_size, Ordering::AcqRel);
        if prev_used + message_size > current_size {
            // Rollback if we exceeded the window
            self.used.fetch_sub(message_size, Ordering::AcqRel);
            return Err(NetworkError::ResourceExhausted(
                "Flow control window full".to_string()
            ));
        }

        // Get sequence number for this message
        let seq = self.sequence.fetch_add(1, Ordering::AcqRel);
        Ok(seq)
    }

    /// Acknowledge received messages and free window space
    pub fn acknowledge(&self, ack_seq: u64, message_size: usize) -> bool {
        let current_ack = self.last_ack.load(Ordering::Acquire);

        // Only process if this is a newer acknowledgment
        if ack_seq > current_ack {
            self.last_ack.store(ack_seq, Ordering::Release);
            self.used.fetch_sub(message_size, Ordering::AcqRel);

            // Check if we should send a window update
            let current_used = self.used.load(Ordering::Acquire);
            let current_size = self.size.load(Ordering::Acquire);

            current_size - current_used >= self.update_threshold
        } else {
            false
        }
    }

    /// Get current window utilization (0.0 to 1.0)
    pub fn utilization(&self) -> f64 {
        let size = self.size.load(Ordering::Acquire) as f64;
        let used = self.used.load(Ordering::Acquire) as f64;

        if size > 0.0 {
            used / size
        } else {
            0.0
        }
    }

    /// Update window size
    pub fn update_size(&self, new_size: usize) {
        self.size.store(new_size, Ordering::Release);
    }
}

/// Rate limiter using token bucket algorithm
#[derive(Debug)]
pub struct RateLimiter {
    /// Maximum tokens (rate limit)
    max_tokens: f64,
    /// Current tokens
    tokens: Arc<RwLock<f64>>,
    /// Last refill time
    last_refill: Arc<RwLock<Instant>>,
    /// Refill rate (tokens per second)
    refill_rate: f64,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(rate: f64) -> Self {
        Self {
            max_tokens: rate,
            tokens: Arc::new(RwLock::new(rate)),
            last_refill: Arc::new(RwLock::new(Instant::now())),
            refill_rate: rate,
        }
    }

    /// Try to acquire tokens for sending
    pub async fn try_acquire(&self, tokens: f64) -> bool {
        self.refill().await;

        let mut current_tokens = self.tokens.write().await;
        if *current_tokens >= tokens {
            *current_tokens -= tokens;
            true
        } else {
            false
        }
    }

    /// Wait for tokens to become available
    pub async fn acquire(&self, tokens: f64) -> NetworkResult<()> {
        loop {
            if self.try_acquire(tokens).await {
                return Ok(());
            }

            // Calculate wait time based on deficit
            let current_tokens = *self.tokens.read().await;
            let deficit = tokens - current_tokens;
            let wait_time = Duration::from_secs_f64(deficit / self.refill_rate);

            sleep(wait_time.min(Duration::from_millis(100))).await;
        }
    }

    /// Refill tokens based on time elapsed
    async fn refill(&self) {
        let now = Instant::now();
        let mut last_refill = self.last_refill.write().await;
        let elapsed = now.duration_since(*last_refill).as_secs_f64();

        if elapsed > 0.0 {
            let mut tokens = self.tokens.write().await;
            *tokens = (*tokens + elapsed * self.refill_rate).min(self.max_tokens);
            *last_refill = now;
        }
    }
}

/// Backpressure detector and handler
#[derive(Debug)]
pub struct BackpressureDetector {
    /// Flow control configuration
    config: FlowControlConfig,
    /// System memory usage tracker
    memory_usage: Arc<AtomicUsize>,
    /// Message queue sizes per connection
    queue_sizes: Arc<RwLock<HashMap<String, usize>>>,
    /// Backpressure state per connection
    backpressure_state: Arc<RwLock<HashMap<String, bool>>>,
    /// Global backpressure state
    global_backpressure: Arc<watch::Sender<bool>>,
    /// Backpressure receiver for clients
    _backpressure_rx: watch::Receiver<bool>,
}

impl BackpressureDetector {
    /// Create a new backpressure detector
    pub fn new(config: FlowControlConfig) -> Self {
        let (tx, rx) = watch::channel(false);

        Self {
            config,
            memory_usage: Arc::new(AtomicUsize::new(0)),
            queue_sizes: Arc::new(RwLock::new(HashMap::new())),
            backpressure_state: Arc::new(RwLock::new(HashMap::new())),
            global_backpressure: Arc::new(tx),
            _backpressure_rx: rx,
        }
    }

    /// Update queue size for a connection
    pub async fn update_queue_size(&self, connection_id: &str, size: usize) {
        {
            let mut queue_sizes = self.queue_sizes.write().await;
            queue_sizes.insert(connection_id.to_string(), size);
        }

        self.check_backpressure(connection_id).await;
    }

    /// Update system memory usage
    pub fn update_memory_usage(&self, usage: usize) {
        self.memory_usage.store(usage, Ordering::Release);
    }

    /// Check if connection is under backpressure
    pub async fn is_backpressure(&self, connection_id: &str) -> bool {
        let state = self.backpressure_state.read().await;
        state.get(connection_id).copied().unwrap_or(false)
    }

    /// Get global backpressure receiver
    pub fn global_backpressure_receiver(&self) -> watch::Receiver<bool> {
        self.global_backpressure.subscribe()
    }

    /// Check and update backpressure state
    async fn check_backpressure(&self, connection_id: &str) {
        let is_backpressure = self.detect_backpressure(connection_id).await;

        {
            let mut state = self.backpressure_state.write().await;
            state.insert(connection_id.to_string(), is_backpressure);
        }

        // Update global backpressure state
        let global_backpressure = self.detect_global_backpressure().await;
        let _ = self.global_backpressure.send(global_backpressure);
    }

    /// Detect backpressure for a specific connection
    async fn detect_backpressure(&self, connection_id: &str) -> bool {
        let queue_sizes = self.queue_sizes.read().await;

        if let Some(&queue_size) = queue_sizes.get(connection_id) {
            let threshold = (self.config.max_outstanding_messages as f64 * self.config.backpressure_threshold) as usize;
            queue_size > threshold
        } else {
            false
        }
    }

    /// Detect global backpressure
    async fn detect_global_backpressure(&self) -> bool {
        // Check memory pressure
        let memory_usage = self.memory_usage.load(Ordering::Acquire);
        if memory_usage > self.config.memory_pressure_threshold {
            return true;
        }

        // Check if too many connections are under backpressure
        let state = self.backpressure_state.read().await;
        let total_connections = state.len();
        let backpressure_connections = state.values().filter(|&&bp| bp).count();

        if total_connections > 0 {
            let backpressure_ratio = backpressure_connections as f64 / total_connections as f64;
            backpressure_ratio > self.config.backpressure_threshold
        } else {
            false
        }
    }

    /// Start background monitoring
    pub fn start_monitoring(&self) {
        let detector = self.clone_for_monitoring();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));

            loop {
                interval.tick().await;
                detector.periodic_check().await;
            }
        });
    }

    /// Periodic backpressure check
    async fn periodic_check(&self) {
        let connection_ids: Vec<String> = {
            let queue_sizes = self.queue_sizes.read().await;
            queue_sizes.keys().cloned().collect()
        };

        for connection_id in connection_ids {
            self.check_backpressure(&connection_id).await;
        }
    }

    /// Clone for monitoring task
    fn clone_for_monitoring(&self) -> Self {
        Self {
            config: self.config.clone(),
            memory_usage: self.memory_usage.clone(),
            queue_sizes: self.queue_sizes.clone(),
            backpressure_state: self.backpressure_state.clone(),
            global_backpressure: self.global_backpressure.clone(),
            _backpressure_rx: self.global_backpressure.subscribe(),
        }
    }
}

/// Per-connection flow control manager
#[derive(Debug)]
#[allow(dead_code)]
pub struct ConnectionFlowControl {
    /// Connection ID
    connection_id: String,
    /// Send window
    send_window: FlowWindow,
    /// Receive window
    receive_window: FlowWindow,
    /// Rate limiter
    rate_limiter: RateLimiter,
    /// Outstanding message tracking
    outstanding_messages: Arc<RwLock<HashMap<u64, OutstandingMessage>>>,
    /// Send permits semaphore
    send_permits: Arc<Semaphore>,
    /// Configuration
    config: FlowControlConfig,
}

/// Outstanding message tracking
#[derive(Debug, Clone)]
pub struct OutstandingMessage {
    pub sequence: u64,
    pub size: usize,
    pub sent_at: Instant,
    pub retries: u32,
}

impl ConnectionFlowControl {
    /// Create new connection flow control
    pub fn new(connection_id: String, config: FlowControlConfig) -> Self {
        Self {
            connection_id: connection_id.clone(),
            send_window: FlowWindow::new(config.window_size, config.window_update_threshold),
            receive_window: FlowWindow::new(config.window_size, config.window_update_threshold),
            rate_limiter: RateLimiter::new(config.max_message_rate),
            outstanding_messages: Arc::new(RwLock::new(HashMap::new())),
            send_permits: Arc::new(Semaphore::new(config.max_outstanding_messages)),
            config,
        }
    }

    /// Reserve resources for sending a message
    pub async fn reserve_send(&self, message_size: usize) -> NetworkResult<(u64, tokio::sync::SemaphorePermit<'_>)> {
        // Acquire rate limit token
        self.rate_limiter.acquire(1.0).await?;

        // Acquire send permit
        let permit = self.send_permits.acquire().await
            .map_err(|_| NetworkError::ResourceExhausted("Send permits exhausted".to_string()))?;

        // Reserve window space
        let sequence = self.send_window.reserve(message_size)?;

        // Track outstanding message
        {
            let mut outstanding = self.outstanding_messages.write().await;
            outstanding.insert(sequence, OutstandingMessage {
                sequence,
                size: message_size,
                sent_at: Instant::now(),
                retries: 0,
            });
        }

        Ok((sequence, permit))
    }

    /// Acknowledge received message
    pub async fn acknowledge_message(&self, sequence: u64) -> NetworkResult<()> {
        let mut outstanding = self.outstanding_messages.write().await;

        if let Some(msg) = outstanding.remove(&sequence) {
            // Update send window
            self.send_window.acknowledge(sequence, msg.size);
        }

        Ok(())
    }

    /// Process received message and update receive window
    pub async fn process_received(&self, message_size: usize) -> NetworkResult<bool> {
        // Check if we should send window update
        let should_update = self.receive_window.acknowledge(0, message_size);
        Ok(should_update)
    }

    /// Get send window utilization
    pub fn send_utilization(&self) -> f64 {
        self.send_window.utilization()
    }

    /// Get receive window utilization
    pub fn receive_utilization(&self) -> f64 {
        self.receive_window.utilization()
    }

    /// Check for message timeouts and retransmissions
    pub async fn check_timeouts(&self, timeout: Duration) -> Vec<u64> {
        let now = Instant::now();
        let outstanding = self.outstanding_messages.read().await;

        outstanding
            .values()
            .filter(|msg| now.duration_since(msg.sent_at) > timeout)
            .map(|msg| msg.sequence)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flow_window_creation() {
        let window = FlowWindow::new(1024, 512);
        assert!(window.has_space(100));
        assert_eq!(window.utilization(), 0.0);
    }

    #[test]
    fn test_flow_window_reservation() {
        let window = FlowWindow::new(1024, 512);

        // Reserve space
        let seq1 = window.reserve(200).unwrap();
        assert_eq!(seq1, 0);
        assert!(window.utilization() > 0.0);

        // Reserve more space
        let seq2 = window.reserve(300).unwrap();
        assert_eq!(seq2, 1);

        // Should not have space for large message
        assert!(window.reserve(600).is_err());
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(10.0); // 10 tokens per second

        // Should be able to acquire tokens initially
        assert!(limiter.try_acquire(5.0).await);
        assert!(limiter.try_acquire(5.0).await);

        // Should not have more tokens
        assert!(!limiter.try_acquire(1.0).await);
    }

    #[tokio::test]
    async fn test_backpressure_detector() {
        let config = FlowControlConfig::default();
        let detector = BackpressureDetector::new(config);

        // Initially no backpressure
        assert!(!detector.is_backpressure("test_conn").await);

        // Update queue size to trigger backpressure
        detector.update_queue_size("test_conn", 900).await;
        assert!(detector.is_backpressure("test_conn").await);
    }

    #[tokio::test]
    async fn test_connection_flow_control() {
        let config = FlowControlConfig::default();
        let flow_control = ConnectionFlowControl::new("test".to_string(), config);

        // Reserve send resources
        let (seq, _permit) = flow_control.reserve_send(100).await.unwrap();
        assert_eq!(seq, 0);

        // Acknowledge message
        flow_control.acknowledge_message(seq).await.unwrap();
    }
}
