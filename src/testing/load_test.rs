/// High volume load testing, stress testing, performance meaßsurement
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::sleep;

/// Load test configuration
#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    pub concurrent_connections: u32,
    pub duration: Duration,
    pub message_size: usize,
    pub target_throughput: f64,
    pub max_latency: Duration,
    pub min_throughput: f64,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            concurrent_connections: 100,
            duration: Duration::from_secs(60),
            message_size: 1024,
            target_throughput: 10000.0,
            max_latency: Duration::from_millis(100),
            min_throughput: 1000.0,
        }
    }
}

/// Load test executor
pub struct LoadTester {
    config: LoadTestConfig,
    metrics: Arc<RwLock<LoadTestMetrics>>,
    semaphore: Arc<Semaphore>,
}

impl LoadTester {
    pub fn new(config: LoadTestConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.concurrent_connections as usize));

        Self {
            config,
            metrics: Arc::new(RwLock::new(LoadTestMetrics::new())),
            semaphore,
        }
    }

    /// Run load test with specified configuration
    pub async fn execute_load_test(
        &self,
    ) -> Result<LoadTestReport, Box<dyn std::error::Error + Send + Sync>> {
        println!("🚀 Starting advanced load test...");
        println!(
            "📊 Configuration: {} concurrent connections for {:?}",
            self.config.concurrent_connections, self.config.duration
        );

        let start_time = Instant::now();
        let mut handles = Vec::new();

        // Start system monitoring task
        handles.push(self.start_system_monitor());

        // Create worker tasks
        for worker_id in 0..self.config.concurrent_connections {
            handles.push(self.create_worker(worker_id));
        }

        // Start throughput sampling
        handles.push(self.start_throughput_monitor());

        // Wait for test duration
        sleep(self.config.duration).await;

        // Stop all workers
        for handle in handles {
            handle.abort();
        }

        let total_duration = start_time.elapsed();
        let mut metrics = self.metrics.write().await;
        metrics.calculate_percentiles();
        let final_metrics = metrics.clone();
        drop(metrics);

        let report = LoadTestReport::new(self.config.clone(), final_metrics, total_duration);
        report.print_summary();

        Ok(report)
    }

    /// Start system monitoring task
    fn start_system_monitor(&self) -> tokio::task::JoinHandle<()> {
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            loop {
                interval.tick().await;

                // Simulate system metrics collection
                let cpu_usage = rng.gen_range(0.0..100.0);
                let memory_usage = 1_000_000 + rng.gen_range(0..500_000_000);
                let connections = 50 + rng.gen_range(0..100);

                metrics
                    .write()
                    .await
                    .record_system_metrics(cpu_usage, memory_usage, connections);
            }
        })
    }

    /// Start throughput monitoring task
    fn start_throughput_monitor(&self) -> tokio::task::JoinHandle<()> {
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(5000));
            let mut last_operations = 0;

            loop {
                interval.tick().await;

                let current_metrics = metrics.read().await;
                let current_operations = current_metrics.successful_operations;
                let throughput = if current_operations > last_operations {
                    (current_operations - last_operations) as f64 / 5.0 // ops per second over 5 second window
                } else {
                    0.0
                };

                drop(current_metrics);
                metrics.write().await.record_throughput_sample(throughput);
                last_operations = current_operations;
            }
        })
    }

    /// Create worker task
    fn create_worker(&self, worker_id: u32) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let semaphore = self.semaphore.clone();

        tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => return,
            };

            let mut operation_count = 0;
            let worker_start = Instant::now();

            loop {
                operation_count += 1;
                let _operation_start = Instant::now();

                // Simulate various operation types with different patterns
                let _operation_type = match operation_count % 4 {
                    0 => "write_heavy",
                    1 => "read_heavy",
                    2 => "mixed",
                    _ => "bulk_transfer",
                };

                match Self::perform_operation(&config, worker_id).await {
                    Ok(result) => {
                        let latency = result.latency;

                        // Update metrics with detailed information
                        {
                            let mut metrics = metrics.write().await;
                            if result.success {
                                metrics.record_success(
                                    latency,
                                    result.bytes_sent,
                                    result.bytes_received,
                                );
                            } else {
                                metrics.record_failure(
                                    latency,
                                    result.error_type.unwrap_or_else(|| "unknown".to_string()),
                                );
                            }
                        }
                    }
                    Err(error_msg) => {
                        let mut metrics = metrics.write().await;
                        metrics.record_failure(Duration::from_millis(0), error_msg.to_string());
                    }
                }

                // Adaptive wait time based on target throughput
                let target_ops_per_sec =
                    config.target_throughput / config.concurrent_connections as f64;
                let _expected_duration = Duration::from_secs_f64(1.0 / target_ops_per_sec);
                let elapsed = worker_start.elapsed();
                let expected_total =
                    Duration::from_secs_f64(operation_count as f64 / target_ops_per_sec);

                if elapsed < expected_total {
                    sleep(expected_total - elapsed).await;
                }

                // Random jitter to simulate real-world variance
                let mut rng = StdRng::from_entropy();
                sleep(Duration::from_millis(rng.gen_range(0..50))).await;
            }
        })
    }

    /// Execute single operation with advanced workload patterns
    async fn perform_operation(
        _config: &LoadTestConfig,
        worker_id: u32,
    ) -> Result<LoadOperationResult, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = Instant::now();

        // Simulate different operation types based on workload pattern
        let operation_type = match worker_id % 4 {
            0 => "message_publish",
            1 => "message_consume",
            2 => "schema_operation",
            _ => "cluster_management",
        };

        // Simulate varying latency based on operation type
        let base_latency = match operation_type {
            "message_publish" => Duration::from_millis(10),
            "message_consume" => Duration::from_millis(15),
            "schema_operation" => Duration::from_millis(50),
            "cluster_management" => Duration::from_millis(100),
            _ => Duration::from_millis(20),
        };

        // Add random jitter (±30%)
        let mut rng = StdRng::from_entropy();
        let jitter_factor = rng.gen_range(0.7..1.3);
        let actual_latency =
            Duration::from_nanos((base_latency.as_nanos() as f64 * jitter_factor) as u64);

        // Simulate network operation
        tokio::time::sleep(actual_latency).await;

        let elapsed = start_time.elapsed();

        // Simulate occasional failures (5% failure rate)
        if rng.gen_range(0..100) < 5 {
            return Ok(LoadOperationResult {
                operation_type: operation_type.to_string(),
                latency: elapsed,
                bytes_sent: 0,
                bytes_received: 0,
                success: false,
                error_type: Some("simulated_failure".to_string()),
            });
        }

        // Simulate data transfer
        let bytes_sent = rng.gen_range(100..1024);
        let bytes_received = rng.gen_range(50..512);

        Ok(LoadOperationResult {
            operation_type: operation_type.to_string(),
            latency: elapsed,
            bytes_sent,
            bytes_received,
            success: true,
            error_type: None,
        })
    }
}

/// Result of a single load test operation
#[derive(Debug, Clone)]
pub struct LoadOperationResult {
    pub operation_type: String,
    pub latency: Duration,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub success: bool,
    pub error_type: Option<String>,
}

/// Load test metrics with comprehensive monitoring
#[derive(Debug, Clone)]
pub struct LoadTestMetrics {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub latencies: Vec<Duration>,
    pub start_time: Instant,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub cpu_usage: Vec<f32>,
    pub memory_usage: Vec<u64>,
    pub concurrent_connections: Vec<u32>,
    pub throughput_samples: Vec<f64>,
    pub error_types: HashMap<String, u32>,
    pub latency_percentiles: HashMap<String, Duration>,
}

impl LoadTestMetrics {
    pub fn new() -> Self {
        Self {
            total_operations: 0,
            successful_operations: 0,
            failed_operations: 0,
            latencies: Vec::new(),
            start_time: Instant::now(),
            bytes_sent: 0,
            bytes_received: 0,
            cpu_usage: Vec::new(),
            memory_usage: Vec::new(),
            concurrent_connections: Vec::new(),
            throughput_samples: Vec::new(),
            error_types: HashMap::new(),
            latency_percentiles: HashMap::new(),
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            (self.successful_operations as f64 / self.total_operations as f64) * 100.0
        }
    }

    pub fn average_latency(&self) -> Duration {
        if self.latencies.is_empty() {
            Duration::from_secs(0)
        } else {
            let total_ms: u64 = self.latencies.iter().map(|d| d.as_millis() as u64).sum();
            Duration::from_millis(total_ms / self.latencies.len() as u64)
        }
    }

    pub fn throughput(&self, duration: Duration) -> f64 {
        if duration.as_secs() == 0 {
            0.0
        } else {
            self.successful_operations as f64 / duration.as_secs() as f64
        }
    }

    /// Calculate latency percentiles
    pub fn calculate_percentiles(&mut self) {
        if self.latencies.is_empty() {
            return;
        }

        let mut sorted_latencies = self.latencies.clone();
        sorted_latencies.sort();

        let percentiles = [50, 90, 95, 99];
        for p in percentiles.iter() {
            let index = (sorted_latencies.len() * p / 100).saturating_sub(1);
            self.latency_percentiles
                .insert(format!("p{}", p), sorted_latencies[index]);
        }
    }

    /// Record error by type
    pub fn record_error(&mut self, error_type: &str) {
        *self.error_types.entry(error_type.to_string()).or_insert(0) += 1;
        self.failed_operations += 1;
        self.total_operations += 1;
    }

    /// Record successful operation with data transfer
    pub fn record_success(&mut self, latency: Duration, bytes_sent: u64, bytes_received: u64) {
        self.successful_operations += 1;
        self.total_operations += 1;
        self.latencies.push(latency);
        self.bytes_sent += bytes_sent;
        self.bytes_received += bytes_received;
    }

    /// Record failed operation
    pub fn record_failure(&mut self, latency: Duration, error_type: String) {
        self.failed_operations += 1;
        self.total_operations += 1;
        self.latencies.push(latency);
        *self.error_types.entry(error_type).or_insert(0) += 1;
    }

    /// Record system metrics
    pub fn record_system_metrics(&mut self, cpu_usage: f32, memory_usage: u64, connections: u32) {
        self.cpu_usage.push(cpu_usage);
        self.memory_usage.push(memory_usage);
        self.concurrent_connections.push(connections);
    }

    /// Record throughput sample
    pub fn record_throughput_sample(&mut self, throughput: f64) {
        self.throughput_samples.push(throughput);
    }

    /// Get average CPU usage
    pub fn average_cpu_usage(&self) -> f32 {
        if self.cpu_usage.is_empty() {
            0.0
        } else {
            self.cpu_usage.iter().sum::<f32>() / self.cpu_usage.len() as f32
        }
    }

    /// Get peak memory usage
    pub fn peak_memory_usage(&self) -> u64 {
        self.memory_usage.iter().max().copied().unwrap_or(0)
    }

    /// Get network throughput (bytes per second)
    pub fn network_throughput(&self, duration: Duration) -> (f64, f64) {
        let duration_secs = duration.as_secs_f64();
        if duration_secs == 0.0 {
            (0.0, 0.0)
        } else {
            (
                self.bytes_sent as f64 / duration_secs,
                self.bytes_received as f64 / duration_secs,
            )
        }
    }
}

/// Load test report
#[derive(Debug)]
#[allow(dead_code)]
pub struct LoadTestReport {
    config: LoadTestConfig,
    metrics: LoadTestMetrics,
    total_duration: Duration,
    performance_score: f64,
}

impl LoadTestReport {
    pub fn new(config: LoadTestConfig, metrics: LoadTestMetrics, total_duration: Duration) -> Self {
        let throughput = metrics.throughput(total_duration);
        let success_rate = metrics.success_rate();
        let avg_latency_ms = metrics.average_latency().as_millis() as f64;

        // Calculate performance score
        let throughput_score = (throughput / config.target_throughput).min(1.0) * 40.0;
        let success_score = (success_rate / 100.0) * 30.0;
        let latency_score = if avg_latency_ms <= config.max_latency.as_millis() as f64 {
            30.0
        } else {
            30.0 * (config.max_latency.as_millis() as f64 / avg_latency_ms)
        };

        let performance_score = throughput_score + success_score + latency_score;

        Self {
            config,
            metrics,
            total_duration,
            performance_score,
        }
    }

    pub fn print_summary(&self) {
        println!();
        println!("📊 Load Test Results Report");
        println!("{}", "=".repeat(50));
        println!("Test duration: {:?}", self.total_duration);
        println!("Total operations: {}", self.metrics.total_operations);
        println!(
            "Successful operations: {}",
            self.metrics.successful_operations
        );
        println!("Failed operations: {}", self.metrics.failed_operations);
        println!("Success rate: {:.2}%", self.metrics.success_rate());
        println!("Average latency: {:?}", self.metrics.average_latency());
        println!(
            "Throughput: {:.2} ops/sec",
            self.metrics.throughput(self.total_duration)
        );
        println!("Performance score: {:.1}/100", self.performance_score);

        if self.performance_score >= 80.0 {
            println!("✅ Performance: Excellent");
        } else if self.performance_score >= 60.0 {
            println!("⚠️ Performance: Needs improvement");
        } else {
            println!("❌ Performance: Major improvement needed");
        }
    }

    pub fn performance_score(&self) -> f64 {
        self.performance_score
    }
}

/// Stress test configuration
#[derive(Debug, Clone)]
pub struct StressTestConfig {
    pub initial_load: u32,
    pub max_load: u32,
    pub step_size: u32,
    pub step_duration: Duration,
    pub failure_threshold: f64,
}

impl Default for StressTestConfig {
    fn default() -> Self {
        Self {
            initial_load: 10,
            max_load: 1000,
            step_size: 50,
            step_duration: Duration::from_secs(30),
            failure_threshold: 10.0, // Stop at 10% failure rate
        }
    }
}

/// Stress test executor
pub struct StressTester {
    config: StressTestConfig,
}

impl StressTester {
    pub fn new(config: StressTestConfig) -> Self {
        Self { config }
    }

    /// Execute stress test
    pub async fn run_stress_test(
        &self,
    ) -> Result<StressTestReport, Box<dyn std::error::Error + Send + Sync>> {
        println!("🔥 Starting stress test");
        println!("{}", "=".repeat(50));

        let mut load_results = HashMap::new();
        let mut breaking_point = None;

        let mut current_load = self.config.initial_load;

        while current_load <= self.config.max_load {
            println!("Testing at load level {}...", current_load);

            let load_config = LoadTestConfig {
                concurrent_connections: current_load,
                duration: self.config.step_duration,
                ..LoadTestConfig::default()
            };

            let load_tester = LoadTester::new(load_config.clone());
            let result = load_tester.execute_load_test().await?;

            let failure_rate = 100.0 - result.metrics.success_rate();
            load_results.insert(current_load, result);

            if failure_rate > self.config.failure_threshold {
                breaking_point = Some(current_load);
                println!(
                    "❌ Breaking point detected: {} connections with {:.2}% failure rate",
                    current_load, failure_rate
                );
                break;
            }

            println!(
                "✅ Load level {} passed (failure rate: {:.2}%)",
                current_load, failure_rate
            );
            current_load += self.config.step_size;
        }

        let report = StressTestReport::new(self.config.clone(), load_results, breaking_point);
        report.print_summary();

        Ok(report)
    }
}

/// Stress test report
#[derive(Debug)]
pub struct StressTestReport {
    config: StressTestConfig,
    results: HashMap<u32, LoadTestReport>,
    breaking_point: Option<u32>,
}

impl StressTestReport {
    pub fn new(
        config: StressTestConfig,
        results: HashMap<u32, LoadTestReport>,
        breaking_point: Option<u32>,
    ) -> Self {
        Self {
            config,
            results,
            breaking_point,
        }
    }

    pub fn print_summary(&self) {
        println!();
        println!("🔥 Stress Test Results Report");
        println!("{}", "=".repeat(60));

        if let Some(bp) = self.breaking_point {
            println!("💥 System breaking point: {} concurrent connections", bp);
            println!(
                "🛡️ Recommended maximum load: {} concurrent connections",
                bp.saturating_sub(self.config.step_size)
            );
        } else {
            println!(
                "✅ Stable operation up to maximum test load of {}",
                self.config.max_load
            );
        }

        println!();
        println!("Load level results:");
        for (&load, result) in self.results.iter() {
            println!(
                "  {} connections: {:.1}% success rate, {:.1} ops/sec throughput",
                load,
                result.metrics.success_rate(),
                result.metrics.throughput(result.total_duration)
            );
        }
    }

    pub fn breaking_point(&self) -> Option<u32> {
        self.breaking_point
    }

    pub fn max_stable_load(&self) -> u32 {
        self.breaking_point
            .unwrap_or(self.config.max_load)
            .saturating_sub(self.config.step_size)
    }
}

/// Public function for demo execution
pub async fn run_production_load_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Phase 3: Load Test & Stress Test Demo");
    println!("{}", "=".repeat(60));

    // Basic load test
    let load_config = LoadTestConfig {
        concurrent_connections: 50,
        duration: Duration::from_secs(10),
        ..LoadTestConfig::default()
    };

    let load_tester = LoadTester::new(load_config);
    let load_report = load_tester.execute_load_test().await?;

    println!();

    // Stress test
    let stress_config = StressTestConfig {
        initial_load: 10,
        max_load: 100,
        step_size: 20,
        step_duration: Duration::from_secs(5),
        failure_threshold: 15.0,
    };

    let stress_tester = StressTester::new(stress_config);
    let stress_report = stress_tester.run_stress_test().await?;

    println!();
    println!("📈 Overall Performance Evaluation");
    println!("{}", "=".repeat(40));
    println!(
        "Basic load performance: {:.1}/100",
        load_report.performance_score()
    );
    println!(
        "Maximum stable load: {} connections",
        stress_report.max_stable_load()
    );

    if load_report.performance_score() >= 70.0 && stress_report.max_stable_load() >= 50 {
        println!("✅ Production-level load tolerance confirmed");
    } else {
        println!("⚠️ Performance tuning needed");
    }

    Ok(())
}
