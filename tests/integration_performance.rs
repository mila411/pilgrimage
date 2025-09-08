/// Performance and metrics functionality integration tests
///
/// This test module tests the following functions:
/// - System metrics
/// - Distributed metrics collection
/// - Prometheus metrics integration
/// - Cluster health monitoring
/// - Network metrics
use pilgrimage::broker::metrics::{DistributedMetrics, SystemMetrics};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_system_metrics_creation() {
    // System metrics creation test
    let cpu_usage = 45.5;
    let memory_usage = 78.2;
    let network_io = 23.1;

    let metrics = SystemMetrics::new(cpu_usage, memory_usage, network_io);

    // Verify created values
    assert_eq!(
        metrics.cpu_usage, cpu_usage,
        "CPU usage is not set correctly"
    );
    assert_eq!(
        metrics.memory_usage, memory_usage,
        "Memory usage is not set correctly"
    );
    assert_eq!(
        metrics.network_io, network_io,
        "Network I/O is not set correctly"
    );
}

#[tokio::test]
async fn test_system_metrics_overload_detection() {
    // Overload detection test
    let threshold = 80.0;

    // Overloaded metrics
    let overloaded_metrics = SystemMetrics::new(85.0, 90.0, 50.0);
    assert!(
        overloaded_metrics.is_overloaded(threshold),
        "Overload condition was not detected"
    );

    // Normal metrics
    let normal_metrics = SystemMetrics::new(45.0, 60.0, 30.0);
    assert!(
        !normal_metrics.is_overloaded(threshold),
        "Overload falsely detected in normal condition"
    );

    // CPU overload only
    let cpu_overload = SystemMetrics::new(85.0, 50.0, 30.0);
    assert!(
        cpu_overload.is_overloaded(threshold),
        "CPU overload was not detected"
    );

    // Memory overload only
    let memory_overload = SystemMetrics::new(50.0, 85.0, 30.0);
    assert!(
        memory_overload.is_overloaded(threshold),
        "Memory overload was not detected"
    );
}

#[tokio::test]
async fn test_system_metrics_underutilized_detection() {
    // Low utilization detection test
    let threshold = 20.0;

    // Low utilization metrics
    let underutilized_metrics = SystemMetrics::new(15.0, 10.0, 5.0);
    assert!(
        underutilized_metrics.is_underutilized(threshold),
        "Low utilization condition was not detected"
    );

    // Normal utilization metrics
    let normal_metrics = SystemMetrics::new(45.0, 60.0, 30.0);
    assert!(
        !normal_metrics.is_underutilized(threshold),
        "Low utilization falsely detected in normal utilization"
    );

    // Low CPU usage, high memory usage (not underutilized)
    let mixed_metrics = SystemMetrics::new(15.0, 50.0, 30.0);
    assert!(
        !mixed_metrics.is_underutilized(threshold),
        "Low utilization falsely detected in mixed condition"
    );
}

#[tokio::test]
async fn test_distributed_metrics_creation() {
    // Distributed metrics creation test
    let metrics_result = DistributedMetrics::new();
    assert!(
        metrics_result.is_ok(),
        "Failed to create distributed metrics: {:?}",
        metrics_result.err()
    );

    let metrics = metrics_result.unwrap();

    // Verify registry
    let registry = metrics.registry();
    assert!(
        registry.gather().len() > 0,
        "Metrics are not registered in registry"
    );
}

#[tokio::test]
async fn test_raft_state_metrics() {
    // Raft state metrics test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    // Follower state
    metrics.set_raft_state(0);
    // Candidate state
    metrics.set_raft_state(1);
    // Leader state
    metrics.set_raft_state(2);

    // Export metrics
    let exported = metrics.export_metrics();
    assert!(
        exported.contains("raft_state"),
        "Raft state metrics are not exported"
    );
    assert!(!exported.is_empty(), "Exported metrics are empty");
}

#[tokio::test]
async fn test_replication_metrics() {
    // Replication metrics test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    // Successful replication
    let latency_seconds = 0.025; // 25ms
    metrics.record_replication_success(latency_seconds);
    metrics.record_replication_success(0.030);
    metrics.record_replication_success(0.020);

    // Failed replication
    metrics.record_replication_failure();
    metrics.record_replication_failure();

    // Verify metrics
    let exported = metrics.export_metrics();
    assert!(
        exported.contains("replication_success_total"),
        "Replication success metrics not found"
    );
    assert!(
        exported.contains("replication_failures_total"),
        "Replication failure metrics not found"
    );
    assert!(
        exported.contains("replication_latency_seconds"),
        "Replication latency metrics not found"
    );
}

#[tokio::test]
async fn test_cluster_health_metrics() {
    // Cluster health metrics test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    // Healthy cluster
    metrics.update_cluster_health(5, true, 0, 0);

    // Problematic cluster
    metrics.update_cluster_health(5, false, 2, 1);

    // Reduced cluster
    metrics.update_cluster_health(3, true, 0, 0);

    // Verify metrics
    let exported = metrics.export_metrics();
    assert!(
        exported.contains("cluster_size"),
        "Cluster size metrics not found"
    );
    assert!(
        exported.contains("cluster_quorum_status"),
        "Quorum status metrics not found"
    );
    assert!(
        exported.contains("cluster_partition_suspects"),
        "Partition suspect metrics not found"
    );
    assert!(
        exported.contains("cluster_isolated_nodes"),
        "Isolated node metrics not found"
    );
}

#[tokio::test]
async fn test_network_activity_metrics() {
    // Network activity metrics test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    // Normal communication
    metrics.record_network_activity(1024, 2048, false);
    metrics.record_network_activity(512, 1024, false);

    // TLS communication
    metrics.record_network_activity(2048, 4096, true);
    metrics.record_network_activity(1536, 3072, true);

    // Verify metrics
    let exported = metrics.export_metrics();
    assert!(
        exported.contains("network_bytes_sent_total"),
        "Sent bytes metrics not found"
    );
    assert!(
        exported.contains("network_bytes_received_total"),
        "Received bytes metrics not found"
    );
    assert!(
        exported.contains("tls_connections_total"),
        "TLS connection metrics not found"
    );
}

#[tokio::test]
async fn test_message_processing_metrics() {
    // Message processing metrics test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    // Record various processing times
    let processing_times = vec![0.001, 0.005, 0.010, 0.002, 0.015, 0.003, 0.008];

    for latency in processing_times {
        metrics.record_message_processing(latency);
    }

    // Verify metrics
    let exported = metrics.export_metrics();
    assert!(
        exported.contains("message_processing_latency_seconds"),
        "Message processing latency metrics not found"
    );
}

#[tokio::test]
async fn test_comprehensive_metrics_scenario() {
    // Comprehensive metrics scenario test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    // Scenario 1: Leader election
    metrics.set_raft_state(1); // Candidate
    metrics.election_count.inc();
    metrics.vote_requests_sent.inc_by(4.0); // Vote requests to 4 nodes

    // Scenario 2: Become leader and send heartbeats
    metrics.set_raft_state(2); // Leader
    for _ in 0..10 {
        metrics.heartbeat_sent.inc();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Scenario 3: Execute replication
    for i in 0..20 {
        if i % 5 == 0 {
            // 20% of replications fail
            metrics.record_replication_failure();
        } else {
            // 80% of replications succeed
            let latency = 0.010 + (i as f64 * 0.001); // Gradually increase latency
            metrics.record_replication_success(latency);
        }
    }

    // Scenario 4: Network activity
    for _ in 0..50 {
        metrics.record_network_activity(1024, 2048, false);
        if rand::random::<bool>() {
            metrics.network_errors.inc();
        }
    }

    // Scenario 5: Cluster health changes
    metrics.update_cluster_health(5, true, 0, 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    metrics.update_cluster_health(5, false, 1, 0); // 1 node suspected
    tokio::time::sleep(Duration::from_millis(50)).await;
    metrics.update_cluster_health(4, true, 0, 1); // 1 node isolated

    // Verify final metrics
    let exported = metrics.export_metrics();

    // Verify presence of main metrics
    let expected_metrics = vec![
        "raft_state",
        "raft_elections_total",
        "raft_heartbeats_sent_total",
        "replication_success_total",
        "replication_failures_total",
        "network_bytes_sent_total",
        "cluster_size",
        "cluster_quorum_status",
    ];

    for expected_metric in expected_metrics {
        assert!(
            exported.contains(expected_metric),
            "Expected metric '{}' not found",
            expected_metric
        );
    }

    // Verify exported data is not empty
    assert!(!exported.is_empty(), "Exported metrics are empty");

    // Verify there are at least several lines of metrics data
    let lines: Vec<&str> = exported.lines().collect();
    assert!(lines.len() > 10, "Exported metrics data is too little");
}

#[tokio::test]
async fn test_metrics_performance() {
    // Metrics collection performance test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    let operation_count = 1000;
    let start_time = Instant::now();

    // Execute a large number of metrics operations
    for i in 0..operation_count {
        metrics.record_message_processing(0.001 + (i as f64 / 1000000.0));

        if i % 10 == 0 {
            metrics.record_replication_success(0.005);
        }

        if i % 100 == 0 {
            metrics.record_network_activity(1024, 2048, false);
        }
    }

    let total_time = start_time.elapsed();
    let operations_per_second = operation_count as f64 / total_time.as_secs_f64();

    println!("Metrics collection performance:");
    println!("  Total operations: {}", operation_count);
    println!("  Total time: {:?}", total_time);
    println!("  Operations/sec: {:.2}", operations_per_second);

    // Performance requirement (1000+ operations per second)
    assert!(
        operations_per_second > 1000.0,
        "Metrics collection performance does not meet requirements: {:.2} ops/sec",
        operations_per_second
    );

    // Verify overall execution time is reasonable
    assert!(
        total_time < Duration::from_secs(2),
        "Metrics collection takes too much time: {:?}",
        total_time
    );
}

#[tokio::test]
async fn test_concurrent_metrics_collection() {
    // Concurrent metrics collection test
    let metrics = Arc::new(DistributedMetrics::new().expect("Failed to create metrics"));
    let task_count = 10;
    let operations_per_task = 100;

    let mut handles = vec![];
    let start_time = Instant::now();

    // Collect metrics concurrently with multiple tasks
    for task_id in 0..task_count {
        let metrics_clone = Arc::clone(&metrics);

        let handle = tokio::spawn(async move {
            for i in 0..operations_per_task {
                // Each task collects different types of metrics
                match task_id % 4 {
                    0 => metrics_clone.record_message_processing(0.001 * i as f64),
                    1 => metrics_clone.record_replication_success(0.005 * i as f64),
                    2 => metrics_clone.record_network_activity(
                        1024 * i as u64,
                        2048 * i as u64,
                        false,
                    ),
                    3 => {
                        if i % 2 == 0 {
                            metrics_clone.heartbeat_sent.inc();
                        } else {
                            metrics_clone.heartbeat_received.inc();
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle
            .await
            .expect("Failed to execute concurrent metrics collection task");
    }

    let total_time = start_time.elapsed();
    let total_operations = task_count * operations_per_task;
    let operations_per_second = total_operations as f64 / total_time.as_secs_f64();

    println!("Concurrent metrics collection results:");
    println!("  Task count: {}", task_count);
    println!("  Operations per task: {}", operations_per_task);
    println!("  Total operations: {}", total_operations);
    println!("  Total time: {:?}", total_time);
    println!("  Operations/sec: {:.2}", operations_per_second);

    // Performance requirement for concurrent processing
    assert!(
        operations_per_second > 500.0,
        "Concurrent metrics collection performance does not meet requirements: {:.2} ops/sec",
        operations_per_second
    );

    // Verify metrics consistency
    let exported = metrics.export_metrics();
    assert!(
        !exported.is_empty(),
        "Metrics are empty after concurrent processing"
    );
}

#[tokio::test]
async fn test_edge_case_metrics() {
    // Edge case metrics test
    let metrics = DistributedMetrics::new().expect("Failed to create metrics");

    // Extremely high values
    metrics.record_message_processing(10.0); // 10 seconds
    metrics.record_replication_success(5.0); // 5 seconds
    metrics.record_network_activity(u64::MAX / 2, u64::MAX / 2, true);

    // Zero values
    metrics.record_message_processing(0.0);
    metrics.record_replication_success(0.0);
    metrics.record_network_activity(0, 0, false);

    // Extremely small values
    metrics.record_message_processing(0.000001); // 1 microsecond
    metrics.record_replication_success(0.000001);
    metrics.record_network_activity(1, 1, true);

    // Cluster size boundary values
    metrics.update_cluster_health(1, false, 0, 0); // Minimum cluster
    metrics.update_cluster_health(1000, true, 0, 0); // Large cluster

    // Verify metrics
    let exported = metrics.export_metrics();
    assert!(!exported.is_empty(), "Metrics are empty after edge cases");

    // Verify no panics or exceptions occurred (successful at this point)
    println!(
        "Edge case metrics test completed: {} lines of metrics",
        exported.lines().count()
    );
}
