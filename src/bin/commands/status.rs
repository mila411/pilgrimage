use crate::error::{CliError, CliResult};
use clap::ArgMatches;
use serde::Deserialize;
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

/// Health check response from broker HTTP endpoint
#[derive(Debug, Deserialize)]
struct HealthCheckResponse {
    status: String,
    uptime_seconds: u64,
    node_id: String,
    cluster_id: Option<String>,
    active_connections: u32,
    memory_usage: MemoryUsage,
    performance_metrics: PerformanceMetrics,
    cluster_status: ClusterStatus,
}

/// Memory usage information
#[derive(Debug, Deserialize)]
struct MemoryUsage {
    used_mb: u64,
    total_mb: u64,
    usage_percent: f64,
}

/// Performance metrics
#[derive(Debug, Deserialize)]
struct PerformanceMetrics {
    messages_per_second: f64,
    avg_latency_ms: f64,
    error_rate: f64,
    cpu_usage_percent: f64,
}

/// Cluster status information
#[derive(Debug, Deserialize)]
struct ClusterStatus {
    total_nodes: u32,
    healthy_nodes: u32,
    is_leader: bool,
    replication_lag_ms: Option<u64>,
}

/// Process status information
#[derive(Debug)]
struct ProcessStatus {
    pid: u32,
    cpu_percent: f64,
    memory_mb: u64,
    open_files: u32,
    threads: u32,
    uptime_seconds: u64,
}

/// Comprehensive broker status
#[derive(Debug)]
struct BrokerStatus {
    broker_id: String,
    is_running: bool,
    process_status: Option<ProcessStatus>,
    health_check: Option<HealthCheckResponse>,
    last_check_time: SystemTime,
    response_time_ms: Option<u64>,
}

pub async fn handle_status_command(matches: &ArgMatches) -> CliResult<()> {
    let id = matches.value_of("id").ok_or_else(|| CliError::ParseError {
        field: "broker_id".to_string(),
        message: "Broker ID is not specified".to_string(),
    })?;

    println!("Checking status of broker {}...", id);

    // 1. Verify process existence via PID file
    // 2. Call health check endpoint
    // 3. Display detailed status information

    let pid_file = format!("{}.pid", id);
    let is_running = Path::new(&pid_file).exists();

    if !is_running {
        println!("❌ Broker {} is not running (no PID file found)", id);
        return Ok(());
    }

    // Read the actual process ID from the PID file
    match fs::read_to_string(&pid_file) {
        Ok(pid_str) => {
            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                println!("✅ Broker {} is running (PID: {})", id, pid);

                // Perform comprehensive health check
                let status = check_comprehensive_status(id, pid).await;
                display_detailed_status(&status);
            } else {
                println!("⚠️  Invalid PID file format for broker {}", id);
            }
        }
        Err(e) => {
            println!("❌ Failed to read PID file for broker {}: {}", id, e);
        }
    }

    Ok(())
}

/// Perform comprehensive status check including HTTP health check and process monitoring
async fn check_comprehensive_status(broker_id: &str, pid: u32) -> BrokerStatus {
    let start_time = SystemTime::now();

    // 1. Get process status
    let process_status = get_process_status(pid).await;

    // 2. Perform HTTP health check
    let (health_check, response_time) = perform_health_check(broker_id).await;

    BrokerStatus {
        broker_id: broker_id.to_string(),
        is_running: true,
        process_status,
        health_check,
        last_check_time: start_time,
        response_time_ms: response_time,
    }
}

/// Get detailed process status information
async fn get_process_status(pid: u32) -> Option<ProcessStatus> {
    // On Unix systems, we can read from /proc/{pid}/stat and /proc/{pid}/status
    #[cfg(unix)]
    {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        // Read basic process information from /proc/{pid}/stat
        if let Ok(stat_content) = fs::read_to_string(format!("/proc/{}/stat", pid)) {
            let parts: Vec<&str> = stat_content.split_whitespace().collect();
            if parts.len() >= 24 {
                // Parse key fields from stat file
                let _user_time = parts
                    .get(13)
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let _sys_time = parts
                    .get(14)
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let _threads = parts
                    .get(19)
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);
                let _start_time = parts
                    .get(21)
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let memory_kb = parts
                    .get(23)
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                // Read memory information from /proc/{pid}/status
                let mut memory_mb = memory_kb / 1024;
                let mut open_files = 0;

                if let Ok(file) = File::open(format!("/proc/{}/status", pid)) {
                    let reader = BufReader::new(file);
                    for line in reader.lines() {
                        if let Ok(line) = line {
                            if line.starts_with("VmRSS:") {
                                if let Some(kb_str) = line.split_whitespace().nth(1) {
                                    if let Ok(kb) = kb_str.parse::<u64>() {
                                        memory_mb = kb / 1024;
                                    }
                                }
                            }
                        }
                    }
                }

                // Count open file descriptors
                if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", pid)) {
                    open_files = entries.count() as u32;
                }

                // Calculate uptime (simplified)
                let uptime_seconds = get_system_uptime().saturating_sub(_start_time / 100);

                return Some(ProcessStatus {
                    pid,
                    cpu_percent: calculate_cpu_percentage(_user_time, _sys_time),
                    memory_mb,
                    open_files,
                    threads: _threads,
                    uptime_seconds,
                });
            }
        }
    }

    // Fallback for non-Unix systems or if /proc is not available
    Some(ProcessStatus {
        pid,
        cpu_percent: 0.0,
        memory_mb: 0,
        open_files: 0,
        threads: 1,
        uptime_seconds: 0,
    })
}

/// Perform HTTP health check on broker
async fn perform_health_check(broker_id: &str) -> (Option<HealthCheckResponse>, Option<u64>) {
    let health_check_url = format!("http://localhost:8080/health/{}", broker_id);
    let start_time = SystemTime::now();

    // Create HTTP client with timeout
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(_) => return (None, None),
    };

    // Perform the health check request
    match timeout(Duration::from_secs(5), client.get(&health_check_url).send()).await {
        Ok(Ok(response)) => {
            let response_time = start_time
                .elapsed()
                .ok()
                .and_then(|d| u64::try_from(d.as_millis()).ok());

            if response.status().is_success() {
                // Try to parse the JSON response
                match response.json::<HealthCheckResponse>().await {
                    Ok(health_data) => (Some(health_data), response_time),
                    Err(_) => {
                        // If JSON parsing fails, create a basic response
                        let basic_response = HealthCheckResponse {
                            status: "healthy".to_string(),
                            uptime_seconds: 0,
                            node_id: broker_id.to_string(),
                            cluster_id: None,
                            active_connections: 0,
                            memory_usage: MemoryUsage {
                                used_mb: 0,
                                total_mb: 0,
                                usage_percent: 0.0,
                            },
                            performance_metrics: PerformanceMetrics {
                                messages_per_second: 0.0,
                                avg_latency_ms: 0.0,
                                error_rate: 0.0,
                                cpu_usage_percent: 0.0,
                            },
                            cluster_status: ClusterStatus {
                                total_nodes: 1,
                                healthy_nodes: 1,
                                is_leader: false,
                                replication_lag_ms: None,
                            },
                        };
                        (Some(basic_response), response_time)
                    }
                }
            } else {
                (None, response_time)
            }
        }
        _ => (None, None), // Timeout or connection error
    }
}

/// Display comprehensive status information
fn display_detailed_status(status: &BrokerStatus) {
    println!("📊 Detailed Status for Broker '{}':", status.broker_id);
    println!();

    // Basic information
    println!("🔹 Basic Information:");
    println!("   • Broker ID: {}", status.broker_id);
    println!(
        "   • Status: {}",
        if status.is_running {
            "✅ Running"
        } else {
            "❌ Stopped"
        }
    );
    println!("   • Last Check: {:?}", status.last_check_time);

    // Process status
    if let Some(ref process) = status.process_status {
        println!();
        println!("🔹 Process Information:");
        println!("   • Process ID: {}", process.pid);
        println!("   • Memory Usage: {} MB", process.memory_mb);
        println!("   • CPU Usage: {:.1}%", process.cpu_percent);
        println!("   • Open Files: {}", process.open_files);
        println!("   • Thread Count: {}", process.threads);
        println!("   • Uptime: {} seconds", process.uptime_seconds);
    }

    // HTTP health check results
    if let Some(ref health) = status.health_check {
        println!();
        println!("🔹 Health Check Results:");
        if let Some(response_time) = status.response_time_ms {
            println!("   • Response Time: {} ms", response_time);
        }
        println!("   • Health Status: {}", health.status);
        println!("   • Node ID: {}", health.node_id);
        if let Some(ref cluster_id) = health.cluster_id {
            println!("   • Cluster ID: {}", cluster_id);
        }
        println!("   • Active Connections: {}", health.active_connections);
        println!("   • Uptime: {} seconds", health.uptime_seconds);

        println!();
        println!("🔹 Performance Metrics:");
        println!(
            "   • Messages/sec: {:.2}",
            health.performance_metrics.messages_per_second
        );
        println!(
            "   • Avg Latency: {:.2} ms",
            health.performance_metrics.avg_latency_ms
        );
        println!(
            "   • Error Rate: {:.3}%",
            health.performance_metrics.error_rate * 100.0
        );
        println!(
            "   • CPU Usage: {:.1}%",
            health.performance_metrics.cpu_usage_percent
        );

        println!();
        println!("🔹 Memory Usage:");
        println!("   • Used: {} MB", health.memory_usage.used_mb);
        println!("   • Total: {} MB", health.memory_usage.total_mb);
        println!("   • Usage: {:.1}%", health.memory_usage.usage_percent);

        println!();
        println!("🔹 Cluster Status:");
        println!("   • Total Nodes: {}", health.cluster_status.total_nodes);
        println!(
            "   • Healthy Nodes: {}",
            health.cluster_status.healthy_nodes
        );
        println!(
            "   • Is Leader: {}",
            if health.cluster_status.is_leader {
                "Yes"
            } else {
                "No"
            }
        );
        if let Some(lag) = health.cluster_status.replication_lag_ms {
            println!("   • Replication Lag: {} ms", lag);
        }
    } else {
        println!();
        println!("🔹 Health Check Results:");
        println!("   • ⚠️  HTTP health check failed or unavailable");
        println!("   • The broker process is running but may not be responding to HTTP requests");
        println!("   • This could indicate:");
        println!("      - Health check endpoint is not implemented");
        println!("      - Broker is starting up");
        println!("      - Network connectivity issues");
        println!("      - Broker is overloaded");
    }
}

/// Calculate CPU percentage from user and system time
fn calculate_cpu_percentage(user_time: u64, sys_time: u64) -> f64 {
    // This is a simplified calculation
    // In a real implementation, you would track these values over time
    let total_time = user_time + sys_time;
    (total_time as f64 / 100.0).min(100.0)
}

/// Get system uptime in seconds
fn get_system_uptime() -> u64 {
    #[cfg(unix)]
    {
        if let Ok(uptime_str) = fs::read_to_string("/proc/uptime") {
            if let Some(uptime_seconds_str) = uptime_str.split_whitespace().next() {
                if let Ok(uptime_seconds) = uptime_seconds_str.parse::<f64>() {
                    return uptime_seconds as u64;
                }
            }
        }
    }

    // Fallback: use current time since UNIX epoch
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
