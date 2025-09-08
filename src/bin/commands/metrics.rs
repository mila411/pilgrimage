//! Metrics and monitoring commands
//!
//! This module provides comprehensive metrics and monitoring functionality
//! including real-time metrics, historical data, and performance monitoring.

use clap::ArgMatches;
use serde_json::{Value, json};
use std::error::Error;

/// Handle metrics display command
pub async fn handle_metrics_show_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let component = matches.get_one::<String>("component");
    let default_format = "table".to_string();
    let format = matches
        .get_one::<String>("format")
        .unwrap_or(&default_format);
    let interval = matches.get_one::<String>("interval");

    if let Some(interval_str) = interval {
        println!(
            "📊 Starting real-time metrics monitoring (interval: {})...",
            interval_str
        );
        println!("Press Ctrl+C to stop monitoring");

        // Parse interval
        let seconds = parse_interval(interval_str)?;

        // Start monitoring loop
        for i in 1..=10 {
            // Limit to 10 iterations for demo
            if i > 1 {
                println!("\n{}", "=".repeat(80));
                println!(
                    "📊 Metrics Update #{} ({})",
                    i,
                    chrono::Utc::now().format("%H:%M:%S")
                );
                println!("{}", "=".repeat(80));
            }

            let metrics = simulate_get_metrics(component.map(|s| s.as_str())).await?;
            display_metrics(&metrics, format)?;

            if i < 10 {
                tokio::time::sleep(tokio::time::Duration::from_secs(seconds)).await;
            }
        }

        println!("\n🛑 Monitoring stopped (demo limit reached)");
    } else {
        println!("📊 Fetching current metrics...");
        let metrics = simulate_get_metrics(component.map(|s| s.as_str())).await?;
        display_metrics(&metrics, format)?;
    }

    Ok(())
}

/// Handle metrics export command
pub async fn handle_metrics_export_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let output_file = matches.get_one::<String>("output").unwrap();
    let default_format = "json".to_string();
    let format = matches
        .get_one::<String>("format")
        .unwrap_or(&default_format);
    let default_duration = "1h".to_string();
    let duration = matches
        .get_one::<String>("duration")
        .unwrap_or(&default_duration);

    println!("📤 Exporting metrics...");
    println!("   Duration: {}", duration);
    println!("   Format: {}", format);
    println!("   Output: {}", output_file);

    // Simulate metrics collection
    let historical_metrics = simulate_get_historical_metrics(duration).await?;

    // Simulate export
    export_metrics(&historical_metrics, output_file, format).await?;

    println!("✅ Metrics exported successfully to '{}'!", output_file);
    Ok(())
}

/// Handle health check command
pub async fn handle_health_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let detailed = matches.get_flag("detailed");
    let component = matches.get_one::<String>("component");

    println!("🩺 Checking system health...");

    let health_data = simulate_get_health_status(component.map(|s| s.as_str())).await?;

    if detailed {
        display_detailed_health(&health_data)?;
    } else {
        display_simple_health(&health_data)?;
    }

    Ok(())
}

/// Handle performance analysis command
pub async fn handle_performance_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let default_type = "overview".to_string();
    let analysis_type = matches.get_one::<String>("type").unwrap_or(&default_type);
    let default_duration = "1h".to_string();
    let duration = matches
        .get_one::<String>("duration")
        .unwrap_or(&default_duration);

    println!("⚡ Running performance analysis...");
    println!("   Type: {}", analysis_type);
    println!("   Duration: {}", duration);

    let performance_data = simulate_get_performance_data(analysis_type, duration).await?;
    display_performance_analysis(&performance_data, analysis_type)?;

    Ok(())
}

/// Handle alerting configuration
pub async fn handle_alerts_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    match matches.subcommand() {
        Some(("list", sub_matches)) => handle_alerts_list_command(sub_matches).await,
        Some(("create", sub_matches)) => handle_alerts_create_command(sub_matches).await,
        Some(("delete", sub_matches)) => handle_alerts_delete_command(sub_matches).await,
        Some(("test", sub_matches)) => handle_alerts_test_command(sub_matches).await,
        _ => Err("Invalid alerts subcommand".into()),
    }
}

// Alert subcommands
async fn handle_alerts_list_command(_matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    println!("🚨 Listing configured alerts...");

    let alerts = simulate_get_alerts().await?;
    display_alerts(&alerts)?;

    Ok(())
}

async fn handle_alerts_create_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let name = matches.get_one::<String>("name").unwrap();
    let metric = matches.get_one::<String>("metric").unwrap();
    let threshold = matches.get_one::<String>("threshold").unwrap();
    let default_operator = "gt".to_string();
    let operator = matches
        .get_one::<String>("operator")
        .unwrap_or(&default_operator);
    let default_duration = "5m".to_string();
    let duration = matches
        .get_one::<String>("duration")
        .unwrap_or(&default_duration);

    println!("➕ Creating new alert rule...");
    println!("   Name: {}", name);
    println!("   Metric: {}", metric);
    println!("   Condition: {} {} for {}", metric, operator, threshold);
    println!("   Duration: {}", duration);

    let alert_payload = json!({
        "name": name,
        "metric": metric,
        "threshold": threshold,
        "operator": operator,
        "duration": duration,
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    simulate_api_call("/api/metrics/alerts", "POST", &alert_payload).await?;

    println!("✅ Alert rule created successfully!");
    Ok(())
}

async fn handle_alerts_delete_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let alert_id = matches.get_one::<String>("alert-id").unwrap();

    println!("🗑️  Deleting alert rule '{}'...", alert_id);

    simulate_api_call(
        &format!("/api/metrics/alerts/{}", alert_id),
        "DELETE",
        &json!({}),
    )
    .await?;

    println!("✅ Alert rule deleted successfully!");
    Ok(())
}

async fn handle_alerts_test_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let alert_id = matches.get_one::<String>("alert-id").unwrap();

    println!("🧪 Testing alert rule '{}'...", alert_id);

    let test_payload = json!({
        "alert_id": alert_id,
        "action": "test",
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    simulate_api_call(
        &format!("/api/metrics/alerts/{}/test", alert_id),
        "POST",
        &test_payload,
    )
    .await?;

    println!("✅ Alert test completed! Check your notification channels.");
    Ok(())
}

// Helper functions
async fn simulate_api_call(
    endpoint: &str,
    method: &str,
    payload: &Value,
) -> Result<(), Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    println!("📡 API Call: {} {}", method, endpoint);
    if method != "GET" && !payload.is_null() {
        println!("📤 Payload: {}", serde_json::to_string_pretty(payload)?);
    }

    Ok(())
}

async fn simulate_get_metrics(_component: Option<&str>) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "broker": {
            "cpu_usage": rand_between(15.0, 45.0),
            "memory_usage": rand_between(60.0, 85.0),
            "disk_usage": rand_between(30.0, 70.0),
            "network_in": rand_between(100.0, 500.0),
            "network_out": rand_between(80.0, 400.0),
            "connections": rand_between(150.0, 300.0) as i64,
            "uptime": "15d 8h 32m"
        },
        "topics": {
            "total_topics": 25,
            "total_partitions": 150,
            "messages_per_second": rand_between(1200.0, 2500.0),
            "bytes_per_second": rand_between(15000.0, 35000.0),
            "replication_lag_max": rand_between(0.0, 50.0)
        },
        "consumers": {
            "active_groups": 12,
            "total_consumers": 45,
            "consumer_lag_total": rand_between(100.0, 1000.0) as i64,
            "consumer_lag_max": rand_between(10.0, 200.0) as i64
        },
        "storage": {
            "total_size": "2.5 TB",
            "available_space": "1.2 TB",
            "log_segments": 3420,
            "compaction_rate": rand_between(5.0, 15.0)
        },
        "replication": {
            "in_sync_replicas": 98.5,
            "under_replicated_partitions": rand_between(0.0, 5.0) as i64,
            "offline_partitions": 0,
            "leader_elections_per_hour": rand_between(0.0, 3.0) as i64
        }
    }))
}

async fn simulate_get_historical_metrics(duration: &str) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Generate sample time series data
    let mut data_points = vec![];
    let hours = match duration {
        "1h" => 1,
        "6h" => 6,
        "24h" => 24,
        "7d" => 168,
        _ => 1,
    };

    for i in 0..hours {
        data_points.push(json!({
            "timestamp": chrono::Utc::now().checked_sub_signed(chrono::Duration::hours(hours - i as i64)).unwrap().to_rfc3339(),
            "cpu_usage": rand_between(10.0, 50.0),
            "memory_usage": rand_between(50.0, 90.0),
            "messages_per_second": rand_between(1000.0, 3000.0),
            "consumer_lag": rand_between(50.0, 500.0) as i64
        }));
    }

    Ok(json!({
        "duration": duration,
        "data_points": data_points,
        "summary": {
            "avg_cpu": rand_between(20.0, 40.0),
            "max_cpu": rand_between(40.0, 70.0),
            "avg_memory": rand_between(60.0, 80.0),
            "max_memory": rand_between(80.0, 95.0)
        }
    }))
}

async fn simulate_get_health_status(_component: Option<&str>) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(json!({
        "overall_status": "healthy",
        "components": {
            "broker": {
                "status": "healthy",
                "checks": {
                    "disk_space": {"status": "healthy", "value": "1.2TB available"},
                    "memory": {"status": "healthy", "value": "72% used"},
                    "cpu": {"status": "healthy", "value": "23% used"},
                    "network": {"status": "healthy", "value": "all interfaces up"}
                }
            },
            "storage": {
                "status": "healthy",
                "checks": {
                    "log_segments": {"status": "healthy", "value": "3420 segments"},
                    "compaction": {"status": "healthy", "value": "running normally"},
                    "replication": {"status": "healthy", "value": "98.5% in-sync"}
                }
            },
            "authentication": {
                "status": "healthy",
                "checks": {
                    "oauth_provider": {"status": "healthy", "value": "responding"},
                    "certificate_validity": {"status": "warning", "value": "expires in 30 days"},
                    "token_validation": {"status": "healthy", "value": "operational"}
                }
            },
            "monitoring": {
                "status": "healthy",
                "checks": {
                    "metrics_collection": {"status": "healthy", "value": "collecting"},
                    "log_aggregation": {"status": "healthy", "value": "operational"},
                    "alerting": {"status": "healthy", "value": "4 rules active"}
                }
            }
        },
        "last_check": chrono::Utc::now().to_rfc3339()
    }))
}

async fn simulate_get_performance_data(
    analysis_type: &str,
    duration: &str,
) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

    Ok(json!({
        "analysis_type": analysis_type,
        "duration": duration,
        "throughput": {
            "avg_messages_per_second": rand_between(1500.0, 2500.0),
            "peak_messages_per_second": rand_between(3000.0, 5000.0),
            "avg_bytes_per_second": rand_between(20000.0, 40000.0),
            "peak_bytes_per_second": rand_between(50000.0, 80000.0)
        },
        "latency": {
            "p50": rand_between(2.0, 5.0),
            "p95": rand_between(10.0, 25.0),
            "p99": rand_between(50.0, 100.0),
            "max": rand_between(200.0, 500.0)
        },
        "bottlenecks": [
            {
                "component": "disk_io",
                "severity": "medium",
                "description": "Occasional disk I/O spikes during compaction",
                "recommendation": "Consider spreading compaction across more brokers"
            },
            {
                "component": "network",
                "severity": "low",
                "description": "Network utilization peaks at 70% during high load",
                "recommendation": "Monitor for potential upgrade need"
            }
        ],
        "recommendations": [
            "Consider increasing partition count for high-throughput topics",
            "Optimize consumer group configuration for better parallelism",
            "Review and optimize log retention policies"
        ]
    }))
}

async fn simulate_get_alerts() -> Result<Vec<Value>, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    Ok(vec![
        json!({
            "id": "alert-001",
            "name": "High CPU Usage",
            "metric": "cpu_usage",
            "operator": "gt",
            "threshold": 80.0,
            "duration": "5m",
            "status": "active",
            "last_triggered": "never",
            "created_at": "2024-01-15T10:30:00Z"
        }),
        json!({
            "id": "alert-002",
            "name": "Consumer Lag Alert",
            "metric": "consumer_lag_max",
            "operator": "gt",
            "threshold": 1000,
            "duration": "10m",
            "status": "active",
            "last_triggered": "2024-01-20T14:45:00Z",
            "created_at": "2024-01-15T10:35:00Z"
        }),
        json!({
            "id": "alert-003",
            "name": "Disk Space Low",
            "metric": "disk_usage",
            "operator": "gt",
            "threshold": 85.0,
            "duration": "1m",
            "status": "active",
            "last_triggered": "never",
            "created_at": "2024-01-15T10:40:00Z"
        }),
        json!({
            "id": "alert-004",
            "name": "Replication Issues",
            "metric": "under_replicated_partitions",
            "operator": "gt",
            "threshold": 0,
            "duration": "30s",
            "status": "disabled",
            "last_triggered": "2024-01-18T09:15:00Z",
            "created_at": "2024-01-15T10:45:00Z"
        }),
    ])
}

fn parse_interval(interval_str: &str) -> Result<u64, Box<dyn Error>> {
    let interval_str = interval_str.to_lowercase();
    if interval_str.ends_with('s') {
        Ok(interval_str[..interval_str.len() - 1].parse()?)
    } else if interval_str.ends_with('m') {
        Ok(interval_str[..interval_str.len() - 1].parse::<u64>()? * 60)
    } else {
        Ok(interval_str.parse()?)
    }
}

fn rand_between(min: f64, max: f64) -> f64 {
    min + (max - min) * rand::random::<f64>()
}

async fn export_metrics(
    metrics: &Value,
    output_file: &str,
    format: &str,
) -> Result<(), Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!(
        "💾 Writing {} bytes to {} in {} format",
        serde_json::to_string(metrics)?.len(),
        output_file,
        format
    );

    // In a real implementation, you would write to the file here
    Ok(())
}

fn display_metrics(metrics: &Value, format: &str) -> Result<(), Box<dyn Error>> {
    match format {
        "json" => {
            println!("{}", serde_json::to_string_pretty(metrics)?);
        }
        "table" | _ => {
            display_metrics_table(metrics)?;
        }
    }
    Ok(())
}

fn display_metrics_table(metrics: &Value) -> Result<(), Box<dyn Error>> {
    println!();
    println!(
        "📊 System Metrics - {}",
        metrics["timestamp"].as_str().unwrap_or("Unknown")
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Broker metrics
    if let Some(broker) = metrics["broker"].as_object() {
        println!("🖥️  Broker:");
        println!(
            "   CPU Usage: {:.1}%",
            broker["cpu_usage"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Memory Usage: {:.1}%",
            broker["memory_usage"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Disk Usage: {:.1}%",
            broker["disk_usage"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Network In: {:.1} MB/s",
            broker["network_in"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Network Out: {:.1} MB/s",
            broker["network_out"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Connections: {}",
            broker["connections"].as_i64().unwrap_or(0)
        );
        println!(
            "   Uptime: {}",
            broker["uptime"].as_str().unwrap_or("Unknown")
        );
    }

    println!();

    // Topics metrics
    if let Some(topics) = metrics["topics"].as_object() {
        println!("📋 Topics:");
        println!(
            "   Total Topics: {}",
            topics["total_topics"].as_i64().unwrap_or(0)
        );
        println!(
            "   Total Partitions: {}",
            topics["total_partitions"].as_i64().unwrap_or(0)
        );
        println!(
            "   Messages/sec: {:.0}",
            topics["messages_per_second"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Bytes/sec: {:.0}",
            topics["bytes_per_second"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Max Replication Lag: {:.1}ms",
            topics["replication_lag_max"].as_f64().unwrap_or(0.0)
        );
    }

    println!();

    // Consumers metrics
    if let Some(consumers) = metrics["consumers"].as_object() {
        println!("👥 Consumers:");
        println!(
            "   Active Groups: {}",
            consumers["active_groups"].as_i64().unwrap_or(0)
        );
        println!(
            "   Total Consumers: {}",
            consumers["total_consumers"].as_i64().unwrap_or(0)
        );
        println!(
            "   Total Lag: {}",
            consumers["consumer_lag_total"].as_i64().unwrap_or(0)
        );
        println!(
            "   Max Lag: {}",
            consumers["consumer_lag_max"].as_i64().unwrap_or(0)
        );
    }

    println!();

    // Storage metrics
    if let Some(storage) = metrics["storage"].as_object() {
        println!("💾 Storage:");
        println!(
            "   Total Size: {}",
            storage["total_size"].as_str().unwrap_or("Unknown")
        );
        println!(
            "   Available Space: {}",
            storage["available_space"].as_str().unwrap_or("Unknown")
        );
        println!(
            "   Log Segments: {}",
            storage["log_segments"].as_i64().unwrap_or(0)
        );
        println!(
            "   Compaction Rate: {:.1}%",
            storage["compaction_rate"].as_f64().unwrap_or(0.0)
        );
    }

    println!();

    // Replication metrics
    if let Some(replication) = metrics["replication"].as_object() {
        println!("🔄 Replication:");
        println!(
            "   In-Sync Replicas: {:.1}%",
            replication["in_sync_replicas"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Under-Replicated: {}",
            replication["under_replicated_partitions"]
                .as_i64()
                .unwrap_or(0)
        );
        println!(
            "   Offline Partitions: {}",
            replication["offline_partitions"].as_i64().unwrap_or(0)
        );
        println!(
            "   Leader Elections/hour: {}",
            replication["leader_elections_per_hour"]
                .as_i64()
                .unwrap_or(0)
        );
    }

    println!();
    Ok(())
}

fn display_simple_health(health: &Value) -> Result<(), Box<dyn Error>> {
    let overall_status = health["overall_status"].as_str().unwrap_or("unknown");
    let status_icon = match overall_status {
        "healthy" => "🟢",
        "warning" => "🟡",
        "critical" => "🔴",
        _ => "⚫",
    };

    println!();
    println!("🩺 System Health: {}{}", status_icon, overall_status);

    if let Some(components) = health["components"].as_object() {
        println!();
        println!("Components Status:");
        for (name, component) in components {
            let comp_status = component["status"].as_str().unwrap_or("unknown");
            let comp_icon = match comp_status {
                "healthy" => "🟢",
                "warning" => "🟡",
                "critical" => "🔴",
                _ => "⚫",
            };
            println!("  {}{} {}", comp_icon, comp_status, name);
        }
    }

    println!();
    Ok(())
}

fn display_detailed_health(health: &Value) -> Result<(), Box<dyn Error>> {
    let overall_status = health["overall_status"].as_str().unwrap_or("unknown");
    let status_icon = match overall_status {
        "healthy" => "🟢",
        "warning" => "🟡",
        "critical" => "🔴",
        _ => "⚫",
    };

    println!();
    println!("🩺 Detailed System Health Report");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Overall Status: {}{}", status_icon, overall_status);
    println!(
        "Last Check: {}",
        health["last_check"].as_str().unwrap_or("Unknown")
    );

    if let Some(components) = health["components"].as_object() {
        for (comp_name, component) in components {
            println!();
            let comp_status = component["status"].as_str().unwrap_or("unknown");
            let comp_icon = match comp_status {
                "healthy" => "🟢",
                "warning" => "🟡",
                "critical" => "🔴",
                _ => "⚫",
            };

            println!("🔧 Component: {} {}{}", comp_name, comp_icon, comp_status);

            if let Some(checks) = component["checks"].as_object() {
                for (check_name, check) in checks {
                    let check_status = check["status"].as_str().unwrap_or("unknown");
                    let check_icon = match check_status {
                        "healthy" => "✅",
                        "warning" => "⚠️",
                        "critical" => "❌",
                        _ => "❓",
                    };
                    let check_value = check["value"].as_str().unwrap_or("No details");

                    println!("   {} {}: {}", check_icon, check_name, check_value);
                }
            }
        }
    }

    println!();
    Ok(())
}

fn display_performance_analysis(data: &Value, analysis_type: &str) -> Result<(), Box<dyn Error>> {
    println!();
    println!("⚡ Performance Analysis: {}", analysis_type);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Throughput metrics
    if let Some(throughput) = data["throughput"].as_object() {
        println!("📈 Throughput:");
        println!(
            "   Average Messages/sec: {:.0}",
            throughput["avg_messages_per_second"]
                .as_f64()
                .unwrap_or(0.0)
        );
        println!(
            "   Peak Messages/sec: {:.0}",
            throughput["peak_messages_per_second"]
                .as_f64()
                .unwrap_or(0.0)
        );
        println!(
            "   Average Bytes/sec: {:.0}",
            throughput["avg_bytes_per_second"].as_f64().unwrap_or(0.0)
        );
        println!(
            "   Peak Bytes/sec: {:.0}",
            throughput["peak_bytes_per_second"].as_f64().unwrap_or(0.0)
        );
    }

    println!();

    // Latency metrics
    if let Some(latency) = data["latency"].as_object() {
        println!("⏱️  Latency (ms):");
        println!("   P50: {:.1}", latency["p50"].as_f64().unwrap_or(0.0));
        println!("   P95: {:.1}", latency["p95"].as_f64().unwrap_or(0.0));
        println!("   P99: {:.1}", latency["p99"].as_f64().unwrap_or(0.0));
        println!("   Max: {:.1}", latency["max"].as_f64().unwrap_or(0.0));
    }

    println!();

    // Bottlenecks
    if let Some(bottlenecks) = data["bottlenecks"].as_array() {
        println!("🚫 Identified Bottlenecks:");
        for bottleneck in bottlenecks {
            let severity = bottleneck["severity"].as_str().unwrap_or("unknown");
            let severity_icon = match severity {
                "high" => "🔴",
                "medium" => "🟡",
                "low" => "🟢",
                _ => "⚫",
            };

            println!(
                "   {} {}: {}",
                severity_icon,
                bottleneck["component"].as_str().unwrap_or("Unknown"),
                bottleneck["description"]
                    .as_str()
                    .unwrap_or("No description")
            );
            println!(
                "     💡 {}",
                bottleneck["recommendation"]
                    .as_str()
                    .unwrap_or("No recommendation")
            );
        }
    }

    println!();

    // Recommendations
    if let Some(recommendations) = data["recommendations"].as_array() {
        println!("💡 Performance Recommendations:");
        for (i, rec) in recommendations.iter().enumerate() {
            println!(
                "   {}. {}",
                i + 1,
                rec.as_str().unwrap_or("No recommendation")
            );
        }
    }

    println!();
    Ok(())
}

fn display_alerts(alerts: &[Value]) -> Result<(), Box<dyn Error>> {
    println!();
    println!(
        "┌─────────────┬─────────────────────┬──────────────┬───────────┬─────────────┬─────────────────────┬──────────┐"
    );
    println!(
        "│ Alert ID    │ Name                │ Metric       │ Threshold │ Duration    │ Last Triggered      │ Status   │"
    );
    println!(
        "├─────────────┼─────────────────────┼──────────────┼───────────┼─────────────┼─────────────────────┼──────────┤"
    );

    for alert in alerts {
        let id = alert["id"].as_str().unwrap_or("Unknown");
        let name = alert["name"].as_str().unwrap_or("Unknown");
        let metric = alert["metric"].as_str().unwrap_or("Unknown");
        let threshold = alert["threshold"].as_f64().unwrap_or(0.0);
        let duration = alert["duration"].as_str().unwrap_or("Unknown");
        let last_triggered = alert["last_triggered"].as_str().unwrap_or("never");
        let status = alert["status"].as_str().unwrap_or("Unknown");

        let status_icon = match status {
            "active" => "🟢",
            "disabled" => "🔴",
            "triggered" => "🟡",
            _ => "⚫",
        };

        let display_triggered = if last_triggered == "never" {
            "never".to_string()
        } else {
            last_triggered[..19].to_string()
        };

        println!(
            "│ {:<11} │ {:<19} │ {:<12} │ {:<9} │ {:<11} │ {:<19} │ {}{:<7} │",
            id, name, metric, threshold, duration, display_triggered, status_icon, status
        );
    }

    println!(
        "└─────────────┴─────────────────────┴──────────────┴───────────┴─────────────┴─────────────────────┴──────────┘"
    );
    println!();

    Ok(())
}
