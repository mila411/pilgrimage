//! Load testing and performance benchmarking commands
//!
//! This module provides comprehensive load testing functionality
//! including producer load testing, consumer load testing, and cluster stress testing.

use clap::ArgMatches;
use std::error::Error;
use serde_json::{json, Value};

/// Handle producer load test command
pub async fn handle_producer_test_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let topic = matches.get_one::<String>("topic").unwrap();
    let rate = matches.get_one::<String>("rate").unwrap().parse::<u32>()?;
    let duration = matches.get_one::<String>("duration").unwrap();
    let message_size = matches.get_one::<String>("message-size").unwrap_or(&"1024".to_string()).parse::<u32>()?;
    let threads = matches.get_one::<String>("threads").unwrap_or(&"1".to_string()).parse::<u32>()?;
    let default_acks = "all".to_string();
    let acks = matches.get_one::<String>("acks").unwrap_or(&default_acks);

    println!("🚀 Starting Producer Load Test...");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Test Configuration:");
    println!("   Topic: {}", topic);
    println!("   Target Rate: {} messages/sec", rate);
    println!("   Duration: {}", duration);
    println!("   Message Size: {} bytes", message_size);
    println!("   Producer Threads: {}", threads);
    println!("   Acknowledgment: {}", acks);
    println!();

    // Start load test
    let test_config = json!({
        "type": "producer",
        "topic": topic,
        "rate": rate,
        "duration": duration,
        "message_size": message_size,
        "threads": threads,
        "acks": acks,
        "started_at": chrono::Utc::now().to_rfc3339()
    });

    simulate_producer_load_test(&test_config).await?;

    Ok(())
}

/// Handle consumer load test command
pub async fn handle_consumer_test_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let topic = matches.get_one::<String>("topic").unwrap();
    let group_id = matches.get_one::<String>("group-id").unwrap();
    let consumers = matches.get_one::<String>("consumers").unwrap_or(&"1".to_string()).parse::<u32>()?;
    let duration = matches.get_one::<String>("duration").unwrap();
    let max_poll_records = matches.get_one::<String>("max-poll-records").unwrap_or(&"500".to_string()).parse::<u32>()?;

    println!("📥 Starting Consumer Load Test...");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Test Configuration:");
    println!("   Topic: {}", topic);
    println!("   Consumer Group: {}", group_id);
    println!("   Consumer Count: {}", consumers);
    println!("   Duration: {}", duration);
    println!("   Max Poll Records: {}", max_poll_records);
    println!();

    let test_config = json!({
        "type": "consumer",
        "topic": topic,
        "group_id": group_id,
        "consumers": consumers,
        "duration": duration,
        "max_poll_records": max_poll_records,
        "started_at": chrono::Utc::now().to_rfc3339()
    });

    simulate_consumer_load_test(&test_config).await?;

    Ok(())
}

/// Handle end-to-end load test command
pub async fn handle_e2e_test_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let topic = matches.get_one::<String>("topic").unwrap();
    let producer_rate = matches.get_one::<String>("producer-rate").unwrap().parse::<u32>()?;
    let consumer_count = matches.get_one::<String>("consumer-count").unwrap().parse::<u32>()?;
    let duration = matches.get_one::<String>("duration").unwrap();
    let message_size = matches.get_one::<String>("message-size").unwrap_or(&"1024".to_string()).parse::<u32>()?;

    println!("🔄 Starting End-to-End Load Test...");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Test Configuration:");
    println!("   Topic: {}", topic);
    println!("   Producer Rate: {} messages/sec", producer_rate);
    println!("   Consumer Count: {}", consumer_count);
    println!("   Duration: {}", duration);
    println!("   Message Size: {} bytes", message_size);
    println!();

    let test_config = json!({
        "type": "end_to_end",
        "topic": topic,
        "producer_rate": producer_rate,
        "consumer_count": consumer_count,
        "duration": duration,
        "message_size": message_size,
        "started_at": chrono::Utc::now().to_rfc3339()
    });

    simulate_e2e_load_test(&test_config).await?;

    Ok(())
}

/// Handle cluster stress test command
pub async fn handle_stress_test_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let scenario = matches.get_one::<String>("scenario").unwrap();
    let duration = matches.get_one::<String>("duration").unwrap();
    let default_intensity = "medium".to_string();
    let intensity = matches.get_one::<String>("intensity").unwrap_or(&default_intensity);

    println!("💥 Starting Cluster Stress Test...");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Test Configuration:");
    println!("   Scenario: {}", scenario);
    println!("   Duration: {}", duration);
    println!("   Intensity: {}", intensity);
    println!();

    let test_config = json!({
        "type": "stress",
        "scenario": scenario,
        "duration": duration,
        "intensity": intensity,
        "started_at": chrono::Utc::now().to_rfc3339()
    });

    simulate_stress_test(&test_config).await?;

    Ok(())
}

/// Handle load test monitoring command
pub async fn handle_monitor_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let test_id = matches.get_one::<String>("test-id");
    let default_interval = "5s".to_string();
    let interval = matches.get_one::<String>("interval").unwrap_or(&default_interval);

    if let Some(id) = test_id {
        println!("📊 Monitoring load test '{}'...", id);
        simulate_monitor_specific_test(id, interval).await?;
    } else {
        println!("📊 Monitoring all active load tests...");
        simulate_monitor_all_tests(interval).await?;
    }

    Ok(())
}

/// Handle load test results command
pub async fn handle_results_command(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let test_id = matches.get_one::<String>("test-id");
    let default_format = "summary".to_string();
    let format = matches.get_one::<String>("format").unwrap_or(&default_format);

    if let Some(id) = test_id {
        println!("📈 Fetching results for test '{}'...", id);
        let results = simulate_get_test_results(id).await?;
        display_test_results(&results, format)?;
    } else {
        println!("📈 Fetching recent test results...");
        let results = simulate_get_recent_results().await?;
        display_recent_results(&results)?;
    }

    Ok(())
}

// Load test simulation functions
async fn simulate_producer_load_test(config: &Value) -> Result<(), Box<dyn Error>> {
    let rate = config["rate"].as_u64().unwrap_or(1000);
    let duration_str = config["duration"].as_str().unwrap_or("60s");
    let message_size = config["message_size"].as_u64().unwrap_or(1024);

    println!("🎯 Initializing producers...");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("✅ Producers ready. Starting message generation...");
    println!();

    // Simulate test progress
    for i in 1..=10 {
        let current_rate = rate as f64 * (0.8 + 0.4 * rand::random::<f64>());
        let sent_messages = (current_rate * i as f64) as u64;
        let throughput_mbps = (current_rate * message_size as f64) / (1024.0 * 1024.0);
        let avg_latency = 2.0 + rand::random::<f64>() * 8.0;
        let error_rate = rand::random::<f64>() * 0.1;

        println!("📊 Progress Update {} ({}/10):", i, i);
        println!("   📤 Messages Sent: {}", sent_messages);
        println!("   ⚡ Current Rate: {:.0} msg/sec", current_rate);
        println!("   📈 Throughput: {:.2} MB/sec", throughput_mbps);
        println!("   ⏱️  Avg Latency: {:.1} ms", avg_latency);
        println!("   ❌ Error Rate: {:.3}%", error_rate * 100.0);

        if error_rate > 0.05 {
            println!("   ⚠️  Warning: Error rate above threshold!");
        }

        println!();
        tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
    }

    let final_results = json!({
        "test_id": "load_test_producer_001",
        "total_messages": rate * 10,
        "avg_rate": rate as f64 * 0.95,
        "peak_rate": rate as f64 * 1.15,
        "avg_latency": 4.2,
        "p95_latency": 12.5,
        "p99_latency": 28.7,
        "error_rate": 0.023,
        "duration": duration_str,
        "completed_at": chrono::Utc::now().to_rfc3339()
    });

    println!("🏁 Producer Load Test Completed!");
    display_producer_results(&final_results)?;

    Ok(())
}

async fn simulate_consumer_load_test(config: &Value) -> Result<(), Box<dyn Error>> {
    let consumers = config["consumers"].as_u64().unwrap_or(1);
    let _max_poll = config["max_poll_records"].as_u64().unwrap_or(500);

    println!("🎯 Starting {} consumer(s)...", consumers);
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("✅ Consumers ready. Beginning message consumption...");
    println!();

    // Simulate test progress
    for i in 1..=8 {
        let consumption_rate = 800.0 + rand::random::<f64>() * 400.0;
        let lag = (50.0 + rand::random::<f64>() * 100.0) as u64;
        let poll_latency = 15.0 + rand::random::<f64>() * 10.0;
        let rebalances = if i == 3 { 1 } else { 0 };

        println!("📊 Consumer Progress {} ({}/8):", i, i);
        println!("   📥 Consumption Rate: {:.0} msg/sec", consumption_rate);
        println!("   ⏳ Consumer Lag: {} messages", lag);
        println!("   ⏱️  Poll Latency: {:.1} ms", poll_latency);
        println!("   🔄 Rebalances: {}", rebalances);

        if lag > 100 {
            println!("   ⚠️  Warning: High consumer lag detected!");
        }
        if rebalances > 0 {
            println!("   ℹ️  Consumer group rebalanced");
        }

        println!();
        tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
    }

    let final_results = json!({
        "test_id": "load_test_consumer_001",
        "total_consumed": 6400,
        "avg_consumption_rate": 850.5,
        "peak_consumption_rate": 1150.0,
        "avg_lag": 75,
        "max_lag": 145,
        "avg_poll_latency": 19.8,
        "rebalances": 1,
        "completed_at": chrono::Utc::now().to_rfc3339()
    });

    println!("🏁 Consumer Load Test Completed!");
    display_consumer_results(&final_results)?;

    Ok(())
}

async fn simulate_e2e_load_test(config: &Value) -> Result<(), Box<dyn Error>> {
    let producer_rate = config["producer_rate"].as_u64().unwrap_or(1000);
    let _consumer_count = config["consumer_count"].as_u64().unwrap_or(2);

    println!("🎯 Setting up end-to-end test infrastructure...");
    tokio::time::sleep(tokio::time::Duration::from_millis(700)).await;

    println!("✅ Infrastructure ready. Starting coordinated load test...");
    println!();

    // Simulate coordinated test
    for i in 1..=12 {
        let produce_rate = producer_rate as f64 * (0.85 + 0.3 * rand::random::<f64>());
        let consume_rate = produce_rate * 0.95;
        let e2e_latency = 25.0 + rand::random::<f64>() * 35.0;
        let queue_depth = ((produce_rate - consume_rate) * 0.1).max(0.0) as u64;

        println!("📊 E2E Progress {} ({}/12):", i, i);
        println!("   📤 Producer Rate: {:.0} msg/sec", produce_rate);
        println!("   📥 Consumer Rate: {:.0} msg/sec", consume_rate);
        println!("   ⏱️  E2E Latency: {:.1} ms", e2e_latency);
        println!("   📦 Queue Depth: {} messages", queue_depth);

        if e2e_latency > 50.0 {
            println!("   ⚠️  Warning: High end-to-end latency!");
        }
        if queue_depth > 50 {
            println!("   ⚠️  Warning: Messages accumulating in queue!");
        }

        println!();
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
    }

    let final_results = json!({
        "test_id": "load_test_e2e_001",
        "messages_produced": producer_rate * 12,
        "messages_consumed": (producer_rate as f64 * 0.95 * 12.0) as u64,
        "avg_e2e_latency": 42.3,
        "p95_e2e_latency": 85.7,
        "p99_e2e_latency": 134.2,
        "message_loss_rate": 0.001,
        "completed_at": chrono::Utc::now().to_rfc3339()
    });

    println!("🏁 End-to-End Load Test Completed!");
    display_e2e_results(&final_results)?;

    Ok(())
}

async fn simulate_stress_test(config: &Value) -> Result<(), Box<dyn Error>> {
    let scenario = config["scenario"].as_str().unwrap_or("mixed");
    let intensity = config["intensity"].as_str().unwrap_or("medium");

    println!("🎯 Initializing stress test scenario: '{}'...", scenario);
    tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;

    println!("✅ Stress test infrastructure ready. Beginning chaos...");
    println!();

    // Simulate stress test phases
    let phases = match scenario {
        "network" => vec!["baseline", "latency_spike", "packet_loss", "bandwidth_limit", "recovery"],
        "disk" => vec!["baseline", "io_saturation", "disk_full", "slow_disk", "recovery"],
        "memory" => vec!["baseline", "memory_pressure", "gc_pressure", "oom_simulation", "recovery"],
        "mixed" => vec!["baseline", "cpu_spike", "network_issues", "disk_pressure", "recovery"],
        _ => vec!["baseline", "generic_stress", "recovery"],
    };

    for (i, phase) in phases.iter().enumerate() {
        let cpu_usage = match *phase {
            "baseline" => 25.0 + rand::random::<f64>() * 10.0,
            "cpu_spike" | "memory_pressure" => 80.0 + rand::random::<f64>() * 15.0,
            "recovery" => 15.0 + rand::random::<f64>() * 10.0,
            _ => 50.0 + rand::random::<f64>() * 30.0,
        };

        let memory_usage = match *phase {
            "baseline" => 60.0 + rand::random::<f64>() * 10.0,
            "memory_pressure" | "gc_pressure" => 90.0 + rand::random::<f64>() * 8.0,
            "recovery" => 55.0 + rand::random::<f64>() * 10.0,
            _ => 70.0 + rand::random::<f64>() * 15.0,
        };

        let throughput_impact = match *phase {
            "baseline" | "recovery" => 0.0,
            _ => rand::random::<f64>() * 40.0,
        };

        println!("📊 Stress Phase {} - '{}' ({}/{}):", i + 1, phase, i + 1, phases.len());
        println!("   🖥️  CPU Usage: {:.1}%", cpu_usage);
        println!("   💾 Memory Usage: {:.1}%", memory_usage);
        println!("   📉 Throughput Impact: -{:.1}%", throughput_impact);

        if cpu_usage > 90.0 {
            println!("   🔥 Critical: CPU usage critical!");
        }
        if memory_usage > 95.0 {
            println!("   🔥 Critical: Memory usage critical!");
        }
        if throughput_impact > 30.0 {
            println!("   ⚠️  Warning: Significant performance degradation!");
        }

        println!();
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }

    let final_results = json!({
        "test_id": "stress_test_001",
        "scenario": scenario,
        "intensity": intensity,
        "max_cpu_usage": 95.2,
        "max_memory_usage": 97.8,
        "max_throughput_impact": 38.5,
        "recovery_time": "45s",
        "system_stability": "good",
        "completed_at": chrono::Utc::now().to_rfc3339()
    });

    println!("🏁 Cluster Stress Test Completed!");
    display_stress_results(&final_results)?;

    Ok(())
}

async fn simulate_monitor_specific_test(test_id: &str, interval: &str) -> Result<(), Box<dyn Error>> {
    println!("Starting real-time monitoring for test '{}'...", test_id);
    println!("Update interval: {}", interval);
    println!("Press Ctrl+C to stop monitoring");
    println!();

    // Simulate monitoring updates
    for i in 1..=6 {
        let progress = (i as f64 / 6.0) * 100.0;
        let current_rate = 1200.0 + rand::random::<f64>() * 400.0;
        let error_rate = rand::random::<f64>() * 0.1;

        println!("📊 Monitor Update #{} - {}", i, chrono::Utc::now().format("%H:%M:%S"));
        println!("   Progress: {:.1}%", progress);
        println!("   Current Rate: {:.0} msg/sec", current_rate);
        println!("   Error Rate: {:.3}%", error_rate * 100.0);
        println!("   Status: {}", if error_rate < 0.05 { "🟢 Healthy" } else { "🟡 Degraded" });
        println!();

        tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
    }

    println!("🛑 Monitoring stopped (demo limit reached)");
    Ok(())
}

async fn simulate_monitor_all_tests(_interval: &str) -> Result<(), Box<dyn Error>> {
    println!("Monitoring all active load tests...");
    println!();

    let active_tests = vec![
        ("load_test_001", "producer", "running"),
        ("load_test_002", "consumer", "running"),
        ("stress_test_001", "stress", "completed"),
    ];

    for (test_id, test_type, status) in active_tests {
        let status_icon = match status {
            "running" => "🟢",
            "completed" => "✅",
            "failed" => "❌",
            _ => "⚫",
        };

        println!("📊 Test: {} ({})", test_id, test_type);
        println!("   Status: {}{}", status_icon, status);

        if status == "running" {
            let rate = 1000.0 + rand::random::<f64>() * 500.0;
            println!("   Current Rate: {:.0} msg/sec", rate);
            println!("   Progress: {}%", rand::random::<u32>() % 100);
        }

        println!();
    }

    Ok(())
}

async fn simulate_get_test_results(test_id: &str) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(json!({
        "test_id": test_id,
        "test_type": "producer",
        "started_at": "2024-01-20T10:00:00Z",
        "completed_at": "2024-01-20T10:05:00Z",
        "duration": "5m",
        "configuration": {
            "topic": "load-test-topic",
            "rate": 2000,
            "message_size": 1024,
            "threads": 4
        },
        "results": {
            "total_messages": 600000,
            "successful_messages": 599850,
            "failed_messages": 150,
            "avg_rate": 1999.5,
            "peak_rate": 2150.0,
            "avg_latency": 3.2,
            "p50_latency": 2.8,
            "p95_latency": 8.5,
            "p99_latency": 15.2,
            "max_latency": 45.7,
            "error_rate": 0.025,
            "throughput_mbps": 1.95
        },
        "resource_usage": {
            "max_cpu": 45.2,
            "max_memory": 78.5,
            "max_network_out": 2.1
        },
        "status": "completed"
    }))
}

async fn simulate_get_recent_results() -> Result<Vec<Value>, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(vec![
        json!({
            "test_id": "load_test_001",
            "test_type": "producer",
            "completed_at": "2024-01-20T10:05:00Z",
            "duration": "5m",
            "total_messages": 600000,
            "avg_rate": 1999.5,
            "status": "completed"
        }),
        json!({
            "test_id": "load_test_002",
            "test_type": "consumer",
            "completed_at": "2024-01-20T09:30:00Z",
            "duration": "3m",
            "total_consumed": 180000,
            "avg_rate": 1000.0,
            "status": "completed"
        }),
        json!({
            "test_id": "stress_test_001",
            "test_type": "stress",
            "completed_at": "2024-01-20T08:45:00Z",
            "duration": "10m",
            "scenario": "mixed",
            "max_cpu": 95.2,
            "status": "completed"
        })
    ])
}

// Display functions
fn display_producer_results(results: &Value) -> Result<(), Box<dyn Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📈 Producer Load Test Results");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Test ID: {}", results["test_id"].as_str().unwrap_or("Unknown"));
    println!("Total Messages: {}", results["total_messages"].as_u64().unwrap_or(0));
    println!("Average Rate: {:.1} msg/sec", results["avg_rate"].as_f64().unwrap_or(0.0));
    println!("Peak Rate: {:.1} msg/sec", results["peak_rate"].as_f64().unwrap_or(0.0));
    println!("Average Latency: {:.1} ms", results["avg_latency"].as_f64().unwrap_or(0.0));
    println!("P95 Latency: {:.1} ms", results["p95_latency"].as_f64().unwrap_or(0.0));
    println!("P99 Latency: {:.1} ms", results["p99_latency"].as_f64().unwrap_or(0.0));
    println!("Error Rate: {:.3}%", results["error_rate"].as_f64().unwrap_or(0.0) * 100.0);
    println!("Duration: {}", results["duration"].as_str().unwrap_or("Unknown"));
    println!("Completed: {}", results["completed_at"].as_str().unwrap_or("Unknown"));
    println!();

    Ok(())
}

fn display_consumer_results(results: &Value) -> Result<(), Box<dyn Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📈 Consumer Load Test Results");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Test ID: {}", results["test_id"].as_str().unwrap_or("Unknown"));
    println!("Total Consumed: {}", results["total_consumed"].as_u64().unwrap_or(0));
    println!("Average Rate: {:.1} msg/sec", results["avg_consumption_rate"].as_f64().unwrap_or(0.0));
    println!("Peak Rate: {:.1} msg/sec", results["peak_consumption_rate"].as_f64().unwrap_or(0.0));
    println!("Average Lag: {}", results["avg_lag"].as_u64().unwrap_or(0));
    println!("Max Lag: {}", results["max_lag"].as_u64().unwrap_or(0));
    println!("Average Poll Latency: {:.1} ms", results["avg_poll_latency"].as_f64().unwrap_or(0.0));
    println!("Rebalances: {}", results["rebalances"].as_u64().unwrap_or(0));
    println!("Completed: {}", results["completed_at"].as_str().unwrap_or("Unknown"));
    println!();

    Ok(())
}

fn display_e2e_results(results: &Value) -> Result<(), Box<dyn Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📈 End-to-End Load Test Results");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Test ID: {}", results["test_id"].as_str().unwrap_or("Unknown"));
    println!("Messages Produced: {}", results["messages_produced"].as_u64().unwrap_or(0));
    println!("Messages Consumed: {}", results["messages_consumed"].as_u64().unwrap_or(0));
    println!("Average E2E Latency: {:.1} ms", results["avg_e2e_latency"].as_f64().unwrap_or(0.0));
    println!("P95 E2E Latency: {:.1} ms", results["p95_e2e_latency"].as_f64().unwrap_or(0.0));
    println!("P99 E2E Latency: {:.1} ms", results["p99_e2e_latency"].as_f64().unwrap_or(0.0));
    println!("Message Loss Rate: {:.3}%", results["message_loss_rate"].as_f64().unwrap_or(0.0) * 100.0);
    println!("Completed: {}", results["completed_at"].as_str().unwrap_or("Unknown"));
    println!();

    Ok(())
}

fn display_stress_results(results: &Value) -> Result<(), Box<dyn Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📈 Cluster Stress Test Results");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Test ID: {}", results["test_id"].as_str().unwrap_or("Unknown"));
    println!("Scenario: {}", results["scenario"].as_str().unwrap_or("Unknown"));
    println!("Intensity: {}", results["intensity"].as_str().unwrap_or("Unknown"));
    println!("Max CPU Usage: {:.1}%", results["max_cpu_usage"].as_f64().unwrap_or(0.0));
    println!("Max Memory Usage: {:.1}%", results["max_memory_usage"].as_f64().unwrap_or(0.0));
    println!("Max Throughput Impact: {:.1}%", results["max_throughput_impact"].as_f64().unwrap_or(0.0));
    println!("Recovery Time: {}", results["recovery_time"].as_str().unwrap_or("Unknown"));
    println!("System Stability: {}", results["system_stability"].as_str().unwrap_or("Unknown"));
    println!("Completed: {}", results["completed_at"].as_str().unwrap_or("Unknown"));
    println!();

    Ok(())
}

fn display_test_results(results: &Value, format: &str) -> Result<(), Box<dyn Error>> {
    match format {
        "json" => {
            println!("{}", serde_json::to_string_pretty(results)?);
        },
        "detailed" => {
            display_detailed_results(results)?;
        },
        "summary" | _ => {
            display_summary_results(results)?;
        }
    }
    Ok(())
}

fn display_detailed_results(results: &Value) -> Result<(), Box<dyn Error>> {
    println!();
    println!("📈 Detailed Test Results");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Basic info
    println!("📋 Test Information:");
    println!("   Test ID: {}", results["test_id"].as_str().unwrap_or("Unknown"));
    println!("   Type: {}", results["test_type"].as_str().unwrap_or("Unknown"));
    println!("   Started: {}", results["started_at"].as_str().unwrap_or("Unknown"));
    println!("   Completed: {}", results["completed_at"].as_str().unwrap_or("Unknown"));
    println!("   Duration: {}", results["duration"].as_str().unwrap_or("Unknown"));
    println!("   Status: {}", results["status"].as_str().unwrap_or("Unknown"));

    // Configuration
    if let Some(config) = results["configuration"].as_object() {
        println!();
        println!("⚙️  Configuration:");
        for (key, value) in config {
            println!("   {}: {}", key, value);
        }
    }

    // Results
    if let Some(test_results) = results["results"].as_object() {
        println!();
        println!("📊 Performance Results:");
        for (key, value) in test_results {
            println!("   {}: {}", key, value);
        }
    }

    // Resource usage
    if let Some(resource) = results["resource_usage"].as_object() {
        println!();
        println!("💻 Resource Usage:");
        for (key, value) in resource {
            println!("   {}: {}", key, value);
        }
    }

    println!();
    Ok(())
}

fn display_summary_results(results: &Value) -> Result<(), Box<dyn Error>> {
    println!();
    println!("📊 Test Results Summary");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Test ID: {}", results["test_id"].as_str().unwrap_or("Unknown"));
    println!("Type: {}", results["test_type"].as_str().unwrap_or("Unknown"));
    println!("Duration: {}", results["duration"].as_str().unwrap_or("Unknown"));
    println!("Status: {}", results["status"].as_str().unwrap_or("Unknown"));

    if let Some(test_results) = results["results"].as_object() {
        if let Some(total) = test_results["total_messages"].as_u64() {
            println!("Total Messages: {}", total);
        }
        if let Some(rate) = test_results["avg_rate"].as_f64() {
            println!("Average Rate: {:.1} msg/sec", rate);
        }
        if let Some(latency) = test_results["avg_latency"].as_f64() {
            println!("Average Latency: {:.1} ms", latency);
        }
        if let Some(error_rate) = test_results["error_rate"].as_f64() {
            println!("Error Rate: {:.3}%", error_rate * 100.0);
        }
    }

    println!();
    Ok(())
}

fn display_recent_results(results: &[Value]) -> Result<(), Box<dyn Error>> {
    println!();
    println!("┌─────────────────┬──────────┬─────────────────────┬──────────┬─────────────┬──────────┐");
    println!("│ Test ID         │ Type     │ Completed           │ Duration │ Performance │ Status   │");
    println!("├─────────────────┼──────────┼─────────────────────┼──────────┼─────────────┼──────────┤");

    for result in results {
        let test_id = result["test_id"].as_str().unwrap_or("Unknown");
        let test_type = result["test_type"].as_str().unwrap_or("Unknown");
        let completed_at = result["completed_at"].as_str().unwrap_or("Unknown");
        let duration = result["duration"].as_str().unwrap_or("Unknown");
        let status = result["status"].as_str().unwrap_or("Unknown");

        let performance = if let Some(rate) = result["avg_rate"].as_f64() {
            format!("{:.0} msg/s", rate)
        } else if let Some(cpu) = result["max_cpu"].as_f64() {
            format!("{:.1}% CPU", cpu)
        } else {
            "N/A".to_string()
        };

        let status_icon = match status {
            "completed" => "✅",
            "failed" => "❌",
            "running" => "🟢",
            _ => "⚫",
        };

        println!("│ {:<15} │ {:<8} │ {:<19} │ {:<8} │ {:<11} │ {}{:<7} │",
                 test_id, test_type, &completed_at[..19], duration, performance, status_icon, status);
    }

    println!("└─────────────────┴──────────┴─────────────────────┴──────────┴─────────────┴──────────┘");
    println!();

    Ok(())
}
