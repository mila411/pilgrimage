use chrono::Utc;
use pilgrimage::broker::Broker;
use pilgrimage::schema::MessageSchema;
use pilgrimage::schema::compatibility::Compatibility;
use pilgrimage::schema::version::SchemaVersion;
use pilgrimage::subscriber::types::Subscriber;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 Pilgrimage Distributed Cluster Example");
    println!("============================");

    // Create multiple broker nodes
    let mut brokers = Vec::new();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let nodes = vec![
        (
            "broker-1".to_string(),
            3,
            2,
            format!("./storage/node1_{}", timestamp),
        ),
        (
            "broker-2".to_string(),
            3,
            2,
            format!("./storage/node2_{}", timestamp),
        ),
        (
            "broker-3".to_string(),
            3,
            2,
            format!("./storage/node3_{}", timestamp),
        ),
    ];

    println!("\n📡 Initializing distributed broker nodes...");

    for (id, partitions, replication, storage_path) in &nodes {
        let broker = Broker::new(id, *partitions, *replication, storage_path)
            .expect("Failed to create broker");
        println!("✅ Broker node {} started", id);
        brokers.push(Arc::new(std::sync::Mutex::new(broker)));
    }

    println!(
        "🎉 Distributed cluster ({} nodes) started successfully!",
        brokers.len()
    );

    // Create topics (distributed)
    let topic_name = "distributed_events";

    // Create topics on each node
    for (i, broker_mutex) in brokers.iter().enumerate() {
        let mut broker = broker_mutex.lock().unwrap();
        broker.create_topic(topic_name, None)?;
        println!("📝 Created topic '{}' on node {}", topic_name, i + 1);
    }

    // Define message schema
    let mut schema_metadata = HashMap::new();
    schema_metadata.insert("content_type".to_string(), "application/json".to_string());
    schema_metadata.insert("encoding".to_string(), "utf-8".to_string());

    let schema = MessageSchema {
        id: 1,
        definition: r#"
        {
            "type": "object",
            "properties": {
                "event_type": {"type": "string"},
                "node_id": {"type": "integer"},
                "timestamp": {"type": "string"},
                "data": {"type": "object"}
            },
            "required": ["event_type", "node_id", "timestamp"]
        }
        "#
        .to_string(),
        version: SchemaVersion::new(1),
        compatibility: Compatibility::Forward,
        metadata: Some(schema_metadata),
        topic_id: Some(topic_name.to_string()),
        partition_id: Some(0),
    };

    // Create subscribers on each node
    let mut subscriber_ids = Vec::new();

    for (i, broker_mutex) in brokers.iter().enumerate() {
        let subscriber_id = format!("cluster_subscriber_{}", i + 1);
        let node_id = i + 1;

        let subscriber = Subscriber::new(
            subscriber_id.clone(),
            Box::new(move |message: String| {
                println!(
                    "🔔 Node {} received message: {}",
                    node_id,
                    message.chars().take(100).collect::<String>()
                );
            }),
        );

        let mut broker = broker_mutex.lock().unwrap();
        broker.subscribe(topic_name, subscriber)?;
        println!(
            "👂 Registered subscriber '{}' on node {}",
            subscriber_id,
            i + 1
        );
        subscriber_ids.push(subscriber_id);
    }

    println!("\n🚀 Starting distributed messaging test...");

    // Distributed message sending test
    for round in 1..=3 {
        println!("\n--- Round {} ---", round);

        for (i, broker_mutex) in brokers.iter().enumerate() {
            let message_data = format!(
                r#"{{
                    "event_type": "cluster_test",
                    "node_id": {},
                    "timestamp": "{}",
                    "data": {{
                        "round": {},
                        "message": "Distributed messages from node {}"
                    }}
                }}"#,
                i + 1,
                Utc::now().to_rfc3339(),
                round,
                i + 1
            );

            let mut message = schema.clone();
            message.definition = message_data;
            // Send the message
            let mut broker = broker_mutex.lock().unwrap();
            broker.send_message(message)?;
            println!("📤 Sent message from node {}", i + 1);

            drop(broker); // Release the lock early
            sleep(Duration::from_millis(500)).await;
        }

        // Wait between rounds
        sleep(Duration::from_secs(2)).await;
    }

    println!("\n📊 Cluster Statistics:");
    for (i, broker_mutex) in brokers.iter().enumerate() {
        let broker = broker_mutex.lock().unwrap();
        println!("Node {} (ID: {}): Active", i + 1, broker.id);
    }

    // Message reception test
    println!("\n📥 Message Reception Test...");
    for (i, broker_mutex) in brokers.iter().enumerate() {
        let broker = broker_mutex.lock().unwrap();
        match broker.receive_message(topic_name, 0) {
            Ok(Some(message)) => {
                println!(
                    "✅ Node {} received: {}",
                    i + 1,
                    message.content.chars().take(100).collect::<String>()
                );
            }
            Ok(None) => {
                println!("📭 Node {}: No messages received", i + 1);
            }
            Err(e) => {
                println!("❌ Node {}: Reception error - {}", i + 1, e);
            }
        }
    }

    // Load balancing test
    println!("\n⚖️ Load Balancing Test...");
    let load_test_messages = 10;

    for msg_id in 1..=load_test_messages {
        // Round-robin selection of nodes
        let broker_index = (msg_id - 1) % brokers.len();
        let broker_mutex = &brokers[broker_index];

        let message_data = format!(
            r#"{{
                "event_type": "load_balance_test",
                "node_id": {},
                "timestamp": "{}",
                "data": {{
                    "message_id": {},
                    "load_test": true
                }}
            }}"#,
            broker_index + 1,
            Utc::now().to_rfc3339(),
            msg_id
        );

        let mut message = schema.clone();
        message.definition = message_data;

        let mut broker = broker_mutex.lock().unwrap();
        broker.send_message(message)?;
        println!(
            "📊 Sent load balancing message {} via node {}",
            msg_id,
            broker_index + 1
        );
        drop(broker); // Release the lock early

        sleep(Duration::from_millis(200)).await;
    }

    // Cluster health check
    println!("\n🏥 Cluster Health Check...");
    for (i, broker_mutex) in brokers.iter().enumerate() {
        let broker = broker_mutex.lock().unwrap();
        let health_status = if broker.is_healthy() {
            "Healthy"
        } else {
            "Unhealthy"
        };
        println!("🔍 Node {}: {}", i + 1, health_status);
    }

    println!("\n🎉 Execution of the distributed cluster example completed!");
    println!("📝 Summary:");
    println!("   - Built a cluster with {} broker nodes", brokers.len());
    println!("   - Tested distributed messaging functionality");
    println!("   - Verified message delivery through load balancing");
    println!("   - Monitored the health status of the entire cluster");

    Ok(())
}
