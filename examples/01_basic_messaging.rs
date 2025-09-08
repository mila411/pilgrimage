//! Basic Messaging Example
//!
//! This example demonstrates the fundamental messaging capabilities:
//! - Creating and configuring brokers
//! - Publishing messages to topics
//! - Basic broker statistics
//! - Distributed broker setup

use pilgrimage::broker::distributed::{DistributedBroker, DistributedBrokerConfig};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("🚀 Basic Messaging Example");
    println!("==========================");

    // Create a simple broker configuration
    let config = DistributedBrokerConfig::builder(
        "basic_node".to_string(),
        "basic_cluster".to_string()
    )
    .listen_address("127.0.0.1:9001".parse::<SocketAddr>()?)
    .data_dir("/tmp/pilgrimage_basic")
    .expected_cluster_size(1) // Single node cluster
    .replication_factor(1)    // No replication needed for single node
    .build()?;

    // Create and start the broker
    println!("📦 Creating broker...");
    let broker = DistributedBroker::new(config).await?;

    println!("🚀 Starting broker...");
    broker.start().await?;

    // Wait for broker to be ready
    println!("⏳ Waiting for broker to stabilize...");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Check if broker is ready to accept messages
    println!("🔍 Checking broker status...");
    println!("  Is Leader: {}", broker.is_leader());
    println!("  Cluster Health: {:?}", broker.get_cluster_health());
    println!("  Connected Peers: {:?}", broker.get_connected_peers());

    // Publish some messages
    println!("\n📤 Publishing messages...");
    for i in 1..=5 {
        let topic = format!("test_topic_{}", i % 2 + 1); // Alternate between two topics
        let message_data = format!("Hello World! Message #{}", i).into_bytes();

        match broker.send_broker_message(&topic, message_data.clone()).await {
            Ok(_) => println!("  ✅ Published message #{}", i),
            Err(e) => println!("  ❌ Failed to publish message #{}: {}", i, e),
        }
    }

    // Wait for message processing
    println!("\n📥 Waiting for message processing...");
    time::sleep(Duration::from_secs(2)).await;

    // For this example, we'll focus on the broker's distributed capabilities
    println!("  📋 Messages have been sent through the distributed broker");
    println!("  📋 In a real implementation, message consumption would be handled by subscribers");

    // Get broker statistics
    println!("\n📊 Broker Statistics:");
    println!("  Node ID: {}", broker.get_config().node_id);
    println!("  Cluster ID: {}", broker.get_config().cluster_id);
    println!("  Listen Address: {}", broker.get_config().listen_address);
    println!("  Replication Factor: {}", broker.get_config().replication_factor);
    println!("  Connected Peers: {:?}", broker.get_connected_peers());
    println!("  Is Leader: {}", broker.is_leader());
    println!("  Consensus State: {:?}", broker.get_consensus_state());
    println!("  Current Term: {}", broker.get_term());

    // Stop the broker gracefully
    println!("\n🛑 Stopping broker...");
    broker.stop().await?;

    println!("\n✅ Basic messaging example completed!");
    println!("💡 This example showed how to:");
    println!("   - Create and configure a distributed broker");
    println!("   - Start and stop broker services");
    println!("   - Publish messages to topics");
    println!("   - Check broker statistics");
    println!("   - Handle distributed messaging");

    Ok(())
}
