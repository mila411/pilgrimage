use clap::{Parser, Subcommand, arg};
use pilgrimage::broker::{Broker, cluster::Cluster};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};

#[derive(Parser)]
#[command(name = "Pilgrimage Broker")]
#[command(version = "0.1.0")]
#[command(author = "Your Name <youremail@example.com>")]
#[command(about = "Manages Brokers in the Pilgrimage cluster")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the broker
    Start {
        #[arg(short, long, value_name = "ID")]
        id: String,

        #[arg(short, long, value_name = "STORAGE_PATH")]
        storage: String,
    },
    /// Stop the broker
    Stop {
        #[arg(short, long, value_name = "ID")]
        id: String,
    },
    /// Send a message
    Send {
        #[arg(short, long, value_name = "ID")]
        id: String,

        #[arg(short, long, value_name = "MESSAGE")]
        message: String,
    },
    /// Consume message
    Consume {
        #[arg(short, long, value_name = "ID")]
        id: String,
    },
    /// Check broker status
    Status {
        #[arg(short, long, value_name = "ID")]
        id: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize cluster
    let cluster = Arc::new(Mutex::new(Cluster::new()));

    match cli.command {
        Commands::Start { id, storage } => {
            let broker = Arc::new(Mutex::new(Broker::new(&id, 3, 2, &storage).await));
            cluster
                .lock()
                .await
                .add_broker(id.clone(), broker.clone())
                .await;

            println!("Broker '{}' started.", id);
        }
        Commands::Stop { id } => {
            cluster.lock().await.remove_broker(&id).await;
            println!("Broker '{}' stopped.", id);
        }
        Commands::Send { id, message } => {
            if let Some(broker) = cluster.lock().await.get_broker(&id).await {
                let mut broker = broker.lock().await;
                broker
                    .publish_with_ack("default_topic", message, None)
                    .unwrap();
                println!("Message sent to broker '{}'.", id);
            } else {
                println!("Broker '{}' not found.", id);
            }
        }
        Commands::Consume { id } => {
            if let Some(broker) = cluster.lock().await.get_broker(&id).await {
                let broker = broker.lock().await;
                if let Some(message) = broker.receive_message() {
                    println!("Received message from broker '{}': {}", id, message);
                } else {
                    println!("No messages available in broker '{}'.", id);
                }
            } else {
                println!("Broker '{}' not found.", id);
            }
        }
        Commands::Status { id } => {
            if let Some(broker) = cluster.lock().await.get_broker(&id).await {
                let broker = broker.lock().await;
                println!("Broker '{}': {:?}", id, broker.get_status());
            } else {
                println!("Broker '{}' not found.", id);
            }
        }
    }

    // Start monitoring the cluster
    // let cluster_clone = cluster.clone();
    // tokio::spawn(async move {
    //     cluster_clone.lock().await.monitor_cluster().await;
    // });

    // Pause to check cluster operation
    sleep(Duration::from_secs(10)).await;
}
