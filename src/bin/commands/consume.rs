use crate::{BrokerErrorKind, CliError, CliResult};
use clap::ArgMatches;
use pilgrimage::Broker;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn handle_consume_command(matches: &ArgMatches) -> CliResult<()> {
    let broker_id = matches.value_of("id").ok_or_else(|| CliError::ParseError {
        field: "id".to_string(),
        message: "Broker ID is not specified".to_string(),
    })?;

    let _group_id = matches.value_of("group").unwrap_or("default");
    let partition = matches
        .value_of("partition")
        .map(|p| p.parse::<usize>())
        .transpose()
        .map_err(|_| CliError::ParseError {
            field: "partition".to_string(),
            message: "Invalid partition number".to_string(),
        })?;
    let topic = matches.value_of("topic").unwrap_or("default");
    let storage_path = format!("/tmp/{}", broker_id);

    println!("Receiving messages from broker {}...", broker_id);

    // Initialize broker instance
    let broker = Broker::new(
        broker_id,
        1, // Default number of partitions
        1, // default replication factor
        &storage_path,
    ).map_err(|e| CliError::BrokerError {
        kind: BrokerErrorKind::OperationFailed,
        message: format!("Failed to initialize broker: {}", e),
    })?;

    let broker = Arc::new(Mutex::new(broker));

    // Receiving and processing messages
    let broker = broker.lock().await;
    match broker.receive_message(topic, partition.unwrap_or(0)) {
        Ok(Some(message)) => {
            println!("Received message: {}", message.content);
            Ok(())
        }
        Ok(None) => {
            println!("No messages available to receive");
            Ok(())
        }
        Err(e) => {
            if e.to_string().contains("timeout") {
                println!("Timeout: Could not receive message");
                Ok(())
            } else {
                Err(CliError::BrokerError {
                    kind: BrokerErrorKind::OperationFailed,
                    message: format!("Failed to receive message: {}", e),
                })
            }
        }
    }
}
