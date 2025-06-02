use crate::error::{BrokerErrorKind, CliError, CliResult};
use clap::ArgMatches;
use pilgrimage::broker::Broker;
use std::sync::Arc;
use tokio::sync::Mutex;

fn validate_args(matches: &ArgMatches) -> CliResult<(String, usize, usize, String)> {
    let id = matches.value_of("id").ok_or_else(|| CliError::ParseError {
        field: "broker_id".to_string(),
        message: "Broker ID is not specified. Please use the --id option.".to_string(),
    })?;

    if id.trim().is_empty() {
        return Err(CliError::ParseError {
            field: "broker_id".to_string(),
            message: "Broker ID cannot be empty.".to_string(),
        });
    }

    let partitions: usize = matches
        .value_of("partitions")
        .ok_or_else(|| CliError::ParseError {
            field: "partitions".to_string(),
            message: "Number of partitions not specified. Please use the --partitions option."
                .to_string(),
        })?
        .parse()
        .map_err(|e| CliError::ParseError {
            field: "partitions".to_string(),
            message: format!(
                "Invalid partition number: {} (Please specify a positive integer)",
                e
            ),
        })?;

    if partitions == 0 {
        return Err(CliError::ParseError {
            field: "partitions".to_string(),
            message: "Number of partitions must be at least 1.".to_string(),
        });
    }

    let replication: usize = matches
        .value_of("replication")
        .ok_or_else(|| CliError::ParseError {
            field: "replication".to_string(),
            message: "Replication factor not specified. Please use the --replication option."
                .to_string(),
        })?
        .parse()
        .map_err(|e| CliError::ParseError {
            field: "replication".to_string(),
            message: format!(
                "Invalid replication factor: {} (Please specify a positive integer)",
                e
            ),
        })?;

    if replication == 0 {
        return Err(CliError::ParseError {
            field: "replication".to_string(),
            message: "Replication factor must be at least 1.".to_string(),
        });
    }

    let storage = matches
        .value_of("storage")
        .ok_or_else(|| CliError::ParseError {
            field: "storage".to_string(),
            message: "Storage path not specified. Please use the --storage option.".to_string(),
        })?;

    if storage.trim().is_empty() {
        return Err(CliError::ParseError {
            field: "storage".to_string(),
            message: "Storage path cannot be empty.".to_string(),
        });
    }

    // Check if storage path exists and create it
    if !std::path::Path::new(storage).exists() {
        if let Err(e) = std::fs::create_dir_all(storage) {
            return Err(CliError::IoError(e));
        }
        println!("Created storage directory: {}", storage);
    }

    Ok((id.to_string(), partitions, replication, storage.to_string()))
}

pub async fn handle_start_command(matches: &ArgMatches) -> CliResult<()> {
    let (id, partitions, replication, storage) = validate_args(matches)?;

    println!(
        "Starting broker {}... Number of partitions: {}, Replication factor: {}, Storage path: {}",
        id, partitions, replication, storage
    );

    // Initialize the broker
    let broker = Broker::new(&id, partitions, replication, &storage);
    let _broker = Arc::new(Mutex::new(broker));

    // Create PID file for the broker process
    let pid = std::process::id(); // Get current process ID
    let pid_file = format!("/tmp/pilgrimage_broker_{}.pid", id);
    let pid_path = std::path::Path::new(&pid_file);

    if let Err(e) = std::fs::write(pid_path, pid.to_string()) {
        return Err(CliError::BrokerError {
            kind: BrokerErrorKind::OperationFailed,
            message: format!("Failed to create PID file: {}", e),
        });
    }

    println!("Created PID file: {} (PID: {})", pid_file, pid);

    // Wait for initialization to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Check if storage file exists
    let storage_path = std::path::Path::new(&storage);
    if !storage_path.exists() {
        // If initialization failed, clean up PID file
        if let Err(e) = std::fs::remove_file(pid_path) {
            eprintln!("Warning: Failed to remove PID file: {}", e);
        }

        return Err(CliError::BrokerError {
            kind: BrokerErrorKind::OperationFailed,
            message: format!(
                "Failed to initialize broker {}.\nStorage directory {} was not created.",
                id, storage
            ),
        });
    }

    println!("Broker {} started successfully", id);
    Ok(())
}
