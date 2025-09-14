use crate::{BrokerErrorKind, CliError, CliResult};
use clap::ArgMatches;
use log::{error, info, warn};
use std::time::Duration;

use pilgrimage::broker::pid_manager::PidManager;

pub async fn handle_stop_command(matches: &ArgMatches) -> CliResult<()> {
    let id = matches.value_of("id").ok_or_else(|| CliError::ParseError {
        field: "broker_id".to_string(),
        message: "Broker ID is not specified".to_string(),
    })?;

    let force = matches.is_present("force");
    let timeout_secs = matches
        .value_of("timeout")
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or(30);

    info!("Stopping broker {} (timeout: {}s, force: {})", id, timeout_secs, force);

    let pid_manager = PidManager::new(id.to_string());

    // Clean up any stale PID files first
    if let Err(e) = pid_manager.cleanup_stale_pid_file() {
        warn!("Failed to cleanup stale PID file: {}", e);
    }

    // Check if broker is running
    let running_pid = pid_manager.get_running_broker_pid();
    if running_pid.is_none() {
        info!("Broker {} is not running", id);
        return Ok(());
    }

    let pid = running_pid.unwrap();
    info!("Found running broker {} with PID {}", id, pid);

    // Try API-based shutdown first (graceful)
    if !force {
        info!("Attempting graceful shutdown via API...");
        match stop_broker_via_api(id).await {
            Ok(_) => {
                info!("Broker {} stopped gracefully via API", id);

                // Wait for process to actually stop
                if wait_for_process_stop(&pid_manager, Duration::from_secs(timeout_secs)).await {
                    info!("Broker {} stopped successfully", id);
                    return Ok(());
                } else {
                    warn!("Broker did not stop within timeout, falling back to signal-based stop");
                }
            }
            Err(e) => {
                warn!("API-based stop failed: {}, trying signal-based stop", e);
            }
        }
    }

    // Signal-based shutdown
    info!("Attempting signal-based shutdown...");

    #[cfg(unix)]
    {
        if !force {
            // Try SIGTERM first (graceful)
            match pid_manager.send_sigterm() {
                Ok(_) => {
                    info!("Sent SIGTERM to broker {} (PID: {})", id, pid);

                    // Wait for graceful shutdown
                    if wait_for_process_stop(&pid_manager, Duration::from_secs(timeout_secs)).await {
                        info!("Broker {} stopped gracefully", id);
                        return Ok(());
                    } else {
                        warn!("Graceful shutdown timeout exceeded, forcing termination");
                    }
                }
                Err(e) => {
                    error!("Failed to send SIGTERM: {}", e);
                }
            }
        }

        // Force termination with SIGKILL
        match pid_manager.send_sigkill() {
            Ok(_) => {
                info!("Sent SIGKILL to broker {} (PID: {})", id, pid);

                // Wait a short time for force kill to take effect
                if wait_for_process_stop(&pid_manager, Duration::from_secs(5)).await {
                    info!("Broker {} terminated forcefully", id);
                    Ok(())
                } else {
                    Err(CliError::BrokerError {
                        kind: BrokerErrorKind::OperationFailed,
                        message: format!("Failed to stop broker {} even with SIGKILL", id),
                    })
                }
            }
            Err(e) => Err(CliError::BrokerError {
                kind: BrokerErrorKind::OperationFailed,
                message: format!("Failed to send SIGKILL: {}", e),
            }),
        }
    }

    #[cfg(windows)]
    {
        match pid_manager.terminate_process(force) {
            Ok(_) => {
                info!("Terminated broker {} (PID: {})", id, pid);

                // Wait for termination
                let wait_timeout = if force { Duration::from_secs(5) } else { Duration::from_secs(timeout_secs) };
                if wait_for_process_stop(&pid_manager, wait_timeout).await {
                    info!("Broker {} stopped successfully", id);
                    Ok(())
                } else {
                    Err(CliError::BrokerError {
                        kind: BrokerErrorKind::OperationFailed,
                        message: format!("Failed to stop broker {} within timeout", id),
                    })
                }
            }
            Err(e) => Err(CliError::BrokerError {
                kind: BrokerErrorKind::OperationFailed,
                message: format!("Failed to terminate process: {}", e),
            }),
        }
    }
}

/// Wait for a process to stop with timeout
async fn wait_for_process_stop(pid_manager: &PidManager, timeout: Duration) -> bool {
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < timeout {
        if pid_manager.get_running_broker_pid().is_none() {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

/// Try to stop a broker by calling the web console API
async fn stop_broker_via_api(id: &str) -> Result<(), String> {
    // Use reqwest to call the web console API
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

    let response = client
        .post("http://localhost:8080/stop")
        .json(&serde_json::json!({ "id": id }))
        .send()
        .await;

    match response {
        Ok(res) => {
            if res.status().is_success() {
                Ok(())
            } else {
                Err(format!("API returned error: {:?}", res.status()))
            }
        }
        Err(e) => Err(format!("Failed to connect to web console: {}", e)),
    }
}
