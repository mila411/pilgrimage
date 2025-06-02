use crate::error::{BrokerErrorKind, CliError, CliResult};
use clap::ArgMatches;

use std::fs;
use std::path::Path;
use std::process::Command;

pub async fn handle_stop_command(matches: &ArgMatches) -> CliResult<()> {
    let id = matches.value_of("id").ok_or_else(|| CliError::ParseError {
        field: "broker_id".to_string(),
        message: "Broker ID is not specified".to_string(),
    })?;

    println!("Stopping broker {}...", id);

    // First try to connect to the web console to stop the broker
    let stop_via_api = stop_broker_via_api(id).await;
    if stop_via_api.is_ok() {
        println!("Stopped broker {} via web console API", id);
        return Ok(());
    }

    // If API call failed, try to find and stop the broker process
    let pid_file = format!("/tmp/pilgrimage_broker_{}.pid", id);
    let pid_path = Path::new(&pid_file);

    if pid_path.exists() {
        match fs::read_to_string(pid_path) {
            Ok(pid_str) => {
                let pid = pid_str
                    .trim()
                    .parse::<i32>()
                    .map_err(|_| CliError::BrokerError {
                        kind: BrokerErrorKind::OperationFailed,
                        message: format!("Failed to parse PID from file: {}", pid_file),
                    })?;

                // Try to terminate the process
                match stop_process(pid) {
                    Ok(_) => {
                        // Clean up the PID file
                        if let Err(e) = fs::remove_file(pid_path) {
                            eprintln!("Warning: Failed to remove PID file: {}", e);
                        }

                        println!("Stopped broker {} (PID: {})", id, pid);
                        Ok(())
                    }
                    Err(e) => Err(CliError::BrokerError {
                        kind: BrokerErrorKind::OperationFailed,
                        message: format!("Failed to stop broker process: {}", e),
                    }),
                }
            }
            Err(e) => Err(CliError::BrokerError {
                kind: BrokerErrorKind::OperationFailed,
                message: format!("Failed to read PID file: {}", e),
            }),
        }
    } else {
        Err(CliError::BrokerError {
            kind: BrokerErrorKind::NotFound,
            message: format!("Broker {} is not running or PID file not found", id),
        })
    }
}

/// Try to stop a broker by calling the web console API
async fn stop_broker_via_api(id: &str) -> Result<(), String> {
    // Use reqwest to call the web console API
    let client = reqwest::Client::new();
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

/// Stop a process by sending a signal
fn stop_process(pid: i32) -> Result<(), String> {
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::process::ExitStatusExt;

        // Send SIGTERM signal first
        let status = Command::new("kill")
            .arg(pid.to_string())
            .status()
            .map_err(|e| format!("Failed to send SIGTERM: {}", e))?;

        if !status.success() {
            return Err(format!(
                "Failed to terminate process with exit code: {:?}",
                status.code().or_else(|| status.signal()).unwrap_or(-1)
            ));
        }

        // Wait a bit for the process to terminate
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Check if process still exists
        let ps_check = Command::new("ps")
            .arg("-p")
            .arg(pid.to_string())
            .output()
            .map_err(|e| format!("Failed to check if process exists: {}", e))?;

        if !ps_check.status.success() {
            return Ok(()); // Process is no longer running
        }

        // If still running, send SIGKILL
        let kill_status = Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .status()
            .map_err(|e| format!("Failed to send SIGKILL: {}", e))?;

        if kill_status.success() {
            Ok(())
        } else {
            Err(format!(
                "Failed to kill process with exit code: {:?}",
                kill_status
                    .code()
                    .or_else(|| kill_status.signal())
                    .unwrap_or(-1)
            ))
        }
    }

    #[cfg(target_family = "windows")]
    {
        let status = Command::new("taskkill")
            .arg("/PID")
            .arg(pid.to_string())
            .arg("/F") // Force kill
            .status()
            .map_err(|e| format!("Failed to terminate process: {}", e))?;

        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "Failed to kill process with exit code: {:?}",
                status.code().unwrap_or(-1)
            ))
        }
    }
}
