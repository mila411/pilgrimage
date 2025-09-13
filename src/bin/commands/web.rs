use crate::error::{BrokerErrorKind, CliError};
use clap::ArgMatches;

/// Handle the web command to start the web console server
pub async fn handle_web_command(_matches: &ArgMatches) -> Result<(), CliError> {
    println!("🚀 Starting Pilgrimage Web Console...");
    println!("📊 Initializing dashboard and API endpoints...");

    // Start the web console server
    match pilgrimage::web_console::run_server().await {
        Ok(_) => {
            println!("✅ Web console started successfully!");
            Ok(())
        }
        Err(e) => {
            eprintln!("❌ Failed to start web console: {}", e);
            Err(CliError::BrokerError {
                kind: BrokerErrorKind::Unknown,
                message: format!("Web console startup failed: {}", e),
            })
        }
    }
}
