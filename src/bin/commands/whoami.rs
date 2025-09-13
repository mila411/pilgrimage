use pilgrimage::auth::cli_auth::{CliAuthManager, CliAuthError};
use log::error;

pub struct WhoamiArgs {
    /// Show detailed session information
    pub detailed: bool,
}

pub fn handle_whoami_command(args: WhoamiArgs) -> Result<(), Box<dyn std::error::Error>> {
    let auth_manager = CliAuthManager::default();

    match auth_manager.get_current_session() {
        Ok(session) => {
            println!("👤 Current User: {}", session.username);
            
            if args.detailed {
                println!();
                println!("📋 Session Details:");
                println!("  Username: {}", session.username);
                println!("  Permissions: {}", session.permissions.join(", "));
                println!("  Created: {}", format_timestamp(session.created_at));
                println!("  Last Accessed: {}", format_timestamp(session.last_accessed));
                println!("  Expires: {}", format_timestamp(session.expires_at));
                
                // Calculate time until expiry
                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                
                if session.expires_at > current_time {
                    let remaining = session.expires_at - current_time;
                    println!("  Time Remaining: {}", format_duration(remaining));
                } else {
                    println!("  Status: ⚠️ Expired");
                }
            } else {
                println!("Permissions: {}", session.permissions.join(", "));
            }
        }
        Err(CliAuthError::SessionNotFound) => {
            println!("❌ Not logged in");
            println!("Use 'pilgrimage login' to authenticate");
            return Err("Not authenticated".into());
        }
        Err(CliAuthError::TokenExpired) => {
            println!("❌ Session expired");
            println!("Use 'pilgrimage login' to re-authenticate");
            return Err("Session expired".into());
        }
        Err(e) => {
            error!("Error checking authentication: {}", e);
            println!("❌ Failed to check authentication status: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}

fn format_timestamp(timestamp: u64) -> String {
    use chrono::{DateTime, Utc};

    if let Some(datetime) = DateTime::<Utc>::from_timestamp(timestamp as i64, 0) {
        datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
    } else {
        "Invalid timestamp".to_string()
    }
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    
    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}