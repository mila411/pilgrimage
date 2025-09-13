use pilgrimage::auth::cli_auth::{CliAuthManager, CliAuthError};
use log::{error, info};

pub struct LoginArgs {
    /// Username for authentication
    pub username: Option<String>,

    /// Interactive login (prompts for credentials)
    pub interactive: bool,
}

pub fn handle_login_command(args: LoginArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut auth_manager = CliAuthManager::default();

    match (args.username, args.interactive) {
        (Some(username), false) => {
            // Username provided, prompt for password only
            let password = rpassword::prompt_password("Password: ")
                .map_err(|e| format!("Failed to read password: {}", e))?;

            match auth_manager.login(&username, &password) {
                Ok(session) => {
                    info!("Successfully logged in as '{}'", session.username);
                    println!("✅ Login successful!");
                    println!("User: {}", session.username);
                    println!("Permissions: {}", session.permissions.join(", "));
                    println!("Session expires at: {}", format_timestamp(session.expires_at));
                }
                Err(CliAuthError::InvalidCredentials) => {
                    error!("Invalid username or password");
                    println!("❌ Login failed: Invalid credentials");
                    return Err("Authentication failed".into());
                }
                Err(e) => {
                    error!("Login error: {}", e);
                    println!("❌ Login failed: {}", e);
                    return Err(e.into());
                }
            }
        }
        (None, true) | (None, false) => {
            // Interactive login
            println!("🔐 Pilgrimage CLI Authentication");
            println!("Please enter your credentials:");

            match auth_manager.interactive_login() {
                Ok(session) => {
                    info!("Successfully logged in as '{}'", session.username);
                    println!("✅ Login successful!");
                    println!("User: {}", session.username);
                    println!("Permissions: {}", session.permissions.join(", "));
                    println!("Session expires at: {}", format_timestamp(session.expires_at));
                }
                Err(CliAuthError::InvalidCredentials) => {
                    error!("Invalid username or password");
                    println!("❌ Login failed: Invalid credentials");
                    return Err("Authentication failed".into());
                }
                Err(e) => {
                    error!("Login error: {}", e);
                    println!("❌ Login failed: {}", e);
                    return Err(e.into());
                }
            }
        }
        (Some(_), true) => {
            println!("❌ Cannot specify both username and interactive mode");
            return Err("Invalid argument combination".into());
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
