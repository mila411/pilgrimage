use pilgrimage::auth::cli_auth::{CliAuthManager, CliAuthError};
use log::{error, info};

pub struct LogoutArgs {
    /// Force logout even if no active session
    pub force: bool,
}

pub fn handle_logout_command(args: LogoutArgs) -> Result<(), Box<dyn std::error::Error>> {
    let auth_manager = CliAuthManager::default();

    // Check if user is currently authenticated
    if !args.force {
        match auth_manager.get_current_session() {
            Ok(session) => {
                println!("Current session: {}", session.username);
                print!("Are you sure you want to logout? (y/N): ");

                let mut input = String::new();
                std::io::stdin().read_line(&mut input)
                    .map_err(|e| format!("Failed to read input: {}", e))?;

                let input = input.trim().to_lowercase();
                if input != "y" && input != "yes" {
                    println!("Logout cancelled");
                    return Ok(());
                }
            }
            Err(CliAuthError::SessionNotFound) | Err(CliAuthError::TokenExpired) => {
                println!("No active session found");
                return Ok(());
            }
            Err(e) => {
                error!("Error checking session: {}", e);
                if !args.force {
                    return Err(e.into());
                }
            }
        }
    }

    // Perform logout
    match auth_manager.logout() {
        Ok(()) => {
            info!("User logged out successfully");
            println!("✅ Logout successful!");
            println!("Session cleared.");
        }
        Err(e) => {
            error!("Logout error: {}", e);
            if args.force {
                println!("⚠️ Force logout completed (session may not have existed)");
            } else {
                println!("❌ Logout failed: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}
