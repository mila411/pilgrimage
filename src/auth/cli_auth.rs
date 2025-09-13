//! CLI Authentication and Session Management
//!
//! This module provides CLI-specific authentication functionality,
//! including session management, token storage, and CLI command authorization.

use crate::auth::jwt_auth::DistributedAuthenticator;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// CLI session information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliSession {
    pub username: String,
    pub token: String,
    pub permissions: Vec<String>,
    pub expires_at: u64,
    pub created_at: u64,
    pub last_accessed: u64,
}

/// CLI authentication manager
pub struct CliAuthManager {
    authenticator: DistributedAuthenticator,
    session_file: PathBuf,
    session_duration: u64, // seconds
}

/// CLI authentication error types
#[derive(Debug)]
pub enum CliAuthError {
    NotAuthenticated,
    TokenExpired,
    InvalidCredentials,
    SessionNotFound,
    PermissionDenied,
    IoError(io::Error),
    SerializationError(String),
}

impl std::fmt::Display for CliAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliAuthError::NotAuthenticated => write!(f, "User not authenticated. Please login first."),
            CliAuthError::TokenExpired => write!(f, "Authentication token expired. Please login again."),
            CliAuthError::InvalidCredentials => write!(f, "Invalid username or password."),
            CliAuthError::SessionNotFound => write!(f, "No active session found. Please login first."),
            CliAuthError::PermissionDenied => write!(f, "Permission denied for this operation."),
            CliAuthError::IoError(e) => write!(f, "IO error: {}", e),
            CliAuthError::SerializationError(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl std::error::Error for CliAuthError {}

impl CliAuthManager {
    /// Create a new CLI authentication manager
    pub fn new() -> Result<Self, CliAuthError> {
        // Use a persistent JWT secret for CLI sessions
        let jwt_secret = Self::get_or_create_jwt_secret()?;
        let mut authenticator = DistributedAuthenticator::new(
            jwt_secret,
            "pilgrimage-cli".to_string(),
        );

        // Set a shorter session duration for CLI (2 hours)
        authenticator.set_token_expiry(7200);

        // Create session directory if it doesn't exist
        let session_dir = Self::get_session_directory()?;
        if !session_dir.exists() {
            fs::create_dir_all(&session_dir).map_err(CliAuthError::IoError)?;
        }

        let session_file = session_dir.join("session.json");

        Ok(Self {
            authenticator,
            session_file,
            session_duration: 7200, // 2 hours
        })
    }

    /// Initialize default users for CLI authentication
    pub fn init_default_users(&mut self) {
        // Add default admin user
        self.authenticator.add_user("admin", "admin123");
        self.authenticator.add_user_permissions(
            "admin".to_string(),
            vec![
                "admin".to_string(),
                "broker_start".to_string(),
                "broker_stop".to_string(),
                "message_send".to_string(),
                "message_consume".to_string(),
                "schema_manage".to_string(),
                "metrics_view".to_string(),
                "security_manage".to_string(),
            ],
        );

        // Add default user with limited permissions
        self.authenticator.add_user("user", "user123");
        self.authenticator.add_user_permissions(
            "user".to_string(),
            vec![
                "message_send".to_string(),
                "message_consume".to_string(),
                "metrics_view".to_string(),
            ],
        );

        info!("Initialized default CLI users: admin, user");
    }

    /// Add a new user with specific permissions
    pub fn add_user(&mut self, username: &str, password: &str, permissions: Vec<String>) {
        self.authenticator.add_user(username, password);
        self.authenticator.add_user_permissions(username.to_string(), permissions);
        info!("Added CLI user: {}", username);
    }

    /// Authenticate user and create CLI session
    pub fn login(&mut self, username: &str, password: &str) -> Result<CliSession, CliAuthError> {
        let auth_result = self.authenticator.authenticate_client(username, password);

        if !auth_result.success {
            warn!("CLI login failed for user: {}", username);
            return Err(CliAuthError::InvalidCredentials);
        }

        let token = auth_result.token.unwrap();
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let session = CliSession {
            username: username.to_string(),
            token,
            permissions: auth_result.permissions,
            expires_at: current_time + self.session_duration,
            created_at: current_time,
            last_accessed: current_time,
        };

        // Save session to file
        self.save_session(&session)?;

        info!("CLI user '{}' logged in successfully", username);
        Ok(session)
    }

    /// Interactive login with password masking
    pub fn interactive_login(&mut self) -> Result<CliSession, CliAuthError> {
        print!("Username: ");
        io::stdout().flush().map_err(CliAuthError::IoError)?;

        let mut username = String::new();
        io::stdin()
            .read_line(&mut username)
            .map_err(CliAuthError::IoError)?;
        let username = username.trim();

        // Use rpassword for secure password input
        let password = rpassword::prompt_password("Password: ")
            .map_err(|e| CliAuthError::IoError(io::Error::new(io::ErrorKind::Other, e)))?;

        self.login(username, &password)
    }

    /// Check if user is currently authenticated
    pub fn is_authenticated(&self) -> bool {
        match self.get_current_session() {
            Ok(session) => self.is_session_valid(&session),
            Err(_) => false,
        }
    }

    /// Get current active session
    pub fn get_current_session(&self) -> Result<CliSession, CliAuthError> {
        if !self.session_file.exists() {
            return Err(CliAuthError::SessionNotFound);
        }

        let session_data = fs::read_to_string(&self.session_file)
            .map_err(CliAuthError::IoError)?;

        let session: CliSession = serde_json::from_str(&session_data)
            .map_err(|e| CliAuthError::SerializationError(e.to_string()))?;

        if !self.is_session_valid(&session) {
            self.logout().ok(); // Clean up invalid session
            return Err(CliAuthError::TokenExpired);
        }

        // Update last accessed time
        self.touch_session(&session)?;

        Ok(session)
    }

    /// Check if user has required permission for command
    pub fn has_permission(&self, command: &str) -> Result<bool, CliAuthError> {
        let session = self.get_current_session()?;

        // Map CLI commands to required permissions
        let required_permission = match command {
            "start" => "broker_start",
            "stop" => "broker_stop",
            "send" => "message_send",
            "consume" => "message_consume",
            "schema" => "schema_manage",
            "metrics" | "status" => "metrics_view",
            "security" | "auth-setup" | "acl" | "token" | "cert" => "security_manage",
            "web" => "web_console",
            _ => "user", // Default permission for basic commands
        };

        let has_perm = session.permissions.contains(&required_permission.to_string()) ||
                      session.permissions.contains(&"admin".to_string());

        if !has_perm {
            warn!(
                "User '{}' lacks permission '{}' for command '{}'",
                session.username, required_permission, command
            );
        }

        Ok(has_perm)
    }

    /// Require authentication and permission for command execution
    pub fn require_auth(&self, command: &str) -> Result<CliSession, CliAuthError> {
        let session = self.get_current_session()?;

        if !self.has_permission(command)? {
            return Err(CliAuthError::PermissionDenied);
        }

        debug!("Authentication check passed for command: {}", command);
        Ok(session)
    }

    /// Logout and clear session
    pub fn logout(&self) -> Result<(), CliAuthError> {
        if self.session_file.exists() {
            fs::remove_file(&self.session_file).map_err(CliAuthError::IoError)?;
            info!("CLI session cleared");
        }
        Ok(())
    }

    /// Check if session is still valid
    fn is_session_valid(&self, session: &CliSession) -> bool {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if current_time > session.expires_at {
            debug!("Session expired for user: {}", session.username);
            return false;
        }

        // Validate JWT token
        let validation_result = self.authenticator.validate_token(&session.token);
        validation_result.valid
    }

    /// Save session to file
    fn save_session(&self, session: &CliSession) -> Result<(), CliAuthError> {
        let session_data = serde_json::to_string_pretty(session)
            .map_err(|e| CliAuthError::SerializationError(e.to_string()))?;

        fs::write(&self.session_file, session_data).map_err(CliAuthError::IoError)?;
        debug!("Session saved for user: {}", session.username);
        Ok(())
    }

    /// Update session last accessed time
    fn touch_session(&self, session: &CliSession) -> Result<(), CliAuthError> {
        let mut updated_session = session.clone();
        updated_session.last_accessed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.save_session(&updated_session)
    }

    /// Get CLI session directory
    fn get_session_directory() -> Result<PathBuf, CliAuthError> {
        if let Some(home_dir) = dirs::home_dir() {
            Ok(home_dir.join(".pilgrimage"))
        } else {
            // Fallback to current directory
            Ok(PathBuf::from(".pilgrimage"))
        }
    }

    /// Get or create a persistent JWT secret for CLI
    fn get_or_create_jwt_secret() -> Result<Vec<u8>, CliAuthError> {
        let session_dir = Self::get_session_directory()?;
        if !session_dir.exists() {
            fs::create_dir_all(&session_dir).map_err(CliAuthError::IoError)?;
        }

        let secret_file = session_dir.join("jwt_secret");

        if secret_file.exists() {
            // Load existing secret
            let secret_data = fs::read(&secret_file).map_err(CliAuthError::IoError)?;
            if secret_data.len() == 32 {
                debug!("Loaded existing JWT secret for CLI");
                Ok(secret_data)
            } else {
                warn!("Invalid JWT secret file, regenerating");
                Self::generate_and_save_secret(&secret_file)
            }
        } else {
            // Generate new secret
            Self::generate_and_save_secret(&secret_file)
        }
    }

    /// Generate and save a new JWT secret
    fn generate_and_save_secret(secret_file: &PathBuf) -> Result<Vec<u8>, CliAuthError> {
        let secret = DistributedAuthenticator::generate_jwt_secret();
        fs::write(secret_file, &secret).map_err(CliAuthError::IoError)?;
        info!("Generated new JWT secret for CLI");
        Ok(secret)
    }
}

impl Default for CliAuthManager {
    fn default() -> Self {
        let mut manager = Self::new().expect("Failed to create CLI auth manager");
        manager.init_default_users();
        manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_cli_auth_manager_creation() {
        let manager = CliAuthManager::new();
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_default_users_initialization() {
        let mut manager = CliAuthManager::new().unwrap();
        manager.init_default_users();

        // Test admin login
        let admin_session = manager.login("admin", "admin123");
        assert!(admin_session.is_ok());

        let session = admin_session.unwrap();
        assert_eq!(session.username, "admin");
        assert!(session.permissions.contains(&"admin".to_string()));
    }

    #[tokio::test]
    async fn test_user_permissions() {
        let mut manager = CliAuthManager::new().unwrap();
        manager.init_default_users();

        // Login as user
        let _session = manager.login("user", "user123").unwrap();

        // Test permissions
        assert!(manager.has_permission("send").unwrap());
        assert!(manager.has_permission("consume").unwrap());
        assert!(!manager.has_permission("start").unwrap());
        assert!(!manager.has_permission("security").unwrap());
    }

    #[tokio::test]
    async fn test_session_management() {
        let mut manager = CliAuthManager::new().unwrap();
        manager.init_default_users();

        // Login
        let _session = manager.login("admin", "admin123").unwrap();
        assert!(manager.is_authenticated());

        // Get current session
        let current = manager.get_current_session();
        assert!(current.is_ok());

        // Logout
        manager.logout().unwrap();
        assert!(!manager.is_authenticated());
    }

    #[tokio::test]
    async fn test_authentication_flow() {
        let mut manager = CliAuthManager::new().unwrap();
        manager.init_default_users();

        // Test require_auth without login
        let result = manager.require_auth("start");
        assert!(result.is_err());

        // Login and test again
        let _session = manager.login("admin", "admin123").unwrap();
        let result = manager.require_auth("start");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_custom_user_addition() {
        let mut manager = CliAuthManager::new().unwrap();

        // Add custom user
        manager.add_user(
            "developer",
            "dev123",
            vec!["message_send".to_string(), "metrics_view".to_string()],
        );

        // Test login
        let session = manager.login("developer", "dev123");
        assert!(session.is_ok());

        let session = session.unwrap();
        assert!(session.permissions.contains(&"message_send".to_string()));
        assert!(!session.permissions.contains(&"admin".to_string()));
    }
}
