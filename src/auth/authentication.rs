//! Module for user authentication.
//!
//! This module provides functionality for authenticating users based on username and password.
//!
//! The [`Authenticator`] trait defines the interface for authenticating users.
//!
//! The [`BasicAuthenticator`] struct is a simple implementation
//! of the [`Authenticator`] trait that uses a HashMap for storing credentials.
//! The [`BasicAuthenticator`] struct also provides a method for adding new users to the
//! authenticator.
//!
//! # Examples
//! ```rust
//! use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
//!
//! // Create a new BasicAuthenticator instance
//! let mut authenticator = BasicAuthenticator::new();
//!
//! // Add some users
//! authenticator.add_user("user1", "password");
//! authenticator.add_user("user2", "password");
//!
//! // Authenticate users
//! assert!(authenticator.authenticate("user1", "password").unwrap());
//! assert!(!authenticator.authenticate("user1", "wrong_password").unwrap());
//! assert!(!authenticator.authenticate("user3", "password").unwrap());
//! ```

use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use argon2::{Algorithm, Argon2, PasswordHash, PasswordHasher, PasswordVerifier, Version, Params};
use password_hash::SaltString;

// Argon2 デフォルトパラメータ（RFC 9106 の推奨に近い設定）。
// テストでは後述の setter で下げられます。
const DEFAULT_M_COST_KIB: u32 = 19_456; // ≈19 MiB
const DEFAULT_T_COST: u32 = 2;          // time cost
const DEFAULT_P_COST: u32 = 1;          // parallelism
static M_COST_KIB: AtomicU32 = AtomicU32::new(DEFAULT_M_COST_KIB);
static T_COST: AtomicU32 = AtomicU32::new(DEFAULT_T_COST);
static P_COST: AtomicU32 = AtomicU32::new(DEFAULT_P_COST);
use std::collections::HashMap;
use std::error::Error;

/// User information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_id: String,
    pub username: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_login: Option<chrono::DateTime<chrono::Utc>>,
}

/// A trait for authenticating users based on username and password.
pub trait Authenticator {
    /// Authenticates a user with the given username and password.
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `password`: A string slice representing the password.
    ///
    /// # Returns
    /// * `Result<bool, Box<dyn Error>>`: A result indicating whether the authentication
    ///   was successful (`true` for success, `false` for failure), or an error if an error occurs.
    fn authenticate(&self, username: &str, password: &str) -> Result<bool, Box<dyn Error>>;
}

/// A struct representing a basic authenticator that uses a HashMap for storing credentials.
pub struct BasicAuthenticator {
    /// A HashMap containing the username-password pairs.
    credentials: std::collections::HashMap<String, String>,
}

impl BasicAuthenticator {
    /// Creates a new instance of `BasicAuthenticator`. It simply initializes the credential store.
    ///
    /// # Returns
    /// * A new `BasicAuthenticator` instance with an empty credentials store.
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
        }
    }

    /// Adds a new user with the given username and password to the authenticator.
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `password`: A string slice representing the password.
    pub fn add_user(&mut self, username: &str, password: &str) {
        let hashed = Self::hash_password(password);
        self.credentials
            .insert(username.to_string(), hashed);
    }

    /// Returns true if the user already exists in the credential store
    pub fn has_user(&self, username: &str) -> bool {
        self.credentials.contains_key(username)
    }

    /// Change an existing user's password by verifying the current password
    pub fn change_password(
        &mut self,
        username: &str,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), String> {
        match self.credentials.get(username) {
            Some(stored) => {
                if Self::verify_password(current_password, stored) {
                    let hashed = Self::hash_password(new_password);
                    self.credentials.insert(username.to_string(), hashed);
                    Ok(())
                } else {
                    Err("Invalid current password".to_string())
                }
            }
            None => Err("User not found".to_string()),
        }
    }

    /// Remove an existing user from the credential store
    pub fn remove_user(&mut self, username: &str) -> bool {
        self.credentials.remove(username).is_some()
    }

    /// Set a user's password without requiring the current password (admin operation)
    pub fn set_password(&mut self, username: &str, new_password: &str) -> Result<(), String> {
        if self.credentials.contains_key(username) {
            let hashed = Self::hash_password(new_password);
            self.credentials
                .insert(username.to_string(), hashed);
            Ok(())
        } else {
            Err("User not found".to_string())
        }
    }

    /// List all usernames (unordered)
    pub fn list_users(&self) -> Vec<String> {
        self.credentials.keys().cloned().collect()
    }

    /// Export a copy of credentials for secure persistence (username -> password)
    /// Note: These are the currently stored values used for authentication.
    pub fn export_credentials(&self) -> HashMap<String, String> {
        self.credentials.clone()
    }

    /// Replace all credentials with the provided map (used when loading from persistence)
    pub fn import_credentials(&mut self, creds: HashMap<String, String>) {
        self.credentials = creds;
    }
}

impl Default for BasicAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl Authenticator for BasicAuthenticator {
    /// Authenticates a user with the given username and password.
    ///
    /// This method checks if the given username exists in the credentials store
    /// and if the password matches.
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `password`: A string slice representing the password.
    ///
    /// # Returns
    /// * `Result<bool, Box<dyn Error>>`: A result indicating whether the authentication
    ///   was successful (`true` for success, `false` for failure), or an error if an error occurs.
    fn authenticate(&self, username: &str, password: &str) -> Result<bool, Box<dyn Error>> {
        Ok(self
            .credentials
            .get(username)
            .map(|stored| Self::verify_password(password, stored))
            .unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use ring::pbkdf2 as ring_pbkdf2;
    use std::num::NonZeroU32;

    #[test]
    fn test_basic_authenticator_creation() {
        let authenticator = BasicAuthenticator::new();
        assert!(authenticator.credentials.is_empty());
    }

    #[test]
    fn test_basic_authenticator_default() {
        let authenticator = BasicAuthenticator::default();
        assert!(authenticator.credentials.is_empty());
    }

    #[test]
    fn test_add_user() {
        let mut authenticator = BasicAuthenticator::new();

        authenticator.add_user("test_user", "test_password");
        authenticator.add_user("admin", "admin_pass");

        assert_eq!(authenticator.credentials.len(), 2);
        // Stored values should be hashed (argon2 encoded string starts with "$argon2")
        assert!(authenticator
            .credentials
            .get("test_user")
            .is_some_and(|v| v.starts_with("$argon2")));
        assert!(authenticator
            .credentials
            .get("admin")
            .is_some_and(|v| v.starts_with("$argon2")));
    }

    #[test]
    fn test_add_user_overwrite() {
        let mut authenticator = BasicAuthenticator::new();

        authenticator.add_user("user", "old_password");
        authenticator.add_user("user", "new_password");

    assert_eq!(authenticator.credentials.len(), 1);
    // Should be a new hashed value (not equal to plaintext)
    let stored = authenticator.credentials.get("user").unwrap();
    assert!(stored.starts_with("$argon2"));
    }

    #[test]
    fn test_successful_authentication() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("valid_user", "correct_password");

        let result = authenticator.authenticate("valid_user", "correct_password");
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_failed_authentication_wrong_password() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("user", "correct_password");

        let result = authenticator.authenticate("user", "wrong_password");
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_failed_authentication_nonexistent_user() {
        let authenticator = BasicAuthenticator::new();

        let result = authenticator.authenticate("nonexistent", "any_password");
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_empty_credentials_authentication() {
        let authenticator = BasicAuthenticator::new();

        let result = authenticator.authenticate("", "");
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_case_sensitive_username() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("User", "password");

        // Should fail with different case
        let result1 = authenticator.authenticate("user", "password");
        assert!(result1.is_ok());
        assert!(!result1.unwrap());

        // Should succeed with exact case
        let result2 = authenticator.authenticate("User", "password");
        assert!(result2.is_ok());
        assert!(result2.unwrap());
    }

    #[test]
    fn test_case_sensitive_password() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("user", "Password");

        // Should fail with different case
        let result1 = authenticator.authenticate("user", "password");
        assert!(result1.is_ok());
        assert!(!result1.unwrap());

        // Should succeed with exact case
        let result2 = authenticator.authenticate("user", "Password");
        assert!(result2.is_ok());
        assert!(result2.unwrap());
    }

    #[test]
    fn test_special_characters_in_credentials() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("user@domain.com", "p@ssw0rd!#$%");

        let result = authenticator.authenticate("user@domain.com", "p@ssw0rd!#$%");
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_unicode_credentials() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("user", "password");

        let result = authenticator.authenticate("user", "password");
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_long_credentials() {
        let mut authenticator = BasicAuthenticator::new();
        let long_username = "a".repeat(1000);
        let long_password = "b".repeat(1000);

        authenticator.add_user(&long_username, &long_password);

        let result = authenticator.authenticate(&long_username, &long_password);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_empty_username_with_password() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("", "password");

        let result = authenticator.authenticate("", "password");
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_username_with_empty_password() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("user", "");

        let result = authenticator.authenticate("user", "");
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_multiple_users_authentication() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("user1", "pass1");
        authenticator.add_user("user2", "pass2");
        authenticator.add_user("user3", "pass3");

        // Test all users can authenticate
        assert!(authenticator.authenticate("user1", "pass1").unwrap());
        assert!(authenticator.authenticate("user2", "pass2").unwrap());
        assert!(authenticator.authenticate("user3", "pass3").unwrap());

        // Test cross-authentication fails
        assert!(!authenticator.authenticate("user1", "pass2").unwrap());
        assert!(!authenticator.authenticate("user2", "pass3").unwrap());
        assert!(!authenticator.authenticate("user3", "pass1").unwrap());
    }

    #[test]
    fn test_userinfo_creation() {
        let now = Utc::now();
        let user_info = UserInfo {
            user_id: "user_123".to_string(),
            username: "testuser".to_string(),
            email: Some("test@example.com".to_string()),
            display_name: Some("Test User".to_string()),
            created_at: now,
            last_login: None,
        };

        assert_eq!(user_info.user_id, "user_123");
        assert_eq!(user_info.username, "testuser");
        assert_eq!(user_info.email, Some("test@example.com".to_string()));
        assert_eq!(user_info.display_name, Some("Test User".to_string()));
        assert_eq!(user_info.created_at, now);
        assert!(user_info.last_login.is_none());
    }

    #[test]
    fn test_userinfo_serialization() {
        let now = Utc::now();
        let user_info = UserInfo {
            user_id: "user_456".to_string(),
            username: "serialtest".to_string(),
            email: Some("serial@test.com".to_string()),
            display_name: Some("Serial Test".to_string()),
            created_at: now,
            last_login: Some(now),
        };

        // Test JSON serialization
        let json_result = serde_json::to_string(&user_info);
        assert!(json_result.is_ok());

        let json_str = json_result.unwrap();
        assert!(json_str.contains("user_456"));
        assert!(json_str.contains("serialtest"));
        assert!(json_str.contains("serial@test.com"));

        // Test JSON deserialization
        let deserialized_result: Result<UserInfo, _> = serde_json::from_str(&json_str);
        assert!(deserialized_result.is_ok());

        let deserialized = deserialized_result.unwrap();
        assert_eq!(deserialized.user_id, user_info.user_id);
        assert_eq!(deserialized.username, user_info.username);
        assert_eq!(deserialized.email, user_info.email);
        assert_eq!(deserialized.display_name, user_info.display_name);
    }

    #[test]
    fn test_userinfo_optional_fields() {
        let now = Utc::now();

        // Test with minimal fields
        let minimal_user = UserInfo {
            user_id: "min_user".to_string(),
            username: "minimal".to_string(),
            email: None,
            display_name: None,
            created_at: now,
            last_login: None,
        };

        assert!(minimal_user.email.is_none());
        assert!(minimal_user.display_name.is_none());
        assert!(minimal_user.last_login.is_none());

        // Test serialization of minimal user
        let json_result = serde_json::to_string(&minimal_user);
        assert!(json_result.is_ok());
    }

    #[test]
    fn test_userinfo_clone() {
        let now = Utc::now();
        let original = UserInfo {
            user_id: "clone_test".to_string(),
            username: "cloneuser".to_string(),
            email: Some("clone@test.com".to_string()),
            display_name: Some("Clone User".to_string()),
            created_at: now,
            last_login: Some(now),
        };

        let cloned = original.clone();

        assert_eq!(original.user_id, cloned.user_id);
        assert_eq!(original.username, cloned.username);
        assert_eq!(original.email, cloned.email);
        assert_eq!(original.display_name, cloned.display_name);
        assert_eq!(original.created_at, cloned.created_at);
        assert_eq!(original.last_login, cloned.last_login);
    }

    #[test]
    fn test_userinfo_debug_format() {
        let now = Utc::now();
        let user_info = UserInfo {
            user_id: "debug_user".to_string(),
            username: "debugtest".to_string(),
            email: Some("debug@test.com".to_string()),
            display_name: Some("Debug User".to_string()),
            created_at: now,
            last_login: None,
        };

        let debug_str = format!("{:?}", user_info);
        assert!(debug_str.contains("UserInfo"));
        assert!(debug_str.contains("debug_user"));
        assert!(debug_str.contains("debugtest"));
        assert!(debug_str.contains("debug@test.com"));
    }

    #[test]
    fn test_authenticator_trait_implementation() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("trait_test", "trait_pass");

        // Test that BasicAuthenticator implements Authenticator trait
        let auth_trait: &dyn Authenticator = &authenticator;

        let result = auth_trait.authenticate("trait_test", "trait_pass");
        assert!(result.is_ok());
        assert!(result.unwrap());

        let failed_result = auth_trait.authenticate("trait_test", "wrong_pass");
        assert!(failed_result.is_ok());
        assert!(!failed_result.unwrap());
    }

    #[test]
    fn test_whitespace_handling() {
        let mut authenticator = BasicAuthenticator::new();
        authenticator.add_user("user", "password");

        // Whitespace should not be trimmed - exact match required
        assert!(!authenticator.authenticate(" user", "password").unwrap());
        assert!(!authenticator.authenticate("user ", "password").unwrap());
        assert!(!authenticator.authenticate("user", " password").unwrap());
        assert!(!authenticator.authenticate("user", "password ").unwrap());

        // Test with whitespace in actual credentials
        authenticator.add_user(" spaced user ", " spaced pass ");
        assert!(
            authenticator
                .authenticate(" spaced user ", " spaced pass ")
                .unwrap()
        );
    }

    #[test]
    fn test_upgrade_plaintext_to_argon2_on_login() {
        // make hashing faster for test
        BasicAuthenticator::set_hashing_work_factor(1000);

        let mut auth = BasicAuthenticator::new();
        // Seed legacy plaintext credential via import API
        let mut map = std::collections::HashMap::new();
        map.insert("legacy_user".to_string(), "legacy_pass".to_string());
        auth.import_credentials(map);

        // Authenticate should work with plaintext (legacy support)
        assert!(auth.authenticate("legacy_user", "legacy_pass").unwrap());

        // Trigger transparent upgrade
        let upgraded = auth.upgrade_password_if_legacy("legacy_user", "legacy_pass");
        assert!(upgraded, "Expected upgrade to occur for plaintext");

        // Ensure stored hash is now Argon2
        let stored = auth.export_credentials().get("legacy_user").cloned().unwrap();
        assert!(stored.starts_with("$argon2"));

        // Verify authentication still works with new hash
        assert!(auth.authenticate("legacy_user", "legacy_pass").unwrap());
    }

    #[test]
    fn test_upgrade_pbkdf2_to_argon2_on_login() {
        // make hashing faster for test
        BasicAuthenticator::set_hashing_work_factor(1000);

        // Build a PBKDF2 encoded string compatible with verify_password
        let username = "pb_user";
        let password = "pb_pass";
        let iter: u32 = 1000;
        let n_iter = NonZeroU32::new(iter).unwrap();
        let salt: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut dk = [0u8; 32];
        ring_pbkdf2::derive(ring_pbkdf2::PBKDF2_HMAC_SHA256, n_iter, &salt, password.as_bytes(), &mut dk);
        let stored = format!(
            "{}{}${}${}",
            BasicAuthenticator::HASH_PREFIX_PBKDF2,
            iter,
            hex::encode(salt),
            hex::encode(dk)
        );

        let mut auth = BasicAuthenticator::new();
        let mut map = std::collections::HashMap::new();
        map.insert(username.to_string(), stored);
        auth.import_credentials(map);

        // Authenticate should work with PBKDF2 legacy format
        assert!(auth.authenticate(username, password).unwrap());

        // Trigger transparent upgrade
        let upgraded = auth.upgrade_password_if_legacy(username, password);
        assert!(upgraded, "Expected upgrade to occur for PBKDF2");

        // Ensure stored hash is now Argon2
        let stored_new = auth.export_credentials().get(username).cloned().unwrap();
        assert!(stored_new.starts_with("$argon2"));

        // Verify authentication still works with new hash
        assert!(auth.authenticate(username, password).unwrap());
    }
}

// =====================
// Hashing implementation (Argon2 + 旧形式互換検証)
// =====================
impl BasicAuthenticator {
    const HASH_PREFIX_PBKDF2: &'static str = "pbkdf2$";

    /// テスト・ベンチ向け: Argon2 パラメータを動的調整
    pub fn set_hashing_params(m_cost_kib: u32, t_cost: u32, p_cost: u32) {
        // 下限をある程度設ける（256KiB, t>=1, p>=1）
        M_COST_KIB.store(m_cost_kib.max(256), Ordering::Relaxed);
        T_COST.store(t_cost.max(1), Ordering::Relaxed);
        P_COST.store(p_cost.max(1), Ordering::Relaxed);
    }

    /// 互換API: 旧 set_hashing_work_factor。Argon2では time cost/メモリにマップ。
    pub fn set_hashing_work_factor(_iter: u32) {
        // テスト高速化用にかなり軽量な設定へ。
        // 既存テストが 1000 を指定する前提で、t=1, m=256KiB, p=1 とする。
        // （旧PBKDF2より高速化したいが、完全に0にはしない）
        Self::set_hashing_params(256, 1, 1);
    }

    fn current_argon2() -> Argon2<'static> {
        let m = M_COST_KIB.load(Ordering::Relaxed);
        let t = T_COST.load(Ordering::Relaxed);
        let p = P_COST.load(Ordering::Relaxed);
        let params = Params::new(m, t, p, None).unwrap_or_else(|_| Params::default());
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
    }

    fn hash_password(password: &str) -> String {
        let salt = SaltString::generate(rand::thread_rng());
        let argon2 = Self::current_argon2();
        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .expect("argon2 hash failure")
            .to_string();
        hash
    }

    fn verify_password(password: &str, stored: &str) -> bool {
        if stored.starts_with("$argon2") {
            if let Ok(ph) = PasswordHash::new(stored) {
                return Self::current_argon2()
                    .verify_password(password.as_bytes(), &ph)
                    .is_ok();
            }
            return false;
        }

        if let Some(rest) = stored.strip_prefix(Self::HASH_PREFIX_PBKDF2) {
            // format: pbkdf2$ITER$SALT_HEX$DK_HEX
            let parts: Vec<&str> = rest.split('$').collect();
            if parts.len() != 3 {
                return false;
            }
            let iter: u32 = match parts[0].parse() { Ok(v) => v, Err(_) => return false };
            let salt = match hex::decode(parts[1]) { Ok(v) => v, Err(_) => return false };
            let dk = match hex::decode(parts[2]) { Ok(v) => v, Err(_) => return false };
            let n_iter = match NonZeroU32::new(iter) { Some(v) => v, None => return false };
            ring::pbkdf2::verify(
                ring::pbkdf2::PBKDF2_HMAC_SHA256,
                n_iter,
                &salt,
                password.as_bytes(),
                &dk,
            )
            .is_ok()
        } else {
            // Legacy plaintext comparison for backward compatibility
            stored == password
        }
    }

    /// ログイン後の透明移行: 平文または PBKDF2 なら Argon2 に再ハッシュして保存
    pub fn upgrade_password_if_legacy(&mut self, username: &str, password: &str) -> bool {
        if let Some(stored) = self.credentials.get(username).cloned() {
            let is_legacy = !stored.starts_with("$argon2");
            if is_legacy && Self::verify_password(&password, &stored) {
                let new_hash = Self::hash_password(password);
                self.credentials.insert(username.to_string(), new_hash);
                return true;
            }
        }
        false
    }
}
