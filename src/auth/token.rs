//! This module provides functionality for generating and verifying JSON Web Tokens (JWTs).
//! It includes a [`TokenManager`] struct for managing JWT encoding and decoding,
//! and a [`Claims`] struct for representing the claims in a JWT.

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// A struct representing the [claims of a JWT token][claims].
///
/// The struct can be serialized and deserialized.
///
/// # Fields
/// * `sub`: The subject of the token, which is the username.
/// * `exp`: The expiration time of the token.
/// * `roles`: The roles associated with the token.
///
/// [claims]: https://auth0.com/docs/secure/tokens/json-web-tokens/json-web-token-claims
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// The subject of the token.
    pub sub: String,
    /// The expiration time of the token.
    pub exp: usize,
    /// The roles associated with the token.
    pub roles: Vec<String>,
}

/// A struct responsible for managing the encoding and decoding of JWT tokens.
///
/// # Fields
/// * `encoding_key`: The key used to encode (sign) the JWT token.
/// * `decoding_key`: The key used to decode (verify) the JWT token.
pub struct TokenManager {
    /// The key used to encode (sign) the JWT token.
    encoding_key: EncodingKey,
    /// The key used to decode (verify) the JWT token.
    decoding_key: DecodingKey,
}

impl TokenManager {
    /// Creates a new `TokenManager` with the provided secret key.
    ///
    /// # Arguments
    /// * `secret`: A slice of bytes representing the secret key used for encoding and decoding.
    ///             The key will be used to sign and verify the JWT token.
    ///
    /// # Returns
    /// * A new `TokenManager` instance.
    ///
    /// # Example
    /// ```
    /// use crate::pilgrimage::auth::token::TokenManager;
    ///
    /// // Generate a new token manager with the secret key
    /// let token_manager = TokenManager::new(b"MySuperSecret");
    /// ```
    pub fn new(secret: &[u8]) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret),
            decoding_key: DecodingKey::from_secret(secret),
        }
    }

    /// Generates a JWT for the given username and roles.
    ///
    /// The token will expire in 1 hour.
    ///
    /// The header of the JWT will contain the default algorithm `HS256`,
    /// provided by [Header::default()].
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `roles`: A vector of strings representing the roles.
    ///
    /// # Returns
    /// * `Result<String, jsonwebtoken::errors::Error>`: A result containing
    ///   the encoded JWT as a string, or an error if encoding fails.
    ///
    /// # Example
    /// ```
    /// use crate::pilgrimage::auth::token::TokenManager;
    ///
    /// // Generate a new token manager with the secret key
    /// let token_manager = TokenManager::new(b"MySuperSecret");
    ///
    /// // Generate a new token for the user "admin" with the role "admin"
    /// let token = token_manager.generate_token("admin", vec!["admin".to_string()]);
    /// ```
    pub fn generate_token(
        &self,
        username: &str,
        roles: Vec<String>,
    ) -> Result<String, jsonwebtoken::errors::Error> {
        let expiration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize
            + 3600;

        let claims = Claims {
            sub: username.to_string(),
            exp: expiration,
            roles,
        };

        encode(&Header::default(), &claims, &self.encoding_key)
    }

    /// Verifies a JWT token and returns the claims.
    ///
    /// # Arguments
    /// * `token`: A string slice representing the JWT token.
    ///
    /// # Returns
    /// * `Result<Claims, jsonwebtoken::errors::Error>`: A result containing the decoded claims,
    ///   or an error if decoding fails.
    ///
    /// # Example
    /// ```
    /// use crate::pilgrimage::auth::token::TokenManager;
    ///
    /// // Generate a new token manager with the secret key
    /// let token_manager = TokenManager::new(b"MySuperSecret");
    ///
    /// // Generate a new token for the user "admin" with the role "admin"
    /// let token = token_manager.generate_token("admin", vec!["admin".to_string()]).unwrap();
    ///
    /// // Verify the token and get the claims
    /// let claims = token_manager.verify_token(&token).unwrap();
    /// ```
    pub fn verify_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let validation = Validation::new(Algorithm::HS256);
        decode::<Claims>(token, &self.decoding_key, &validation).map(|token_data| token_data.claims)
    }
}
