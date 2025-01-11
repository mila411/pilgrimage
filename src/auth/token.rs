use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
    pub roles: Vec<String>,
}

pub struct TokenManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
}

impl TokenManager {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret),
            decoding_key: DecodingKey::from_secret(secret),
        }
    }

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

    pub fn verify_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let validation = Validation::new(Algorithm::HS256);
        decode::<Claims>(token, &self.decoding_key, &validation).map(|token_data| token_data.claims)
    }
}
