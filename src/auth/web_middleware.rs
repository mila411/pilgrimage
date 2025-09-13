//! Web Authentication Middleware
//!
//! This module provides JWT-based authentication middleware for the web console,
//! ensuring API endpoints are protected and accessible only to authenticated users.

use crate::auth::{DistributedAuthenticator, token::Claims};
use actix_web::{
    body::EitherBody,
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error, HttpMessage, HttpResponse,
};
use futures_util::future::LocalBoxFuture;
use log::{debug, warn};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

/// JWT Authentication Error types for web middleware
#[derive(Debug)]
pub enum WebAuthError {
    TokenMissing,
    TokenInvalid,
    TokenExpired,
    IoError(std::io::Error),
}

impl std::fmt::Display for WebAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebAuthError::TokenMissing => write!(f, "Authorization token is missing"),
            WebAuthError::TokenInvalid => write!(f, "Authorization token is invalid"),
            WebAuthError::TokenExpired => write!(f, "Authorization token has expired"),
            WebAuthError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for WebAuthError {}

/// JWT authentication middleware factory
pub struct JwtAuth {
    authenticator: Rc<DistributedAuthenticator>,
}

impl JwtAuth {
    /// Create a new JWT authentication middleware
    pub fn new() -> Result<Self, WebAuthError> {
        let jwt_secret = Self::get_jwt_secret()?;
        let authenticator = DistributedAuthenticator::new(
            jwt_secret,
            "pilgrimage-web".to_string(),
        );

        Ok(Self {
            authenticator: Rc::new(authenticator),
        })
    }

    /// Get JWT secret (shared with CLI authentication)
    fn get_jwt_secret() -> Result<Vec<u8>, WebAuthError> {
        let session_dir = Self::get_session_directory()?;
        let secret_file = session_dir.join("jwt_secret");

        if secret_file.exists() {
            let secret_data = fs::read(&secret_file).map_err(WebAuthError::IoError)?;
            if secret_data.len() == 32 {
                debug!("Loaded existing JWT secret for web middleware");
                Ok(secret_data)
            } else {
                warn!("Invalid JWT secret file found, using fallback");
                // If the secret is invalid, create a new one
                let secret = DistributedAuthenticator::generate_jwt_secret();
                fs::write(&secret_file, &secret).map_err(WebAuthError::IoError)?;
                Ok(secret)
            }
        } else {
            // If no secret exists, create one
            if !session_dir.exists() {
                fs::create_dir_all(&session_dir).map_err(WebAuthError::IoError)?;
            }
            let secret = DistributedAuthenticator::generate_jwt_secret();
            fs::write(&secret_file, &secret).map_err(WebAuthError::IoError)?;
            Ok(secret)
        }
    }

    /// Get session directory path
    fn get_session_directory() -> Result<PathBuf, WebAuthError> {
        if let Some(home_dir) = dirs::home_dir() {
            Ok(home_dir.join(".pilgrimage"))
        } else {
            // Fallback to current directory
            Ok(PathBuf::from(".pilgrimage"))
        }
    }

    /// Extract token from Authorization header
    fn extract_token(req: &ServiceRequest) -> Option<String> {
        req.headers()
            .get("Authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| {
                if h.starts_with("Bearer ") {
                    Some(h[7..].to_string())
                } else {
                    None
                }
            })
    }
}

impl<S, B> Transform<S, ServiceRequest> for JwtAuth
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = JwtAuthMiddleware<S>;
    type Future = std::future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        std::future::ready(Ok(JwtAuthMiddleware {
            service: Rc::new(service),
            authenticator: self.authenticator.clone(),
        }))
    }
}

/// JWT authentication middleware service
pub struct JwtAuthMiddleware<S> {
    service: Rc<S>,
    authenticator: Rc<DistributedAuthenticator>,
}

impl<S, B> Service<ServiceRequest> for JwtAuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = self.service.clone();
        let authenticator = self.authenticator.clone();

        Box::pin(async move {
            // Extract token from Authorization header
            let token = match JwtAuth::extract_token(&req) {
                Some(token) => token,
                None => {
                    let response = HttpResponse::Unauthorized()
                        .json(serde_json::json!({
                            "error": "Authorization token required",
                            "message": "Please include 'Authorization: Bearer <token>' header"
                        }));
                    return Ok(req.into_response(response).map_into_right_body());
                }
            };

            // Validate token
            let validation_result = authenticator.validate_token(&token);
            if validation_result.valid {
                debug!("Token validated for user: {:?}", validation_result.client_id);

                // Create a proper claims structure from validation result
                // For client tokens, we need to decode it properly to get the expiration time
                let claims = if let Some(client_id) = validation_result.client_id {
                    // Try to decode the token again to get the actual expiration time
                    match authenticator.decode_client_token(&token) {
                        Ok(client_claims) => Claims {
                            sub: client_claims.username,
                            exp: client_claims.exp as usize,
                            roles: client_claims.roles,
                        },
                        Err(_) => {
                            // Fallback if decoding fails
                            Claims {
                                sub: client_id,
                                exp: (std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() + 3600) as usize, // 1 hour from now
                                roles: validation_result.permissions.clone(),
                            }
                        }
                    }
                } else if let Some(node_id) = validation_result.node_id {
                    // For node tokens
                    Claims {
                        sub: node_id,
                        exp: (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() + 3600) as usize,
                        roles: validation_result.permissions.clone(),
                    }
                } else {
                    // Unknown token type
                    Claims {
                        sub: "unknown".to_string(),
                        exp: 0,
                        roles: validation_result.permissions.clone(),
                    }
                };

                // Add user information to request extensions for use in handlers
                req.extensions_mut().insert(claims);

                // Continue to the actual service
                let res = service.call(req).await?;
                Ok(res.map_into_left_body())
            } else {
                warn!("Token validation failed: {:?}", validation_result.error_message);
                let response = HttpResponse::Unauthorized()
                    .json(serde_json::json!({
                        "error": "Invalid or expired token",
                        "message": validation_result.error_message.unwrap_or_else(|| "Please login again to get a new token".to_string())
                    }));
                Ok(req.into_response(response).map_into_right_body())
            }
        })
    }
}

/// Helper function to extract authenticated user from request
/// Use this function within handler functions where you have access to the HttpRequest
pub fn get_authenticated_user_claims(req: &actix_web::HttpRequest) -> Option<Claims> {
    req.extensions().get::<Claims>().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App, HttpResponse};

    async fn test_handler() -> Result<HttpResponse, Error> {
        Ok(HttpResponse::Ok().json(serde_json::json!({"message": "Success"})))
    }

    #[actix_rt::test]
    async fn test_jwt_middleware_no_token() {
        let auth_middleware = JwtAuth::new().expect("Failed to create auth middleware");

        let app = test::init_service(
            App::new()
                .wrap(auth_middleware)
                .route("/test", web::get().to(test_handler))
        )
        .await;

        let req = test::TestRequest::get().uri("/test").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);
    }

    #[actix_rt::test]
    async fn test_jwt_middleware_invalid_token() {
        let auth_middleware = JwtAuth::new().expect("Failed to create auth middleware");

        let app = test::init_service(
            App::new()
                .wrap(auth_middleware)
                .route("/test", web::get().to(test_handler))
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/test")
            .insert_header(("Authorization", "Bearer invalid-token"))
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);
    }
}
