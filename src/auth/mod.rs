//! Module for authentication, authorization, and token management.
//!
//! This module provides functionality for user authentication, role-based access control,
//! and token management.
//! * The [`authentication`] module provides functionality for authenticating users
//!   based on username and password.
//! * The [`authorization`] module provides functionality for managing roles and permissions.
//! * The [`token`] module provides functionality for managing JWT tokens.
//!
//! # Examples
//! Below we will demonstrate how to use the authentication, authorization, and token modules.
//! ```
//! use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
//! use pilgrimage::auth::authorization::{RoleBasedAccessControl, Permission};
//! use pilgrimage::auth::token::TokenManager;
//!
//! /*************************
//!  * Authentication example
//!  *************************/
//! // Create a new BasicAuthenticator instance
//! let mut authenticator = BasicAuthenticator::new();
//! // Add some users
//! authenticator.add_user("user1", "password");
//! authenticator.add_user("user2", "password");
//! // Authenticate users
//! assert!(authenticator.authenticate("user1", "password").unwrap());
//! assert!(!authenticator.authenticate("user1", "wrong_password").unwrap());
//! assert!(!authenticator.authenticate("user3", "password").unwrap());
//!
//! /*************************
//!  * Authorization example
//!  *************************/
//! // Create a new role-based access control instance
//! let mut rbac = RoleBasedAccessControl::new();
//! // Create a role and a user
//! let role = "guest";
//! let user = "user1";
//! // Add your custom role with the required permissions
//! rbac.add_role(role, vec![ Permission::Read ]);
//! // Assign the role to a user
//! rbac.assign_role(user, role);
//! // Check if the user has the required permission
//! assert!(rbac.has_permission(user, &Permission::Read));
//! // Finally, remove the role from the user
//! rbac.revoke_role(user, role);
//! // Check if the user still has the required permission
//! assert!(!rbac.has_permission(user, &Permission::Read));
//!
//! /**************************
//!  * Token management example
//!  **************************/
//! // Generate a new token manager with the secret key
//! let token_manager = TokenManager::new(b"MySuperSecret");
//! // Generate a new token for the user "admin" with the role "admin"
//! let token = token_manager.generate_token("admin", vec!["admin".to_string()]).unwrap();
//! // Verify the token and get the claims
//! let claims = token_manager.verify_token(&token).unwrap();
//! // Check the claims of the token (username, roles)
//! assert_eq!(claims.sub, "admin");
//! assert_eq!(claims.roles, vec!["admin".to_string()]);
//! ```

pub mod audit;
pub mod authentication;
pub mod authorization;
pub mod jwt_auth;
pub mod token;

#[cfg(test)]
mod tests;

pub use audit::{
    AuditConfig, AuditEvent, AuditEventType, AuditLogLevel, AuditLogger, AuditQuery,
    AuditStatistics, EventOutcome,
};
pub use authentication::{Authenticator, BasicAuthenticator};
pub use jwt_auth::{AuthenticationResult, DistributedAuthenticator, ValidationResult};
pub use token::TokenManager;
