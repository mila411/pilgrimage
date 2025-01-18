//! Module for authentication, authorization, and token management.
//!
//! This module provides functionality for user authentication, role-based access control,
//! and token management.
//! * The [`authentication`] module provides functionality for authenticating users
//!   based on username and password.
//! * The [`authorization`] module provides functionality for managing roles and permissions.
//! * The [`token`] module provides functionality for managing JWT tokens.

pub mod authentication;
pub mod authorization;
pub mod token;

#[cfg(test)]
mod tests;
