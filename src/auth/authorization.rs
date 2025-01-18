//! Module for managing role-based access control.
//!
//! This module provides functionality for managing roles and permissions.
//!
//! The [`RoleBasedAccessControl`] struct is used to manage roles and permissions.
//!
//! The [`Permission`] enum represents the permissions that can be assigned to a role.
//!
//! # Example
//! Below we will demonstrate how to use the [`RoleBasedAccessControl`]
//! structure to manage roles and permissions.
//! ```
//! use crate::pilgrimage::auth::authorization::{RoleBasedAccessControl, Permission};
//!
//! // Create a new role-based access control instance
//! let mut rbac = RoleBasedAccessControl::new();
//!
//! // Create a role and a user
//! let role = "guest";
//! let user = "user1";
//!
//! // Add your custom role with the required permissions
//! rbac.add_role(role, vec![
//!     Permission::Read
//! ]);
//!
//! // Assign the role to a user
//! rbac.assign_role(user, role);
//!
//! // Check if the user has the required permission
//! assert!(rbac.has_permission(user, &Permission::Read));
//!
//! // Finally, remove the role from the user
//! rbac.remove_role(user, role);
//!
//! // Check if the user still has the required permission
//! assert!(!rbac.has_permission(user, &Permission::Read));
//! ```

use std::collections::HashMap;

/// Enum representing the permissions that can be assigned to a role.
///
/// The enum can be cloned and compared for equality.
#[derive(Debug, Clone, PartialEq)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

/// A struct for managing role-based access control.
///
/// The struct can be used to add roles with permissions,
/// assign roles to users, and check if a user has a specific permission.
///
/// # Fields
/// * `roles`: A HashMap containing the roles and their associated [`Permission`].
/// * `user_roles`: A HashMap containing the users and their assigned roles.
pub struct RoleBasedAccessControl {
    /// A HashMap containing the roles and their associated permissions.
    roles: HashMap<String, Vec<Permission>>,
    /// A HashMap containing the users and their assigned roles.
    user_roles: HashMap<String, Vec<String>>,
}

/// The implementation of the `RoleBasedAccessControl` struct offers methods for adding roles,
/// assigning roles to users, and checking permissions.
///
/// The struct can be created with the [`RoleBasedAccessControl::new`] method,
/// and roles can be added with the [`RoleBasedAccessControl::add_role`] method.
///
/// The roles that are added can be assigned to users with the
/// [`RoleBasedAccessControl::assign_role`] method.
///
/// Also, the permissions of a user can be checked with the
/// [`RoleBasedAccessControl::has_permission`] method.
///
/// Finally, roles can be removed from users with the
/// [`RoleBasedAccessControl::remove_role`] method.
impl RoleBasedAccessControl {
    /// Creates a new instance of `RoleBasedAccessControl`.
    ///
    /// # Returns
    /// * A new `RoleBasedAccessControl` instance with empty roles and user roles stores.
    pub fn new() -> Self {
        Self {
            roles: HashMap::new(),
            user_roles: HashMap::new(),
        }
    }

    /// Adds a new role with the specified permissions.
    ///
    /// # Arguments
    /// * `role`: A string slice representing the role name.
    ///           Each role should have a unique name,
    ///           if a role with the same name already exists, it will be overwritten.
    /// * `permissions`: A vector of [`Permission`] associated with the role.
    ///
    /// # Warning
    /// If a role with the same name already exists, it will be overwritten.
    pub fn add_role(&mut self, role: &str, permissions: Vec<Permission>) {
        self.roles.insert(role.to_string(), permissions);
    }

    /// Assigns a role to a user.
    ///
    /// If the user already has roles assigned, the new role will be added to the existing roles.
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `role`: A string slice representing the role name.
    pub fn assign_role(&mut self, username: &str, role: &str) {
        self.user_roles
            .entry(username.to_string())
            .or_default()
            .push(role.to_string());
    }

    /// Removes a role from a user.
    ///
    /// If the user has multiple roles assigned, only the specified role will be removed.
    ///
    /// If the user does not have the specified role, nothing will happen.
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `role`: A string slice representing the role name.
    pub fn remove_role(&mut self, username: &str, role: &str) {
        if let Some(roles) = self.user_roles.get_mut(username) {
            roles.retain(|r| r != role);
        }
    }

    /// Checks if a user has the required permission.
    ///
    /// # Arguments
    /// * `username`: A string slice representing the username.
    /// * `required_permission`: A reference to the [`Permission`] that the user should have.
    ///
    /// # Returns
    /// * `bool`: A boolean indicating whether the user has the required permission.
    ///           Returns `true` if the user has the required permission,
    ///           `false` in all other cases.
    pub fn has_permission(&self, username: &str, required_permission: &Permission) -> bool {
        self.user_roles.get(username).is_some_and(|roles| {
            roles.iter().any(|role| {
                self.roles
                    .get(role)
                    .is_some_and(|permissions| permissions.contains(required_permission))
            })
        })
    }
}

impl Default for RoleBasedAccessControl {
    /// Creates a default instance of `RoleBasedAccessControl`.
    ///
    /// # Returns
    /// * A new `RoleBasedAccessControl` instance with empty roles and user roles stores.
    fn default() -> Self {
        Self::new()
    }
}
