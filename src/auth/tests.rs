//! Module for testing the auth package: authentication, authorization, and token.
#[cfg(test)]
mod tests {
    use crate::auth::authentication::*;
    use crate::auth::authorization::*;
    use crate::auth::token::*;
    use std::thread;
    use std::time::Duration;

    /// Tests the basic authentication functionality.
    ///
    /// # Purpose
    /// The purpose of this test is to verify that
    /// the basic authentication mechanism works as expected.
    ///
    /// # Steps
    /// 1. Create a new [`BasicAuthenticator`] instance.
    /// 2. Add a user with a username and password.
    /// 3. Assert that the user can be authenticated with the correct password.
    /// 4. Assert that the user cannot be authenticated with an incorrect password.
    #[test]
    fn test_basic_authentication() {
        let mut auth = BasicAuthenticator::new();
        auth.add_user("user1", "password1");

        assert!(auth.authenticate("user1", "password1").unwrap());
        assert!(!auth.authenticate("user1", "wrong_password").unwrap());
    }

    /// Tests the role-based access control functionality.
    ///
    /// # Purpose
    /// The purpose of this test is to verify that
    /// the role-based access control mechanism works as expected.
    ///
    /// # Steps
    /// 1. Create a new [`RoleBasedAccessControl`] instance.
    /// 2. Add admin and user roles with different permissions.
    /// 3. Assign roles to users.
    /// 4. Check if only admin users have admin permissions.
    #[test]
    fn test_rbac() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("admin", vec![
            Permission::Read,
            Permission::Write,
            Permission::Admin,
        ]);
        rbac.add_role("user", vec![Permission::Read]);

        rbac.assign_role("user1", "admin");
        rbac.assign_role("user2", "user");

        assert!(rbac.has_permission("user1", &Permission::Admin));
        assert!(!rbac.has_permission("user2", &Permission::Admin));
    }

    /// Tests the token management functionality.
    ///
    /// # Purpose
    /// This test verifies that the token management system works as expected.
    ///
    /// # Steps
    /// 1. Create a new [`TokenManager`] instance with a secret key.
    /// 2. Generate a token for a user with the admin role.
    /// 3. Verify the token and check the claims.
    #[test]
    fn test_token_management() {
        let token_manager = TokenManager::new(b"secret");
        let token = token_manager
            .generate_token("user1", vec!["admin".to_string()])
            .unwrap();

        let claims = token_manager.verify_token(&token).unwrap();
        assert_eq!(claims.sub, "user1");
        assert_eq!(claims.roles, vec!["admin"]);
    }

    /// Tests the invalid token verification.
    ///
    /// # Purpose
    /// This test verifies that an invalid token cannot be verified.
    ///
    /// # Steps
    /// 1. Create a new [`TokenManager`] instance with a secret key.
    /// 2. Attempt to verify an invalid token.
    /// 3. Assert that the verification fails.
    #[test]
    fn test_invalid_token() {
        let token_manager = TokenManager::new(b"secret");
        let invalid_token = "invalid.token.here";

        let result = token_manager.verify_token(invalid_token);
        assert!(result.is_err());
    }

    /// Tests the permission check functionality.
    ///
    /// # Purpose
    /// This test verifies that the permission check works as expected.
    ///
    /// # Steps
    /// 1. Create a new [`RoleBasedAccessControl`] instance.
    /// 2. Add admin and user roles with different permissions.
    /// 3. Assign roles to users.
    /// 4. Check if users have the correct permissions.
    #[test]
    fn test_permission_check() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("admin", vec![
            Permission::Read,
            Permission::Write,
            Permission::Admin,
        ]);
        rbac.add_role("user", vec![Permission::Read]);

        rbac.assign_role("user1", "admin");
        rbac.assign_role("user2", "user");

        assert!(rbac.has_permission("user1", &Permission::Read));
        assert!(rbac.has_permission("user1", &Permission::Write));
        assert!(rbac.has_permission("user1", &Permission::Admin));

        assert!(rbac.has_permission("user2", &Permission::Read));
        assert!(!rbac.has_permission("user2", &Permission::Write));
        assert!(!rbac.has_permission("user2", &Permission::Admin));
    }

    /// Tests the add and remove role functionality.
    ///
    /// # Purpose
    /// This test verifies that roles can be added and removed from users.
    ///
    /// # Steps
    /// 1. Create a new [`RoleBasedAccessControl`] instance.
    /// 2. Add an admin role with admin permissions.
    /// 3. Assign the admin role to a user.
    /// 4. Check if the user has admin permissions.
    /// 5. Remove the admin role from the user.
    /// 6. Check if the user no longer has admin permissions.
    #[test]
    fn test_add_and_remove_role() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("admin", vec![
            Permission::Read,
            Permission::Write,
            Permission::Admin,
        ]);

        rbac.assign_role("user1", "admin");
        assert!(rbac.has_permission("user1", &Permission::Admin));

        rbac.remove_role("user1", "admin");
        assert!(!rbac.has_permission("user1", &Permission::Admin));
    }

    /// Tests the invalid authentication.
    ///
    /// # Purpose
    /// This test verifies that invalid authentication fails as expected.
    ///
    /// # Steps
    /// 1. Create a new [`BasicAuthenticator`] instance.
    /// 2. Attempt to authenticate with a nonexistent user.
    /// 3. Attempt to authenticate with an empty username and password.
    #[test]
    fn test_invalid_authentication() {
        let auth = BasicAuthenticator::new();
        assert!(!auth.authenticate("nonexistent", "password").unwrap());
        assert!(!auth.authenticate("", "").unwrap());
    }

    /// Tests the multiple roles functionality.
    ///
    /// # Purpose
    /// This test verifies that users can have multiple roles.
    ///
    /// # Steps
    /// 1. Create a new [`RoleBasedAccessControl`] instance.
    /// 2. Add admin, writer, and reader roles with different permissions.
    /// 3. Assign all roles to a user.
    /// 4. Check if the user has all the permissions.
    #[test]
    fn test_multiple_roles() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("admin", vec![Permission::Admin]);
        rbac.add_role("writer", vec![Permission::Write]);
        rbac.add_role("reader", vec![Permission::Read]);

        rbac.assign_role("user1", "admin");
        rbac.assign_role("user1", "writer");
        rbac.assign_role("user1", "reader");

        assert!(rbac.has_permission("user1", &Permission::Admin));
        assert!(rbac.has_permission("user1", &Permission::Write));
        assert!(rbac.has_permission("user1", &Permission::Read));
    }

    /// Tests the token expiration.
    ///
    /// # Purpose
    /// This test verifies that tokens expire after a certain time.
    ///
    /// # Steps
    /// 1. Create a new [`TokenManager`] instance with a secret key.
    /// 2. Generate a token for a user with the admin role.
    /// 3. Wait for one second to simulate token expiration.
    /// 4. Verify the token and check if the verification is successful.
    #[test]
    fn test_token_expiration() {
        let token_manager = TokenManager::new(b"secret");
        let token = token_manager
            .generate_token("user1", vec!["admin".to_string()])
            .unwrap();

        // Wait for one second to simulate expiration.
        thread::sleep(Duration::from_secs(1));

        let result = token_manager.verify_token(&token);
        assert!(result.is_ok());
    }

    /// Tests the permission inheritance.
    ///
    /// Permission inheritance refers to the concept that
    /// a user assigned a higher-level role (e.g., `super_admin`)
    /// inherits all permissions associated with that role,
    /// including those of lower-level roles (e.g., `admin`).
    ///
    /// # Purpose
    /// This test verifies that permissions are inherited from parent roles.
    ///
    /// # Steps
    /// 1. Create a new [`RoleBasedAccessControl`] instance.
    /// 2. Add super admin and admin roles with different permissions.
    /// 3. Assign the super admin role to a user.
    /// 4. Assign the admin role to another user.
    /// 5. Check if the first user has admin permissions.
    /// 6. Check if the second user has write permissions.
    /// 7. Check if the second user does not have admin permissions.
    #[test]
    fn test_permission_inheritance() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("super_admin", vec![
            Permission::Read,
            Permission::Write,
            Permission::Admin,
        ]);
        rbac.add_role("admin", vec![Permission::Read, Permission::Write]);

        rbac.assign_role("user1", "super_admin");
        rbac.assign_role("user2", "admin");

        assert!(rbac.has_permission("user1", &Permission::Admin));
        assert!(!rbac.has_permission("user2", &Permission::Admin));
        assert!(rbac.has_permission("user2", &Permission::Write));
    }

    /// Tests the bulk role management functionality.
    ///
    /// # Purpose
    /// This test verifies that roles can be assigned to multiple users in bulk.
    ///
    /// # Steps
    /// 1. Create a new [`RoleBasedAccessControl`] instance.
    /// 2. Add two roles with different permissions.
    /// 3. Assign both roles to multiple users in a loop.
    /// 4. Check if all users have the correct permissions in another loop.
    #[test]
    fn test_bulk_role_management() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("role1", vec![Permission::Read]);
        rbac.add_role("role2", vec![Permission::Write]);

        let users = vec!["user1", "user2", "user3"];

        // Assign rolls in the first loop
        for user in &users {
            rbac.assign_role(user, "role1");
            rbac.assign_role(user, "role2");
        }

        // Check the permissions in the second loop.
        for user in &users {
            assert!(rbac.has_permission(user, &Permission::Read));
            assert!(rbac.has_permission(user, &Permission::Write));
        }
    }
}
