use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
use pilgrimage::auth::authorization::{Permission, RoleBasedAccessControl};
use pilgrimage::auth::token::TokenManager;

fn main() {
    // Authentication Setup
    let mut authenticator = BasicAuthenticator::new();
    authenticator.add_user("user1", "password1");

    // Approval Setup
    let mut rbac = RoleBasedAccessControl::new();
    rbac.add_role(
        "admin",
        vec![Permission::Read, Permission::Write, Permission::Admin],
    );
    rbac.add_role("user", vec![Permission::Read]);
    rbac.assign_role("user1", "admin");

    // Token Manager Setup
    let token_manager = TokenManager::new(b"secret");

    // User Authentication
    let username = "user1";
    let password = "password1";
    if authenticator.authenticate(username, password).unwrap() {
        println!("User {} authenticated successfully", username);

        // Generating Tokens
        let roles = vec!["admin".to_string()];
        let token = token_manager
            .generate_token(username, roles.clone())
            .unwrap();
        println!("Generated token: {}", token);

        // Token verification
        let claims = token_manager.verify_token(&token).unwrap();
        println!("Token verified for user: {}", claims.sub);

        // Approval confirmation
        if rbac.has_permission(&claims.sub, &Permission::Admin) {
            println!("User {} has admin permission", claims.sub);
        } else {
            println!("User {} does not have admin permission", claims.sub);
        }
    } else {
        println!("Authentication failed for user {}", username);
    }
}
