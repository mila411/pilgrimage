use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
use pilgrimage::auth::authorization::{Permission, RoleBasedAccessControl};
use pilgrimage::auth::token::TokenManager;
use pilgrimage::broker::Broker;
use pilgrimage::crypto::Encryptor;
use pilgrimage::message::message::Message;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut authenticator = BasicAuthenticator::new();
    authenticator.add_user("user1", "password1");

    let mut rbac = RoleBasedAccessControl::new();
    rbac.add_role(
        "admin",
        vec![Permission::Read, Permission::Write, Permission::Admin],
    );
    rbac.assign_role("user1", "admin");

    let token_manager = TokenManager::new(b"secret");
    let username = "user1";
    let password = "password1";

    // 32-byte encryption key generation
    let key: [u8; 32] = rand::random();
    let encryptor = Encryptor::new(&key);

    // Authentication process
    if authenticator.authenticate(username, password)? {
        println!("User {} authentication successful", username);

        let roles = vec!["admin".to_string()];
        let _token = token_manager.generate_token(username, roles)?;

        let broker = Broker::new("broker1", 1, 5, "storage");
        let message = "Secret Message";

        // Encrypting and sending messages
        let encrypted_data = encryptor.encrypt(message.as_bytes())?;
        let message = Message::new(String::from_utf8_lossy(&encrypted_data).to_string());

        broker.send_message(message)?;
        println!("Encrypted message sent.");
    }

    Ok(())
}
