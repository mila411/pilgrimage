use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

// Simple authentication function
struct AuthService;

impl AuthService {
    async fn authenticate(username: &str, password: &str) -> bool {
        // Simple authentication logic
        username == "user1" && password == "password1"
    }
}

// Message Broker
struct Broker {
    sender: mpsc::Sender<String>,
    receiver: Arc<Mutex<mpsc::Receiver<String>>>,
}

impl Broker {
    async fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Broker {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }

    async fn send_message(&self, message: String) -> Result<(), mpsc::error::SendError<String>> {
        self.sender.send(message).await
    }

    async fn receive_message(&self) -> Option<String> {
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await
    }
}

#[tokio::main]
async fn main() {
    let username = "user1";
    let password = "password1";

    // User authentication
    if AuthService::authenticate(username, password).await {
        println!("User '{}' authenticated successfully.", username);

        let broker = Arc::new(Broker::new().await);

        // Message sending task
        let broker_clone = broker.clone();
        let sender_task = tokio::spawn(async move {
            let message = format!("Hello from {}", username);
            if let Err(e) = broker_clone.send_message(message).await {
                eprintln!("Failed to send message: {}", e);
            } else {
                println!("Message sent successfully.");
            }
        });

        // Message Receiving Task
        let broker_clone = broker.clone();
        let receiver_task = tokio::spawn(async move {
            if let Some(message) = broker_clone.receive_message().await {
                println!("Received message: {}", message);
            } else {
                println!("No messages available.");
            }
        });

        // Waiting for task completion
        let _ = tokio::join!(sender_task, receiver_task);
    } else {
        println!("Authentication failed for user '{}'.", username);
    }
}
