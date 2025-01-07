use std::fmt::{self, Debug};
use std::sync::Arc;

pub struct Subscriber {
    pub id: String,
    pub callback: Arc<Box<dyn Fn(String) + Send + Sync>>,
}

impl Clone for Subscriber {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            callback: Arc::clone(&self.callback),
        }
    }
}

impl Debug for Subscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriber").field("id", &self.id).finish()
    }
}

impl Subscriber {
    pub fn new<S: Into<String>>(id: S, callback: Box<dyn Fn(String) + Send + Sync>) -> Self {
        Self {
            id: id.into(),
            callback: Arc::new(callback),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_new() {
        let message_received = Arc::new(std::sync::Mutex::new(false));
        let message_clone = message_received.clone();

        let callback = Box::new(move |_| {
            let mut received = message_clone.lock().unwrap();
            *received = true;
        });

        let subscriber = Subscriber::new("test-id", callback);
        assert_eq!(subscriber.id, "test-id");
    }

    #[test]
    fn test_subscriber_clone() {
        let subscriber = Subscriber::new("test-id", Box::new(|_| {}));
        let cloned = subscriber.clone();

        assert_eq!(subscriber.id, cloned.id);
    }
}
