use pilgrimage::broker::Broker;
use pilgrimage::message::message::Message;
use pilgrimage::schema::registry::SchemaRegistry;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    // Create a schema registry
    let schema_registry = SchemaRegistry::new();
    let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    schema_registry
        .register_schema("test_topic", schema_def)
        .unwrap();

    // Create a broker
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs")));

    // Create a topic
    {
        let mut broker = broker.lock().unwrap();
        broker.create_topic("test_topic", Some(1)).unwrap();
    }

    // Create a subscriber
    let broker_clone = Arc::clone(&broker);
    let _subscriber = thread::spawn(move || {
        loop {
            let mut broker = broker_clone.lock().unwrap();
            if let Some(message) = broker.receive_message() {
                println!("Received: ID={}, Content={}", message.id, message.content);
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Send a message
    {
        let mut broker = broker.lock().unwrap();
        let message = Message::new("Hello, world!".to_string());
        println!("Send: ID={}, Content={}", message.id, message.content);
        broker.send_message(message).unwrap();
    }

    // Give the remaining messages in the inbox time to process.
    thread::sleep(Duration::from_secs(1));
}
