use pilgrimage::broker::Broker;
use pilgrimage::message::message::Message;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

fn main() {
    let broker = Arc::new(Broker::new("broker1", 3, 2, "storage_path"));
    let running = Arc::new(AtomicBool::new(true));
    let (tx, rx) = mpsc::channel();

    let broker_sender = Arc::clone(&broker);
    let sender_running = Arc::clone(&running);
    let sender_handle = thread::spawn(move || {
        for i in 0..10 {
            let content = format!("Message {}", i);
            let message = Message::from(content);
            println!("Send: ID={}, Content={}", message.id, message.content);

            if let Err(e) = broker_sender.send_message(message) {
                println!("Transmission error: {}", e);
                return Err(e);
            }
            thread::sleep(Duration::from_millis(100));
        }
        sender_running.store(false, Ordering::SeqCst);
        tx.send(()).unwrap();
        Ok(())
    });

    let broker_receiver = Arc::clone(&broker);
    let receiver_running = Arc::clone(&running);
    let receiver_handle = thread::spawn(move || {
        while receiver_running.load(Ordering::SeqCst) {
            if let Some(message) = broker_receiver.receive_message() {
                println!("Received: ID={}, Content={}", message.id, message.content);
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Waiting for transmission to be completed
    rx.recv().unwrap();

    // Give the remaining messages in the inbox time to process.
    thread::sleep(Duration::from_secs(1));
    running.store(false, Ordering::SeqCst);

    if let Err(e) = sender_handle.join() {
        println!("Transmission thread error: {:?}", e);
    }
    if let Err(e) = receiver_handle.join() {
        println!("Received thread error: {:?}", e);
    }
}
