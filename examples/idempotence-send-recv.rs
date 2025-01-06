use pilgrimage::broker::Broker;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep, timeout};

#[tokio::main]
async fn main() {
    let broker = Arc::new(Mutex::new(
        Broker::new("broker1", 3, 2, "storage_path").await,
    ));

    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let receiver_handle = tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:8081").await.unwrap();
        println!("The message recipient is waiting on port 8081.");
        ready_tx.send(()).unwrap();

        for _ in 0..10 {
            match timeout(Duration::from_secs(10), listener.accept()).await {
                Ok(Ok((mut socket, _))) => {
                    let mut buf = [0; 1024];
                    match socket.read(&mut buf).await {
                        Ok(n) => {
                            let msg = String::from_utf8_lossy(&buf[..n]).trim().to_string();
                            println!("Received message: {}", msg);

                            // ACK transmission
                            let ack_port = 8083; // Direct specification
                            match TcpStream::connect(format!("127.0.0.1:{}", ack_port)).await {
                                Ok(mut ack_socket) => {
                                    if let Err(e) = ack_socket.write_all(b"ACK").await {
                                        println!("ACK transmission error: {}", e);
                                    } else {
                                        println!("ACK sent to port {}.", ack_port);
                                    }
                                }
                                Err(e) => {
                                    println!(
                                        "Could not connect to ACK transmission destination: {}",
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            println!("Message reading error: {}", e);
                        }
                    }
                }
                Ok(Err(e)) => println!("Connection acceptance error: {}", e),
                Err(_) => println!("Connection acceptance timeout"),
            }
        }
    });

    ready_rx.recv().unwrap();
    sleep(Duration::from_secs(1)).await;

    let broker_sender = Arc::clone(&broker);
    let sender_handle = tokio::spawn(async move {
        for i in 0..10 {
            let message = format!("Message {}", i);
            let mut broker = broker_sender.lock().await;
            match broker.send_message(message.clone()).await {
                Ok(_) => println!("Message sent, ACK received: {}", message),
                Err(e) => println!("Error: {}", e),
            }
            sleep(Duration::from_millis(500)).await;
        }
    });

    tokio::try_join!(sender_handle, receiver_handle).unwrap();
}
