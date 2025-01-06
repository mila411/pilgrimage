use aes::Aes256;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use rand::Rng;
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// Used for encryption/decryption
type Aes256Cbc = Cbc<Aes256, Pkcs7>;

async fn start_server(key: Arc<Vec<u8>>, iv: Arc<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:8085").await?;
    println!("Server listening on 127.0.0.1:8085");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        let key = Arc::clone(&key);
        let iv = Arc::clone(&iv);

        tokio::spawn(async move {
            println!("Client connected: {}", addr);
            let mut buf = vec![0u8; 1024];
            if let Ok(n) = socket.read(&mut buf).await {
                if n == 0 {
                    return;
                }
                let encrypted_data = &buf[..n];
                let cipher = Aes256Cbc::new_from_slices(&key, &iv).unwrap();
                let decrypted = cipher.clone().decrypt_vec(encrypted_data).unwrap();
                let msg = String::from_utf8_lossy(&decrypted);
                println!("Decrypted message: {}", msg);

                let response = b"Hello from server";
                let encrypted_response = cipher.clone().encrypt_vec(response);
                let _ = socket.write_all(&encrypted_response).await;
            }
        });
    }
}

async fn start_client(key: Arc<Vec<u8>>, iv: Arc<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:8085").await?;
    let cipher = Aes256Cbc::new_from_slices(&key, &iv).unwrap();
    let msg = b"Hello from client";
    let encrypted_data = cipher.clone().encrypt_vec(msg);
    stream.write_all(&encrypted_data).await?;
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let decrypted = cipher.clone().decrypt_vec(&buf[..n]).unwrap();
    println!(
        "Decrypted response: {}",
        String::from_utf8_lossy(&decrypted)
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut key = vec![0u8; 32];
    rand::thread_rng().fill(&mut key[..]);
    let mut iv = vec![0u8; 16];
    rand::thread_rng().fill(&mut iv[..]);

    let key = Arc::new(key);
    let iv = Arc::new(iv);
    let server_key = Arc::clone(&key);
    let server_iv = Arc::clone(&iv);

    let _server_handle = tokio::spawn(async move {
        let _ = start_server(server_key, server_iv).await;
    });

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let _client_handle = tokio::spawn(async move {
        let _ = start_client(key, iv).await;
    });

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    Ok(())
}
