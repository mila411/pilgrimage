<p align="center">
  <img src=".github/images/logo.png" alt="logo" width=65%>
</p>

<p align="center">
    <em>
        I will draw a picture of a crab someday
    </em>
</p>

<p align="center">
    <a href="https://blog.rust-lang.org/2024/11/28/Rust-1.83.0.html">
      <img src="https://img.shields.io/badge/Rust-1.83-007ACC.svg?logo=Rust">
    </a>
    <a href="https://codecov.io/gh/mila411/pilgrimage">
      <img src="https://codecov.io/gh/mila411/pilgrimage/graph/badge.svg?token=HVMZX0580X"/>
    </a>
    <a href="https://app.deepsource.com/gh/mila411/pilgrimage/" target="_blank">
      <img alt="DeepSource" title="DeepSource" src="https://app.deepsource.com/gh/mila411/pilgrimage.svg/?label=active+issues&show_trend=true&token=tsauTwVl8Nd7UH7xuQCtLR9H"/>
    </a>
</p>

# pilgrimage

This is a Rust implementation of a distributed messaging system. It uses a simple design inspired by Apache Kafka. It simply records messages to local files.

Current Pilgrimage supports **At-least-once**.

## Security

When using Pilgramage as a Crate, client authentication is implemented, but at present, authentication is not implemented for message sending and receiving from the CLI and web client. You can find a sample of authentication with Crate `examples/auth-example.rs`, `examples/auth-send-recv.rs`.


## Features

- Topic-based pub/sub model
- Scalability through partitioning
- Persistent messages (log file based)
- Leader/Follower Replication
- Fault Detection and Automatic Recovery
- Delivery guaranteed by acknowledgement (ACK)
- Fully implemented leader selection mechanism
- Partition Replication
- Persistent messages
- Schema Registry for managing message schemas and ensuring compatibility
- Automatic Scaling
- Broker Clustering
- Message processing in parallel
- Authentication and Authorization Mechanisms
- Data Encryption
- CLI based console
- WEB based console

## Basic Usage

### Dependency

- Rust 1.51.0 or later

### Functionality Implemented

- **Message Queue**: Efficient message queue implementation using `Mutex` and `VecDeque`.
- **Broker**: Core broker functionality including message handling, node management, and leader election.
- **Consumer Groups**: Support for consumer groups to allow multiple consumers to read from the same topic.
- **Leader Election**: Mechanism for electing a leader among brokers to manage partitions and replication.
- **Storage**: Persistent storage of messages using local files.
- **Replication**: Replication of messages across multiple brokers for fault tolerance.
- **Schema Registry**: Management of message schemas to ensure compatibility between producers and consumers.
- **Benchmarking**: Comprehensive benchmarking tests to measure performance of various components.
- **Automatic Scaling:** Automatically scale the number of instances based on load.
- **Log Compressions:** Compress and optimize logs.

### Basic usage

```rust
use pilgrimage::broker::{Broker, Message};
use std::fs;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    // Create the necessary directories and files
    let log_dir = "data/broker1/logs";
    let log_file = "data/broker1/logs/messages.log";
    let processed_message_ids_file = "data/broker1/logs/processed_message_ids.txt";

    fs::create_dir_all(log_dir).expect("Failed to create directory");
    fs::File::create(log_file).expect("Failed to create log file.");
    fs::File::create(processed_message_ids_file)
        .expect("Failed to create processed_message_ids.txt");

    // Checking the existence of a file
    if !std::path::Path::new(log_file).exists() {
        eprintln!("The log file does not exist.: {}", log_file);
        return;
    }
    if !std::path::Path::new(processed_message_ids_file).exists() {
        eprintln!(
            "processed_message_ids.txt does not exist: {}",
            processed_message_ids_file
        );
        return;
    }

    // Broker initialization
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, log_file).await));

    // Send a message
    let broker_producer = Arc::clone(&broker);
    let handle = tokio::spawn(async move {
        let broker_lock = broker_producer.lock().await;

        // Start transaction
        let mut transaction = broker_lock.begin_transaction();

        // Message generation
        let message_content = "Hello, Pilgrimage!".to_string();
        let message = Message::new(message_content.clone());

        // Send a message (send without waiting for ACK)
        match broker_lock
            .send_message_transaction(&mut transaction, message)
            .await
        {
            Ok(_) => {
                if let Err(e) = transaction.commit() {
                    eprintln!("Transaction commit failed.: {:?}", e);
                } else {
                    println!("Message sent.: {}", message_content);
                }
            }
            Err(e) => eprintln!("Failed to send message: {:?}", e),
        }
    });

    // Waiting for the transmission task to finish
    handle.await.unwrap();
}

```

### Exactly-once send messsage

```rust
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
```

### Examples

- Authentication processing example
- User authentication and message sending
- Sending encrypted messages
- Sending and receiving simple messages
- Exactly-once Sending and receiving messages

To execute a basic example, use the following command:

```bash
cargo run --example auth-example
cargo run --example auth-send-recv
cargo run --example encrypt-send-recv
cargo run --example idempotencw-send-recv
cargo run --example simple-send-recv
```

### Bench

If the allocated memory is small, it may fail.

```sh
message_processing/batch_messages/10
                        time:   [86.418 ms 87.079 ms 87.433 ms]
                        change: [+17.513% +18.664% +19.750%] (p = 0.00 < 0.05)
                        Performance has regressed.

message_processing/batch_messages/50
                        time:   [448.69 ms 451.67 ms 454.84 ms]
                        change: [+8.5546% +11.944% +14.329%] (p = 0.00 < 0.05)
                        Performance has regressed.

Benchmarking message_processing/batch_messages/100: Warming up for 3.0000 s
message_processing/batch_messages/100
                        time:   [941.48 ms 948.28 ms 954.95 ms]
                        change: [+11.767% +12.760% +13.740%] (p = 0.00 < 0.05)
                        Performance has regressed.
```

`cargo bench`

## CLI Features

Pilgrimage offers a comprehensive Command-Line Interface (CLI) to manage and interact with your message brokers efficiently. Below are the available commands along with their descriptions and usage examples.

### start

**Description:**
Starts the broker with the specified configurations.

**Usage:**

```bash
pilgrimage start --id <BROKER_ID> --partitions <NUMBER_OF_PARTITIONS> --replication <REPLICATION_FACTOR> --storage <STORAGE_PATH> [--test-mode]
```

**Options:**

- `--id`, `-i` (required): Sets the broker ID.
- `--partitions`, `-p` (required): Sets the number of partitions.
- `--replication`, `-r` (required): Sets the replication factor.
- `--storage`, `-s` (required): Sets the storage path.
- `--test-mode`: Runs the broker in test mode, which breaks out of the main loop quickly for testing purposes.

**Example:**

`pilgrimage start --id broker1 --partitions 3 --replication 2 --storage /data/broker1 --test-mode`

### stop

**Description:**
Stops the specified broker.

**Usage**

`pilgrimage stop --id <BROKER_ID>`

**Options:**

- `--id`, `-i` (required): Sets the broker ID.

**Example**

`pilgrimage stop --id broker1`

### send

**Description:**

Sends a message to the specified broker.

**Usage**

`pilgrimage send <BROKER_ID> <MESSAGE>`

**Arguments:**

- `<BROKER_ID>` (required): The ID of the broker to send the message to.
- `<MESSAGE>` (required): The message content to send.

**Example**

`pilgrimage send broker1 "Hello, World!"`

### consume

**Description:**

Consumes messages from the specified broker.

**Usage**

`pilgrimage consume <BROKER_ID>`

**Arguments:**

- `<BROKER_ID>` (required): The ID of the broker to consume messages from.

**Example:**

`pilgrimage consume broker1`

### status

**Description:**

Checks the status of the specified broker.

**Usage:**

`pilgrimage status --id <BROKER_ID>`

**Options:**
- `--id`, `-i` (required): Sets the broker ID.

**Example:**

`pilgrimage status --id broker1`

### Additional Information

- **Help Command:**
    To view all available commands and options, use the `help` command:

`pilgrimage help`

- **Version Information:**
    To check the current version of Pilgrimage, use:

`pilgrimage --version`

### Running the CLI

To start the web server:

```sh
cargo run --bin pilgrimage
```

## Web Console API

Pilgrimage provides a REST API for managing brokers through HTTP requests. The server runs on `http://localhost:8080` by default.

### Available Endpoints

#### Start Broker

Starts a new broker instance.

**Endpoint:** `POST /start`
**Request:**

```json
{
    "id": "broker1",
    "partitions": 3,
    "replication": 2,
    "storage": "/tmp/broker1"
}
```

**Example:**

```sh
curl -X POST http://localhost:8080/start \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1",
    "partitions": 3,
    "replication": 2,
    "storage": "/tmp/broker1"
  }'
```

#### Stop Broker

Stops a running broker instance.

**Endpoint**: `POST /stop`
**Request:**

```json
{
    "id": "broker1"
}
```

**Example:**

```sh
curl -X POST http://localhost:8080/stop \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1"
  }'
```

#### Send Message

Sends a message to the broker.

**Endpoint**: `POST /send`
**Request:**

```json
{
    "id": "broker1",
    "message": "Hello, World!"
}
```

**Example:**

```sh
curl -X POST http://localhost:8080/send \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1",
    "message": "Hello, World!"
  }'
```

#### Consume Messages

Consumes messages from the broker.

**Endpoint**: `POST /consume`

**Request:**

```sh
{
    "id": "broker1"
}
```

**Example:**

```sh
curl -X POST http://localhost:8080/consume \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1"
  }'
```

#### Check Status

Checks the status of the broker.

**Endpoint**: `POST /status`

`Request:`

```sh
{
    "id": "broker1"
}
```

**Example:**

```sh
curl -X POST http://localhost:8080/status \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1"
  }'
```

### Running the Web Server

To start the web server:

```sh
cargo run --bin web
```

The server will be available at `http://localhost:8080`.

## Version increment on release

- The commit message is parsed and the version of either major, minor or patch is incremented.
- The version of Cargo.toml is updated.
- The updated Cargo.toml is committed and a new tag is created.
- The changes and tag are pushed to the remote repository.

The version is automatically incremented based on the commit message. Here, we treat `feat` as minor, `fix` as patch, and `BREAKING CHANGE` as major.

### License

MIT
