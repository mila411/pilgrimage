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
            let broker = broker_clone.lock().unwrap();
            if let Some(message) = broker.receive_message() {
                println!("Received: ID={}, Content={}", message.id, message.content);
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Send a message
    {
        let broker = broker.lock().unwrap();
        let message = Message::new("Hello, world!".to_string());
        println!("Send: ID={}, Content={}", message.id, message.content);
        broker.send_message(message).unwrap();
    }

    // Give the remaining messages in the inbox time to process.
    thread::sleep(Duration::from_secs(1));
}
```

### Multi-threaded message processing

```rust
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

```

### Examples

To execute a basic example, use the following command:

```bash
cargo run --example ack-send-recv
cargo run --example auth-example
cargo run --example auth-send-recv
cargo run --example simple-send-recv
cargo run --example thread-send-recv
cargo run --example transaction-send-recv
```

### Bench

If the allocated memory is small, it may fail.

```sh
Gnuplot not found, using plotters backend
send_message            time:   [849.75 ns 851.33 ns 853.18 ns]
                        change: [-23.318% -20.131% -18.021%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

send_benchmark_message  time:   [850.69 ns 852.08 ns 853.67 ns]
                        change: [-33.444% -26.669% -22.423%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
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
