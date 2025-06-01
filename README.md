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

<h1>pilgrimage</h1>

Pilgrimage is a Rust implementation of a distributed messaging system inspired by Apache Kafka. It records messages to local files and supports **At-least-once** and **Exactly-once** delivery semantics.

--------------------

<h2>Table of contents</h2>

- [Installation](#installation)
- [Security](#security)
- [Features](#features)
- [Basic Usage](#basic-usage)
  - [Dependency](#dependency)
  - [Functionality Implemented](#functionality-implemented)
  - [Examples](#examples)
  - [Benchmarks](#benchmarks)
    - [execution method](#execution-method)
    - [Benchmark Category](#benchmark-category)
    - [Checking Reports](#checking-reports)
- [CLI Features](#cli-features)
  - [start](#start)
  - [stop](#stop)
  - [send](#send)
  - [consume](#consume)
  - [status](#status)
  - [schema](#schema)
  - [Additional Information](#additional-information)
  - [Running the CLI](#running-the-cli)
- [Web Console API](#web-console-api)
  - [Available Endpoints](#available-endpoints)
    - [Start Broker](#start-broker)
    - [Stop Broker](#stop-broker)
    - [Send Message](#send-message)
    - [Consume Messages](#consume-messages)
    - [Check Status](#check-status)
  - [Running the Web Server](#running-the-web-server)
- [Version increment on release](#version-increment-on-release)
  - [License](#license)

--------------------

## Installation

To use Pilgrimage, add the following to your `Cargo.toml`:

```toml
[dependencies]
pilgrimage = "0.14.0"
```

--------------------

## Security

When using Pilgramage as a Crate, client authentication is implemented, but at present, authentication is not implemented for message sending and receiving from the CLI and web client. You can find a sample of authentication with Crate `examples/auth-example.rs`, `examples/auth-send-recv.rs`.

--------------------

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
- Support AMQP

--------------------

## Basic Usage

```rust
use pilgrimage::broker::{Broker, MessageSchema, TopicConfig};
use pilgrimage::schema::registry::SchemaRegistry;
use chrono::Utc;
use uuid::Uuid;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a schema registry
    let schema_registry = SchemaRegistry::new();
    let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    schema_registry.register_schema("test_topic", schema_def)?;

    // Creating a Broker
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs")));

    // Creating topics (setting the number of partitions and replication factors)
    {
        let mut broker = broker.lock().unwrap();
        let config = TopicConfig {
            num_partitions: 2,
            replication_factor: 1,
            ..Default::default()
        };
        broker.create_topic("test_topic", Some(config))?;
    }

    // Message Receiving Thread
    let broker_clone = Arc::clone(&broker);
    let receiver = tokio::spawn(async move {
        for _ in 0..5 {
            if let Ok(broker) = broker_clone.lock() {
                if let Ok(Some(message)) = broker.receive_message("test_topic", 0) {
                    println!("reception: {}", message.content);
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    });

    // Message Sending Process
    for i in 1..=5 {
        let message = MessageSchema::new()
            .with_content(format!("Message {}", i))
            .with_topic("test_topic".to_string())
            .with_partition(0);

        if let Ok(mut broker) = broker.lock() {
            broker.send_message(message)?;
            println!("Send: Message {}", i);
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Wait for receiving thread to terminate
    receiver.await?;
    Ok(())
}
```

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

### Examples

To execute a basic example, use the following command:

```bash
cargo run --example ack-transaction
cargo run --example amqp-send-recv
cargo run --example auth-send-recv
cargo run --example batch-transaction
cargo run --example broker-integration-test
cargo run --example idempotency-test
cargo run --example improved-transaction
cargo run --example persistent-ack
```

### Benchmarks

Pilgrimage includes a comprehensive suite of benchmarks to measure performance in a variety of scenarios.

#### execution method

```bash
cargo bench
```

#### Benchmark Category

1. **Message Sending** - Transmission performance with different message sizes
   - Small messages (~12 bytes): ~6.0 µs
   - Medium messages (1KB): ~16.2 µs
   - Large messages (10KB): ~19.6 µs

2. **Message Receiving** - Message reception performance
   - Average receive time: ~82.7 µs

3. **Topic Operations** - Topic Management Operations
   - Topic creation: ~1.6 µs
   - Topic listing: ~652 ms (warning: slow operation)

4. **Partition Operations** - Transmission performance by partition
   - 1 partition: ~7.2 µs
   - 2 partitions: ~13.9 µs
   - 4 partitions: ~28.5 µs
   - 8 partitions: ~54.7 µs

5. **Concurrent Operations** - Parallel transmission and reception performance
   - Send + Receive: ~5.5 ms

6. **Throughput Testing** - Batch Processing Performance
   - 10 messages: ~69.0 µs
   - 100 messages: ~693 µs
   - 1000 messages: ~6.7 ms

#### Checking Reports

After the benchmark is run, a detailed HTML report is generated in `target/criterion/report/index.html`.

If the allocated memory is small, it may fail.

```sh
Gnuplot not found, using plotters backend
Benchmarking message_sending/send_message/small: Collecting 100 samples in estimated                                                                                    message_sending/send_message/small
                        time:   [5.9308 µs 6.0175 µs 6.1085 µs]
Found 7 outliers among 100 measurements (7.00%)
  5 (5.00%) high mild
  2 (2.00%) high severe
Benchmarking message_sending/send_message/medium: Collecting 100 samples in estimate                                                                                    message_sending/send_message/medium
                        time:   [15.097 µs 16.234 µs 17.513 µs]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
Benchmarking message_sending/send_message/large: Collecting 100 samples in estimated                                                                                    message_sending/send_message/large
                        time:   [19.128 µs 19.556 µs 20.154 µs]
Found 13 outliers among 100 measurements (13.00%)
  2 (2.00%) low mild
  5 (5.00%) high mild
  6 (6.00%) high severe

Benchmarking message_receiving/receive_message: Collecting 100 samples in estimated                                                                                     message_receiving/receive_message
                        time:   [82.541 µs 82.696 µs 82.857 µs]
Found 11 outliers among 100 measurements (11.00%)
  1 (1.00%) low severe
  1 (1.00%) low mild
  8 (8.00%) high mild
  1 (1.00%) high severe

Benchmarking topic_operations/create_topic: Collecting 100 samples in estimated 5.00                                                                                    topic_operations/create_topic
                        time:   [1.4150 µs 1.5777 µs 1.9184 µs]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe
Benchmarking topic_operations/list_topics: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 65.9s, or reduce sample count to 10.
Benchmarking topic_operations/list_topics: Collecting 100 samples in estimated 65.86                                                                                    topic_operations/list_topics
                        time:   [648.33 ms 652.26 ms 656.59 ms]
Found 13 outliers among 100 measurements (13.00%)
  10 (10.00%) low mild
  1 (1.00%) high mild
  2 (2.00%) high severe

Benchmarking partition_operations/send_to_partitions/1: Collecting 100 samples in es                                                                                    partition_operations/send_to_partitions/1
                        time:   [6.8442 µs 7.1557 µs 7.5364 µs]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high severe
Benchmarking partition_operations/send_to_partitions/2: Collecting 100 samples in es                                                                                    partition_operations/send_to_partitions/2
                        time:   [13.501 µs 13.921 µs 14.396 µs]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe
Benchmarking partition_operations/send_to_partitions/4: Collecting 100 samples in es                                                                                    partition_operations/send_to_partitions/4
                        time:   [27.957 µs 28.450 µs 28.927 µs]
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe
Benchmarking partition_operations/send_to_partitions/8: Collecting 100 samples in es                                                                                    partition_operations/send_to_partitions/8
                        time:   [53.335 µs 54.671 µs 55.994 µs]

Benchmarking concurrent_operations/concurrent_send_receive: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 8.0s, enable flat sampling, or reduce sample count to 50.
Benchmarking concurrent_operations/concurrent_send_receive: Collecting 100 samples i                                                                                    concurrent_operations/concurrent_send_receive
                        time:   [5.2259 ms 5.5166 ms 5.7806 ms]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

Benchmarking throughput/batch_send/10: Collecting 10 samples in estimated 5.0011 s (                                                                                    throughput/batch_send/10
                        time:   [67.767 µs 68.967 µs 70.823 µs]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild
Benchmarking throughput/batch_send/100: Collecting 10 samples in estimated 5.0197 s                                                                                     throughput/batch_send/100
                        time:   [687.47 µs 693.34 µs 700.38 µs]
Benchmarking throughput/batch_send/1000: Collecting 10 samples in estimated 5.0580 s                                                                                    throughput/batch_send/1000
                        time:   [6.6636 ms 6.7133 ms 6.7860 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) low severe
```

To run the benchmark on your local machine, use the command:

```bash
cargo bench
```

--------------------

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

Sends a message to a topic.

**Usage**

`pilgrimage send --topic <TOPIC> --message <MESSAGE> [--schema <SCHEMA>] [--compatibility <COMPATIBILITY>]`

**Options:**

- `--topic`, `-t` (required): Specifies the topic name.
- `--message`, `-m` (required): Specifies the message to send.
- `--schema`, `-s` (optional): Specifies the path to a schema file. If not specified, an existing schema will be used.
- `--compatibility`, `-c` (optional): Specifies the schema compatibility level (BACKWARD, FORWARD, FULL, NONE).

**Example**

`pilgrimage send --topic test_topic --message "Hello, World!"`

### consume

**Description:**

Consumes messages from a broker.

**Usage**

`pilgrimage consume --id <BROKER_ID> [--topic <TOPIC>] [--partition <PARTITION>] [--group <GROUP>]`

**Options:**

- `--id`, `-i` (required): Specifies the broker ID.
- `--topic`, `-t` (optional): Specifies the topic name.
- `--partition`, `-p` (optional): Specifies the partition number.
- `--group`, `-g` (optional): Specifies the consumer group ID.

**Example:**

`pilgrimage consume --id broker1 --topic test_topic --partition 0`

### status

**Description:**

Checks the status of the specified broker.

**Usage:**

`pilgrimage status --id <BROKER_ID>`

**Options:**
- `--id`, `-i` (required): Sets the broker ID.

**Example:**

`pilgrimage status --id broker1`

### schema

**Description:**

Manages message schemas for topics.

**Subcommands:**

1. **register** - Register a new schema for a topic

   **Usage:**

   `pilgrimage schema register --topic <TOPIC> --schema <SCHEMA_FILE> [--compatibility <COMPATIBILITY>]`

   **Options:**

   - `--topic`, `-t` (required): Specifies the topic name to register the schema for.
   - `--schema`, `-s` (required): Specifies the path to the schema file.
   - `--compatibility`, `-c` (optional): Specifies the schema compatibility level (BACKWARD, FORWARD, FULL, NONE).

   **Example:**

   `pilgrimage schema register --topic test_topic --schema ./schemas/test_schema.json --compatibility BACKWARD`

2. **list** - List all schemas for a topic

   **Usage:**

   `pilgrimage schema list --topic <TOPIC>`

   **Options:**

   - `--topic`, `-t` (required): Specifies the topic name.

   **Example:**

   `pilgrimage schema list --topic test_topic`

### Additional Information

- **Help Command:**
    To view all available commands and options, use the `help` command:

`pilgrimage help`

- **Version Information:**
    To check the current version of Pilgrimage, use:

`pilgrimage --version`

### Running the CLI

To run the CLI application:

```sh
cargo run --bin pilgrimage -- [COMMAND] [OPTIONS]
```

Examples:

```sh
# Start a broker
cargo run --bin pilgrimage -- start --id broker1 --partitions 3 --replication 2 --storage ./storage/broker1

# Send a message to a topic
cargo run --bin pilgrimage -- send --topic test_topic --message "Hello, World!"

# Consume messages from a broker
cargo run --bin pilgrimage -- consume --id broker1 --topic test_topic
```

--------------------

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

Sends a message to the broker. If the specified topic does not exist, it will be created automatically.

**Endpoint**: `POST /send`
**Request:**

```json
{
    "id": "broker1",
    "topic": "custom-topic",
    "message": "Hello, World!"
}
```

**Example:**

```sh
curl -X POST http://localhost:8080/send \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1",
    "topic": "custom-topic",
    "message": "Hello, World!"
  }'
```

#### Consume Messages

Consumes messages from the broker. If the specified topic does not exist, it will be created automatically.

**Endpoint**: `POST /consume`

**Request:**

```json
{
    "id": "broker1",
    "topic": "custom-topic",
    "partition": 0
}
```

**Example:**

```sh
# Default topic (default_topic)
curl -X POST http://localhost:8080/consume \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1"
  }'

# Custom topic
curl -X POST http://localhost:8080/consume \
  -H "Content-Type: application/json" \
  -d '{
    "id": "broker1",
    "topic": "custom-topic"
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
