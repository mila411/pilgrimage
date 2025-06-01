use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use log::debug;
use prometheus::{Counter, Encoder, Histogram, TextEncoder, register_counter, register_histogram};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use pilgrimage::{
    broker::{Broker, node::Node},
    message::metadata::MessageMetadata,
    schema::{MessageSchema, registry::SchemaRegistry},
    subscriber::types::Subscriber,
};
use uuid::Uuid;

lazy_static::lazy_static! {
    /// Prometheus [`Counter`] for the total number of brokers started.
    static ref BROKER_START_COUNTER: Counter = register_counter!(
        "broker_start_total",
        "Total number of brokers started"
    ).unwrap();

    /// Prometheus [`Counter`] for the total number of brokers stopped.
    static ref BROKER_STOP_COUNTER: Counter = register_counter!(
        "broker_stop_total",
        "Total number of brokers stopped"
    ).unwrap();

    /// Prometheus [`Histogram`] for the request duration in seconds.
    static ref REQUEST_HISTOGRAM: Histogram = register_histogram!(
        "request_duration_seconds",
        "Request duration in seconds"
    ).unwrap();

    /// Prometheus [`Counter`] for the total number of messages sent.
    static ref MESSAGE_COUNTER: Counter = register_counter!(
        "pilgrimage_messages_total",
        "Total number of messages sent"
    ).unwrap();

    /// Prometheus [`Counter`] for the total number of duplicate messages detected.
    static ref DUPLICATE_MESSAGE_COUNTER: Counter = register_counter!(
        "pilgrimage_duplicate_messages_total",
        "Number of duplicate messages detected"
    ).unwrap();
}

/// The `BrokerWrapper` struct is designed to wrap around a [`Broker`] instance,
/// providing thread-safe access and operations for [sending][`BrokerWrapper::send_message`]
/// and [receiving][`BrokerWrapper::receive_message`] messages.
/// It also includes a [method to check the health][`BrokerWrapper::is_healthy`] of the broker.
///
/// The struct also implements the [`Clone`] property to allow cloning of the state (deep copy).
#[derive(Clone)]
struct BrokerWrapper {
    /// An `Arc<Mutex<Broker>>` that encapsulates the broker instance
    /// to ensure thread-safe operations.
    inner: Arc<Mutex<Broker>>,
}

impl BrokerWrapper {
    /// Creates a new `BrokerWrapper` instance with the given [`Broker`] instance.
    ///
    /// # Arguments
    /// * `broker` - A [`Broker`] instance to be wrapped by the `BrokerWrapper`.
    ///
    /// # Returns
    /// A new `BrokerWrapper` instance with the given [`Broker`] instance.
    fn new(broker: Broker) -> Self {
        Self {
            inner: Arc::new(Mutex::new(broker)),
        }
    }

    /// Checks if the broker is healthy by attempting to lock the broker.
    ///
    /// # Returns
    /// `bool`: Returns `true` if the broker is healthy, otherwise `false`.
    fn is_healthy(&self) -> bool {
        self.inner.lock().is_ok()
    }

    /// Sends a message to the broker.
    ///
    /// # Arguments
    /// * `message` - A [`MessageMetadata`] instance containing the message details.
    ///
    /// # Returns
    /// `Result<(), String>` indicating the success or failure of the operation.
    /// * `Ok(())` if the message was sent successfully.
    /// * `Err(String)` containing the error message if the operation failed.
    fn send_message(&self, message: MessageMetadata) -> Result<(), String> {
        let schema = message
            .schema
            .clone()
            .unwrap_or_else(|| MessageSchema::new());
        let mut schema =
            schema.with_topic(message.topic_id.unwrap_or_else(|| "default".to_string()));
        schema.definition = message.content;

        if let Some(partition_id) = message.partition_id {
            schema = schema.with_partition(partition_id);
        }

        let mut broker = self.inner.lock().map_err(|_| "Failed to lock broker")?;
        broker.send_message(schema)
    }
}

/// Cloneable structure containing a thread-safe, mutable map of broker wrappers.
///
/// This structure contains a (hash) map of broker IDs to [`BrokerWrapper`] instances.
/// It also implements the [`Clone`] trait to allow cloning of the state (deep copy).
///
/// # Examples
/// ```
/// use std::{
///     collections::HashMap,
///     sync::{Arc, Mutex},
/// };
///
/// // Create a new AppState instance
/// let state = AppState {
///     brokers: Arc::new(Mutex::new(HashMap::new()))
/// };
/// ```
///
/// # See also
///
/// * [`BrokerWrapper`]
/// * [`Clone`]
/// * [`Arc`]
/// * [`Mutex`]
/// * [`HashMap`]
#[derive(Clone)]
pub struct AppState {
    /// A thread-safe hash map of broker IDs to [`BrokerWrapper`] instances.
    brokers: Arc<Mutex<HashMap<String, BrokerWrapper>>>,
}

/// Deserializable structure for starting a new broker.
#[derive(Deserialize)]
struct StartRequest {
    /// Unique identifier for the broker.
    id: String,
    /// Number of partitions for the broker.
    partitions: usize,
    /// Replication factor for the broker.
    replication: usize,
    /// Storage path for the broker.
    storage: String,
}

/// Deserializable structure for stopping a broker.
#[derive(Deserialize)]
struct StopRequest {
    /// Unique identifier for the broker.
    id: String,
}

/// Deserializable structure for sending a message to a broker.
#[derive(Deserialize)]
struct SendRequest {
    /// Unique identifier for the broker.
    id: String,
    /// Message to be sent to the broker.
    message: String,
    /// Optional topic to send the message to.
    #[serde(default = "default_topic")]
    topic: String,
    /// Optional number of partitions for the topic.
    #[serde(default)]
    partitions: Option<usize>,
    /// Optional replication factor for the topic.
    #[serde(default)]
    replication_factor: Option<usize>,
    /// Optional schema definition for message validation.
    #[serde(default)]
    schema: Option<String>,
}

fn default_topic() -> String {
    "default_topic".to_string()
}

/// Deserializable structure for consuming messages from a broker.
#[derive(Deserialize)]
struct ConsumeRequest {
    /// Unique identifier for the broker.
    id: String,
    /// Optional topic to consume messages from.
    #[serde(default = "default_topic")]
    topic: String,
    /// Optional consumer group ID.
    group_id: Option<String>,
    /// Optional partition ID to consume from.
    partition: Option<usize>,
}

/// Deserializable structure for checking the status of a broker.
#[derive(Deserialize)]
struct StatusRequest {
    /// Unique identifier for the broker.
    id: String,
}

/// Starts a new broker with the given information.
///
/// # Arguments
/// * `info` - A [`StartRequest`] instance containing the broker details.
/// * `data` - Application state ([`AppState`]) containing the brokers map.
///
/// # Returns
/// An [`HttpResponse`] indicating the success or failure of the operation:
/// * [`actix_web::http::StatusCode::OK`] if the broker was started successfully.
/// * [`actix_web::http::StatusCode::BAD_REQUEST`] if the broker is already running.
async fn start_broker(info: web::Json<StartRequest>, data: web::Data<AppState>) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let mut brokers_lock = data.brokers.lock().unwrap();

    if brokers_lock.contains_key(&info.id) {
        return HttpResponse::BadRequest().json("Broker is already running");
    }

    let mut broker = Broker::new(&info.id, info.partitions, info.replication, &info.storage);

    // Add node and create initial topic
    let node = Node::new("node1", "127.0.0.1:8080", true);
    broker.add_node("node1".to_string(), node);

    // Create default topic
    if let Err(e) = broker.create_topic("default_topic", None) {
        return HttpResponse::InternalServerError()
            .json(format!("Failed to create default topic: {}", e));
    }

    let wrapper = BrokerWrapper::new(broker);
    brokers_lock.insert(info.id.clone(), wrapper);

    BROKER_START_COUNTER.inc();
    timer.observe_duration();
    HttpResponse::Ok().json("Broker started")
}

/// Stops the broker with the given ID.
///
/// # Arguments
/// * `info` - A [`StopRequest`] instance containing the broker ID.
/// * `data` - Application state ([`AppState`]) containing the brokers map.
///
/// # Returns
/// An [`HttpResponse`] indicating the success or failure of the operation:
/// * [`actix_web::http::StatusCode::OK`] if the broker was stopped successfully.
/// * [`actix_web::http::StatusCode::BAD_REQUEST`] if no broker is running with the given ID.
async fn stop_broker(info: web::Json<StopRequest>, data: web::Data<AppState>) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let mut brokers_lock = data.brokers.lock().unwrap();

    if brokers_lock.remove(&info.id).is_some() {
        BROKER_STOP_COUNTER.inc();
        timer.observe_duration();
        HttpResponse::Ok().json("Broker stopped")
    } else {
        timer.observe_duration();
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

/// Sends a message to the broker with the given ID.
///
/// # Arguments
/// * `info` - A [`SendRequest`] instance containing the broker ID and message.
/// * `data` - Application state ([`AppState`]) containing the brokers map.
///
/// # Returns
/// An [`HttpResponse`] indicating the success or failure of the operation:
/// * [`actix_web::http::StatusCode::OK`] if the message was sent successfully.
/// * [`actix_web::http::StatusCode::BAD_REQUEST`] if no broker is running with the given ID.
async fn send_message(info: web::Json<SendRequest>, data: web::Data<AppState>) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker_wrapper) = brokers_lock.get(&info.id) {
        {
            let mut broker = broker_wrapper.inner.lock().unwrap();
            let topics = broker.topics.lock().unwrap();

            if !topics.contains_key(&info.topic) {
                drop(topics);

                if let Err(e) = broker.create_topic(&info.topic, None) {
                    if !e.to_string().contains("already exists") {
                        timer.observe_duration();
                        return HttpResponse::InternalServerError()
                            .json(format!("Failed to create topic: {}", e));
                    }
                }
            }
        }

        let registry = SchemaRegistry::new();
        if let Some(schema_def) = &info.schema {
            if let Err(e) = registry.register_schema(&info.topic, schema_def) {
                timer.observe_duration();
                return HttpResponse::BadRequest()
                    .json(format!("Schema registration failed.: {}", e));
            }
        }

        let metadata = MessageMetadata {
            id: Uuid::new_v4().to_string(),
            content: info.message.clone(),
            timestamp: Utc::now().to_rfc3339(),
            topic_id: Some(info.topic.clone()),
            partition_id: Some(0),
            schema: info.schema.clone().map(|s| {
                let mut schema = MessageSchema::new();
                schema.definition = s;
                schema
            }),
        };

        match broker_wrapper.send_message(metadata) {
            Ok(_) => {
                MESSAGE_COUNTER.inc();
                timer.observe_duration();
                HttpResponse::Ok().json("Message sent.")
            }
            Err(e) => {
                DUPLICATE_MESSAGE_COUNTER.inc();
                timer.observe_duration();
                HttpResponse::InternalServerError().json(format!("Failed to send message: {}", e))
            }
        }
    } else {
        timer.observe_duration();
        HttpResponse::BadRequest().json("The specified broker cannot be found.")
    }
}

/// Consumes a message from the broker with the given ID.
///
/// # Arguments
/// * `info` - A [`ConsumeRequest`] instance containing the broker ID.
/// * `data` - Application state ([`AppState`]) containing the brokers map.
///
/// # Returns
/// An [`HttpResponse`] indicating the success or failure of the operation:
/// * [`actix_web::http::StatusCode::OK`] if a message was consumed successfully or no messages are available.
/// * [`actix_web::http::StatusCode::BAD_REQUEST`] if no broker is running with the given ID.
async fn consume_messages(
    info: web::Json<ConsumeRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        {
            let mut broker = broker.inner.lock().unwrap();
            let topics = broker.topics.lock().unwrap();
            if !topics.contains_key(&info.topic) {
                drop(topics);

                if let Err(e) = broker.create_topic(&info.topic, None) {
                    if !e.to_string().contains("already exists") {
                        timer.observe_duration();
                        return HttpResponse::InternalServerError()
                            .json(format!("Failed to create topic: {}", e));
                    }
                }
            }
        }

        let subscriber = Subscriber::new(
            format!("subscriber_{}", Uuid::new_v4()),
            Box::new(|msg: String| {
                debug!("Message received: {}", msg);
            }),
        );
        {
            let mut broker = broker.inner.lock().unwrap();
            if let Err(e) = broker.subscribe(&info.topic, subscriber) {
                timer.observe_duration();
                return HttpResponse::InternalServerError()
                    .json(format!("Failed to subscribe: {}", e));
            }
        }

        {
            let broker = broker.inner.lock().unwrap();
            match broker.receive_message(&info.topic, info.partition.unwrap_or(0)) {
                Ok(Some(message)) => {
                    debug!(
                        "Message received: Topic={}, Content={}",
                        info.topic, message.content
                    );
                    timer.observe_duration();
                    HttpResponse::Ok().json(message.content)
                }
                Ok(None) => {
                    timer.observe_duration();
                    HttpResponse::NotFound().json("No messages available")
                }
                Err(e) => {
                    timer.observe_duration();
                    HttpResponse::InternalServerError()
                        .json(format!("Failed to receive message: {}", e))
                }
            }
        }
    } else {
        timer.observe_duration();
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

/// Checks the status of the broker with the given ID.
///
/// # Arguments
/// * `info` - A [`StatusRequest`] instance containing the broker ID.
/// * `data` - Application state ([`AppState`]) containing the brokers map.
///
/// # Returns
/// An [`HttpResponse`] indicating the status of the broker:
/// * [`actix_web::http::StatusCode::OK`] if the broker is healthy.
/// * [`actix_web::http::StatusCode::BAD_REQUEST`] if no broker is running with the given ID.
async fn broker_status(
    info: web::Json<StatusRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        timer.observe_duration();
        HttpResponse::Ok().json(broker.is_healthy())
    } else {
        timer.observe_duration();
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

/// Exposes the Prometheus metrics for the application.
///
/// # Returns
/// An [`HttpResponse`] containing the Prometheus metrics as a string in the body.
async fn metrics() -> impl Responder {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    let response = String::from_utf8(buffer).unwrap();
    HttpResponse::Ok().body(response)
}

/// Initializes the application state (see [`AppState`])
/// and starts an HTTP server at `127.0.0.1:8080`.
///
/// It sets up and runs an [Actix Web Server](https://actix.rs/) with multiple
/// [endpoints](#endpoints) for managing brokers.
///
/// # Returns
/// `std::io::Result<()>` indicating the success or failure of the server execution.
///
/// # Examples
/// ```
/// #[tokio::main]
/// async fn main() -> std::io::Result<()> {
///     // Start the web console server
///     web_console::run_server().await
/// }
/// ```
///
/// # Endpoints
///
/// The server provides the following REST API endpoints:
///
/// | Function              | Type  | Endpoint      | Description                                                                               |
/// |-----------------------|-------|---------------|-------------------------------------------------------------------------------------------|
/// | [`start_broker`]      | POST  | `/start`      | Starts a new broker with the given ID, partitions, replication factor, and storage path.  |
/// | [`stop_broker`]       | POST  | `/stop`       | Stops the broker with the given ID.                                                       |
/// | [`send_message`]      | POST  | `/send`       | Sends a message to the broker with the given ID.                                          |
/// | [`consume_messages`]  | POST  | `/consume`    | Consumes a message from the broker with the given ID.                                     |
/// | [`broker_status`]     | POST  | `/status`     | Checks the status of the broker with the given ID.                                        |
/// | [`metrics`]           | GET   | `/metrics`    | Exposes the Prometheus metrics for the application.                                       |
///
pub async fn run_server() -> std::io::Result<()> {
    let state = AppState {
        brokers: Arc::new(Mutex::new(HashMap::new())),
    };

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .route("/start", web::post().to(start_broker))
            .route("/stop", web::post().to(stop_broker))
            .route("/send", web::post().to(send_message))
            .route("/consume", web::post().to(consume_messages))
            .route("/status", web::post().to(broker_status))
            .route("/metrics", web::get().to(metrics))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, test, web};
    use serde_json::json;

    /// Test for starting a new broker.
    ///
    /// # Purpose
    /// This test ensures that a broker can be successfully started
    /// by making a POST request to the `/start` endpoint with valid JSON data.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`start_broker`] route.
    /// 3. Create a test request with valid broker start information.
    /// 4. Call the service with the test request.
    /// 5. Assert that the response status is successful.
    #[actix_rt::test]
    async fn test_start_broker() {
        // Use a unique temp directory for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let storage_path = format!("./target/test_storage_{}", test_id);

        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Clean up test resources
        let _ = std::fs::remove_dir_all(storage_path);
    }

    /// Test for stopping a broker.
    ///
    /// # Purpose
    /// This test ensures that a broker can be started and then stopped successfully
    /// by making POST requests to the `/start` and `/stop` endpoints with valid JSON data.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`start_broker`]
    ///    and [`stop_broker`] routes.
    /// 3. Create a test request with valid broker start information.
    /// 4. Call the service with the test request.
    /// 5. Create a test request to stop the previously started broker.
    /// 6. Call the service with the stop request.
    /// 7. Assert that the response status is successful,
    ///    indicating that the broker has been stopped.
    #[actix_rt::test]
    async fn test_stop_broker() {
        // Use a unique temp directory for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let storage_path = format!("./target/test_storage_{}", test_id);

        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/stop", web::post().to(stop_broker)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Give the broker time to initialize fully
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Now stop the broker
        let req = test::TestRequest::post()
            .uri("/stop")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Clean up test resources
        let _ = std::fs::remove_dir_all(storage_path);
    }

    /// Test for sending a message to a broker.
    ///
    /// # Purpose
    /// This test ensures that a broker can be started and
    /// a message can be sent to it successfully by making POST requests
    /// to the `/start` and `/send` endpoints with valid JSON data.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`start_broker`] and `send_message` routes.
    /// 3. Create a test request with valid broker start information.
    /// 4. Call the service with the start request.
    /// 5. Create a test request to send a message to the started broker.
    /// 6. Call the service with the send request.
    /// 7. Assert that the response status is successful, indicating that the message was sent.
    #[actix_rt::test]
    async fn test_send_message() {
        // Use a unique temp directory for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let storage_path = format!("./target/test_storage_{}", test_id);

        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/send", web::post().to(send_message)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Give the broker time to initialize fully
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Now send a message
        let req = test::TestRequest::post()
            .uri("/send")
            .set_json(json!({
                "id": "broker1",
                "message": "Hello, World!"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Clean up test resources
        let _ = std::fs::remove_dir_all(storage_path);
    }

    /// Test for consuming messages from a broker.
    ///
    /// # Purpose
    /// This test ensures that a broker can be started,
    /// a message can be sent to it,
    /// and the message can be consumed successfully by making POST requests to the `/start`,
    /// `/send`, and `/consume` endpoints with valid JSON data.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`start_broker`],
    ///    `send_message`, and `consume_messages` routes.
    /// 3. Create a test request to start a broker with valid information.
    /// 4. Call the service with the start request.
    /// 5. Create a test request to send a message to the started broker.
    /// 6. Call the service with the send request.
    /// 7. Create a test request to consume the message from the broker.
    /// 8. Call the service with the consume request.
    /// 9. Assert that the response status is successful,
    ///    indicating that the message was consumed.
    #[actix_rt::test]
    async fn test_consume_messages() {
        // Use a unique temp directory for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let storage_path = format!("./target/test_storage_{}", test_id);

        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/send", web::post().to(send_message))
                .route("/consume", web::post().to(consume_messages)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Give the broker time to initialize fully
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Send a message
        let req = test::TestRequest::post()
            .uri("/send")
            .set_json(json!({
                "id": "broker1",
                "message": "Hello, World!"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Give the system time to process the message
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Now consume the message
        let req = test::TestRequest::post()
            .uri("/consume")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Clean up test resources
        let _ = std::fs::remove_dir_all(storage_path);
    }

    /// Test for checking the status of a broker.
    ///
    /// # Purpose
    /// This test ensures that a broker can be started and
    /// its status can be checked successfully by making POST requests
    /// to the `/start` and `/status` endpoints with valid JSON data.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`start_broker`] and `broker_status` routes.
    /// 3. Create a test request to start a broker with valid information.
    /// 4. Call the service with the start request.
    /// 5. Create a test request to check the status of the started broker.
    /// 6. Call the service with the status request.
    /// 7. Assert that the response status is successful, indicating that the broker is healthy.
    #[actix_rt::test]
    async fn test_broker_status() {
        // Use a unique temp directory for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let storage_path = format!("./target/test_storage_{}", test_id);

        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/status", web::post().to(broker_status)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Give the broker time to initialize fully
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Now check the broker status
        let req = test::TestRequest::post()
            .uri("/status")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Clean up test resources
        let _ = std::fs::remove_dir_all(storage_path);
    }

    /// Test for starting a broker that is already running.
    ///
    /// # Purpose
    /// This test ensures that attempting to start a broker that is already running
    /// returns a [`actix_web::http::StatusCode::BAD_REQUEST`] status code.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`start_broker`] route.
    /// 3. Create a test request to start a broker with valid information.
    /// 4. Call the service with the start request.
    /// 5. Create another test request to start the same broker again with the same information.
    /// 6. Call the service with the second start request (step 5).
    /// 7. Assert that the response status is [`actix_web::http::StatusCode::BAD_REQUEST`],
    ///    indicating that the broker is already running.
    #[actix_rt::test]
    async fn test_start_broker_already_running() {
        // Use a unique temp directory for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let storage_path = format!("./target/test_storage_{}", test_id);

        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path.clone()
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // Give the broker time to initialize fully
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Try to start the same broker again
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path.clone()
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    /// Test for stopping a broker that is not running.
    ///
    /// # Purpose
    /// This test ensures that attempting to stop a broker that is not running returns a
    /// [`actix_web::http::StatusCode::BAD_REQUEST`] status code.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`stop_broker`] route.
    /// 3. Create a test request to stop a broker that is not running.
    /// 4. Call the service with the stop request.
    /// 5. Assert that the response status is [`actix_web::http::StatusCode::BAD_REQUEST`],
    ///    indicating that the broker is not running.
    #[actix_rt::test]
    async fn test_stop_broker_not_running() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/stop", web::post().to(stop_broker)),
        )
        .await;

        // Try to stop a broker that is not running.
        let req = test::TestRequest::post()
            .uri("/stop")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    /// Test for sending a message to a broker that is not running.
    ///
    /// # Purpose
    /// This test ensures that attempting to send a message to a broker
    /// that is not running returns a [`actix_web::http::StatusCode::BAD_REQUEST`] status code.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`send_message`] route.
    /// 3. Create a test request to send a message to a broker that is not running.
    /// 4. Call the service with the send request.
    /// 5. Assert that the response status is [`actix_web::http::StatusCode::BAD_REQUEST`],
    ///    indicating that the broker is not running.
    #[actix_rt::test]
    async fn test_send_message_no_broker() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/send", web::post().to(send_message)),
        )
        .await;

        // Try to send a message to a broker that is not running
        let req = test::TestRequest::post()
            .uri("/send")
            .set_json(json!({
                "id": "broker1",
                "message": "Hello, World!"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    /// Test for consuming messages from a broker that is not running.
    ///
    /// # Purpose
    /// This test ensures that attempting to consume messages from a broker
    /// that is not running returns a [`actix_web::http::StatusCode::BAD_REQUEST`] status code.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`consume_messages`] route.
    /// 3. Create a test request to consume messages from a broker that is not running.
    /// 4. Call the service with the consume request.
    /// 5. Assert that the response status is [`actix_web::http::StatusCode::BAD_REQUEST`],
    ///    indicating that the broker is not running.
    #[actix_rt::test]
    async fn test_consume_messages_no_broker() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/consume", web::post().to(consume_messages)),
        )
        .await;

        // Try to consume messages from a broker that is not running
        let req = test::TestRequest::post()
            .uri("/consume")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    /// Test for checking the status of a broker that is not running.
    ///
    /// # Purpose
    /// This test ensures that attempting to check the status of a broker that is not running
    /// returns a [`actix_web::http::StatusCode::BAD_REQUEST`] status code.
    ///
    /// # Steps
    /// 1. Create a new [`AppState`] instance with an empty brokers map.
    /// 2. Initialize the Actix web application with the [`broker_status`] route.
    /// 3. Create a test request to check the status of a broker that is not running.
    /// 4. Call the service with the status request.
    /// 5. Assert that the response status is [`actix_web::http::StatusCode::BAD_REQUEST`],
    ///    indicating that the broker is not running.
    #[actix_rt::test]
    async fn test_broker_status_no_broker() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/status", web::post().to(broker_status)),
        )
        .await;

        // Try to check the status of a broker that is not running
        let req = test::TestRequest::post()
            .uri("/status")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    /// Test for running the server and starting a broker.
    ///
    /// # Purpose
    /// This test ensures that the server can be started,
    /// and a broker can be successfully started by making a POST request
    /// to the `/start` endpoint with valid JSON data.
    ///
    /// # Steps
    /// 1. Initialize the Actix web server with all necessary routes
    ///    (`/start`, `/stop`, `/send`, `/consume`, `/status`).
    /// 2. Create a test request to start a broker with valid information.
    /// 3. Call the service with the start request.
    /// 4. Assert that the response status is successful,
    ///    indicating that the broker has been started and the server is running.
    #[actix_rt::test]
    async fn test_run_server() {
        // Use a unique temp directory for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let storage_path = format!("./target/test_storage_{}", test_id);

        let srv = actix_test::start(|| {
            App::new()
                .app_data(web::Data::new(AppState {
                    brokers: Arc::new(Mutex::new(HashMap::new())),
                }))
                .route("/start", web::post().to(start_broker))
                .route("/stop", web::post().to(stop_broker))
                .route("/send", web::post().to(send_message))
                .route("/consume", web::post().to(consume_messages))
                .route("/status", web::post().to(broker_status))
        });

        let req = srv
            .post("/start")
            .send_json(&json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": storage_path
            }))
            .await
            .unwrap();

        assert!(req.status().is_success());

        // Clean up test resources
        let _ = std::fs::remove_dir_all(storage_path);
    }
}
