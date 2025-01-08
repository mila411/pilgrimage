use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use log::debug;
use pilgrimage::message::message::Message;
use pilgrimage::{Broker, broker::Node};
use prometheus::{Counter, Encoder, Histogram, TextEncoder, register_counter, register_histogram};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

#[derive(Clone)]
struct BrokerWrapper {
    inner: Arc<Mutex<Broker>>,
}

impl BrokerWrapper {
    fn new(broker: Broker) -> Self {
        Self {
            inner: Arc::new(Mutex::new(broker)),
        }
    }

    fn send_message(&self, message: String) -> Result<(), String> {
        if let Ok(broker) = self.inner.lock() {
            let message: Message = message.into();
            debug!(
                "Send a message: ID={}, Content={}",
                message.id, message.content
            );
            let result = broker.send_message(message);
            if result.is_ok() {
                MESSAGE_COUNTER.inc();
            } else {
                DUPLICATE_MESSAGE_COUNTER.inc();
            }
            result
        } else {
            Err("Broker lock failed.".to_string())
        }
    }

    fn receive_message(&self) -> Option<String> {
        if let Ok(broker) = self.inner.lock() {
            broker.receive_message().map(|msg| {
                debug!("Message received: ID={}, content={}", msg.id, msg.content);
                String::from(msg)
            })
        } else {
            None
        }
    }

    fn is_healthy(&self) -> bool {
        self.inner.lock().is_ok()
    }
}

/// Cloneable structure containing a thread-safe, mutable map of broker wrappers.
///
/// This structure contains a (hash) map of broker IDs to [`BrokerWrapper`] instances.
/// It also implements the [`Clone`] trait to allow cloning of the state (deep copy).
///
/// # Example
/// ```
/// use std::{
///     collections::HashMap,
///     sync::{Arc, Mutex},
/// };
///
/// fn main() {
///     // Create a new AppState instance
///     let state = AppState {
///         brokers: Arc::new(Mutex::new(HashMap::new())),
///     };
/// }
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

#[derive(Deserialize)]
struct StartRequest {
    id: String,
    partitions: usize,
    replication: usize,
    storage: String,
}

#[derive(Deserialize)]
struct StopRequest {
    id: String,
}

#[derive(Deserialize)]
struct SendRequest {
    id: String,
    message: String,
}

#[derive(Deserialize)]
struct ConsumeRequest {
    id: String,
}

#[derive(Deserialize)]
struct StatusRequest {
    id: String,
}

async fn start_broker(info: web::Json<StartRequest>, data: web::Data<AppState>) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let mut brokers_lock = data.brokers.lock().unwrap();

    if brokers_lock.contains_key(&info.id) {
        return HttpResponse::BadRequest().json("Broker is already running");
    }

    let broker = Broker::new(&info.id, info.partitions, info.replication, &info.storage);

    let node = Node {
        id: "node1".to_string(),
        address: "127.0.0.1:8080".to_string(),
        is_active: true,
        data: Arc::new(Mutex::new(Vec::new())),
    };
    broker.add_node("node1".to_string(), node);

    let wrapper = BrokerWrapper::new(broker);
    brokers_lock.insert(info.id.clone(), wrapper);

    BROKER_START_COUNTER.inc();
    timer.observe_duration();
    HttpResponse::Ok().json("Broker started")
}

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

async fn send_message(info: web::Json<SendRequest>, data: web::Data<AppState>) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        let _ = broker.send_message(info.message.clone());
        timer.observe_duration();
        HttpResponse::Ok().json("Message sent")
    } else {
        timer.observe_duration();
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

async fn consume_messages(
    info: web::Json<ConsumeRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let timer = REQUEST_HISTOGRAM.start_timer();
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        if let Some(message) = broker.receive_message() {
            timer.observe_duration();
            HttpResponse::Ok().json(message)
        } else {
            timer.observe_duration();
            HttpResponse::Ok().json("No messages available")
        }
    } else {
        timer.observe_duration();
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

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
/// # Example
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

    #[actix_rt::test]
    async fn test_start_broker() {
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
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_stop_broker() {
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
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Now stop the broker
        let req = test::TestRequest::post()
            .uri("/stop")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_send_message() {
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
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

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
    }

    #[actix_rt::test]
    async fn test_consume_messages() {
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
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Send a message
        let req = test::TestRequest::post()
            .uri("/send")
            .set_json(json!({
                "id": "broker1",
                "message": "Hello, World!"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Now consume the message
        let req = test::TestRequest::post()
            .uri("/consume")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_broker_status() {
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
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Now check the broker status
        let req = test::TestRequest::post()
            .uri("/status")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_start_broker_already_running() {
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
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Try to start the same broker again
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

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

        // Try to stop a broker that is not running
        let req = test::TestRequest::post()
            .uri("/stop")
            .set_json(json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

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

    #[actix_rt::test]
    async fn test_run_server() {
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
                "storage": "/tmp/broker1"
            }))
            .await
            .unwrap();

        assert!(req.status().is_success());
    }
}
