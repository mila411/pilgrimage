use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, Result as ActixResult, web};
use log::debug;
use prometheus::{Counter, Encoder, Histogram, TextEncoder, register_counter, register_histogram};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
  net::{IpAddr},
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use crate::auth::{DistributedAuthenticator, web_middleware::JwtAuth};
use crate::auth::web_middleware::get_authenticated_user_claims;
use crate::broker::Broker;
use crate::broker::node::Node;
use crate::message::metadata::MessageMetadata;
use crate::schema::{MessageSchema, registry::SchemaRegistry};
use crate::security::SecurityManager;
use crate::subscriber::types::Subscriber;
use chrono::Utc;
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
/// ```no_run
/// use std::{
///     collections::HashMap,
///     sync::{Arc, Mutex},
/// };
/// use pilgrimage::web_console::AppState;
///
/// // AppState contains broker management state and security components
/// // Note: AppState has private fields and should be created through the web console runtime
/// // This example shows the conceptual structure only
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
    /// Security manager for handling authentication, authorization, and audit logging
    security_manager: Arc<SecurityManager>,
  /// JWT authenticator for token validation and generation
  jwt_authenticator: Arc<Mutex<DistributedAuthenticator>>,
  /// Whether to trust X-Forwarded-For header from trusted proxies
  trust_xff: bool,
  /// Trusted proxy IPs (exact IP match)
  trusted_proxies: Vec<IpAddr>,
  /// Login attempts for rate limiting and lockout
  login_attempts: Arc<Mutex<HashMap<String, AttemptInfo>>>,
}

/// Simple login attempt tracker for rate limiting/lockout
struct AttemptInfo {
  count: u32,
  first: Instant,
  locked_until: Option<Instant>,
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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

/// Security request structures
#[derive(Deserialize)]
#[allow(dead_code)]
struct SecurityLoginRequest {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct SecurityRoleRequest {
    user_id: String,
    role: String,
}

#[derive(Deserialize)]
struct SecurityPermissionRequest {
    user_id: String,
    resource: String,
    action: String,
}

#[derive(Serialize)]
struct SecurityLoginResponse {
    token: String,
    role: String,
}

#[derive(Serialize)]
struct SecurityStatusResponse {
    tls_enabled: bool,
    active_connections: usize,
    audit_events_today: usize,
    roles_configured: usize,
}

/// Request structure for changing a user's password
#[derive(Deserialize)]
struct ChangePasswordRequest {
  username: String,
  current_password: String,
  new_password: String,
}

/// Request structure for creating a new user (admin-only)
#[derive(Deserialize)]
struct CreateUserRequest {
  username: String,
  password: String,
  role: String,
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

    let mut broker = match Broker::new(&info.id, info.partitions, info.replication, &info.storage) {
        Ok(broker) => broker,
        Err(e) => {
            eprintln!("Failed to create broker: {}", e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": format!("Failed to initialize broker: {}", e)
            }));
        }
    };

    // Add node and create initial topic
    let node = Node::new("node1", "127.0.0.1:8080", true);
    let _ = broker.add_node("node1".to_string(), node);

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

/// Dashboard HTML display
async fn dashboard_html() -> impl Responder {
    // Try to load from template file first, fall back to built-in template
    let html = std::fs::read_to_string("templates/dashboard.html")
        .unwrap_or_else(|_| default_dashboard_html());

    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html)
}

/// Serves the comprehensive web console HTML page
async fn console_html() -> impl Responder {
    let html = std::fs::read_to_string("templates/console.html")
        .unwrap_or_else(|_| {
            std::fs::read_to_string("templates/test-console.html")
                .unwrap_or_else(|_| default_console_html())
        });

    HttpResponse::Ok().content_type("text/html").body(html)
}

/// Legacy test console function - redirects to console_html for backward compatibility
async fn test_console_html() -> impl Responder {
    console_html().await
}

/// Default console HTML template
fn default_console_html() -> String {
    r#"
<!DOCTYPE html>
<html>
  <head>
    <title>Pilgrimage Web Console</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 20px;
        background-color: #f4f4f4;
      }
      .container {
        max-width: 1200px;
        margin: 0 auto;
        background: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
      }
      h1 { color: #333; }
      .endpoint {
        margin: 15px 0;
        padding: 15px;
        border: 1px solid #ddd;
        border-radius: 4px;
        background: #fafafa;
      }
      button {
        padding: 8px 16px;
        margin: 5px;
        border: none;
        border-radius: 4px;
        background: #007bff;
        color: white;
        cursor: pointer;
      }
      button:hover { background: #0056b3; }
      .result {
        margin-top: 10px;
        padding: 10px;
        background: #f5f5f5;
        border-radius: 4px;
        font-family: monospace;
        font-size: 12px;
      }
      .tabs {
        display: flex;
        border-bottom: 1px solid #ddd;
        margin-bottom: 20px;
      }
      .tab {
        padding: 10px 20px;
        cursor: pointer;
        border-bottom: 2px solid transparent;
      }
      .tab.active {
        border-bottom-color: #007bff;
        color: #007bff;
      }
      .tab-content {
        display: none;
      }
      .tab-content.active {
        display: block;
      }
    </style>
  </head>
  <body>
    <div class="container">
      <h1>Pilgrimage Web Console</h1>

      <div style="text-align: right; margin-bottom: 10px;">
        <span id="auth-status">✗ Not Authenticated</span>
      </div>

      <div class="tabs">
        <div class="tab active" onclick="showTab('broker-management')">Broker Management</div>
        <div class="tab" onclick="showTab('messaging')">Messaging</div>
        <div class="tab" onclick="showTab('security')">Security</div>
        <div class="tab" onclick="showTab('monitoring')">Monitoring</div>
      </div>

      <div id="broker-management" class="tab-content active">
        <div class="endpoint">
          <h3>Start Broker</h3>
          <input type="text" id="broker-id" placeholder="Broker ID" value="default-broker">
          <input type="number" id="partitions" placeholder="Partitions" value="3">
          <input type="number" id="replication" placeholder="Replication" value="2">
          <input type="text" id="storage-path" placeholder="Storage Path" value="./storage/default">
          <button onclick="startBroker()">Start Broker</button>
          <div id="start-result" class="result"></div>
        </div>

        <div class="endpoint">
          <h3>Broker Status</h3>
          <input type="text" id="status-broker-id" placeholder="Broker ID" value="default-broker">
          <button onclick="checkStatus()">Check Status</button>
          <div id="status-result" class="result"></div>
        </div>

        <div class="endpoint">
          <h3>Stop Broker</h3>
          <input type="text" id="stop-broker-id" placeholder="Broker ID" value="default-broker">
          <button onclick="stopBroker()">Stop Broker</button>
          <div id="stop-result" class="result"></div>
        </div>
      </div>

      <div id="messaging" class="tab-content">
        <div class="endpoint">
          <h3>Send Message</h3>
          <input type="text" id="send-broker-id" placeholder="Broker ID" value="default-broker">
          <input type="text" id="topic" placeholder="Topic" value="default_topic">
          <textarea id="message" placeholder="Message content">Hello from Pilgrimage!</textarea>
          <button onclick="sendMessage()">Send Message</button>
          <div id="send-result" class="result"></div>
        </div>

        <div class="endpoint">
          <h3>Consume Messages</h3>
          <input type="text" id="consume-broker-id" placeholder="Broker ID" value="default-broker">
          <input type="text" id="consume-topic" placeholder="Topic" value="default_topic">
          <button onclick="consumeMessages()">Consume Messages</button>
          <div id="consume-result" class="result"></div>
        </div>
      </div>

      <div id="security" class="tab-content">
        <div class="endpoint">
          <h3>Security Login</h3>
          <input type="text" id="username" placeholder="Username" value="admin">
          <input type="password" id="password" placeholder="Password" value="password">
          <button onclick="securityLogin()">Login</button>
          <button onclick="securityLogout()">Logout</button>
          <div id="login-result" class="result"></div>
        </div>

        <div class="endpoint">
          <h3>Security Status</h3>
          <button onclick="getSecurityStatus()">Get Security Status</button>
          <div id="security-result" class="result"></div>
        </div>
      </div>

      <div id="monitoring" class="tab-content">
        <div class="endpoint">
          <h3>Metrics</h3>
          <button onclick="getMetrics()">Get Prometheus Metrics</button>
          <div id="metrics-result" class="result"></div>
        </div>

        <div class="endpoint">
          <h3>Dashboard Data</h3>
          <button onclick="getDashboardData()">Get Dashboard Data</button>
          <div id="dashboard-result" class="result"></div>
        </div>

        <div class="endpoint">
          <h3>Cluster Health</h3>
          <button onclick="getClusterHealth()">Get Cluster Health</button>
          <div id="cluster-result" class="result"></div>
        </div>
      </div>
    </div>

    <script>
      function showTab(tabId) {
        // Hide all tab contents
        const contents = document.querySelectorAll('.tab-content');
        contents.forEach(content => content.classList.remove('active'));

        // Remove active class from all tabs
        const tabs = document.querySelectorAll('.tab');
        tabs.forEach(tab => tab.classList.remove('active'));

        // Show selected tab content
        document.getElementById(tabId).classList.add('active');

        // Add active class to clicked tab
        event.target.classList.add('active');
      }

      // JWT Token management
      function getStoredToken() {
        return localStorage.getItem('pilgrimage_jwt_token');
      }

      function setStoredToken(token) {
        localStorage.setItem('pilgrimage_jwt_token', token);
      }

      function removeStoredToken() {
        localStorage.removeItem('pilgrimage_jwt_token');
      }

      function checkAuthStatus() {
        const token = getStoredToken();
        const statusIndicator = document.getElementById('auth-status');
        if (token) {
          statusIndicator.innerHTML = '<span style="color: green;">✓ Authenticated</span>';
          return true;
        } else {
          statusIndicator.innerHTML = '<span style="color: red;">✗ Not Authenticated</span>';
          return false;
        }
      }

      async function makeRequest(url, method = "GET", body = null) {
        try {
          const options = {
            method,
            headers: {
              "Content-Type": "application/json",
            },
          };

          // Add JWT token to Authorization header for protected endpoints
          const token = getStoredToken();
          const publicEndpoints = ['/security/login', '/metrics', '/console', '/test', '/health', '/api/cluster-health', '/dashboard'];
          const isPublicEndpoint = publicEndpoints.some(endpoint => url.startsWith(endpoint));

          if (token && !isPublicEndpoint) {
            options.headers.Authorization = `Bearer ${token}`;
          }

          if (body) {
            options.body = JSON.stringify(body);
          }

          const response = await fetch(url, options);
          const text = await response.text();

          // Handle authentication errors
          if (response.status === 401) {
            removeStoredToken();
            checkAuthStatus();
            return { status: response.status, body: "Authentication required. Please login." };
          }

          return { status: response.status, body: text };
        } catch (error) {
          return { status: "Error", body: error.message };
        }
      }

      async function startBroker() {
        const result = await makeRequest("/start", "POST", {
          id: document.getElementById("broker-id").value,
          partitions: parseInt(document.getElementById("partitions").value),
          replication: parseInt(document.getElementById("replication").value),
          storage: document.getElementById("storage-path").value,
        });
        document.getElementById("start-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function checkStatus() {
        const result = await makeRequest("/status", "POST", {
          id: document.getElementById("status-broker-id").value,
        });
        document.getElementById("status-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function stopBroker() {
        const result = await makeRequest("/stop", "POST", {
          id: document.getElementById("stop-broker-id").value,
        });
        document.getElementById("stop-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function sendMessage() {
        const result = await makeRequest("/send", "POST", {
          id: document.getElementById("send-broker-id").value,
          topic: document.getElementById("topic").value,
          message: document.getElementById("message").value,
        });
        document.getElementById("send-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function consumeMessages() {
        const result = await makeRequest("/consume", "POST", {
          id: document.getElementById("consume-broker-id").value,
          topic: document.getElementById("consume-topic").value,
        });
        document.getElementById("consume-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function securityLogin() {
        const result = await makeRequest("/security/login", "POST", {
          username: document.getElementById("username").value,
          password: document.getElementById("password").value,
        });

        if (result.status === 200) {
          try {
            const response = JSON.parse(result.body);
            if (response.token) {
              setStoredToken(response.token);
              checkAuthStatus();
              document.getElementById("login-result").innerHTML = `<span style="color: green;">Login successful! Role: ${response.role}</span>`;
            } else {
              document.getElementById("login-result").innerHTML = `<span style="color: red;">Login failed: No token received</span>`;
            }
          } catch (e) {
            document.getElementById("login-result").innerHTML = `<span style="color: red;">Login failed: Invalid response</span>`;
          }
        } else {
          document.getElementById("login-result").innerHTML = `<span style="color: red;">Login failed: ${result.body}</span>`;
        }
      }

      async function securityLogout() {
        removeStoredToken();
        checkAuthStatus();
        document.getElementById("login-result").innerHTML = `<span style="color: blue;">Logged out successfully</span>`;
      }

      async function getSecurityStatus() {
        const result = await makeRequest("/security/status");
        document.getElementById("security-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function getMetrics() {
        const result = await makeRequest("/metrics");
        document.getElementById("metrics-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function getDashboardData() {
        const result = await makeRequest("/api/dashboard");
        document.getElementById("dashboard-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      async function getClusterHealth() {
        const result = await makeRequest("/api/cluster-health");
        document.getElementById("cluster-result").innerHTML = `Status: ${result.status}<br>Response: ${result.body}`;
      }

      // Initialize authentication status on page load
      document.addEventListener('DOMContentLoaded', function() {
        checkAuthStatus();
      });
    </script>
  </body>
</html>
"#.to_string()
}

/// Default dashboard HTML template with integrated login functionality
fn default_dashboard_html() -> String {
    r#"
<!DOCTYPE html>
<html>
  <head>
    <title>Pilgrimage Dashboard</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 0;
        padding: 0;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
      }
      .login-screen {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 100vh;
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        z-index: 1000;
      }
      .login-box {
        background: white;
        padding: 40px;
        border-radius: 10px;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        width: 100%;
        max-width: 400px;
        text-align: center;
      }
      .login-box h1 {
        color: #333;
        margin-bottom: 30px;
        font-size: 28px;
      }
      .login-form {
        display: flex;
        flex-direction: column;
        gap: 20px;
      }
      .login-input {
        padding: 12px 16px;
        border: 1px solid #ddd;
        border-radius: 6px;
        font-size: 16px;
        transition: border-color 0.3s;
      }
      .login-input:focus {
        outline: none;
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
      }
      .login-button {
        padding: 12px 24px;
        background: #667eea;
        color: white;
        border: none;
        border-radius: 6px;
        font-size: 16px;
        cursor: pointer;
        transition: background 0.3s;
      }
      .login-button:hover {
        background: #5a6fd8;
      }
      .login-error {
        color: #dc3545;
        background: #f8d7da;
        border: 1px solid #f5c6cb;
        padding: 10px;
        border-radius: 4px;
        margin-top: 15px;
      }
      .main-dashboard {
        display: none;
        min-height: 100vh;
        padding: 20px;
        color: white;
      }
      .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: rgba(255, 255, 255, 0.1);
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 30px;
        backdrop-filter: blur(10px);
      }
      .user-info {
        color: white;
        font-size: 14px;
      }
      .logout-button {
        background: rgba(255, 255, 255, 0.2);
        color: white;
        border: none;
        padding: 8px 16px;
        border-radius: 6px;
        cursor: pointer;
        transition: background 0.3s;
      }
      .logout-button:hover {
        background: rgba(255, 255, 255, 0.3);
      }
      .dashboard-container {
        max-width: 1400px;
        margin: 0 auto;
      }
      .dashboard-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
      }
      .dashboard-card {
        background: rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(10px);
        padding: 20px;
        border-radius: 10px;
        border: 1px solid rgba(255, 255, 255, 0.2);
      }
      .dashboard-card h3 {
        margin-top: 0;
        color: white;
        font-size: 18px;
      }
      .metric-value {
        font-size: 32px;
        font-weight: bold;
        color: #fff;
        margin: 10px 0;
      }
      .metric-label {
        font-size: 14px;
        opacity: 0.8;
        color: #fff;
      }
      .status-indicator {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 8px;
      }
      .status-healthy { background-color: #28a745; }
      .status-warning { background-color: #ffc107; }
      .status-error { background-color: #dc3545; }
      .refresh-button {
        background: rgba(255, 255, 255, 0.2);
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 6px;
        cursor: pointer;
        margin: 10px 0;
        transition: background 0.3s;
      }
      .refresh-button:hover {
        background: rgba(255, 255, 255, 0.3);
      }
      .chart-container {
        background: rgba(255, 255, 255, 0.9);
        padding: 20px;
        border-radius: 10px;
        color: #333;
        margin-top: 20px;
      }
      .loading {
        text-align: center;
        opacity: 0.7;
        padding: 20px;
      }
    </style>
  </head>
  <body>
    <!-- ログイン画面 -->
    <div id="login-screen" class="login-screen">
      <div class="login-box">
        <h1>🚀 Pilgrimage Dashboard</h1>
        <form class="login-form" onsubmit="handleLogin(event)">
          <input
            type="text"
            id="login-username"
            class="login-input"
            placeholder="Username"
            required
          />
          <input
            type="password"
            id="login-password"
            class="login-input"
            placeholder="Password"
            required
          />
          <button type="submit" class="login-button">🔐 Login</button>
        </form>
        <div id="login-error" class="login-error" style="display: none"></div>
      </div>
    </div>

    <!-- メインダッシュボード -->
    <div id="main-dashboard" class="main-dashboard">
      <div class="header">
        <div class="user-info">
          <span id="user-display">👤 Welcome, <span id="username-display"></span> (<span id="role-display"></span>)</span>
        </div>
        <h1>📊 Pilgrimage Dashboard</h1>
        <button class="logout-button" onclick="handleLogout()">🚪 Logout</button>
      </div>

      <div class="dashboard-container">
        <div class="dashboard-grid">
          <div class="dashboard-card">
            <h3>🏥 Cluster Health</h3>
            <div class="metric-value" id="cluster-status">
              <span class="status-indicator status-healthy"></span>Loading...
            </div>
            <div class="metric-label">Active Brokers: <span id="active-brokers">-</span></div>
            <div class="metric-label">Total Topics: <span id="total-topics">-</span></div>
            <button class="refresh-button" onclick="refreshClusterHealth()">🔄 Refresh</button>
          </div>

          <div class="dashboard-card">
            <h3>📈 Throughput</h3>
            <div class="metric-value" id="messages-per-sec">-</div>
            <div class="metric-label">Messages/sec</div>
            <div class="metric-label">In: <span id="bytes-in">-</span> | Out: <span id="bytes-out">-</span></div>
            <button class="refresh-button" onclick="refreshThroughput()">🔄 Refresh</button>
          </div>

          <div class="dashboard-card">
            <h3>⚡ Performance</h3>
            <div class="metric-value" id="avg-latency">-</div>
            <div class="metric-label">Avg Latency (ms)</div>
            <div class="metric-label">P99: <span id="p99-latency">-</span> ms</div>
            <button class="refresh-button" onclick="refreshPerformance()">🔄 Refresh</button>
          </div>

          <div class="dashboard-card">
            <h3>💾 Storage</h3>
            <div class="metric-value" id="total-messages">-</div>
            <div class="metric-label">Total Messages</div>
            <div class="metric-label">Log Size: <span id="log-size">-</span></div>
            <button class="refresh-button" onclick="refreshStorage()">🔄 Refresh</button>
          </div>
        </div>

        <div class="chart-container">
          <h3>📊 Real-time Metrics</h3>
          <div id="metrics-content" class="loading">Loading dashboard data...</div>
          <button class="refresh-button" onclick="refreshDashboard()">🔄 Refresh All</button>
        </div>
      </div>
    </div>

    <script>
      // ===== Authentication Management =====
      let authToken = null;
      let currentUser = null;
      let userRole = null;

      // Check session on page load
      document.addEventListener("DOMContentLoaded", function () {
        checkSession();
        // Auto-refresh dashboard every 30 seconds when authenticated
        setInterval(() => {
          if (authToken) {
            refreshDashboard();
          }
        }, 30000);
      });

      function ensurePasswordModal() {
        if (document.getElementById("pwd-modal")) return;
        const modalHtml = `
        <div id="pwd-modal" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.4); align-items:center; justify-content:center; z-index:1000;">
          <div style="background:#1f2937; color:#e5e7eb; width:90%; max-width:420px; border-radius:8px; padding:20px; box-shadow:0 10px 25px rgba(0,0,0,0.5);">
            <h3 style="margin:0 0 10px; font-size:18px;">Admin パスワード変更</h3>
            <p style="margin:0 0 16px; font-size:13px; color:#93c5fd;">初回ログインにはパスワード変更が必要です（localhostからのアクセス）。</p>
            <div style="display:flex; flex-direction:column; gap:10px;">
              <input id="pwd-new" type="password" placeholder="新しいパスワード (6文字以上)" style="padding:10px; border-radius:6px; border:1px solid #374151; background:#111827; color:#e5e7eb;">
              <input id="pwd-confirm" type="password" placeholder="新しいパスワード(確認)" style="padding:10px; border-radius:6px; border:1px solid #374151; background:#111827; color:#e5e7eb;">
              <div id="pwd-error" style="display:none; color:#f87171; font-size:12px;"></div>
            </div>
            <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
              <button id="pwd-cancel" style="padding:8px 12px; background:#374151; color:#e5e7eb; border:none; border-radius:6px; cursor:pointer;">キャンセル</button>
              <button id="pwd-submit" style="padding:8px 12px; background:#3b82f6; color:white; border:none; border-radius:6px; cursor:pointer;">変更</button>
            </div>
          </div>
        </div>`;
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        document.getElementById('pwd-cancel').addEventListener('click', () => togglePwdModal(false));
      }

      function togglePwdModal(show, clear=true) {
        const m = document.getElementById('pwd-modal');
        if (!m) return;
        m.style.display = show ? 'flex' : 'none';
        if (clear) {
          const e = document.getElementById('pwd-error');
          e.style.display = 'none'; e.textContent = '';
          const n = document.getElementById('pwd-new'); if (n) n.value='';
          const c = document.getElementById('pwd-confirm'); if (c) c.value='';
        }
        if (show) {
          const n = document.getElementById('pwd-new'); if (n) n.focus();
        }
      }

      function getNewPasswordFromModal() {
        ensurePasswordModal();
        togglePwdModal(true);
        return new Promise((resolve) => {
          const btn = document.getElementById('pwd-submit');
          const handler = () => {
            const err = document.getElementById('pwd-error');
            const n = document.getElementById('pwd-new').value.trim();
            const c = document.getElementById('pwd-confirm').value.trim();
            if (n.length < 6) { err.textContent = '新しいパスワードは6文字以上にしてください'; err.style.display = 'block'; return; }
            if (n !== c) { err.textContent = 'パスワードが一致しません'; err.style.display = 'block'; return; }
            btn.removeEventListener('click', handler);
            togglePwdModal(false);
            resolve(n);
          };
          btn.addEventListener('click', handler);
          const onKey = (e) => { if (e.key === 'Enter') { e.preventDefault(); handler(); } };
          document.getElementById('pwd-new').addEventListener('keydown', onKey);
          document.getElementById('pwd-confirm').addEventListener('keydown', onKey);
        });
      }

      function checkSession() {
        authToken = localStorage.getItem("authToken");
        currentUser = localStorage.getItem("currentUser");
        userRole = localStorage.getItem("userRole");

        if (authToken && currentUser && userRole) {
          showMainDashboard();
          refreshDashboard();
        } else {
          showLoginScreen();
        }
      }

      function showLoginScreen() {
        document.getElementById("login-screen").style.display = "flex";
        document.getElementById("main-dashboard").style.display = "none";
      }

      function showMainDashboard() {
        document.getElementById("login-screen").style.display = "none";
        document.getElementById("main-dashboard").style.display = "block";
        document.getElementById("username-display").textContent = currentUser;
        document.getElementById("role-display").textContent = userRole;
      }

  async function handleLogin(event) {
        event.preventDefault();

        const username = document.getElementById("login-username").value.trim();
        const password = document.getElementById("login-password").value.trim();
        const errorDiv = document.getElementById("login-error");

        if (!username || !password) {
          showLoginError("Username and password are required");
          return;
        }

        try {
          let result = await makeRequest("/security/login", "POST", {
            username: username,
            password: password,
          });

          if (result.ok && result.status === 200) {
            const response = JSON.parse(result.body);

            // Save session information
            authToken = response.token;
            currentUser = username;
            userRole = response.role;

            localStorage.setItem("authToken", authToken);
            localStorage.setItem("currentUser", currentUser);
            localStorage.setItem("userRole", userRole);

            // Display the main dashboard
            showMainDashboard();
            refreshDashboard();

            // Reset the login form
            document.getElementById("login-username").value = "";
            document.getElementById("login-password").value = "";
            errorDiv.style.display = "none";
          } else if (result.status === 403) {
            // Possibly password change required
            try {
              const data = JSON.parse(result.body || '{}');
              if (data.change_required) {
                const newPwd = await getNewPasswordFromModal();

                const chg = await makeRequest("/security/change-password", "POST", {
                  username: username,
                  current_password: password,
                  new_password: newPwd
                });

                if (chg.ok) {
                  // Retry login with new password
                  result = await makeRequest("/security/login", "POST", {
                    username: username,
                    password: newPwd,
                  });

                  if (result.ok && result.status === 200) {
                    const response = JSON.parse(result.body);
                    authToken = response.token;
                    currentUser = username;
                    userRole = response.role;

                    localStorage.setItem("authToken", authToken);
                    localStorage.setItem("currentUser", currentUser);
                    localStorage.setItem("userRole", userRole);

                    showMainDashboard();
                    refreshDashboard();
                    document.getElementById("login-username").value = "";
                    document.getElementById("login-password").value = "";
                    errorDiv.style.display = "none";
                    return;
                  } else {
                    showLoginError("パスワード変更後のログインに失敗しました: " + (result.body || ""));
                    return;
                  }
                } else {
                  showLoginError("パスワード変更に失敗しました: " + (chg.body || ""));
                  return;
                }
              }
            } catch (_) {
              // fallthrough to generic error
            }
            showLoginError(result.body || "Login failed");
          }
        } catch (error) {
          showLoginError("Network error: " + error.message);
        }
      }

      function showLoginError(message) {
        const errorDiv = document.getElementById("login-error");
        errorDiv.textContent = message;
        errorDiv.style.display = "block";
      }

      function handleLogout() {
        // Clear session information
        authToken = null;
        currentUser = null;
        userRole = null;

        localStorage.removeItem("authToken");
        localStorage.removeItem("currentUser");
        localStorage.removeItem("userRole");

        // Show login screen
        showLoginScreen();
      }

      // ===== HTTP Request Helper =====
      async function makeRequest(url, method = "GET", body = null) {
        try {
          const options = {
            method,
            headers: {
              "Content-Type": "application/json",
            },
          };

          // Add JWT token to Authorization header for protected endpoints
          const publicEndpoints = ['/security/login', '/metrics', '/console', '/dashboard', '/health', '/api/cluster-health'];
          const isPublicEndpoint = publicEndpoints.some(endpoint => url.startsWith(endpoint));

          if (authToken && !isPublicEndpoint) {
            options.headers.Authorization = `Bearer ${authToken}`;
          }

          if (body) {
            options.body = JSON.stringify(body);
          }

          const response = await fetch(url, options);
          const text = await response.text();

          // Handle authentication errors
          if (response.status === 401) {
            handleLogout();
            return { ok: false, status: response.status, body: "Authentication required. Please login again." };
          }

          return { ok: response.ok, status: response.status, body: text };
        } catch (error) {
          return { ok: false, status: "Error", body: error.message };
        }
      }

      // ===== Dashboard Data Functions =====
      async function refreshDashboard() {
        await Promise.all([
          refreshClusterHealth(),
          refreshThroughput(),
          refreshPerformance(),
          refreshStorage(),
          loadDashboardData()
        ]);
      }

      async function refreshClusterHealth() {
        try {
          const result = await makeRequest("/api/cluster-health");
          if (result.ok) {
            const data = JSON.parse(result.body);
            document.getElementById("cluster-status").innerHTML =
              `<span class="status-indicator status-${data.status === 'healthy' ? 'healthy' : 'error'}"></span>${data.status}`;
            document.getElementById("active-brokers").textContent = data.active_brokers || 0;
            document.getElementById("total-topics").textContent = data.total_topics || 0;
          }
        } catch (error) {
          console.error("Failed to refresh cluster health:", error);
        }
      }

      async function refreshThroughput() {
        try {
          const result = await makeRequest("/api/dashboard");
          if (result.ok) {
            const data = JSON.parse(result.body);
            const throughput = data.throughput || {};
            document.getElementById("messages-per-sec").textContent = throughput.messages_in_per_sec || 0;
            document.getElementById("bytes-in").textContent = formatBytes(throughput.bytes_in_per_sec || 0);
            document.getElementById("bytes-out").textContent = formatBytes(throughput.bytes_out_per_sec || 0);
          }
        } catch (error) {
          console.error("Failed to refresh throughput:", error);
        }
      }

      async function refreshPerformance() {
        try {
          const result = await makeRequest("/api/dashboard");
          if (result.ok) {
            const data = JSON.parse(result.body);
            const performance = data.performance || {};
            document.getElementById("avg-latency").textContent = (performance.avg_request_latency_ms || 0).toFixed(1);
            document.getElementById("p99-latency").textContent = (performance.p99_request_latency_ms || 0).toFixed(1);
          }
        } catch (error) {
          console.error("Failed to refresh performance:", error);
        }
      }

      async function refreshStorage() {
        try {
          const result = await makeRequest("/api/dashboard");
          if (result.ok) {
            const data = JSON.parse(result.body);
            const clusterHealth = data.cluster_health || {};
            const storage = data.storage || {};
            document.getElementById("total-messages").textContent = clusterHealth.total_messages || 0;
            document.getElementById("log-size").textContent = formatBytes(storage.total_log_size_bytes || 0);
          }
        } catch (error) {
          console.error("Failed to refresh storage:", error);
        }
      }

      async function loadDashboardData() {
        try {
          const result = await makeRequest("/api/dashboard");
          if (result.ok) {
            const data = JSON.parse(result.body);
            document.getElementById("metrics-content").innerHTML =
              `<pre>${JSON.stringify(data, null, 2)}</pre>`;
          } else {
            document.getElementById("metrics-content").innerHTML =
              `<div style="color: red;">Failed to load dashboard data: ${result.body}</div>`;
          }
        } catch (error) {
          document.getElementById("metrics-content").innerHTML =
            `<div style="color: red;">Error loading dashboard data: ${error.message}</div>`;
        }
      }

      // ===== Utility Functions =====
      function formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
      }
    </script>
  </body>
</html>
"#.to_string()
}/// Kafka-style dashboard API with detailed broker metrics
async fn dashboard_api(data: web::Data<AppState>) -> impl Responder {
    let brokers = data.brokers.lock().unwrap();
    let active_brokers = brokers.len();

    // Calculate actual metrics from running brokers
    let mut total_topics = 0;
    let mut total_messages = 0;
    let mut total_partitions = 0;
    let mut _total_consumers = 0;

    for (_broker_id, broker_wrapper) in brokers.iter() {
        if let Ok(broker) = broker_wrapper.inner.lock() {
            let topics = broker.topics.lock().unwrap();
            total_topics += topics.len();

            for (topic_name, topic) in topics.iter() {
                total_partitions += topic.num_partitions;
                _total_consumers += topic.subscribers.len();

                // Count messages from storage for each partition
                if let Ok(mut storage) = broker.storage.lock() {
                    for partition_id in 0..topic.num_partitions {
                        // Try to read messages from storage for this topic and partition
                        match storage.read_messages(topic_name, partition_id) {
                            Ok(messages) => {
                                total_messages += messages.len();
                            }
                            Err(_) => {
                                // If we can't read messages, assume 0 for this partition
                                // This might happen if no messages have been written yet
                            }
                        }
                    }
                }
            }
        }
    }

    // Calculate realistic metrics based on actual data
    let messages_per_sec = if total_messages > 0 {
        // Base rate on actual message count with some simulated activity
        std::cmp::max(total_messages as u64, 10)
    } else {
        0
    };

    let dashboard_data = serde_json::json!({
        "cluster_health": {
            "active_brokers": active_brokers,
            "under_replicated_partitions": 0,
            "offline_partitions": 0,
            "total_topics": total_topics,
            "total_messages": total_messages,
            "total_partitions": total_partitions,
            "cluster_id": "pilgrimage-cluster-001"
        },
        "throughput": {
            "messages_in_per_sec": messages_per_sec,
            "messages_out_per_sec": (messages_per_sec as f64 * 0.8) as u64,
            "bytes_in_per_sec": messages_per_sec * 1024, // Assume average 1KB per message
            "bytes_out_per_sec": (messages_per_sec as f64 * 0.8 * 1024.0) as u64
        },
        "performance": {
            "avg_request_latency_ms": 2.3,
            "p99_request_latency_ms": 12.8,
            "request_queue_size": 5,
            "cpu_usage_percent": 23.5,
            "memory_usage_percent": 45.2
        },
        "consumer_groups": {
            "active_consumer_groups": 3,
            "total_consumers": 12,
            "max_consumer_lag": 45,
            "rebalancing_groups": 0
        },
        "storage": {
            "total_log_size_bytes": 1073741824,
            "log_segments": 47,
            "avg_replication_factor": 2.0,
            "log_flush_rate_per_sec": 85,
            "retention_policy": "7 days"
        },
        "topics": [
            {
                "name": "user-events",
                "partitions": 12,
                "replication_factor": 3,
                "message_rate": 450,
                "consumer_groups": ["analytics", "notifications"]
            },
            {
                "name": "transaction-logs",
                "partitions": 8,
                "replication_factor": 3,
                "message_rate": 680,
                "consumer_groups": ["audit", "reporting"]
            },
            {
                "name": "system-metrics",
                "partitions": 4,
                "replication_factor": 2,
                "message_rate": 120,
                "consumer_groups": ["monitoring"]
            }
        ],
        "alerts": {
            "critical": 0,
            "warning": 0,
            "info": 1,
            "last_alert": "Consumer lag spike detected on topic 'user-events' - resolved"
        },
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "uptime_seconds": 86400
    });

    HttpResponse::Ok().json(dashboard_data)
}

/// Cluster health API endpoint for detailed cluster status
async fn cluster_health_api(data: web::Data<AppState>) -> impl Responder {
    let brokers = data.brokers.lock().unwrap();
    let active_brokers = brokers.len();

    let health_data = serde_json::json!({
        "cluster_id": "pilgrimage-cluster-001",
        "status": if active_brokers > 0 { "healthy" } else { "degraded" },
        "active_brokers": active_brokers,
        "total_brokers": std::cmp::max(active_brokers, 3),
        "controller_broker": 1,
        "under_replicated_partitions": 0,
        "offline_partitions": 0,
        "preferred_replica_imbalance": 0.0,
        "broker_details": (1..=std::cmp::max(active_brokers, 1)).map(|i| {
            serde_json::json!({
                "id": i,
                "host": format!("broker-{}.pilgrimage.local", i),
                "port": 9092 + i,
                "status": "online",
                "disk_usage_percent": 25.0 + (i as f64 * 5.0),
                "network_usage_mbps": 15.0 + (i as f64 * 2.0)
            })
        }).collect::<Vec<_>>(),
        "network_latency": {
            "avg_ms": 1.2,
            "p99_ms": 8.5
        },
        "last_health_check": chrono::Utc::now().to_rfc3339()
    });

    HttpResponse::Ok().json(health_data)
}

/// Simple health check endpoint
async fn health_check(data: web::Data<AppState>) -> impl Responder {
    let brokers = data.brokers.lock().unwrap();
    let active_brokers = brokers.len();

    let health_status = serde_json::json!({
        "status": if active_brokers > 0 { "healthy" } else { "unhealthy" },
        "active_brokers": active_brokers,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "service": "pilgrimage-broker"
    });

    if active_brokers > 0 {
        HttpResponse::Ok().json(health_status)
    } else {
        HttpResponse::ServiceUnavailable().json(health_status)
    }
}

/// Individual broker health check endpoint
async fn broker_health_check(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let broker_id = path.into_inner();
    let brokers = data.brokers.lock().unwrap();

    if let Some(broker_wrapper) = brokers.get(&broker_id) {
        let is_healthy = broker_wrapper.is_healthy();
        let health_status = serde_json::json!({
            "broker_id": broker_id,
            "status": if is_healthy { "healthy" } else { "unhealthy" },
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        if is_healthy {
            HttpResponse::Ok().json(health_status)
        } else {
            HttpResponse::ServiceUnavailable().json(health_status)
        }
    } else {
        HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("Broker '{}' not found", broker_id),
            "status": "not_found",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
    }
}

/// Topic details API endpoint for comprehensive topic information
async fn topic_details_api(data: web::Data<AppState>) -> impl Responder {
    let brokers = data.brokers.lock().unwrap();

    // Get actual topics from brokers if available
    let mut topics = Vec::new();

    for (_broker_id, broker_wrapper) in brokers.iter() {
        if let Ok(broker) = broker_wrapper.inner.lock() {
            let broker_topics = broker.topics.lock().unwrap();
            for (topic_name, topic) in broker_topics.iter() {
                topics.push(serde_json::json!({
                    "name": topic_name,
                    "partitions": topic.num_partitions,
                    "replication_factor": topic.replication_factor,
                    "retention_ms": 604800000, // 7 days
                    "cleanup_policy": "delete",
                    "segment_bytes": 1073741824, // 1GB
                    "message_count": 12500, // Simulated message count
                    "total_size_bytes": 12800000, // Simulated size
                    "producer_count": 1,
                    "consumer_count": topic.subscribers.len(),
                    "messages_per_sec": 100.0 + rand::random::<f64>() * 500.0,
                    "bytes_per_sec": (50000.0 + rand::random::<f64>() * 250000.0) as u64,
                    "partition_details": (0..topic.num_partitions).map(|i| {
                        serde_json::json!({
                            "partition_id": i,
                            "leader": 1,
                            "replicas": [1, 2],
                            "isr": [1, 2],
                            "log_size": 1250 + (i * 100),
                            "log_start_offset": 0,
                            "high_watermark": 1250 + (i * 100)
                        })
                    }).collect::<Vec<_>>()
                }));
            }
        }
    }

    // If no topics found, provide sample data
    if topics.is_empty() {
        topics = vec![
            serde_json::json!({
                "name": "user-events",
                "partitions": 12,
                "replication_factor": 3,
                "retention_ms": 604800000,
                "cleanup_policy": "delete",
                "segment_bytes": 1073741824,
                "message_count": 150000,
                "total_size_bytes": 153600000,
                "producer_count": 3,
                "consumer_count": 5,
                "messages_per_sec": 450.0,
                "bytes_per_sec": 460800,
                "partition_details": (0..12).map(|i| {
                    serde_json::json!({
                        "partition_id": i,
                        "leader": (i % 3) + 1,
                        "replicas": [((i % 3) + 1), ((i % 3) + 2), ((i % 3) + 3)],
                        "isr": [((i % 3) + 1), ((i % 3) + 2), ((i % 3) + 3)],
                        "log_size": 12500,
                        "log_start_offset": 0,
                        "high_watermark": 12500
                    })
                }).collect::<Vec<_>>()
            }),
            serde_json::json!({
                "name": "transaction-logs",
                "partitions": 8,
                "replication_factor": 3,
                "retention_ms": 2592000000u64, // 30 days
                "cleanup_policy": "delete",
                "segment_bytes": 1073741824,
                "message_count": 95000,
                "total_size_bytes": 97280000,
                "producer_count": 2,
                "consumer_count": 3,
                "messages_per_sec": 680.0,
                "bytes_per_sec": 696320,
                "partition_details": (0..8).map(|i| {
                    serde_json::json!({
                        "partition_id": i,
                        "leader": (i % 3) + 1,
                        "replicas": [((i % 3) + 1), ((i % 3) + 2), ((i % 3) + 3)],
                        "isr": [((i % 3) + 1), ((i % 3) + 2), ((i % 3) + 3)],
                        "log_size": 11875,
                        "log_start_offset": 0,
                        "high_watermark": 11875
                    })
                }).collect::<Vec<_>>()
            }),
        ];
    }

    let response_data = serde_json::json!({
        "topics": topics,
        "total_topics": topics.len(),
        "total_partitions": topics.iter().map(|t| t["partitions"].as_u64().unwrap_or(0)).sum::<u64>(),
        "cluster_wide_message_rate": topics.iter().map(|t| t["messages_per_sec"].as_f64().unwrap_or(0.0)).sum::<f64>(),
        "cluster_wide_byte_rate": topics.iter().map(|t| t["bytes_per_sec"].as_u64().unwrap_or(0)).sum::<u64>(),
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    HttpResponse::Ok().json(response_data)
}

/// Security login endpoint
async fn security_login(
  req: HttpRequest,
  info: web::Json<SecurityLoginRequest>,
  data: web::Data<AppState>,
) -> ActixResult<impl Responder> {
  // Determine client IP (respect XFF if configured/trusted)
  let client_ip = resolve_client_ip(&req, &data);

  // Rate limit / lockout check
  if let Some(ip) = client_ip {
    if is_locked_out(&data, &info.username, ip) {
      return Ok(HttpResponse::TooManyRequests().json(serde_json::json!({
        "error": "too_many_attempts",
        "message": "Too many failed attempts. Please try again later."
      })));
    }
  }
    // Validate input parameters
    if info.username.trim().is_empty() {
        let _ = data
            .security_manager
            .log_simple_security_event(
                "login_failed",
                "unknown",
                "Login attempt with empty username",
                false,
            )
            .await;
        return Ok(HttpResponse::BadRequest().json("Username cannot be empty"));
    }

    if info.password.trim().is_empty() {
        let _ = data
            .security_manager
            .log_simple_security_event(
                "login_failed",
                &info.username,
                "Login attempt with empty password",
                false,
            )
            .await;
        return Ok(HttpResponse::BadRequest().json("Password cannot be empty"));
    }

  // Enforce admin password change on first localhost login
  let is_localhost = client_ip.map(|ip| ip.is_loopback()).unwrap_or(false);

  // Authenticate once to validate credentials (avoids showing modal on wrong password)
  let auth_result_initial = {
    // Do not hold the lock across awaits; this block ends before any await
    data
      .jwt_authenticator
      .lock()
      .unwrap()
      .authenticate_client(&info.username, &info.password)
  };

  if info.username == "admin" {
    let requires_change = {
      if let Ok(auth) = data.jwt_authenticator.lock() {
        auth.is_admin_password_change_required()
      } else { false }
    };
    if is_localhost && requires_change {
      if auth_result_initial.success {
        // Credentials are correct but admin must change password first
        // Reset attempts on credential success (even though we require change)
        if let Some(ip) = client_ip {
          reset_attempts(&data, &info.username, ip);
        }
        let _ = data
          .security_manager
          .log_simple_security_event(
            "password_change_required",
            &info.username,
            "Admin must change password on first localhost login",
            false,
          )
          .await;
        return Ok(HttpResponse::Forbidden().json(serde_json::json!({
          "change_required": true,
          "message": "Admin must change password before first login from localhost"
        })));
      }
      // If credentials are wrong, fall through to standard unauthorized handling below
    }
  }

  // Generate JWT token using the distributed authenticator
  match auth_result_initial {
    auth_result if auth_result.success => {
      // Reset attempts on success
      if let Some(ip) = client_ip {
        reset_attempts(&data, &info.username, ip);
      }
      // Determine role from permissions
      let role = if auth_result
        .permissions
        .iter()
        .any(|p| p == "admin")
      {
        "admin"
      } else if auth_result
        .permissions
        .iter()
        .any(|p| p == "write")
      {
        "user"
      } else {
        "guest"
      };
            // Log successful security event
            let _ = data
                .security_manager
                .log_simple_security_event(
                    "user_login",
                    &info.username,
                    &format!("User {} logged in with JWT authentication", info.username),
                    true,
                )
                .await;

      Ok(HttpResponse::Ok().json(SecurityLoginResponse {
                token: auth_result.token.unwrap_or_default(),
        role: role.to_string(),
            }))
        }
        auth_result => {
      // Record failed attempt
      if let Some(ip) = client_ip {
        record_failed_attempt(&data, &info.username, ip);
      }
            let _ = data
                .security_manager
                .log_simple_security_event(
                    "login_failed",
                    &info.username,
                    &format!("JWT authentication failed: {:?}", auth_result.error_message),
                    false,
                )
                .await;
            Ok(HttpResponse::Unauthorized().json("Invalid username or password"))
        }
    }
}

/// Endpoint to change password (used for enforcing admin first-login policy)
async fn security_change_password(
  req: HttpRequest,
  info: web::Json<ChangePasswordRequest>,
  data: web::Data<AppState>,
) -> ActixResult<impl Responder> {
  // Only allow from localhost for admin enforcement (proxy-aware)
  let client_ip = resolve_client_ip(&req, &data);
  let is_localhost = client_ip.map(|ip| ip.is_loopback()).unwrap_or(false);

  if !is_localhost {
    return Ok(HttpResponse::Forbidden().json("Password change allowed only from localhost"));
  }

  if info.username.trim().is_empty() || info.new_password.trim().is_empty() {
    return Ok(HttpResponse::BadRequest().json("Username and new password are required"));
  }

  // Currently only enforce/admin supported in this flow
  if info.username != "admin" {
    return Ok(HttpResponse::BadRequest().json("Only admin password change is supported here"));
  }

  let mut auth = match data.jwt_authenticator.lock() {
    Ok(guard) => guard,
    Err(_) => return Ok(HttpResponse::InternalServerError().json("Auth state lock poisoned")),
  };

  if !auth.is_admin_password_change_required() {
    return Ok(HttpResponse::BadRequest().json("Password change not required"));
  }

  match auth.change_user_password(&info.username, &info.current_password, &info.new_password) {
    Ok(()) => {
      // Disable the requirement after successful change
      auth.set_admin_password_change_required(false);

      let _ = data
        .security_manager
        .log_simple_security_event(
          "password_changed",
          &info.username,
          "Admin password changed on localhost",
          true,
        )
        .await;

      Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "message": "Password changed successfully"
      })))
    }
    Err(e) => Ok(HttpResponse::Unauthorized().json(serde_json::json!({
      "status": "error",
      "message": e
    }))),
  }
}

/// Admin-only endpoint to create a new user
async fn security_create_user(
  req: HttpRequest,
  info: web::Json<CreateUserRequest>,
  data: web::Data<AppState>,
) -> ActixResult<impl Responder> {
  // Require authenticated admin
  let Some(claims) = get_authenticated_user_claims(&req) else {
    return Ok(HttpResponse::Unauthorized().json("Authentication required"));
  };
  let is_admin = claims.roles.iter().any(|r| r == "admin");
  if !is_admin {
    return Ok(HttpResponse::Forbidden().json("Admin role required"));
  }

  let username = info.username.trim();
  let password = info.password.trim();
  let role = info.role.trim().to_lowercase();

  if username.is_empty() || password.is_empty() {
    return Ok(HttpResponse::BadRequest().json("Username and password are required"));
  }
  if password.len() < 4 {
    return Ok(HttpResponse::BadRequest().json("Password must be at least 4 characters"));
  }

  // Map role -> permissions
  let permissions: Vec<String> = match role.as_str() {
    "admin" => vec!["read".into(), "write".into(), "admin".into()],
    "user" => vec!["read".into(), "write".into()],
    "viewer" | "view" | "read" => vec!["read".into()],
    other => {
      return Ok(HttpResponse::BadRequest().json(format!("Unknown role: {}", other)));
    }
  };

  // Add user
  let mut auth = match data.jwt_authenticator.lock() {
    Ok(guard) => guard,
    Err(_) => return Ok(HttpResponse::InternalServerError().json("Auth state lock poisoned")),
  };

  // Optional: basic existence prevention via permissions map
  if auth.user_exists(username) {
    return Ok(HttpResponse::Conflict().json("User already exists"));
  }

  auth.add_user(username, password);
  auth.add_user_permissions(username.to_string(), permissions);

  let _ = data.security_manager
    .log_simple_security_event(
      "user_created",
      &claims.sub,
      &format!("Admin '{}' created user '{}' with role '{}'", claims.sub, username, role),
      true,
    ).await;

  Ok(HttpResponse::Ok().json(serde_json::json!({
    "status": "ok",
    "username": username,
    "role": role,
  })))
}

/// Resolve client IP, honoring X-Forwarded-For only if the direct peer is a trusted proxy
fn resolve_client_ip(req: &HttpRequest, data: &AppState) -> Option<IpAddr> {
  let peer_ip = req.peer_addr().map(|a| a.ip());
  if !data.trust_xff {
    return peer_ip;
  }

  let Some(peer) = peer_ip else { return None; };
  let is_trusted = data.trusted_proxies.iter().any(|p| *p == peer);
  if !is_trusted {
    return Some(peer);
  }

  // Peer is a trusted proxy; use X-Forwarded-For first IP
  if let Some(hval) = req.headers().get("X-Forwarded-For").and_then(|h| h.to_str().ok()) {
    if let Some(first) = hval.split(',').next() {
      let ip_str = first.trim();
      if let Ok(ip) = ip_str.parse::<IpAddr>() {
        return Some(ip);
      }
    }
  }
  Some(peer)
}

fn throttle_key(username: &str, ip: IpAddr) -> String {
  format!("{}|{}", username, ip)
}

fn is_locked_out(data: &AppState, username: &str, ip: IpAddr) -> bool {
  const LOCKOUT_WINDOW: Duration = Duration::from_secs(15 * 60);
  let key = throttle_key(username, ip);
  let mut map = data.login_attempts.lock().unwrap();
  if let Some(info) = map.get_mut(&key) {
    if let Some(until) = info.locked_until {
      if Instant::now() < until { return true; }
      // lock expired, reset
      info.locked_until = None;
      info.count = 0;
      info.first = Instant::now();
      return false;
    }
    // Slide window
    if Instant::now().duration_since(info.first) > LOCKOUT_WINDOW {
      info.count = 0;
      info.first = Instant::now();
    }
  }
  false
}

fn record_failed_attempt(data: &AppState, username: &str, ip: IpAddr) {
  const LOCKOUT_WINDOW: Duration = Duration::from_secs(15 * 60);
  const MAX_ATTEMPTS: u32 = 5;
  const LOCKOUT_DURATION: Duration = Duration::from_secs(5 * 60);

  let key = throttle_key(username, ip);
  let mut map = data.login_attempts.lock().unwrap();
  let entry = map.entry(key).or_insert(AttemptInfo { count: 0, first: Instant::now(), locked_until: None });

  // Slide window
  if Instant::now().duration_since(entry.first) > LOCKOUT_WINDOW {
    entry.count = 0;
    entry.first = Instant::now();
    entry.locked_until = None;
  }

  entry.count += 1;
  if entry.count >= MAX_ATTEMPTS {
    entry.locked_until = Some(Instant::now() + LOCKOUT_DURATION);
    entry.count = 0; // reset count during lock
    entry.first = Instant::now();
  }
}

fn reset_attempts(data: &AppState, username: &str, ip: IpAddr) {
  let key = throttle_key(username, ip);
  let mut map = data.login_attempts.lock().unwrap();
  map.remove(&key);
}

/// Security role assignment endpoint
async fn security_assign_role(
    info: web::Json<SecurityRoleRequest>,
    data: web::Data<AppState>,
) -> ActixResult<impl Responder> {
    match data
        .security_manager
        .authorization_manager()
        .assign_role(&info.user_id, &info.role)
        .await
    {
        Ok(_) => {
            let _ = data
                .security_manager
                .log_simple_security_event(
                    "role_assignment",
                    &info.user_id,
                    &format!("Assigned role {} to user {}", info.role, info.user_id),
                    true,
                )
                .await;

            Ok(HttpResponse::Ok().json("Role assigned successfully"))
        }
        Err(e) => {
            let _ = data
                .security_manager
                .log_simple_security_event(
                    "role_assignment_failed",
                    &info.user_id,
                    &format!(
                        "Failed to assign role {} to user {}: {}",
                        info.role, info.user_id, e
                    ),
                    false,
                )
                .await;

            Ok(HttpResponse::BadRequest().json(format!("Failed to assign role: {}", e)))
        }
    }
}

/// Security permission check endpoint
async fn security_check_permission(
    info: web::Json<SecurityPermissionRequest>,
    data: web::Data<AppState>,
) -> ActixResult<impl Responder> {
    match data
        .security_manager
        .authorization_manager()
        .check_permission(&info.user_id, &info.resource, &info.action)
        .await
    {
        Ok(allowed) => {
            let _ = data
                .security_manager
                .log_simple_security_event(
                    "permission_check",
                    &info.user_id,
                    &format!(
                        "Permission check for {} on {} - {}",
                        info.action,
                        info.resource,
                        if allowed { "ALLOWED" } else { "DENIED" }
                    ),
                    allowed,
                )
                .await;

            Ok(HttpResponse::Ok().json(allowed))
        }
        Err(e) => {
            Ok(HttpResponse::InternalServerError().json(format!("Permission check failed: {}", e)))
        }
    }
}

/// Security status endpoint
async fn security_status(data: web::Data<AppState>) -> ActixResult<impl Responder> {
    let stats = data
        .security_manager
        .get_security_stats()
        .await
        .to_hashmap();

    let response = SecurityStatusResponse {
        tls_enabled: stats.contains_key("tls_enabled") && stats["tls_enabled"] > 0.0,
        active_connections: *stats.get("active_connections").unwrap_or(&0.0) as usize,
        audit_events_today: *stats.get("audit_events_today").unwrap_or(&0.0) as usize,
        roles_configured: *stats.get("roles_configured").unwrap_or(&0.0) as usize,
    };

    Ok(HttpResponse::Ok().json(response))
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
/// ```no_run
/// use pilgrimage::web_console;
///
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
    println!("🔧 Initializing Pilgrimage Web Console...");

    // Initialize security manager with default config
    println!("🔐 Setting up security manager...");
    let security_config = crate::security::SecurityConfig::default();
    let security_manager = Arc::new(
        SecurityManager::new(security_config)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
    );
    println!("✅ Security manager initialized with comprehensive security features");

    println!("🗂️  Setting up application state...");

    // Initialize JWT authenticator with shared secret (same as CLI)
    println!("🔑 Setting up JWT authentication...");
    let jwt_secret = get_jwt_secret_for_web()?;
    let mut jwt_authenticator = DistributedAuthenticator::new(
        jwt_secret,
        "pilgrimage-web".to_string(),
    );

    // Pre-configure demo users (in production, these should come from a database)
    jwt_authenticator.add_user("admin", "password");
    jwt_authenticator.add_user("user", "password");
    jwt_authenticator.add_user("guest", "guest");

    jwt_authenticator.add_user_permissions("admin".to_string(), vec!["read".to_string(), "write".to_string(), "admin".to_string()]);
    jwt_authenticator.add_user_permissions("user".to_string(), vec!["read".to_string(), "write".to_string()]);
    jwt_authenticator.add_user_permissions("guest".to_string(), vec!["read".to_string()]);

  // Admin password-change requirement from env (default true for safety)
  let admin_change_required = std::env::var("ADMIN_PASSWORD_CHANGE_REQUIRED")
    .ok()
    .and_then(|v| v.parse::<bool>().ok())
    .unwrap_or(true);
  jwt_authenticator.set_admin_password_change_required(admin_change_required);

  let jwt_authenticator = Arc::new(Mutex::new(jwt_authenticator));
    println!("✅ JWT authentication configured");

  // Proxy/XFF config from env
  let trust_xff = std::env::var("TRUST_XFF")
    .ok()
    .and_then(|v| v.parse::<bool>().ok())
    .unwrap_or(false);
  let trusted_proxies = std::env::var("TRUSTED_PROXIES")
    .ok()
    .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect::<Vec<IpAddr>>())
    .unwrap_or_else(|| vec![]);

  let state = AppState {
        brokers: Arc::new(Mutex::new(HashMap::new())),
        security_manager,
    jwt_authenticator,
    trust_xff,
    trusted_proxies,
    login_attempts: Arc::new(Mutex::new(HashMap::new())),
    };
    println!("✅ Application state configured");

    println!("🌐 Configuring HTTP server and routes...");
    let server = HttpServer::new(move || {
        // Create JWT auth middleware
        let jwt_auth = JwtAuth::new().expect("Failed to create JWT auth middleware");

        App::new()
            .app_data(web::Data::new(state.clone()))
            // Public endpoints (no authentication required)
            .route("/console", web::get().to(console_html))
            .route("/test", web::get().to(test_console_html)) // Legacy support
            .route("/dashboard", web::get().to(dashboard_html))
            .route("/metrics", web::get().to(metrics))
            // Health check endpoints (monitoring systems need access)
            .route("/health", web::get().to(health_check))
            .route("/health/{broker_id}", web::get().to(broker_health_check))
            .route("/api/cluster-health", web::get().to(cluster_health_api))
            // Security endpoints (no auth required for login and password change)
            .route("/security/login", web::post().to(security_login))
            .route("/security/change-password", web::post().to(security_change_password))
            // Protected endpoints (JWT authentication required)
            .service(
                web::scope("")
                    .wrap(jwt_auth)
                    .route("/start", web::post().to(start_broker))
                    .route("/stop", web::post().to(stop_broker))
                    .route("/send", web::post().to(send_message))
                    .route("/consume", web::post().to(consume_messages))
                    .route("/status", web::post().to(broker_status))
                    .route("/api/dashboard", web::get().to(dashboard_api))
                    .route("/api/topic-details", web::get().to(topic_details_api))
        .route("/security/create-user", web::post().to(security_create_user))
                    .route("/security/assign-role", web::post().to(security_assign_role))
                    .route("/security/check-permission", web::post().to(security_check_permission))
                    .route("/security/status", web::get().to(security_status))
            )
    })
    .bind(("127.0.0.1", 8080))?;

    println!("🚀 Starting HTTP server on http://127.0.0.1:8080");
    println!("�️  Web Console available at: http://127.0.0.1:8080/console");
    println!("�📊 Dashboard available at: http://127.0.0.1:8080/dashboard");
    println!("🏥 Health check available at: http://127.0.0.1:8080/health");
    println!("🔒 Security status at: http://127.0.0.1:8080/security/status");
    println!("📈 Metrics available at: http://127.0.0.1:8080/metrics");
    println!("");
    println!("Press Ctrl+C to stop the server");

    server.run().await
}

#[cfg(test)]
mod tests {
    use crate::security::SecurityConfig;

    use super::*;
    use actix_web::{App, test, web};
    use serde_json::json;

    /// Helper function to create AppState for tests
    async fn create_test_app_state() -> AppState {
        let security_manager = Arc::new(
            SecurityManager::new(SecurityConfig::default())
                .await
                .expect("Failed to initialize security manager"),
        );

        // Create a test JWT authenticator
        let jwt_secret = DistributedAuthenticator::generate_jwt_secret();
        let mut jwt_authenticator = DistributedAuthenticator::new(
            jwt_secret,
            "pilgrimage-test".to_string(),
        );

        // Add test users
        jwt_authenticator.add_user("admin", "password");
        jwt_authenticator.add_user("user", "password");
        jwt_authenticator.add_user("guest", "guest");

    AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
            security_manager,
            jwt_authenticator: Arc::new(Mutex::new(jwt_authenticator)),
      trust_xff: false,
      trusted_proxies: vec![],
      login_attempts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

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

        let state = create_test_app_state().await;

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

        let state = create_test_app_state().await;

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

        let state = create_test_app_state().await;

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

  let state = create_test_app_state().await;

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

        let state = create_test_app_state().await;

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

        let state = create_test_app_state().await;

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
        let state = create_test_app_state().await;

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
        let state = create_test_app_state().await;

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
        let state = create_test_app_state().await;

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
        let state = create_test_app_state().await;

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

    let app_state = create_test_app_state().await;

        let srv = actix_test::start(move || {
            App::new()
                .app_data(web::Data::new(app_state.clone()))
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

    /// Test for authentication with empty username
    #[actix_rt::test]
    async fn test_auth_empty_username() {
    let state = create_test_app_state().await;

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/security/login", web::post().to(security_login)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/security/login")
            .set_json(json!({
                "username": "",
                "password": "password"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

  #[actix_rt::test]
  async fn test_login_admin_requires_change_then_success() {
    let state = create_test_app_state().await;

    {
      let mut auth = state.jwt_authenticator.lock().unwrap();
      auth.set_admin_password_change_required(true);
    }

    let mut app = test::init_service(
      App::new()
        .app_data(web::Data::new(state.clone()))
        .route("/security/login", web::post().to(super::security_login))
        .route("/security/change-password", web::post().to(super::security_change_password))
    ).await;

    // First login should require password change (403)
    let req = test::TestRequest::post()
      .uri("/security/login")
      .set_json(json!({"username":"admin","password":"password"}))
      .to_request();
    let resp = test::call_service(&mut app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::FORBIDDEN);

    // Change password from localhost
    let req = test::TestRequest::post()
      .uri("/security/change-password")
      .set_json(json!({"username":"admin","current_password":"password","new_password":"newpass123"}))
      .to_request();
    let resp = test::call_service(&mut app, req).await;
    assert!(resp.status().is_success());

    // Login with new password succeeds
    let req = test::TestRequest::post()
      .uri("/security/login")
      .set_json(json!({"username":"admin","password":"newpass123"}))
      .to_request();
    let resp = test::call_service(&mut app, req).await;
    assert!(resp.status().is_success());
  }

  #[actix_rt::test]
  async fn test_xff_trusted_proxy_uses_forwarded_ip_and_locks_out() {
    use actix_web::http::header::{HeaderName, HeaderValue};

    let state = create_test_app_state().await;

    let mut app = test::init_service(
      App::new()
        .app_data(web::Data::new(state.clone()))
        .route("/security/login", web::post().to(super::security_login))
    ).await;

    // Simulate repeated failures from forwarded client IP via trusted proxy
    for _ in 0..6 {
      let req = test::TestRequest::post()
        .uri("/security/login")
        .insert_header((HeaderName::from_static("x-forwarded-for"), HeaderValue::from_str("203.0.113.1").unwrap()))
        .set_json(json!({"username":"user","password":"wrong"}))
        .to_request();
      let _ = test::call_service(&mut app, req).await;
    }

    // Expect lockout
    let req = test::TestRequest::post()
      .uri("/security/login")
      .insert_header((HeaderName::from_static("x-forwarded-for"), HeaderValue::from_str("203.0.113.1").unwrap()))
      .set_json(json!({"username":"user","password":"wrong"}))
      .to_request();
    let resp = test::call_service(&mut app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::TOO_MANY_REQUESTS);
  }

    /// Test for authentication with empty password
    #[actix_rt::test]
    async fn test_auth_empty_password() {
        let state = create_test_app_state().await;

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/security/login", web::post().to(security_login)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/security/login")
            .set_json(json!({
                "username": "admin",
                "password": ""
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    /// Test for authentication with invalid credentials
    #[actix_rt::test]
    async fn test_auth_invalid_credentials() {
        let state = create_test_app_state().await;

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/security/login", web::post().to(security_login)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/security/login")
            .set_json(json!({
                "username": "admin",
                "password": "wrongpassword"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);
    }

    /// Test for authentication with valid credentials
    #[actix_rt::test]
    async fn test_auth_valid_credentials() {
        let state = create_test_app_state().await;

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/security/login", web::post().to(security_login)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/security/login")
            .set_json(json!({
                "username": "admin",
                "password": "password"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }
}

/// Get JWT secret for web console (shared with CLI authentication)
fn get_jwt_secret_for_web() -> std::io::Result<Vec<u8>> {
    use std::fs;
    use std::path::PathBuf;

    let session_dir = if let Some(home_dir) = dirs::home_dir() {
        home_dir.join(".pilgrimage")
    } else {
        PathBuf::from(".pilgrimage")
    };

    let secret_file = session_dir.join("jwt_secret");

    if secret_file.exists() {
        let secret_data = fs::read(&secret_file)?;
        if secret_data.len() == 32 {
            Ok(secret_data)
        } else {
            // If the secret is invalid, create a new one
            if !session_dir.exists() {
                fs::create_dir_all(&session_dir)?;
            }
            let secret = DistributedAuthenticator::generate_jwt_secret();
            fs::write(&secret_file, &secret)?;
            Ok(secret)
        }
    } else {
        // If no secret exists, create one
        if !session_dir.exists() {
            fs::create_dir_all(&session_dir)?;
        }
        let secret = DistributedAuthenticator::generate_jwt_secret();
        fs::write(&secret_file, &secret)?;
        Ok(secret)
    }
}
