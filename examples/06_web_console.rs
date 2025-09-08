//! # Web Console and Management Dashboard Example
//!
//! This example demonstrates the web console and management features:
//! - REST API management interface
//! - Real-time dashboard with WebSocket updates
//! - Administrative operations
//! - Monitoring and control panel
//! - Interactive web interface for broker management

use chrono::Utc;
use pilgrimage::auth::authentication::BasicAuthenticator;
use pilgrimage::auth::authorization::RoleBasedAccessControl;
use pilgrimage::auth::token::TokenManager;
use pilgrimage::broker::Broker;
use pilgrimage::schema::MessageSchema;
use pilgrimage::schema::version::SchemaVersion;
use pilgrimage::subscriber::types::Subscriber;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;

// Simulated dashboard data structures
#[derive(Debug, Clone)]
struct DashboardMetrics {
    total_messages: u64,
    active_topics: u32,
    active_subscribers: u32,
    system_health: String,
    uptime_seconds: u64,
    memory_usage_mb: f64,
    cpu_usage_percent: f64,
}

impl DashboardMetrics {
    fn new() -> Self {
        Self {
            total_messages: 0,
            active_topics: 0,
            active_subscribers: 0,
            system_health: "Healthy".to_string(),
            uptime_seconds: 0,
            memory_usage_mb: 256.0,
            cpu_usage_percent: 15.0,
        }
    }

    fn update_metrics(&mut self, messages: u64, topics: u32, subscribers: u32) {
        self.total_messages = messages;
        self.active_topics = topics;
        self.active_subscribers = subscribers;
        self.uptime_seconds += 1;

        // Simulate varying system metrics
        self.memory_usage_mb = 256.0 + (self.total_messages as f64 * 0.1);
        self.cpu_usage_percent = 15.0 + (messages % 50) as f64;

        self.system_health = if self.cpu_usage_percent < 80.0 && self.memory_usage_mb < 1024.0 {
            "Healthy".to_string()
        } else {
            "Warning".to_string()
        };
    }

    fn display_dashboard(&self) {
        println!("📊 Real-time Dashboard Metrics:");
        println!("   Total Messages: {}", self.total_messages);
        println!("   Active Topics: {}", self.active_topics);
        println!("   Active Subscribers: {}", self.active_subscribers);
        println!("   System Health: {}", self.system_health);
        println!("   Uptime: {}s", self.uptime_seconds);
        println!("   Memory Usage: {:.1}MB", self.memory_usage_mb);
        println!("   CPU Usage: {:.1}%", self.cpu_usage_percent);
    }
}

#[derive(Debug, Clone)]
struct WebAPIResponse {
    status: String,
    message: String,
    data: Option<serde_json::Value>,
}

impl WebAPIResponse {
    fn success(message: &str, data: Option<serde_json::Value>) -> Self {
        Self {
            status: "success".to_string(),
            message: message.to_string(),
            data,
        }
    }

    fn error(message: &str) -> Self {
        Self {
            status: "error".to_string(),
            message: message.to_string(),
            data: None,
        }
    }

    fn display(&self) {
        let icon = if self.status == "success" {
            "✅"
        } else {
            "❌"
        };
        println!(
            "   {} API Response: {} - {}",
            icon,
            self.status.to_uppercase(),
            self.message
        );
        if let Some(data) = &self.data {
            println!(
                "      Data: {}",
                serde_json::to_string_pretty(data).unwrap_or_default()
            );
        }
    }
}

async fn simulate_rest_api_calls(broker: Arc<Mutex<Broker>>) -> Vec<WebAPIResponse> {
    let mut responses = Vec::new();

    // GET /api/broker/status
    println!("🌐 GET /api/broker/status");
    let broker_guard = broker.lock().unwrap();
    let status_data = json!({
        "broker_id": broker_guard.id,
        "healthy": broker_guard.is_healthy(),
        "uptime": "1h 23m",
        "version": "1.0.0"
    });
    responses.push(WebAPIResponse::success(
        "Broker status retrieved",
        Some(status_data),
    ));
    drop(broker_guard);

    // GET /api/topics
    println!("🌐 GET /api/topics");
    let broker_guard = broker.lock().unwrap();
    match broker_guard.list_topics() {
        Ok(topics) => {
            let topics_data = json!({
                "topics": topics,
                "count": topics.len()
            });
            responses.push(WebAPIResponse::success(
                "Topics retrieved successfully",
                Some(topics_data),
            ));
        }
        Err(e) => {
            responses.push(WebAPIResponse::error(&format!(
                "Failed to retrieve topics: {}",
                e
            )));
        }
    }
    drop(broker_guard);

    // POST /api/topics/web-console-demo
    println!("🌐 POST /api/topics/web-console-demo");
    let mut broker_guard = broker.lock().unwrap();
    match broker_guard.create_topic("web-console-demo", None) {
        Ok(_) => {
            let topic_data = json!({
                "topic_name": "web-console-demo",
                "partitions": 4,
                "created_at": Utc::now().to_rfc3339()
            });
            responses.push(WebAPIResponse::success(
                "Topic created successfully",
                Some(topic_data),
            ));
        }
        Err(e) => {
            responses.push(WebAPIResponse::error(&format!(
                "Failed to create topic: {}",
                e
            )));
        }
    }
    drop(broker_guard);

    // POST /api/messages
    println!("🌐 POST /api/messages");
    let message_data = json!({
        "event_type": "web_console_test",
        "message": "Test message from web console",
        "timestamp": Utc::now().to_rfc3339(),
        "source": "web_dashboard",
        "priority": "normal"
    });

    let schema = MessageSchema {
        id: 100,
        definition: message_data.to_string(),
        version: SchemaVersion::new(1),
        compatibility: pilgrimage::schema::compatibility::Compatibility::Forward,
        metadata: Some({
            let mut meta = HashMap::new();
            meta.insert("source".to_string(), "web_console".to_string());
            meta.insert("api_endpoint".to_string(), "/api/messages".to_string());
            meta
        }),
        topic_id: Some("web-console-demo".to_string()),
        partition_id: Some(0),
    };

    let mut broker_guard = broker.lock().unwrap();
    match broker_guard.send_message(schema) {
        Ok(_) => {
            responses.push(WebAPIResponse::success(
                "Message sent successfully",
                Some(message_data),
            ));
        }
        Err(e) => {
            responses.push(WebAPIResponse::error(&format!(
                "Failed to send message: {}",
                e
            )));
        }
    }
    drop(broker_guard);

    responses
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("🌐 Pilgrimage Web Console & Management Dashboard Example");
    println!("========================================================");

    // Step 1: Initialize broker and web console infrastructure
    println!("\n🔧 Step 1: Setting Up Web Console Infrastructure");

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let storage_path = format!("storage/web_console_{}", timestamp);

    let broker = Arc::new(Mutex::new(
        Broker::new("web-console-broker-001", 4, 2, &storage_path)
            .expect("Failed to create broker"),
    ));

    // Initialize dashboard metrics
    let dashboard_metrics = Arc::new(Mutex::new(DashboardMetrics::new()));

    println!("   ✓ Broker initialized for web console");
    println!("   ✓ Dashboard metrics system ready");

    // Step 2: Set up authentication and authorization
    println!("\n🔐 Step 2: Setting Up Web Authentication");

    let _auth = BasicAuthenticator::new();
    let token_manager = TokenManager::new(b"web-console-secret-key");
    let _rbac = RoleBasedAccessControl::new();

    // Simulate user authentication
    let admin_token = token_manager.generate_token(
        "admin_user",
        vec![
            "TopicCreate".to_string(),
            "TopicDelete".to_string(),
            "MessageSend".to_string(),
            "MessageReceive".to_string(),
            "Admin".to_string(),
        ],
    )?;

    let user_token = token_manager.generate_token(
        "regular_user",
        vec!["MessageSend".to_string(), "MessageReceive".to_string()],
    )?;

    println!("   ✓ Authentication system configured");
    println!("   ✓ Admin token generated: {}...", &admin_token[..20]);
    println!("   ✓ User token generated: {}...", &user_token[..20]);
    println!("   ✓ Role-based access control enabled");

    // Step 3: Create initial topics for demo
    println!("\n📁 Step 3: Creating Demo Topics for Web Console");

    let demo_topics = vec![
        "dashboard-events",
        "user-activity",
        "system-notifications",
        "api-logs",
        "websocket-messages",
    ];

    for topic in &demo_topics {
        {
            let mut broker_guard = broker.lock().unwrap();
            broker_guard.create_topic(topic, None)?;
        }
        println!("   ✓ Created demo topic: {}", topic);
    }

    // Step 4: Set up real-time WebSocket simulation
    println!("\n📡 Step 4: Setting Up Real-time WebSocket Updates");

    // Create subscribers for real-time updates
    let websocket_topics = vec!["dashboard-events", "system-notifications"];

    for topic in &websocket_topics {
        let topic_clone = topic.to_string();
        let metrics_clone = Arc::clone(&dashboard_metrics);

        let subscriber = Subscriber::new(
            format!("websocket-{}", topic),
            Box::new(move |message: String| {
                println!(
                    "📨 WebSocket Update [{}]: {}",
                    topic_clone,
                    message.chars().take(60).collect::<String>()
                );

                // Update dashboard metrics
                if let Ok(mut metrics) = metrics_clone.try_lock() {
                    metrics.total_messages += 1;
                }
            }),
        );

        {
            let mut broker_guard = broker.lock().unwrap();
            broker_guard.subscribe(topic, subscriber)?;
        }
        println!("   ✓ WebSocket subscriber for: {}", topic);
    }

    // Step 5: Simulate web console HTTP endpoints
    println!("\n🌐 Step 5: Simulating Web Console REST API");

    let api_responses = simulate_rest_api_calls(Arc::clone(&broker)).await;

    for response in &api_responses {
        response.display();
    }

    // Step 6: Dashboard data simulation
    println!("\n📊 Step 6: Real-time Dashboard Updates");

    for update_cycle in 1..=5 {
        println!("\n--- Dashboard Update Cycle {} ---", update_cycle);

        // Send dashboard events
        let dashboard_event = json!({
            "event_type": "dashboard_update",
            "cycle": update_cycle,
            "timestamp": Utc::now().to_rfc3339(),
            "metrics": {
                "active_connections": update_cycle * 3,
                "messages_per_second": update_cycle * 15,
                "error_rate": 0.1
            }
        });

        let schema = MessageSchema {
            id: 200 + update_cycle,
            definition: dashboard_event.to_string(),
            version: SchemaVersion::new(1),
            compatibility: pilgrimage::schema::compatibility::Compatibility::Forward,
            metadata: Some({
                let mut meta = HashMap::new();
                meta.insert("event_type".to_string(), "dashboard_update".to_string());
                meta.insert("priority".to_string(), "high".to_string());
                meta
            }),
            topic_id: Some("dashboard-events".to_string()),
            partition_id: Some((update_cycle % 4) as usize),
        };

        // Send via broker
        {
            let mut broker_guard = broker.lock().unwrap();
            broker_guard.send_message(schema)?;
        }

        // Update dashboard metrics
        let topics = {
            let broker_guard = broker.lock().unwrap();
            broker_guard.list_topics().unwrap_or_default()
        };

        let mut metrics = dashboard_metrics.lock().unwrap();
        let current_messages = metrics.total_messages;
        metrics.update_metrics(
            current_messages + (update_cycle as u64),
            topics.len() as u32,
            websocket_topics.len() as u32,
        );
        metrics.display_dashboard();
        drop(metrics);

        time::sleep(Duration::from_secs(2)).await;
    }

    // Step 7: Administrative operations through web interface
    println!("\n⚙️ Step 7: Administrative Operations via Web Interface");

    // Simulate admin operations
    let admin_operations = vec![
        ("GET", "/api/admin/broker/health", "Checking broker health"),
        (
            "POST",
            "/api/admin/topics/bulk-create",
            "Creating multiple topics",
        ),
        ("GET", "/api/admin/metrics/export", "Exporting metrics data"),
        (
            "POST",
            "/api/admin/maintenance/cleanup",
            "Running maintenance cleanup",
        ),
        ("GET", "/api/admin/logs/recent", "Fetching recent logs"),
    ];

    for (method, endpoint, description) in &admin_operations {
        println!("🔧 {} {} - {}", method, endpoint, description);

        // Simulate admin operation
        match *method {
            "GET" => {
                let response = WebAPIResponse::success(
                    "Operation completed successfully",
                    Some(json!({
                        "endpoint": endpoint,
                        "method": method,
                        "timestamp": Utc::now().to_rfc3339()
                    })),
                );
                response.display();
            }
            "POST" => {
                let response = WebAPIResponse::success(
                    "Administrative action executed",
                    Some(json!({
                        "action": description,
                        "result": "success",
                        "affected_resources": 3
                    })),
                );
                response.display();
            }
            _ => {}
        }

        time::sleep(Duration::from_millis(500)).await;
    }

    // Step 8: WebSocket message broadcasting simulation
    println!("\n📡 Step 8: WebSocket Message Broadcasting");

    // Simulate real-time notifications
    let notifications = vec![
        "🔔 System maintenance scheduled for 2:00 AM",
        "⚠️ High memory usage detected on partition 3",
        "✅ Backup completed successfully",
        "📈 Message throughput increased by 25%",
        "🔄 Automatic scaling triggered",
    ];

    for (i, notification) in notifications.iter().enumerate() {
        let notification_data = json!({
            "id": format!("NOTIF-{:03}", i + 1),
            "type": "system_notification",
            "message": notification,
            "severity": match i % 3 {
                0 => "info",
                1 => "warning",
                2 => "success",
                _ => "info"
            },
            "timestamp": Utc::now().to_rfc3339(),
            "requires_action": i % 4 == 1
        });

        let schema = MessageSchema {
            id: 300 + i as u32,
            definition: notification_data.to_string(),
            version: SchemaVersion::new(1),
            compatibility: pilgrimage::schema::compatibility::Compatibility::Forward,
            metadata: Some({
                let mut meta = HashMap::new();
                meta.insert("notification_type".to_string(), "system".to_string());
                meta.insert("broadcast".to_string(), "true".to_string());
                meta
            }),
            topic_id: Some("system-notifications".to_string()),
            partition_id: Some(0),
        };

        {
            let mut broker_guard = broker.lock().unwrap();
            broker_guard.send_message(schema)?;
        }

        println!("📢 Broadcasted: {}", notification);
        time::sleep(Duration::from_secs(1)).await;
    }

    // Step 9: Final dashboard status and summary
    println!("\n📋 Step 9: Final Dashboard Status and Summary");

    // Display final metrics
    let final_metrics = dashboard_metrics.lock().unwrap();
    final_metrics.display_dashboard();
    drop(final_metrics);

    // Display broker summary
    let broker_guard = broker.lock().unwrap();
    let topics = broker_guard.list_topics()?;
    println!("\n🏗️ Broker Summary:");
    println!("   Broker ID: {}", broker_guard.id);
    println!("   Total Topics: {}", topics.len());
    println!(
        "   Health Status: {}",
        if broker_guard.is_healthy() {
            "✅ Healthy"
        } else {
            "❌ Unhealthy"
        }
    );
    drop(broker_guard);

    // Display API endpoints summary
    println!("\n🌐 Available Web Console Endpoints:");
    let endpoints = vec![
        "GET /api/broker/status - Get broker health and status",
        "GET /api/topics - List all topics",
        "POST /api/topics/{name} - Create new topic",
        "DELETE /api/topics/{name} - Delete topic",
        "GET /api/messages/{topic} - Get messages from topic",
        "POST /api/messages - Send new message",
        "GET /api/metrics - Get system metrics",
        "GET /api/admin/health - Administrative health check",
        "WebSocket /ws/dashboard - Real-time dashboard updates",
        "WebSocket /ws/notifications - Real-time notifications",
    ];

    for endpoint in &endpoints {
        println!("   🔗 {}", endpoint);
    }

    println!("\n🎉 Web Console & Management Dashboard Example Completed!");
    println!("Successfully demonstrated:");
    println!("   - ✅ REST API management interface");
    println!("   - ✅ Real-time WebSocket updates");
    println!("   - ✅ Administrative operations");
    println!("   - ✅ Interactive dashboard metrics");
    println!("   - ✅ Authentication and authorization");
    println!("   - ✅ System monitoring and notifications");

    Ok(())
}
