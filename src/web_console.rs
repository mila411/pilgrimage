use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use pilgrimage::broker::{Broker, Message, Node};
use prometheus::{Encoder, TextEncoder};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct BrokerWrapper {
    inner: Arc<Mutex<Broker>>,
}

impl BrokerWrapper {
    fn new(broker: Broker) -> Self {
        BrokerWrapper {
            inner: Arc::new(Mutex::new(broker)),
        }
    }

    async fn send_message_transaction(&self, message: Message) {
        if let Ok(broker) = self.inner.lock() {
            // Start transaction
            let mut transaction = broker.begin_transaction();

            // Send a message (send without waiting for ACK)
            match broker
                .send_message_transaction(&mut transaction, message)
                .await
            {
                Ok(_) => {
                    // Commit the transaction
                    if let Err(e) = transaction.commit() {
                        println!("Transaction commit failed.: {:?}", e);
                    }
                }
                Err(e) => println!("Failed to send message: {:?}", e),
            }
        }
    }

    fn receive_message(&self) -> Option<String> {
        if let Ok(broker) = self.inner.lock() {
            broker.receive_message()
        } else {
            None
        }
    }

    fn is_healthy(&self) -> bool {
        self.inner.lock().is_ok()
    }
}

#[derive(Clone)]
pub struct AppState {
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
    let mut brokers_lock = data.brokers.lock().unwrap();

    if brokers_lock.contains_key(&info.id) {
        return HttpResponse::BadRequest().json("Broker is already running");
    }

    let broker = Broker::new(&info.id, info.partitions, info.replication, &info.storage).await;

    let node = Node {
        id: "node1".to_string(),
        address: "127.0.0.1:8080".to_string(),
        is_active: true,
        data: Arc::new(Mutex::new(Vec::new())),
    };
    broker.add_node("node1".to_string(), node);

    let wrapper = BrokerWrapper::new(broker);
    brokers_lock.insert(info.id.clone(), wrapper);

    HttpResponse::Ok().json("Broker started")
}

async fn stop_broker(info: web::Json<StopRequest>, data: web::Data<AppState>) -> impl Responder {
    let mut brokers_lock = data.brokers.lock().unwrap();

    if brokers_lock.remove(&info.id).is_some() {
        HttpResponse::Ok().json("Broker stopped")
    } else {
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

async fn send_message(info: web::Json<SendRequest>, data: web::Data<AppState>) -> impl Responder {
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        let message = Message::new(info.message.clone());
        broker.send_message_transaction(message).await;
        HttpResponse::Ok().json("Message sent")
    } else {
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

async fn consume_messages(
    info: web::Json<ConsumeRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        if let Some(message) = broker.receive_message() {
            HttpResponse::Ok().json(message)
        } else {
            HttpResponse::NoContent().finish()
        }
    } else {
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

async fn broker_status(
    info: web::Json<StatusRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        HttpResponse::Ok().json(broker.is_healthy())
    } else {
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
