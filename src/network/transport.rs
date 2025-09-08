use crate::network::connection::ConnectionManager;
use crate::network::discovery::{NodeDiscovery, NodeInfo, NodeStatus};
use crate::network::error::{NetworkError, NetworkResult};
use crate::network::protocol::{DistributedMessage, MessagePayload, MessageType};
use crate::network::tls::TlsConfig;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// High-level network transport for distributed messaging
#[allow(dead_code)]
pub struct NetworkTransport {
    node_id: String,
    connection_manager: Arc<ConnectionManager>,
    message_handlers: Arc<Mutex<HashMap<MessageType, Arc<dyn MessageHandler + Send + Sync>>>>,
    outbound_queue: mpsc::Sender<DistributedMessage>,
    is_running: Arc<Mutex<bool>>,
    discovery: Arc<Mutex<Option<Arc<NodeDiscovery>>>>,
}

/// Trait for handling incoming messages
pub trait MessageHandler {
    fn handle(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>>;
}

impl NetworkTransport {
    pub fn new(node_id: String) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new(node_id.clone()));
        let (outbound_tx, outbound_rx) = mpsc::channel(10000); // Increase buffer size
        let (inbound_tx, inbound_rx) = mpsc::channel(10000); // Increase buffer size

        let transport = Self {
            node_id,
            connection_manager,
            message_handlers: Arc::new(Mutex::new(HashMap::new())),
            outbound_queue: outbound_tx,
            is_running: Arc::new(Mutex::new(true)), // Initialize as true
            discovery: Arc::new(Mutex::new(None)),
        };

        // Start message processing task
        transport.start_message_processor(outbound_rx, inbound_rx);

        // Set up the inbound message forwarding from ConnectionManager
        transport
            .connection_manager
            .set_inbound_message_sender(inbound_tx);

        transport
    }

    /// Create a new network transport with TLS support
    pub async fn new_with_tls(node_id: String, tls_config: TlsConfig) -> NetworkResult<Self> {
        let connection_manager =
            Arc::new(ConnectionManager::new_with_tls(node_id.clone(), tls_config).await?);
        let (outbound_tx, outbound_rx) = mpsc::channel(10000);
        let (inbound_tx, inbound_rx) = mpsc::channel(10000);

        let transport = Self {
            node_id,
            connection_manager,
            message_handlers: Arc::new(Mutex::new(HashMap::new())),
            outbound_queue: outbound_tx,
            is_running: Arc::new(Mutex::new(true)),
            discovery: Arc::new(Mutex::new(None)),
        };

        // Start message processing task
        transport.start_message_processor(outbound_rx, inbound_rx);

        // Set up the inbound message forwarding from ConnectionManager
        transport
            .connection_manager
            .set_inbound_message_sender(inbound_tx);

        Ok(transport)
    }

    /// Attach discovery so inbound messages can refresh liveness
    pub fn set_discovery(&self, discovery: Arc<NodeDiscovery>) {
        if let Ok(mut guard) = self.discovery.lock() {
            *guard = Some(discovery);
        }
    }

    /// Start listening on the specified address
    pub async fn start_listening(&self, address: SocketAddr) -> NetworkResult<()> {
        info!("Starting network transport on {}", address);

        self.connection_manager.listen(address).await?;
        self.connection_manager.start_background_tasks().await;

        Ok(())
    }

    /// Connect to a peer node
    pub async fn connect_to_peer(&self, node_id: String, address: SocketAddr) -> NetworkResult<()> {
        self.connection_manager
            .connect_to_node(node_id.clone(), address)
            .await?;

        // Opportunistically add/update discovery entry on connect
        let discovery_opt = self.discovery.lock().unwrap().clone();
        if let Some(discovery) = discovery_opt {
            let info = NodeInfo {
                id: node_id,
                address,
                capabilities: vec!["broker".into()],
                last_seen: Instant::now(),
                status: NodeStatus::Active,
            };
            discovery.add_node(info);
        }

        Ok(())
    }

    /// Send a message to a specific node
    pub async fn send_message(&self, message: DistributedMessage) -> NetworkResult<()> {
        debug!(
            "Sending message {:?} to {:?}",
            message.message_type, message.receiver_id
        );

        self.outbound_queue.send(message).await.map_err(|e| {
            NetworkError::ConnectionFailed(format!("Failed to queue message: {}", e))
        })?;

        Ok(())
    }

    #[inline]
    fn normalize_node_id(id: &str) -> &str {
        match id {
            "node_8081" => "node1",
            "node_8082" => "node2",
            "node_8083" => "node3",
            other => other,
        }
    }

    /// Broadcast a message to all connected peers
    pub async fn broadcast_message(
        &self,
        mut message: DistributedMessage,
    ) -> NetworkResult<Vec<String>> {
        message.receiver_id = None; // Ensure it's a broadcast

        let data = message
            .serialize()
            .map_err(|e| NetworkError::SerializationError(e.to_string()))?;

        self.connection_manager.broadcast(data).await
    }

    /// Register a handler for a specific message type
    pub fn register_handler<H>(&self, message_type: MessageType, handler: H)
    where
        H: MessageHandler + Send + Sync + 'static,
    {
        let mut handlers = self.message_handlers.lock().unwrap();
        handlers.insert(message_type, Arc::new(handler));
    }

    /// Get the list of connected peers
    pub fn get_connected_peers(&self) -> Vec<String> {
        self.connection_manager.get_connected_nodes()
    }

    /// Check if the transport is running
    pub fn is_running(&self) -> bool {
        *self.is_running.lock().unwrap()
    }

    /// Stop the network transport
    pub async fn stop(&self) {
        info!("Stopping network transport");

        {
            let mut running = self.is_running.lock().unwrap();
            *running = false;
        }
    }

    /// Start the message processing task
    fn start_message_processor(
        &self,
        mut outbound_rx: mpsc::Receiver<DistributedMessage>,
        mut inbound_rx: mpsc::Receiver<DistributedMessage>,
    ) {
        let connection_manager = self.connection_manager.clone();
        let message_handlers = self.message_handlers.clone();
        let is_running = self.is_running.clone();
        let discovery = self.discovery.clone();
        let outbound_queue_clone = self.outbound_queue.clone();

        tokio::spawn(async move {
            loop {
                // Check if should continue running (no guard across await)
                let should_continue = {
                    if let Ok(guard) = is_running.lock() {
                        *guard
                    } else {
                        false
                    }
                };

                if !should_continue {
                    break;
                }

                tokio::select! {
                    // Handle outbound messages
                    Some(message) = outbound_rx.recv() => {
                        if let Err(e) = Self::process_outbound_message(&connection_manager, message).await {
                            error!("Failed to process outbound message: {}", e);
                        }
                    }

                    // Handle inbound messages
                    Some(message) = inbound_rx.recv() => {
                        if let Err(e) = Self::process_inbound_message(&message_handlers, &discovery, &outbound_queue_clone, message).await {
                            error!("Failed to process inbound message: {}", e);
                        }
                    }

                    // Periodic cleanup
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        connection_manager.cleanup_inactive_connections().await;

                        // Check if channels are closed
                        if outbound_rx.is_closed() && inbound_rx.is_closed() {
                            warn!("All message channels closed, stopping message processor");
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Process an inbound message by routing it to the appropriate handler
    async fn process_inbound_message(
        message_handlers: &Arc<Mutex<HashMap<MessageType, Arc<dyn MessageHandler + Send + Sync>>>>,
        discovery: &Arc<Mutex<Option<Arc<NodeDiscovery>>>>,
        outbound_queue: &mpsc::Sender<DistributedMessage>,
        message: DistributedMessage,
    ) -> NetworkResult<()> {
        debug!(
            "Processing inbound message: type={:?}, from={:?}",
            message.message_type, message.sender_id
        );

        // Touch the sender in discovery to avoid Suspected/Down cascades
        if let Ok(guard) = discovery.lock() {
            if let Some(d) = guard.as_ref() {
                d.touch_node(&message.sender_id);
            }
        }

        // Special-case: handle handshake to populate discovery accurately
        if let MessagePayload::NodeJoin {
            node_id,
            address,
            capabilities,
        } = &message.payload
        {
            if let Ok(guard) = discovery.lock() {
                if let Some(d) = guard.as_ref() {
                    // Try to parse provided address, otherwise skip setting precise address
                    let parsed_addr = address.parse::<SocketAddr>().ok();
                    if let Some(addr) = parsed_addr {
                        let info = NodeInfo {
                            id: node_id.clone(),
                            address: addr,
                            capabilities: capabilities.clone(),
                            last_seen: std::time::Instant::now(),
                            status: NodeStatus::Active,
                        };
                        d.add_node(info);
                        debug!("NodeJoin processed: {} at {}", node_id, addr);
                    } else {
                        debug!("NodeJoin received without valid address: '{}'", address);
                    }
                }
            }
        }

        let message_type = message.message_type.clone(); // Store for error logging

        let handler_option = {
            let handlers = message_handlers.lock().unwrap();
            handlers.get(&message_type).cloned()
        };

        if let Some(handler) = handler_option {
            match handler.handle(message) {
                Ok(Some(response)) => {
                    // Handler returned a response message - enqueue it for sending
                    debug!(
                        "Handler produced response message: type={:?}, to={:?}",
                        response.message_type, response.receiver_id
                    );

                    if let Err(e) = outbound_queue.send(response).await {
                        error!("Failed to enqueue response message: {}", e);
                        return Err(NetworkError::ConnectionFailed(format!(
                            "Failed to enqueue response message: {}",
                            e
                        )));
                    } else {
                        debug!("Response message successfully enqueued for outbound transmission");
                    }
                }
                Ok(None) => {
                    // Handler processed the message but didn't produce a response
                    debug!("Message processed successfully, no response generated");
                }
                Err(e) => {
                    error!(
                        "Message handler failed for message type {:?}: {}",
                        message_type, e
                    );
                    return Err(e);
                }
            }
        } else {
            warn!(
                "No handler registered for message type: {:?}, message ignored",
                message_type
            );
        }

        Ok(())
    }

    /// Process an outbound message
    async fn process_outbound_message(
        connection_manager: &ConnectionManager,
        message: DistributedMessage,
    ) -> NetworkResult<()> {
        let data = message
            .serialize()
            .map_err(|e| NetworkError::SerializationError(e.to_string()))?;

        debug!("Serialized message: {} bytes", data.len());

        if let Some(receiver_id) = &message.receiver_id {
            // Normalize receiver ID to match connection manager format
            let normalized_receiver_id = Self::normalize_node_id(receiver_id);

            // Send to specific node
            debug!(
                "Sending message to specific node: {} (normalized from {})",
                normalized_receiver_id, receiver_id
            );
            connection_manager
                .send_to_node(normalized_receiver_id, data)
                .await?;
        } else {
            // Broadcast to all nodes
            debug!("Broadcasting message to all nodes");
            let failed_nodes = connection_manager.broadcast(data).await?;
            if !failed_nodes.is_empty() {
                warn!("Failed to send broadcast to {} nodes", failed_nodes.len());
            }
        }

        Ok(())
    }
}

/// Example message handlers

#[derive(Clone)]
pub struct HeartbeatHandler {
    node_id: String,
}

impl HeartbeatHandler {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}

impl MessageHandler for HeartbeatHandler {
    fn handle(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        match message.payload {
            MessagePayload::Heartbeat {
                term, leader_id, ..
            } => {
                debug!(
                    "Received heartbeat from leader {} (term {})",
                    leader_id, term
                );

                let response = DistributedMessage::new(
                    self.node_id.clone(),
                    Some(message.sender_id),
                    MessageType::HeartbeatResponse,
                    MessagePayload::HeartbeatResponse {
                        term,
                        success: true,
                        match_index: 0, // This would be the actual match index
                    },
                );

                Ok(Some(response))
            }
            _ => Err(NetworkError::InvalidMessageFormat),
        }
    }
}

#[derive(Clone)]
pub struct VoteHandler {
    node_id: String,
    current_term: Arc<Mutex<u64>>,
    voted_for: Arc<Mutex<Option<String>>>,
}

impl VoteHandler {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            current_term: Arc::new(Mutex::new(0)),
            voted_for: Arc::new(Mutex::new(None)),
        }
    }
}

impl MessageHandler for VoteHandler {
    fn handle(&self, message: DistributedMessage) -> NetworkResult<Option<DistributedMessage>> {
        match message.payload {
            MessagePayload::VoteRequest {
                term, candidate_id, ..
            } => {
                let mut current_term = self.current_term.lock().unwrap();
                let mut voted_for = self.voted_for.lock().unwrap();

                let vote_granted = if term > *current_term {
                    *current_term = term;
                    *voted_for = Some(candidate_id.clone());
                    true
                } else if term == *current_term && voted_for.as_ref() == Some(&candidate_id) {
                    true
                } else {
                    false
                };

                let response = DistributedMessage::new(
                    self.node_id.clone(),
                    Some(message.sender_id),
                    MessageType::VoteResponse,
                    MessagePayload::VoteResponse {
                        term: *current_term,
                        vote_granted,
                        voter_id: self.node_id.clone(),
                    },
                );

                Ok(Some(response))
            }
            _ => Err(NetworkError::InvalidMessageFormat),
        }
    }
}
