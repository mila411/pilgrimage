use crate::network::error::{NetworkError, NetworkResult};
use crate::network::protocol::{DistributedMessage, MessageType, MessagePayload};
use crate::network::tls::{TlsConfig, TlsManager};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio_rustls::{TlsAcceptor, TlsConnector, server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use log::{debug, error, info, warn};

/// Stream abstraction to handle both TLS and plain TCP streams
pub enum StreamType {
    Plain(TcpStream),
    TlsServer(ServerTlsStream<TcpStream>),
    TlsClient(ClientTlsStream<TcpStream>),
}

impl std::fmt::Debug for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::Plain(_) => write!(f, "StreamType::Plain"),
            StreamType::TlsServer(_) => write!(f, "StreamType::TlsServer"),
            StreamType::TlsClient(_) => write!(f, "StreamType::TlsClient"),
        }
    }
}

impl StreamType {
    /// Read exact amount of data from stream
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        use tokio::io::AsyncReadExt;

        let mut pos = 0;
        while pos < buf.len() {
            let bytes_read = match self {
                StreamType::Plain(stream) => stream.read(&mut buf[pos..]).await?,
                StreamType::TlsServer(stream) => stream.read(&mut buf[pos..]).await?,
                StreamType::TlsClient(stream) => stream.read(&mut buf[pos..]).await?,
            };

            if bytes_read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "stream closed before reading complete buffer",
                ));
            }

            pos += bytes_read;
        }

        Ok(())
    }

    /// Write all data to stream
    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self {
            StreamType::Plain(stream) => AsyncWriteExt::write_all(stream, buf).await,
            StreamType::TlsServer(stream) => AsyncWriteExt::write_all(stream, buf).await,
            StreamType::TlsClient(stream) => AsyncWriteExt::write_all(stream, buf).await,
        }
    }
}

/// Manages TCP connections to other nodes in the cluster
pub struct ConnectionManager {
    node_id: String,
    connections: Arc<Mutex<HashMap<String, Connection>>>,
    listeners: Arc<Mutex<HashMap<SocketAddr, Arc<TcpListener>>>>,
    message_sender: mpsc::Sender<(String, Vec<u8>)>,
    message_receiver: Arc<Mutex<mpsc::Receiver<(String, Vec<u8>)>>>,
    connection_timeout: Duration,

    // TLS configuration
    tls_manager: Option<Arc<TlsManager>>,

    // Channel for forwarding received messages to NetworkTransport
    inbound_message_sender: Arc<Mutex<Option<mpsc::Sender<DistributedMessage>>>>,

    // Known peer addresses for reconnection attempts
    known_addresses: Arc<Mutex<HashMap<String, SocketAddr>>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Connection {
    node_id: String,
    address: SocketAddr,
    stream: Option<Arc<AsyncMutex<StreamType>>>,
    last_activity: Instant,
    is_active: bool,
}

impl ConnectionManager {
    pub fn new(node_id: String) -> Self {
        let (tx, rx) = mpsc::channel(1000);

        Self {
            node_id,
            connections: Arc::new(Mutex::new(HashMap::new())),
            listeners: Arc::new(Mutex::new(HashMap::new())),
            message_sender: tx,
            message_receiver: Arc::new(Mutex::new(rx)),
            connection_timeout: Duration::from_secs(30),
            tls_manager: None,
            inbound_message_sender: Arc::new(Mutex::new(None)),
            known_addresses: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new connection manager with TLS support
    pub async fn new_with_tls(node_id: String, tls_config: TlsConfig) -> NetworkResult<Self> {
        let (tx, rx) = mpsc::channel(1000);
        let tls_manager = Arc::new(TlsManager::new(tls_config).await?);

        Ok(Self {
            node_id,
            connections: Arc::new(Mutex::new(HashMap::new())),
            listeners: Arc::new(Mutex::new(HashMap::new())),
            message_sender: tx,
            message_receiver: Arc::new(Mutex::new(rx)),
            connection_timeout: Duration::from_secs(30),
            tls_manager: Some(tls_manager),
            inbound_message_sender: Arc::new(Mutex::new(None)),
            known_addresses: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Set the inbound message sender for forwarding received messages
    pub fn set_inbound_message_sender(&self, sender: mpsc::Sender<DistributedMessage>) {
        // Store in a cell or use interior mutability
        // For now, we'll modify the field to use Arc<Mutex<Option<...>>>
        if let Ok(mut current_sender) = self.inbound_message_sender.lock() {
            *current_sender = Some(sender);
        }
    }

    /// Start listening on the specified address
    pub async fn listen(&self, address: SocketAddr) -> NetworkResult<()> {
        let listener = TcpListener::bind(address).await
            .map_err(|e| NetworkError::ConnectionFailed(format!("Failed to bind to {}: {}", address, e)))?;

        info!("Listening on {} with TLS: {}", address, self.tls_manager.is_some());

        {
            let mut listeners = self.listeners.lock().unwrap();
            listeners.insert(address, Arc::new(listener));
        }

        // Start accepting connections
        self.accept_connections(address).await;

        Ok(())
    }

    /// Accept incoming connections
    async fn accept_connections(&self, address: SocketAddr) {
        let connections = self.connections.clone();
        let node_id = self.node_id.clone();
        let listeners = self.listeners.clone();
        let inbound_message_sender = self.inbound_message_sender.clone();
        let known_addresses = self.known_addresses.clone();
        let tls_manager = self.tls_manager.clone();

        tokio::spawn(async move {
            // Get the listener from the stored listeners
            let listener = {
                let listeners_guard = listeners.lock().unwrap();
                listeners_guard.get(&address).cloned()
            };

            if let Some(listener) = listener {
                loop {
                    match listener.accept().await {
                        Ok((stream, peer_addr)) => {
                            info!("Accepted connection from {}", peer_addr);

                            // Convert to appropriate stream type
                            let stream_type = if let Some(ref tls_manager) = tls_manager {
                                // For TLS, we need to perform handshake on the accepted stream
                                if let Some(server_config) = tls_manager.server_config() {
                                    let acceptor = TlsAcceptor::from(server_config.clone());
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            info!("TLS handshake completed with {}", peer_addr);
                                            StreamType::TlsServer(tls_stream)
                                        }
                                        Err(e) => {
                                            warn!("TLS handshake failed with {}: {}", peer_addr, e);
                                            continue; // Skip this connection
                                        }
                                    }
                                } else {
                                    warn!("TLS transport configured but no server config available");
                                    StreamType::Plain(stream)
                                }
                            } else {
                                StreamType::Plain(stream)
                            };

                            // Handle the connection
                            let connections_clone = connections.clone();
                            let node_id_clone = node_id.clone();
                            let inbound_sender_clone = inbound_message_sender.clone();
                            let known_addresses_for_conn = known_addresses.clone();

                            tokio::spawn(async move {
                                Self::handle_incoming_connection(
                                    stream_type,
                                    peer_addr,
                                    connections_clone,
                                    node_id_clone,
                                    inbound_sender_clone,
                                    known_addresses_for_conn,
                                ).await;
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            } else {
                error!("Failed to recreate listener for {}", address);
            }
        });
    }

    /// Handle an incoming connection
    async fn handle_incoming_connection(
        stream: StreamType,
        peer_addr: SocketAddr,
        connections: Arc<Mutex<HashMap<String, Connection>>>,
        _node_id: String,
        inbound_message_sender: Arc<Mutex<Option<mpsc::Sender<DistributedMessage>>>>,
        known_addresses: Arc<Mutex<HashMap<String, SocketAddr>>>,
    ) {
        // Wrap the inbound stream so we can both read and later write via send_to_node
        let stream_arc = Arc::new(AsyncMutex::new(stream));

        // Heuristic: register this inbound connection using the peer port to a normalized node id
        let provisional_id: String = match peer_addr.port() {
            8081 => "node1".to_string(),
            8082 => "node2".to_string(),
            8083 => "node3".to_string(),
            port => format!("node_{}", port),
        };

        {
            let mut conns = connections.lock().unwrap();
            // Only insert if not present to avoid overwriting an existing active connection
            conns.entry(provisional_id.clone()).or_insert(Connection {
                node_id: provisional_id.clone(),
                address: peer_addr,
                stream: Some(stream_arc.clone()),
                last_activity: Instant::now(),
                is_active: true,
            });
            debug!("Registered inbound connection from {} as {}", peer_addr, provisional_id);
        }

        loop {
            // Read length-prefixed frame: 4-byte BE length + payload
            let len_result = {
                let mut locked = stream_arc.lock().await;
                let mut len_buf = [0u8; 4];
                match locked.read_exact(&mut len_buf).await {
                    Ok(_n) => Ok(u32::from_be_bytes(len_buf) as usize),
                    Err(e) => Err(e),
                }
            };

            let frame_len = match len_result {
                Ok(len) => len,
                Err(e) => {
                    debug!("Connection from {} closed or errored reading length: {}", peer_addr, e);
                    break;
                }
            };

            let payload = {
                let mut locked = stream_arc.lock().await;
                let mut buf = vec![0u8; frame_len];
                match locked.read_exact(&mut buf).await {
                    Ok(_n) => Ok(buf),
                    Err(e) => Err(e),
                }
            };

            let data = match payload {
                Ok(d) => d,
                Err(e) => {
                    error!("Error reading payload from {}: {}", peer_addr, e);
                    break;
                }
            };

            // Deserialize message
            match DistributedMessage::deserialize(&data) {
                Ok(message) => {
                    debug!("Received message: {:?} from {}", message.message_type, message.sender_id);

                    // On first message, create/update a connection entry keyed by normalized sender_id
                    let normalized_sender_id = match message.sender_id.as_str() {
                        "node_8081" => "node1".to_string(),
                        "node_8082" => "node2".to_string(),
                        "node_8083" => "node3".to_string(),
                        other => other.to_string(),
                    };
                    {
                        let mut conns = connections.lock().unwrap();
                        if !conns.contains_key(&normalized_sender_id) {
                            conns.insert(
                                normalized_sender_id.clone(),
                                Connection {
                                    node_id: normalized_sender_id.clone(),
                                    address: peer_addr,
                                    stream: Some(stream_arc.clone()),
                                    last_activity: Instant::now(),
                                    is_active: true,
                                },
                            );
                            debug!("Associated inbound stream from {} with sender id {}", peer_addr, normalized_sender_id);
                        }
                    }

                    // Record known address for reconnection purposes
                    {
                        let mut known = known_addresses.lock().unwrap();
                        known.insert(normalized_sender_id.clone(), peer_addr);
                    }

                    // Forward message to NetworkTransport
                    let sender_option = {
                        if let Ok(sender_guard) = inbound_message_sender.lock() {
                            sender_guard.clone()
                        } else {
                            None
                        }
                    };

                    if let Some(sender) = sender_option {
                        if let Err(e) = sender.send(message).await {
                            error!("Failed to forward inbound message: {}", e);
                        }
                    } else {
                        debug!("No inbound message sender configured");
                    }
                }
                Err(e) => {
                    warn!("Failed to deserialize message from {}: {} bytes, error: {}", peer_addr, data.len(), e);
                }
            }
        }

        // Clean up connection (best-effort): mark as inactive for all ids matching this address
        {
            let mut conns = connections.lock().unwrap();
            for (_id, conn) in conns.iter_mut() {
                if conn.address == peer_addr {
                    conn.is_active = false;
                }
            }
        }
    }

    /// Connect to a remote node
    pub async fn connect_to_node(&self, node_id: String, address: SocketAddr) -> NetworkResult<()> {
        info!("Connecting to node {} at {} with TLS: {}", node_id, address, self.tls_manager.is_some());

        let stream_type = if let Some(ref tls_manager) = self.tls_manager {
            // Use TLS connection
            if let Some(client_config) = tls_manager.client_config() {
                let connector = TlsConnector::from(client_config.clone());

                let tcp_stream = tokio::time::timeout(
                    self.connection_timeout,
                    TcpStream::connect(address)
                ).await
                .map_err(|_| NetworkError::Timeout)?
                .map_err(|e| NetworkError::ConnectionFailed(format!("Failed to connect to {}: {}", address, e)))?;

                // Extract hostname for SNI
                let hostname = address.ip().to_string();
                let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from(hostname.clone())
                    .map_err(|e| NetworkError::TlsError(format!("Invalid hostname {}: {}", hostname, e)))?;

                match connector.connect(server_name, tcp_stream).await {
                    Ok(tls_stream) => {
                        info!("TLS connection established to {} at {}", node_id, address);
                        StreamType::TlsClient(tls_stream)
                    }
                    Err(e) => {
                        warn!("TLS connection failed to {}: {}, falling back to plain TCP", address, e);
                        let tcp_stream = TcpStream::connect(address).await
                            .map_err(|e| NetworkError::ConnectionFailed(format!("Failed to connect to {}: {}", address, e)))?;
                        StreamType::Plain(tcp_stream)
                    }
                }
            } else {
                // No client config, use plain TCP
                let tcp_stream = tokio::time::timeout(
                    self.connection_timeout,
                    TcpStream::connect(address)
                ).await
                .map_err(|_| NetworkError::Timeout)?
                .map_err(|e| NetworkError::ConnectionFailed(format!("Failed to connect to {}: {}", address, e)))?;
                StreamType::Plain(tcp_stream)
            }
        } else {
            // No TLS transport, use plain TCP
            let tcp_stream = tokio::time::timeout(
                self.connection_timeout,
                TcpStream::connect(address)
            ).await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(|e| NetworkError::ConnectionFailed(format!("Failed to connect to {}: {}", address, e)))?;
            StreamType::Plain(tcp_stream)
        };

        let connection = Connection {
            node_id: node_id.clone(),
            address,
            stream: Some(Arc::new(AsyncMutex::new(stream_type))),
            last_activity: Instant::now(),
            is_active: true,
        };

        {
            let mut connections = self.connections.lock().unwrap();
            connections.insert(node_id.clone(), connection);
        }

    // Track known address for reconnection
    self.known_addresses.lock().unwrap().insert(node_id.clone(), address);

        info!("Successfully connected to node {} at {}", node_id, address);

        // Send a simple handshake (NodeJoin) immediately after connecting
        if let Some(stream_arc) = {
            let conns = self.connections.lock().unwrap();
            conns.get(&node_id).and_then(|c| c.stream.as_ref().cloned())
        } {
            let join_msg = DistributedMessage::new(
                self.node_id.clone(),
                Some(node_id.clone()),
                MessageType::NodeJoin,
                MessagePayload::NodeJoin {
                    node_id: self.node_id.clone(),
                    address: {
                        // Try to find our listen address from listeners map
                        let listeners = self.listeners.lock().unwrap();
                        if let Some(addr) = listeners.keys().next() {
                            addr.to_string()
                        } else {
                            String::new()
                        }
                    },
                    capabilities: vec!["broker".to_string()],
                },
            );
            if let Ok(bytes) = join_msg.serialize() {
                let mut s = stream_arc.lock().await;
                let len = (bytes.len() as u32).to_be_bytes();
                if let Err(e) = s.write_all(&len).await {
                    debug!("Failed to send handshake length to {}: {}", node_id, e);
                } else if let Err(e) = s.write_all(&bytes).await {
                    debug!("Failed to send handshake payload to {}: {}", node_id, e);
                } else {
                    debug!("Sent handshake (NodeJoin) to {}", node_id);
                }
            }
        }
        Ok(())
    }

    /// Send a message to a specific node
    pub async fn send_to_node(&self, node_id: &str, data: Vec<u8>) -> NetworkResult<()> {
        debug!("Attempting to send {} bytes to node: {}", data.len(), node_id);

        // Debug: List all available connections
        {
            let connections = self.connections.lock().unwrap();
            debug!("Available connections: {:?}", connections.keys().collect::<Vec<_>>());
        }

        // Extract a clone of the stream handle without holding the std::Mutex across await
        let stream_opt = {
            let mut connections = self.connections.lock().unwrap();
            if let Some(connection) = connections.get_mut(node_id) {
                connection.last_activity = Instant::now();
                connection.stream.as_ref().cloned()
            } else {
                None
            }
        };

        if let Some(stream_arc) = stream_opt {
            let mut stream = stream_arc.lock().await;

            // Length-prefixed framing: 4-byte BE length + payload
            let len = (data.len() as u32).to_be_bytes();
            if let Err(e) = stream.write_all(&len).await {
                warn!("Write failed (len) to {}: {}", node_id, e);
                return Err(NetworkError::ConnectionFailed(format!("Failed to send to {}: {}", node_id, e)));
            }
            if let Err(e) = stream.write_all(&data).await {
                warn!("Write failed (payload) to {}: {}", node_id, e);
                return Err(NetworkError::ConnectionFailed(format!("Failed to send to {}: {}", node_id, e)));
            }
            debug!("Successfully sent {} bytes to {}", data.len(), node_id);
            return Ok(());
        }

        // Attempt opportunistic reconnection if we know the address
        let known_addr = { self.known_addresses.lock().unwrap().get(node_id).cloned() };
        if let Some(addr) = known_addr {
            warn!("Node {} not connected, attempting reconnection to {}", node_id, addr);
            // Schedule reconnection without immediate retry to avoid recursion
            if let Err(e) = self.connect_to_node(node_id.to_string(), addr).await {
                debug!("Immediate reconnection to {} failed: {}", node_id, e);
            } else {
                // Try sending again after reconnection (inline to avoid recursion)
                let stream_opt = {
                    let mut connections = self.connections.lock().unwrap();
                    if let Some(connection) = connections.get_mut(node_id) {
                        connection.last_activity = Instant::now();
                        connection.stream.as_ref().cloned()
                    } else {
                        None
                    }
                };

                if let Some(stream_arc) = stream_opt {
                    let mut stream = stream_arc.lock().await;
                    let len = (data.len() as u32).to_be_bytes();
                    if let Err(e) = stream.write_all(&len).await {
                        warn!("Write failed (len) after reconnect to {}: {}", node_id, e);
                        return Err(NetworkError::ConnectionFailed(format!("Failed to send to {}: {}", node_id, e)));
                    }
                    if let Err(e) = stream.write_all(&data).await {
                        warn!("Write failed (payload) after reconnect to {}: {}", node_id, e);
                        return Err(NetworkError::ConnectionFailed(format!("Failed to send to {}: {}", node_id, e)));
                    }
                    debug!("Successfully sent {} bytes to {} after reconnect", data.len(), node_id);
                    return Ok(());
                }
            }
            return Err(NetworkError::NodeUnreachable(node_id.to_string()));
        }

        warn!("Node {} not found in connections", node_id);
        Err(NetworkError::NodeUnreachable(node_id.to_string()))
    }

    /// Broadcast a message to all connected nodes
    pub async fn broadcast(&self, data: Vec<u8>) -> NetworkResult<Vec<String>> {
        let mut failed_nodes = Vec::new();
        // Take a snapshot of active node ids to avoid holding the lock across await
        let node_ids: Vec<String> = {
            let connections = self.connections.lock().unwrap();
            connections
                .iter()
                .filter(|(_, conn)| conn.is_active)
                .map(|(id, _)| id.clone())
                .collect()
        };

        for node_id in node_ids {
            if let Err(e) = self.send_to_node(&node_id, data.clone()).await {
                warn!("Failed to send broadcast to {}: {}", node_id, e);
                failed_nodes.push(node_id);
            }
        }

        Ok(failed_nodes)
    }

    /// Get the list of connected nodes
    pub fn get_connected_nodes(&self) -> Vec<String> {
        let connections = self.connections.lock().unwrap();
        connections.iter()
            .filter(|(_, conn)| conn.is_active)
            .map(|(node_id, _)| node_id.clone())
            .collect()
    }

    /// Check and cleanup inactive connections
    pub async fn cleanup_inactive_connections(&self) {
        let mut connections = self.connections.lock().unwrap();
        let now = Instant::now();

        let inactive_nodes: Vec<String> = connections.iter()
            .filter(|(_, conn)| now.duration_since(conn.last_activity) > self.connection_timeout)
            .map(|(node_id, _)| node_id.clone())
            .collect();

        for node_id in inactive_nodes {
            warn!("Removing inactive connection to {}", node_id);
            connections.remove(&node_id);
        }
    }

    /// Start the connection management background task
    pub async fn start_background_tasks(&self) {
    let connections = self.connections.clone();
        let timeout = self.connection_timeout;
    let _known_addresses = self.known_addresses.clone();
    let this_for_reconnect = self.clone_for_tasks();
    let connections_for_reconnect = self.connections.clone();
    let known_addresses_for_reconnect = self.known_addresses.clone();

        // Cleanup task
    tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                interval.tick().await;

                let mut conns = connections.lock().unwrap();
                let now = Instant::now();

                let inactive: Vec<String> = conns.iter()
                    .filter(|(_, conn)| now.duration_since(conn.last_activity) > timeout)
                    .map(|(id, _)| id.clone())
                    .collect();

                for node_id in inactive {
                    warn!("Removing inactive connection to {}", node_id);
                    conns.remove(&node_id);
                }
            }
        });

        // Reconnection task (with light jitter)
    tokio::spawn(async move {
            let base = Duration::from_millis(2500);
            let mut interval = tokio::time::interval(base);
            loop {
                interval.tick().await;
                // Small random jitter to avoid thundering herd
                let jitter_ms = (rand::random::<u16>() % 500) as u64; // 0-499ms
                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;

                // Snapshot of targets
                let targets: Vec<(String, SocketAddr)> = {
            let known = known_addresses_for_reconnect.lock().unwrap();
            let conns = connections_for_reconnect.lock().unwrap();
                    known
                        .iter()
                        .filter(|(id, _)| !conns.contains_key(*id) || !conns.get(*id).map(|c| c.is_active).unwrap_or(false))
                        .map(|(id, addr)| (id.clone(), *addr))
                        .collect()
                };

                for (id, addr) in targets {
            if let Err(e) = this_for_reconnect.connect_to_node(id.clone(), addr).await {
                        debug!("Reconnection attempt to {} at {} failed: {}", id, addr, e);
                    } else {
                        info!("Reconnected to {} at {}", id, addr);
                    }
                }
            }
        });
    }
}

impl ConnectionManager {
    fn clone_for_tasks(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            connections: self.connections.clone(),
            listeners: self.listeners.clone(),
            message_sender: self.message_sender.clone(),
            message_receiver: self.message_receiver.clone(),
            connection_timeout: self.connection_timeout,
            tls_manager: self.tls_manager.clone(),
            inbound_message_sender: self.inbound_message_sender.clone(),
            known_addresses: self.known_addresses.clone(),
        }
    }
}
