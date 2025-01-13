/// The `web_console` module provides functionality for managing brokers
/// via HTTP endpoints.
///
/// The module exposes a method to start a new HTTP server that listens on
/// `localhost:8080` and provides the REST API available on the section
/// [Available Endpoints](#available-endpoints).
///
/// ## Available Endpoints
///
/// ### Start Broker
///
/// Starts a new broker instance.
///
/// **Endpoint:** `POST /start`
/// **Request:**
///
/// ```json
/// {
///     "id": "broker1",
///     "partitions": 3,
///     "replication": 2,
///     "storage": "/tmp/broker1"
/// }
/// ```
///
/// **Example:**
///
/// ```sh
/// curl -X POST http://localhost:8080/start \
///   -H "Content-Type: application/json" \
///   -d '{
///     "id": "broker1",
///     "partitions": 3,
///     "replication": 2,
///     "storage": "/tmp/broker1"
///   }'
/// ```
///
/// ### Stop Broker
///
/// Stops a running broker instance.
///
/// **Endpoint**: `POST /stop`
/// **Request:**
///
/// ```json
/// {
///     "id": "broker1"
/// }
/// ```
///
/// **Example:**
///
/// ```sh
/// curl -X POST http://localhost:8080/stop \
///   -H "Content-Type: application/json" \
///   -d '{
///     "id": "broker1"
///   }'
/// ```
///
/// ### Send Message
///
/// Sends a message to the broker.
///
/// **Endpoint**: `POST /send`
/// **Request:**
///
/// ```json
/// {
///     "id": "broker1",
///     "message": "Hello, World!"
/// }
/// ```
///
/// **Example:**
///
/// ```sh
/// curl -X POST http://localhost:8080/send \
///   -H "Content-Type: application/json" \
///   -d '{
///     "id": "broker1",
///     "message": "Hello, World!"
///   }'
/// ```
///
/// ### Consume Messages
///
/// Consumes messages from the broker.
///
/// **Endpoint**: `POST /consume`
///
/// **Request:**
///
/// ```sh
/// {
///     "id": "broker1"
/// }
/// ```
///
/// **Example:**
///
/// ```sh
/// curl -X POST http://localhost:8080/consume \
///   -H "Content-Type: application/json" \
///   -d '{
///     "id": "broker1"
///   }'
/// ```
///
/// ### Check Status
///
/// Checks the status of the broker.
///
/// **Endpoint**: `POST /status`
///
/// `Request:`
///
/// ```sh
/// {
///     "id": "broker1"
/// }
/// ```
///
/// **Example:**
///
/// ```sh
/// curl -X POST http://localhost:8080/status \
///   -H "Content-Type: application/json" \
///   -d '{
///     "id": "broker1"
///   }'
/// ```
///
/// ## Running the Web Server
///
/// To start the web server:
///
/// ```sh
/// cargo run --bin web
/// ```
///
/// The server will be available at [http://localhost:8080](http://localhost:8080).
///
/// ## Internal Implementation
///
/// Internally, the module uses the [`actix_web`](https://actix.rs/) crate to create an HTTP server
/// that listens on the `localhost:8080` address and provides REST API endpoints to manage brokers.
///
/// The module also uses the [`prometheus`](https://crates.io/crates/prometheus) crate to expose
/// metrics for the application.
///
/// Finally, the module uses the [`tokio`](https://tokio.rs/) crate to provide asynchronous support
/// for the application.
mod web_console;

/// The main entry point for the Pilgrimage application.
///
/// This function creates and initializes a new HTTP server to provide the user
/// with various commands to manage brokers via REST API (`/start`, `/stop`, `/status`, etc.).
/// For more details about the HTTP server, see [`web_console::run_server`].
///
/// The application also uses the [Tokio runtime](https://tokio.rs/),
/// which provides asynchronous support for Rust,
/// enabling the development of high performance network applications.
///
/// # Returns
///
/// Returns a `std::io::Result<()>` indicating the success or failure of the
/// web server execution.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    web_console::run_server().await
}
