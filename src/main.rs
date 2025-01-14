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
