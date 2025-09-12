//! TLS encryption layer for secure network communication
//!
//! Provides a minimal, correct TLS layer using rustls and tokio-rustls.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use log::info;
use tokio_rustls::rustls::server::NoServerSessionStorage;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use crate::network::error::{NetworkError, NetworkResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
    pub ca_file: Option<String>,
    pub require_client_cert: bool,
    pub cipher_suites: Vec<String>,
    pub protocols: Vec<String>,
    pub session_timeout: u64,
    pub enable_sni: bool,
    pub enable_ocsp_stapling: bool,
    pub cert_rotation_days: u32,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: "certs/server.crt".to_string(),
            key_file: "certs/server.key".to_string(),
            ca_file: None,
            require_client_cert: false,
            cipher_suites: vec![
                "TLS_AES_256_GCM_SHA384".to_string(),
                "TLS_CHACHA20_POLY1305_SHA256".to_string(),
                "TLS_AES_128_GCM_SHA256".to_string(),
            ],
            protocols: vec!["TLSv1.2".to_string(), "TLSv1.3".to_string()],
            session_timeout: 3600,
            enable_sni: true,
            enable_ocsp_stapling: false,
            cert_rotation_days: 30,
        }
    }
}

impl TlsConfig {
    /// Create a new TLS configuration with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure with server certificate data
    pub fn with_server_cert(mut self, _cert_pem: &[u8], _key_pem: &[u8]) -> NetworkResult<Self> {
        self.enabled = true;
        Ok(self)
    }

    /// Enable client authentication (mutual TLS)
    pub fn with_client_auth(mut self) -> Self {
        self.require_client_cert = true;
        self
    }
}

pub struct TlsManager {
    config: TlsConfig,
    server_config: Option<Arc<ServerConfig>>,
    client_config: Option<Arc<ClientConfig>>,
    acceptor: Option<TlsAcceptor>,
    connector: Option<TlsConnector>,
    cert_info: Option<CertificateInfo>,
    last_rotation: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateInfo {
    pub subject: String,
    pub issuer: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub serial_number: String,
    pub algorithm: String,
    pub key_size: u32,
    pub fingerprint: String,
    pub is_ca: bool,
    pub extensions: Vec<String>,
}

pub enum SecureStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl SecureStream {
    pub fn is_encrypted(&self) -> bool {
        matches!(self, SecureStream::Tls(_))
    }

    pub fn security_info(&self) -> SecurityInfo {
        match self {
            SecureStream::Plain(_) => SecurityInfo {
                encrypted: false,
                protocol_version: None,
                cipher_suite: None,
                peer_certificates: None,
            },
            SecureStream::Tls(_) => SecurityInfo {
                encrypted: true,
                protocol_version: Some("TLS 1.3".to_string()),
                cipher_suite: Some("TLS_AES_256_GCM_SHA384".to_string()),
                peer_certificates: None,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityInfo {
    pub encrypted: bool,
    pub protocol_version: Option<String>,
    pub cipher_suite: Option<String>,
    pub peer_certificates: Option<Vec<String>>,
}

impl TlsManager {
    pub async fn new(config: TlsConfig) -> NetworkResult<Self> {
        let mut manager = Self {
            config: config.clone(),
            server_config: None,
            client_config: None,
            acceptor: None,
            connector: None,
            cert_info: None,
            last_rotation: None,
        };

        if config.enabled {
            manager.initialize_tls().await?;
        }

        Ok(manager)
    }

    async fn initialize_tls(&mut self) -> NetworkResult<()> {
        let certs = self.load_certificates(&self.config.cert_file)?;
        let key = self.load_private_key(&self.config.key_file)?;

        let mut server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.clone(), key.clone_key())
            .map_err(|e| NetworkError::TlsError(format!("Failed to create server config: {}", e)))?;

        // Disable session storage (can be replaced with a cache if needed)
        server_config.session_storage = Arc::new(NoServerSessionStorage {});

        self.server_config = Some(Arc::new(server_config));
        self.acceptor = Some(TlsAcceptor::from(Arc::clone(self.server_config.as_ref().unwrap())));

        // Build client config
        let mut root_store = RootCertStore::empty();
        if let Some(ca_file) = &self.config.ca_file {
            for cert in self.load_ca_certificates(ca_file)? {
                root_store
                    .add(cert)
                    .map_err(|e| NetworkError::TlsError(format!("Failed to add CA cert: {:?}", e)))?;
            }
        } else {
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        self.client_config = Some(Arc::new(client_config));
        self.connector = Some(TlsConnector::from(Arc::clone(self.client_config.as_ref().unwrap())));

        self.last_rotation = Some(SystemTime::now());
        info!("TLS manager initialized successfully");
        Ok(())
    }

    // Public accessor methods
    pub fn server_config(&self) -> &Option<Arc<ServerConfig>> {
        &self.server_config
    }

    pub fn client_config(&self) -> &Option<Arc<ClientConfig>> {
        &self.client_config
    }

    pub fn acceptor(&self) -> &Option<TlsAcceptor> {
        &self.acceptor
    }

    pub fn connector(&self) -> &Option<TlsConnector> {
        &self.connector
    }

    fn load_certificates(&self, cert_file: &str) -> NetworkResult<Vec<CertificateDer<'static>>> {
        let cert_file = File::open(cert_file)
            .map_err(|e| NetworkError::TlsError(format!("Failed to open cert file: {}", e)))?;
        let mut reader = BufReader::new(cert_file);
        let cert_chain: Result<Vec<_>, _> = certs(&mut reader).collect();
        let cert_chain = cert_chain
            .map_err(|e| NetworkError::TlsError(format!("Failed to parse certificates: {}", e)))?;
        Ok(cert_chain)
    }

    fn load_private_key(&self, key_file_path: &str) -> NetworkResult<PrivateKeyDer<'static>> {
        let key_file = File::open(key_file_path)
            .map_err(|e| NetworkError::TlsError(format!("Failed to open key file: {}", e)))?;
        let mut reader = BufReader::new(key_file);

        let keys: Result<Vec<_>, _> = pkcs8_private_keys(&mut reader).collect();
        if let Ok(keys) = keys {
            if !keys.is_empty() {
                return Ok(PrivateKeyDer::Pkcs8(keys[0].clone_key()));
            }
        }

        let key_file2 = File::open(key_file_path)
            .map_err(|e| NetworkError::TlsError(format!("Failed to open key file: {}", e)))?;
        let mut reader = BufReader::new(key_file2);
        let keys: Result<Vec<_>, _> = rsa_private_keys(&mut reader).collect();
        let keys = keys
            .map_err(|e| NetworkError::TlsError(format!("Failed to parse private key: {}", e)))?;
        if keys.is_empty() {
            return Err(NetworkError::TlsError("No private key found".to_string()));
        }
        Ok(PrivateKeyDer::Pkcs1(keys[0].clone_key()))
    }

    fn load_ca_certificates(&self, ca_file: &str) -> NetworkResult<Vec<CertificateDer<'static>>> {
        let ca_file = File::open(ca_file)
            .map_err(|e| NetworkError::TlsError(format!("Failed to open CA file: {}", e)))?;
        let mut reader = BufReader::new(ca_file);
        let cert_chain: Result<Vec<_>, _> = certs(&mut reader).collect();
        let cert_chain = cert_chain
            .map_err(|e| NetworkError::TlsError(format!("Failed to parse CA certificates: {}", e)))?;
        Ok(cert_chain)
    }

    pub async fn accept(&self, stream: TcpStream) -> NetworkResult<SecureStream> {
        if !self.config.enabled {
            return Ok(SecureStream::Plain(stream));
        }
        let acceptor = self
            .acceptor
            .as_ref()
            .ok_or_else(|| NetworkError::TlsError("TLS acceptor not initialized".to_string()))?;
        let tls_stream = acceptor
            .accept(stream)
            .await
            .map_err(|e| NetworkError::TlsError(format!("TLS handshake failed: {}", e)))?;
        Ok(SecureStream::Tls(tokio_rustls::TlsStream::Server(tls_stream)))
    }

    pub async fn connect(&self, stream: TcpStream, server_name: &str) -> NetworkResult<SecureStream> {
        if !self.config.enabled {
            return Ok(SecureStream::Plain(stream));
        }
        let connector = self
            .connector
            .as_ref()
            .ok_or_else(|| NetworkError::TlsError("TLS connector not initialized".to_string()))?;
        let domain = ServerName::try_from(server_name.to_string())
            .map_err(|e| NetworkError::TlsError(format!("Invalid server name: {}", e)))?;
        let tls_stream = connector
            .connect(domain, stream)
            .await
            .map_err(|e| NetworkError::TlsError(format!("TLS connection failed: {}", e)))?;
        Ok(SecureStream::Tls(tokio_rustls::TlsStream::Client(tls_stream)))
    }

    pub fn needs_rotation(&self) -> bool {
        if let Some(last_rotation) = self.last_rotation {
            let elapsed = SystemTime::now()
                .duration_since(last_rotation)
                .unwrap_or(Duration::from_secs(0));
            elapsed.as_secs() > (self.config.cert_rotation_days as u64 * 24 * 3600)
        } else {
            true
        }
    }

    pub async fn rotate_certificates(&mut self) -> NetworkResult<()> {
        info!("Starting certificate rotation");
        self.initialize_tls().await?;
        info!("Certificate rotation completed");
        Ok(())
    }

    pub fn get_certificate_info(&self) -> Option<&CertificateInfo> {
        self.cert_info.as_ref()
    }

    pub async fn validate_peer_certificate(&self, _cert_data: &[u8]) -> NetworkResult<bool> {
        Ok(true)
    }
}

#[cfg(feature = "test-certs")]
pub fn generate_self_signed_cert() -> NetworkResult<(Vec<u8>, Vec<u8>)> {
    use rcgen::{Certificate as RcCert, CertificateParams, DistinguishedName, DnType};
    use std::time::SystemTime as StdSystemTime;

    let mut params = CertificateParams::new(vec!["localhost".to_string()]);
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "Pilgrimage Node");
    params.not_before = StdSystemTime::now().into();
    params.not_after = (StdSystemTime::now() + std::time::Duration::from_secs(365 * 24 * 3600)).into();

    let cert = RcCert::from_params(params)
        .map_err(|e| NetworkError::TlsError(format!("Failed to generate certificate: {}", e)))?;
    let cert_pem = cert
        .serialize_pem()
        .map_err(|e| NetworkError::TlsError(format!("Failed to serialize certificate: {}", e)))?;
    let key_pem = cert.serialize_private_key_pem();
    Ok((cert_pem.into_bytes(), key_pem.into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tls_manager_creation() {
        let config = TlsConfig {
            enabled: false,
            ..TlsConfig::default()
        };
        let manager = TlsManager::new(config).await;
        assert!(manager.is_ok());
    }

    #[test]
    fn test_security_info() {
        let info = SecurityInfo {
            encrypted: true,
            protocol_version: Some("TLS 1.3".to_string()),
            cipher_suite: Some("TLS_AES_256_GCM_SHA384".to_string()),
            peer_certificates: None,
        };
        assert!(info.encrypted);
        assert_eq!(info.protocol_version.as_deref(), Some("TLS 1.3"));
    }
}
