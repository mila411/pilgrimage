//! Module for secure encryption and decryption of data using
//! [AES-256-GCM algorithm][AES-256-GCM algorithm].
//!
//! This module provides a convenient abstraction for symmetric encryption and decryption
//! using the AES-256-GCM (Galois/Counter Mode) algorithm. It ensures data confidentiality
//! and integrity by encrypting the data with a 32-byte key and a unique nonce (Number used ONCE).
//!
//! # Main components
//! - [`Encryptor`]: A struct that provides encryption and decryption functionality.
//! - [`CryptoError`]: An enum that represents errors that may occur during encryption
//!   and decryption. Each method of the [`Encryptor`] is documented with the possible errors.
//!
//! # Examples
//! Encrypt and decrypt data using the [`Encryptor`]:
//! ```rust
//! use pilgrimage::crypto::Encryptor;
//!
//! // Create a new Encryptor with a 32-byte key
//! let key = [0u8; 32];
//! let encryptor = Encryptor::new(&key);
//! let data = b"Pilgrimage";
//!
//! // Encrypt the data
//! let encrypted = encryptor.encrypt(data).unwrap();
//!
//! // Decrypt the data
//! let decrypted = encryptor.decrypt(&encrypted).unwrap();
//!
//! // Ensure the decrypted data is the same as the original data
//! assert_eq!(data, decrypted.as_slice());
//! ```
//!
//! [AES-256-GCM algorithm]: https://en.wikipedia.org/wiki/Galois/Counter_Mode

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use rand::Rng;
use std::fmt;

/// Errors that may occur during encryption and decryption.
#[derive(Debug)]
pub enum CryptoError {
    /// Error during encryption.
    EncryptionError(String),
    /// Error during decryption.
    DecryptionError(String),
    /// Error due to invalid data.
    InvalidData(String),
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CryptoError::EncryptionError(msg) => write!(f, "Encryption error: {}", msg),
            CryptoError::DecryptionError(msg) => write!(f, "Decryption error: {}", msg),
            CryptoError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
        }
    }
}

impl std::error::Error for CryptoError {}

/// Encryptor/Decryptor using [`Aes256Gcm`].
///
/// # See also
/// - [AES-256-GCM](https://en.wikipedia.org/wiki/Galois/Counter_Mode)
#[derive(Clone)]
pub struct Encryptor {
    /// The cipher used for encryption and decryption.
    cipher: Aes256Gcm,
}

impl Encryptor {
    /// Creates a new `Encryptor` with the specified key.
    ///
    /// # Examples
    /// ```rust
    /// use pilgrimage::crypto::Encryptor;
    ///
    /// // Create a new Encryptor with a 32-byte key.
    /// let key = [0u8; 32];
    /// let encryptor = Encryptor::new(&key);
    /// ```
    pub fn new(key: &[u8; 32]) -> Self {
        Self {
            cipher: Aes256Gcm::new(key.into()),
        }
    }

    /// Encrypts the provided data and returns the result.
    ///
    /// The result includes the nonce (Number used ONCE) and the ciphertext.
    ///
    /// # Errors
    /// Returns [`CryptoError::EncryptionError`] if encryption fails.
    ///
    /// # Examples
    /// ```rust
    /// use pilgrimage::crypto::Encryptor;
    ///
    /// // Create a new Encryptor with a 32-byte key
    /// let key = [0u8; 32];
    /// let encryptor = Encryptor::new(&key);
    /// let data = b"Pilgrimage";
    /// // Encrypt the data
    /// let encrypted = encryptor.encrypt(data).unwrap();
    /// ```
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let mut rng = rand::thread_rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, data)
            .map_err(|e| CryptoError::EncryptionError(e.to_string()))?;

        let mut result = nonce_bytes.to_vec();
        result.extend(ciphertext);
        Ok(result)
    }

    /// Decrypts the provided data and returns the result.
    ///
    /// The input data must include the nonce (Number used ONCE) and the ciphertext.
    ///
    /// # Errors
    /// Returns:
    /// * [`CryptoError::InvalidData`] if the input data is too short.
    /// * [`CryptoError::DecryptionError`] if decryption fails.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::crypto::Encryptor;
    ///
    /// // Create a new Encryptor with a 32-byte key
    /// let key = [0u8; 32];
    /// let encryptor = Encryptor::new(&key);
    /// let data = b"Pilgrimage";
    /// // Encrypt the data
    /// let encrypted = encryptor.encrypt(data).unwrap();
    /// // Decrypt the data
    /// let decrypted = encryptor.decrypt(&encrypted).unwrap();
    /// // Ensure the decrypted data is the same as the original data
    /// assert_eq!(data, decrypted.as_slice());
    /// ```
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if data.len() < 12 {
            return Err(CryptoError::InvalidData("Data too short".to_string()));
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| CryptoError::DecryptionError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the basic encryption and decryption functionality.
    ///
    /// # Purpose
    /// Ensure that the [`Encryptor`] can encrypt and decrypt data correctly.
    ///
    /// # Steps
    /// 1. Create a new [`Encryptor`] with a 32-byte key.
    /// 2. Encrypt the data `Hello, world!`.
    /// 3. Decrypt the encrypted data.
    /// 4. Ensure the decrypted data is the same as the original data.
    #[test]
    fn test_basic_encryption_decryption() {
        let key = [0u8; 32];
        let encryptor = Encryptor::new(&key);
        let data = b"Hello, world!";

        let encrypted = encryptor.encrypt(data).unwrap();
        let decrypted = encryptor.decrypt(&encrypted).unwrap();

        assert_eq!(data, decrypted.as_slice());
    }

    /// Tests the error handling when the input data is too short.
    ///
    /// # Purpose
    /// Ensure that the [`Encryptor`] returns an error when the input data is too short.
    ///
    /// # Steps
    /// 1. Create a new [`Encryptor`] with a 32-byte key.
    /// 2. Attempt to decrypt an empty slice.
    /// 3. Ensure the error is [`CryptoError::InvalidData`].
    #[test]
    fn test_invalid_data_decrypt() {
        let key = [0u8; 32];
        let encryptor = Encryptor::new(&key);
        let invalid_data = vec![0u8; 8];

        match encryptor.decrypt(&invalid_data) {
            Err(CryptoError::InvalidData(_)) => (),
            _ => panic!("Expected InvalidData error"),
        }
    }

    /// Tests the error handling when the input data is invalid.
    ///
    /// # Purpose
    /// Ensure that data encrypted with one key cannot be decrypted with another key.
    ///
    /// # Steps
    /// 1. Create two new [`Encryptor`]s with different 32-byte keys.
    /// 2. Encrypt the data `Secret message` with the first key.
    /// 3. Attempt to decrypt the encrypted data with the second key.
    /// 4. Ensure the error is [`CryptoError::DecryptionError`].
    #[test]
    fn test_different_keys() {
        let key1 = [0u8; 32];
        let key2 = [1u8; 32];
        let encryptor1 = Encryptor::new(&key1);
        let encryptor2 = Encryptor::new(&key2);

        let data = b"Secret message";
        let encrypted = encryptor1.encrypt(data).unwrap();

        match encryptor2.decrypt(&encrypted) {
            Err(CryptoError::DecryptionError(_)) => (),
            _ => panic!("Expected DecryptionError"),
        }
    }

    /// Tests the encryption and decryption of random data.
    ///
    /// # Purpose
    /// Ensure that the [`Encryptor`] can encrypt and decrypt random data correctly.
    ///
    /// # Steps
    /// 1. Create a new [`Encryptor`] with a 32-byte key.
    /// 2. Generate 100 random bytes.
    /// 3. Encrypt the random data.
    /// 4. Decrypt the encrypted data.
    /// 5. Ensure the decrypted data is the same as the original data.
    #[test]
    fn test_random_data() {
        let key = [0u8; 32];
        let encryptor = Encryptor::new(&key);
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; 100];
        rng.fill(&mut data[..]);

        let encrypted = encryptor.encrypt(&data).unwrap();
        let decrypted = encryptor.decrypt(&encrypted).unwrap();

        assert_eq!(data, decrypted);
    }
}
