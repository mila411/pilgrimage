use aes_gcm::{aead::{Aead, KeyInit}, Aes256Gcm, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::{fs, path::{Path, PathBuf}};

/// On-disk persisted structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedCreds {
    pub users: std::collections::HashMap<String, String>,       // username -> password
    pub permissions: std::collections::HashMap<String, Vec<String>>, // username -> roles/permissions
    pub version: u32,
}

impl Default for PersistedCreds {
    fn default() -> Self {
        Self {
            users: std::collections::HashMap::new(),
            permissions: std::collections::HashMap::new(),
            version: 1,
        }
    }
}

/// Small helper for where to store security data
pub fn default_security_dir() -> PathBuf {
    PathBuf::from("./storage/security")
}

fn creds_file_at(dir: &Path) -> PathBuf {
    dir.join("credentials.json.enc")
}

/// Load persisted credentials if file exists; returns None if not found
pub fn load(dir: &Path, key: &[u8; 32]) -> Result<Option<PersistedCreds>, String> {
    let path = creds_file_at(dir);
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read(&path).map_err(|e| format!("read error: {}", e))?;
    if data.len() < 12 { return Err("ciphertext too short".to_string()); }

    // first 12 bytes = nonce
    let (nonce_bytes, ct) = data.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    let plaintext = cipher.decrypt(nonce, ct).map_err(|_| "decrypt failed".to_string())?;

    let obj: PersistedCreds = serde_json::from_slice(&plaintext).map_err(|e| e.to_string())?;
    Ok(Some(obj))
}

/// Save credentials atomically (write temp then rename)
pub fn save(dir: &Path, key: &[u8; 32], data: &PersistedCreds) -> Result<(), String> {
    if !dir.exists() {
        fs::create_dir_all(dir).map_err(|e| format!("mkdir error: {}", e))?;
    }
    let json = serde_json::to_vec(data).map_err(|e| e.to_string())?;

    // random 12-byte nonce
    let mut nonce_buf = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_buf);
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    let ct = cipher.encrypt(Nonce::from_slice(&nonce_buf), json.as_ref()).map_err(|_| "encrypt failed".to_string())?;

    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce_buf);
    out.extend_from_slice(&ct);

    let path = creds_file_at(dir);
    let tmp = path.with_extension("enc.tmp");
    fs::write(&tmp, &out).map_err(|e| format!("write error: {}", e))?;
    fs::rename(&tmp, &path).map_err(|e| format!("rename error: {}", e))?;
    Ok(())
}

/// Derive/store the encryption key for credential store
/// Priority:
/// 1) ENV PILGRIMAGE_CRED_KEY (hex 64 chars)
/// 2) file ./.pilgrimage/cred_store.key (32 bytes)
/// 3) generate and save to (2)
pub fn get_or_create_key() -> Result<[u8; 32], String> {
    if let Ok(hex_key) = std::env::var("PILGRIMAGE_CRED_KEY") {
        let bytes = hex::decode(hex_key).map_err(|e| e.to_string())?;
        if bytes.len() != 32 { return Err("PILGRIMAGE_CRED_KEY must be 32 bytes (hex-64)".into()); }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        return Ok(arr);
    }

    let dir = if let Some(home) = dirs::home_dir() { home.join(".pilgrimage") } else { std::path::PathBuf::from(".pilgrimage") };
    if !dir.exists() { fs::create_dir_all(&dir).map_err(|e| e.to_string())?; }
    let key_file = dir.join("cred_store.key");
    if key_file.exists() {
        let bytes = fs::read(&key_file).map_err(|e| e.to_string())?;
        if bytes.len() != 32 { return Err("cred_store.key invalid length".into()); }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        return Ok(arr);
    }

    let mut arr = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut arr);
    fs::write(&key_file, &arr).map_err(|e| e.to_string())?;
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn roundtrip_save_load() {
        let dir = tempdir();
        let dir_path = dir.path();
        let key: [u8; 32] = [42u8; 32];
        let mut data = PersistedCreds::default();
        data.users.insert("alice".into(), "secret".into());
        data.permissions.insert("alice".into(), vec!["read".into()]);

        save(dir_path, &key, &data).expect("save ok");
        let loaded = load(dir_path, &key).expect("load ok").expect("some");
        assert_eq!(loaded.users.get("alice").unwrap(), "secret");
    let expected: Vec<String> = vec!["read".to_string()];
    assert_eq!(loaded.permissions.get("alice").unwrap(), &expected);
    }

    #[test]
    fn wrong_key_fails() {
        let dir = tempdir();
        let dir_path = dir.path();
        let key1: [u8; 32] = [1u8; 32];
        let key2: [u8; 32] = [2u8; 32];
        let mut data = PersistedCreds::default();
        data.users.insert("bob".into(), "pw".into());
        save(dir_path, &key1, &data).expect("save ok");
        let err = load(dir_path, &key2).unwrap_err();
        assert!(err.contains("decrypt failed") || err.contains("too short"));
    }

    #[test]
    fn short_ciphertext_error() {
        let dir = tempdir();
        let dir_path = dir.path();
        let path = creds_file_at(dir_path);
        fs::create_dir_all(dir_path).unwrap();
        fs::write(&path, b"short").unwrap();
        let key: [u8; 32] = [0u8; 32];
        let err = load(dir_path, &key).unwrap_err();
        assert!(err.contains("too short"));
    }

    #[test]
    fn key_file_generate_and_reuse() {
        // Use a temp HOME so we don't touch the real one
        let dir = tempdir();
        let home = dir.path().join(".pilgrimage");
        // First call creates file in provided home
        let k1 = get_or_create_key_in_home(dir.path()).expect("k1");
        let file = home.join("cred_store.key");
        assert!(file.exists());
        // Second call returns the same key
        let k2 = get_or_create_key_in_home(dir.path()).expect("k2");
        assert_eq!(k1, k2);
    }

    // Test-only helper: behave like get_or_create_key() but use provided HOME path
    fn get_or_create_key_in_home(home_dir: &Path) -> Result<[u8; 32], String> {
        let dir = home_dir.join(".pilgrimage");
        if !dir.exists() { fs::create_dir_all(&dir).map_err(|e| e.to_string())?; }
        let key_file = dir.join("cred_store.key");
        if key_file.exists() {
            let bytes = fs::read(&key_file).map_err(|e| e.to_string())?;
            if bytes.len() != 32 { return Err("cred_store.key invalid length".into()); }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            return Ok(arr);
        }
        let mut arr = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut arr);
        fs::write(&key_file, &arr).map_err(|e| e.to_string())?;
        Ok(arr)
    }
}
