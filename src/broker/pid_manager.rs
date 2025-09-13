use log::{error, info, warn};
use std::fs;
use std::path::{Path, PathBuf};
use std::process;

/// PID file manager for tracking broker processes
#[derive(Clone)]
pub struct PidManager {
    pid_file_path: PathBuf,
    broker_id: String,
}

impl PidManager {
    /// Create a new PID manager
    pub fn new(broker_id: String) -> Self {
        let pid_file_path = Self::get_pid_file_path(&broker_id);
        Self {
            pid_file_path,
            broker_id,
        }
    }

    /// Create a new PID manager with custom PID file directory
    pub fn new_with_dir<P: AsRef<Path>>(broker_id: String, pid_dir: P) -> Self {
        let pid_file_path = pid_dir.as_ref().join(format!("pilgrimage_broker_{}.pid", broker_id));
        Self {
            pid_file_path,
            broker_id,
        }
    }

    /// Get the default PID file path for a broker
    fn get_pid_file_path(broker_id: &str) -> PathBuf {
        // Use system temp directory or /var/run if available
        let base_dir = if Path::new("/var/run").exists() && Path::new("/var/run").is_dir() {
            PathBuf::from("/var/run")
        } else {
            std::env::temp_dir()
        };
        
        base_dir.join(format!("pilgrimage_broker_{}.pid", broker_id))
    }

    /// Create and write the PID file
    pub fn create_pid_file(&self) -> Result<(), std::io::Error> {
        let current_pid = process::id();
        
        // Check if PID file already exists
        if self.pid_file_path.exists() {
            match self.read_pid_file() {
                Ok(existing_pid) => {
                    // Check if the process is still running
                    if self.is_process_running(existing_pid) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            format!("Broker {} is already running with PID {}", self.broker_id, existing_pid)
                        ));
                    } else {
                        warn!("Stale PID file found for broker {}, removing...", self.broker_id);
                        if let Err(e) = fs::remove_file(&self.pid_file_path) {
                            warn!("Failed to remove stale PID file: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read existing PID file: {}, removing...", e);
                    if let Err(e) = fs::remove_file(&self.pid_file_path) {
                        warn!("Failed to remove corrupted PID file: {}", e);
                    }
                }
            }
        }

        // Ensure parent directory exists
        if let Some(parent) = self.pid_file_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        // Write the current PID
        fs::write(&self.pid_file_path, current_pid.to_string())?;
        info!("Created PID file for broker {}: {} (PID: {})", 
              self.broker_id, self.pid_file_path.display(), current_pid);
        
        Ok(())
    }

    /// Read the PID from the PID file
    pub fn read_pid_file(&self) -> Result<u32, std::io::Error> {
        let content = fs::read_to_string(&self.pid_file_path)?;
        content.trim().parse::<u32>()
            .map_err(|e| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid PID in file: {}", e)
            ))
    }

    /// Remove the PID file
    pub fn remove_pid_file(&self) -> Result<(), std::io::Error> {
        if self.pid_file_path.exists() {
            fs::remove_file(&self.pid_file_path)?;
            info!("Removed PID file for broker {}: {}", 
                  self.broker_id, self.pid_file_path.display());
        }
        Ok(())
    }

    /// Check if the PID file exists
    pub fn pid_file_exists(&self) -> bool {
        self.pid_file_path.exists()
    }

    /// Get the PID file path
    pub fn get_pid_file_path_str(&self) -> String {
        self.pid_file_path.to_string_lossy().to_string()
    }

    /// Check if a process with the given PID is running
    pub fn is_process_running(&self, pid: u32) -> bool {
        #[cfg(unix)]
        {
            use std::process::Command;
            
            // Use kill with signal 0 to check if process exists
            let output = Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .output();
                
            match output {
                Ok(output) => output.status.success(),
                Err(_) => false,
            }
        }

        #[cfg(windows)]
        {
            use std::process::Command;
            
            // Use tasklist to check if process exists
            let output = Command::new("tasklist")
                .arg("/FI")
                .arg(format!("PID eq {}", pid))
                .arg("/NH")
                .output();
                
            match output {
                Ok(output) => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    stdout.contains(&pid.to_string())
                }
                Err(_) => false,
            }
        }
    }

    /// Get the current broker's PID if it's running
    pub fn get_running_broker_pid(&self) -> Option<u32> {
        if self.pid_file_exists() {
            match self.read_pid_file() {
                Ok(pid) => {
                    if self.is_process_running(pid) {
                        Some(pid)
                    } else {
                        warn!("PID file exists but process {} is not running", pid);
                        None
                    }
                }
                Err(e) => {
                    error!("Failed to read PID file: {}", e);
                    None
                }
            }
        } else {
            None
        }
    }

    /// Force cleanup of stale PID files
    pub fn cleanup_stale_pid_file(&self) -> Result<(), std::io::Error> {
        if self.pid_file_exists() {
            match self.read_pid_file() {
                Ok(pid) => {
                    if !self.is_process_running(pid) {
                        info!("Cleaning up stale PID file for non-running process {}", pid);
                        self.remove_pid_file()?;
                    }
                }
                Err(_) => {
                    info!("Cleaning up corrupted PID file");
                    self.remove_pid_file()?;
                }
            }
        }
        Ok(())
    }

    /// Send a signal to the broker process
    #[cfg(unix)]
    pub fn send_signal(&self, signal: i32) -> Result<(), std::io::Error> {
        if let Some(pid) = self.get_running_broker_pid() {
            use std::process::Command;
            
            let status = Command::new("kill")
                .arg(format!("-{}", signal))
                .arg(pid.to_string())
                .status()?;
                
            if status.success() {
                info!("Sent signal {} to broker {} (PID: {})", signal, self.broker_id, pid);
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to send signal {} to PID {}", signal, pid)
                ))
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Broker {} is not running", self.broker_id)
            ))
        }
    }

    /// Send SIGTERM for graceful shutdown
    #[cfg(unix)]
    pub fn send_sigterm(&self) -> Result<(), std::io::Error> {
        self.send_signal(15) // SIGTERM
    }

    /// Send SIGKILL for forced termination
    #[cfg(unix)]
    pub fn send_sigkill(&self) -> Result<(), std::io::Error> {
        self.send_signal(9) // SIGKILL
    }

    /// Terminate process on Windows
    #[cfg(windows)]
    pub fn terminate_process(&self, force: bool) -> Result<(), std::io::Error> {
        if let Some(pid) = self.get_running_broker_pid() {
            use std::process::Command;
            
            let mut cmd = Command::new("taskkill");
            cmd.arg("/PID").arg(pid.to_string());
            
            if force {
                cmd.arg("/F");
            }
            
            let status = cmd.status()?;
            
            if status.success() {
                info!("Terminated broker {} (PID: {})", self.broker_id, pid);
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to terminate PID {}", pid)
                ))
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Broker {} is not running", self.broker_id)
            ))
        }
    }
}

impl Drop for PidManager {
    fn drop(&mut self) {
        // Attempt to clean up PID file when the manager is dropped
        if let Err(e) = self.remove_pid_file() {
            error!("Failed to clean up PID file in drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_pid_manager_basic() {
        let temp_dir = tempdir().unwrap();
        let pid_manager = PidManager::new_with_dir("test_broker".to_string(), temp_dir.path());
        
        // Should not exist initially
        assert!(!pid_manager.pid_file_exists());
        
        // Create PID file
        pid_manager.create_pid_file().unwrap();
        assert!(pid_manager.pid_file_exists());
        
        // Read PID should match current process
        let read_pid = pid_manager.read_pid_file().unwrap();
        assert_eq!(read_pid, process::id());
        
        // Remove PID file
        pid_manager.remove_pid_file().unwrap();
        assert!(!pid_manager.pid_file_exists());
    }

    #[test]
    fn test_stale_pid_cleanup() {
        let temp_dir = tempdir().unwrap();
        let pid_manager = PidManager::new_with_dir("test_broker".to_string(), temp_dir.path());
        
        // Create a fake PID file with a non-existent PID
        let fake_pid = 999999u32;
        fs::write(pid_manager.get_pid_file_path_str(), fake_pid.to_string()).unwrap();
        
        // Cleanup should remove the stale file
        pid_manager.cleanup_stale_pid_file().unwrap();
        assert!(!pid_manager.pid_file_exists());
    }

    #[test]
    fn test_get_running_broker_pid() {
        let temp_dir = tempdir().unwrap();
        let pid_manager = PidManager::new_with_dir("test_broker".to_string(), temp_dir.path());
        
        // No PID file initially
        assert!(pid_manager.get_running_broker_pid().is_none());
        
        // Create PID file
        pid_manager.create_pid_file().unwrap();
        
        // Should return current PID
        let running_pid = pid_manager.get_running_broker_pid();
        assert!(running_pid.is_some());
        assert_eq!(running_pid.unwrap(), process::id());
    }
}