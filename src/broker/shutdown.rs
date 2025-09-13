use log::{error, info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Graceful shutdown manager for handling system signals and coordinating broker shutdown
#[derive(Clone)]
pub struct GracefulShutdown {
    shutdown_requested: Arc<AtomicBool>,
    shutdown_completed: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    timeout: Duration,
}

impl GracefulShutdown {
    /// Create a new graceful shutdown manager
    pub fn new(timeout: Duration) -> Self {
        Self {
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            shutdown_completed: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            timeout,
        }
    }

    /// Initialize signal handlers for graceful shutdown
    pub async fn initialize_signal_handlers(&self) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let shutdown = self.clone();
            let mut sigterm = signal(SignalKind::terminate())?;
            tokio::spawn(async move {
                sigterm.recv().await;
                info!("Received SIGTERM, initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });

            let shutdown = self.clone();
            let mut sigint = signal(SignalKind::interrupt())?;
            tokio::spawn(async move {
                sigint.recv().await;
                info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });

            let shutdown = self.clone();
            let mut sigquit = signal(SignalKind::quit())?;
            tokio::spawn(async move {
                sigquit.recv().await;
                info!("Received SIGQUIT, initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });
        }

        #[cfg(windows)]
        {
            use tokio::signal::windows::{ctrl_c, ctrl_break, ctrl_close, ctrl_logoff, ctrl_shutdown};

            let shutdown = self.clone();
            let mut ctrl_c_signal = ctrl_c()?;
            tokio::spawn(async move {
                ctrl_c_signal.recv().await;
                info!("Received Ctrl+C, initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });

            let shutdown = self.clone();
            let mut ctrl_break_signal = ctrl_break()?;
            tokio::spawn(async move {
                ctrl_break_signal.recv().await;
                info!("Received Ctrl+Break, initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });

            let shutdown = self.clone();
            let mut ctrl_close_signal = ctrl_close()?;
            tokio::spawn(async move {
                ctrl_close_signal.recv().await;
                info!("Received Ctrl+Close, initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });

            let shutdown = self.clone();
            let mut ctrl_logoff_signal = ctrl_logoff()?;
            tokio::spawn(async move {
                ctrl_logoff_signal.recv().await;
                info!("Received Ctrl+Logoff, initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });

            let shutdown = self.clone();
            let mut ctrl_shutdown_signal = ctrl_shutdown()?;
            tokio::spawn(async move {
                ctrl_shutdown_signal.recv().await;
                info!("Received Ctrl+Shutdown, initiating graceful shutdown");
                shutdown.initiate_shutdown();
            });
        }

        info!("Signal handlers initialized for graceful shutdown");
        Ok(())
    }

    /// Initiate graceful shutdown
    pub fn initiate_shutdown(&self) {
        if !self.shutdown_requested.swap(true, Ordering::SeqCst) {
            info!("Graceful shutdown initiated");
            self.shutdown_notify.notify_waiters();
        }
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Mark shutdown as completed
    pub fn mark_shutdown_completed(&self) {
        self.shutdown_completed.store(true, Ordering::SeqCst);
        info!("Graceful shutdown completed");
    }

    /// Wait for shutdown signal with timeout
    pub async fn wait_for_shutdown(&self) -> ShutdownResult {
        // If already requested, return immediately
        if self.is_shutdown_requested() {
            return ShutdownResult::Requested;
        }

        // Wait for shutdown notification
        self.shutdown_notify.notified().await;
        ShutdownResult::Requested
    }

    /// Wait for shutdown completion with timeout
    pub async fn wait_for_completion(&self) -> ShutdownResult {
        let timeout_duration = self.timeout;
        
        tokio::select! {
            _ = async {
                while !self.shutdown_completed.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            } => {
                info!("Graceful shutdown completed within timeout");
                ShutdownResult::Completed
            }
            _ = tokio::time::sleep(timeout_duration) => {
                warn!("Graceful shutdown timeout ({:?}) exceeded", timeout_duration);
                ShutdownResult::Timeout
            }
        }
    }

    /// Get the configured timeout
    pub fn get_timeout(&self) -> Duration {
        self.timeout
    }

    /// Set a new timeout (useful for testing or runtime configuration changes)
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShutdownResult {
    Requested,
    Completed,
    Timeout,
}

/// Task manager for tracking and gracefully stopping background tasks
#[derive(Clone)]
pub struct TaskManager {
    active_tasks: Arc<std::sync::Mutex<Vec<TaskHandle>>>,
    shutdown: GracefulShutdown,
}

impl TaskManager {
    /// Create a new task manager
    pub fn new(shutdown: GracefulShutdown) -> Self {
        Self {
            active_tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            shutdown,
        }
    }

    /// Register a task for graceful shutdown
    pub fn register_task(&self, name: String, handle: tokio::task::JoinHandle<()>) {
        let task_handle = TaskHandle::new(name, handle);
        
        if let Ok(mut tasks) = self.active_tasks.lock() {
            tasks.push(task_handle);
        }
    }

    /// Shutdown all registered tasks gracefully
    pub async fn shutdown_all_tasks(&self) -> Result<(), Vec<String>> {
        let tasks = {
            if let Ok(mut task_list) = self.active_tasks.lock() {
                std::mem::take(&mut *task_list)
            } else {
                error!("Failed to acquire task list lock");
                return Err(vec!["Failed to acquire task list lock".to_string()]);
            }
        };

        if tasks.is_empty() {
            info!("No active tasks to shutdown");
            return Ok(());
        }

        info!("Shutting down {} active tasks", tasks.len());
        let mut errors = Vec::new();

        // Wait for all tasks to complete or timeout
        let timeout = self.shutdown.get_timeout();
        
        for mut task in tasks {
            let task_name = task.name.clone();
            
            tokio::select! {
                result = &mut task.handle => {
                    match result {
                        Ok(_) => {
                            info!("Task '{}' completed successfully", task_name);
                        }
                        Err(e) => {
                            if e.is_cancelled() {
                                info!("Task '{}' was cancelled", task_name);
                            } else {
                                error!("Task '{}' failed: {}", task_name, e);
                                errors.push(format!("Task '{}' failed: {}", task_name, e));
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    warn!("Task '{}' did not complete within timeout, aborting", task_name);
                    errors.push(format!("Task '{}' timeout", task_name));
                    task.handle.abort();
                }
            }
        }

        if errors.is_empty() {
            info!("All tasks shut down successfully");
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Handle for a background task
pub struct TaskHandle {
    pub name: String,
    pub handle: tokio::task::JoinHandle<()>,
}

impl TaskHandle {
    pub fn new(name: String, handle: tokio::task::JoinHandle<()>) -> Self {
        Self { name, handle }
    }
}

/// Resource cleanup manager for ensuring proper resource disposal
#[derive(Clone)]
pub struct ResourceManager {
    cleanup_callbacks: Arc<std::sync::Mutex<Vec<Box<dyn Fn() -> Result<(), String> + Send + Sync>>>>,
}

impl ResourceManager {
    /// Create a new resource manager
    pub fn new() -> Self {
        Self {
            cleanup_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Register a cleanup callback
    pub fn register_cleanup<F>(&self, callback: F) 
    where
        F: Fn() -> Result<(), String> + Send + Sync + 'static,
    {
        if let Ok(mut callbacks) = self.cleanup_callbacks.lock() {
            callbacks.push(Box::new(callback));
        }
    }

    /// Execute all cleanup callbacks
    pub fn cleanup_all(&self) -> Result<(), Vec<String>> {
        let callbacks = {
            if let Ok(mut callback_list) = self.cleanup_callbacks.lock() {
                std::mem::take(&mut *callback_list)
            } else {
                error!("Failed to acquire cleanup callbacks lock");
                return Err(vec!["Failed to acquire cleanup callbacks lock".to_string()]);
            }
        };

        if callbacks.is_empty() {
            info!("No cleanup callbacks to execute");
            return Ok(());
        }

        info!("Executing {} cleanup callbacks", callbacks.len());
        let mut errors = Vec::new();

        for (index, callback) in callbacks.into_iter().enumerate() {
            match callback() {
                Ok(_) => {
                    info!("Cleanup callback {} executed successfully", index + 1);
                }
                Err(e) => {
                    error!("Cleanup callback {} failed: {}", index + 1, e);
                    errors.push(format!("Cleanup callback {} failed: {}", index + 1, e));
                }
            }
        }

        if errors.is_empty() {
            info!("All cleanup callbacks executed successfully");
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Default for ResourceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_graceful_shutdown_basic() {
        let shutdown = GracefulShutdown::new(Duration::from_secs(1));
        
        assert!(!shutdown.is_shutdown_requested());
        
        shutdown.initiate_shutdown();
        assert!(shutdown.is_shutdown_requested());
        
        shutdown.mark_shutdown_completed();
        
        let result = shutdown.wait_for_completion().await;
        assert_eq!(result, ShutdownResult::Completed);
    }

    #[tokio::test]
    async fn test_shutdown_timeout() {
        let shutdown = GracefulShutdown::new(Duration::from_millis(100));
        
        shutdown.initiate_shutdown();
        
        // Don't mark as completed, should timeout
        let result = shutdown.wait_for_completion().await;
        assert_eq!(result, ShutdownResult::Timeout);
    }

    #[tokio::test]
    async fn test_task_manager() {
        let shutdown = GracefulShutdown::new(Duration::from_secs(1));
        let task_manager = TaskManager::new(shutdown);
        
        // Register a simple task
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
        
        task_manager.register_task("test_task".to_string(), handle);
        
        // Shutdown should complete successfully
        let result = task_manager.shutdown_all_tasks().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_resource_manager() {
        let resource_manager = ResourceManager::new();
        
        let cleanup_called = Arc::new(AtomicBool::new(false));
        let flag = cleanup_called.clone();

        resource_manager.register_cleanup(move || {
            flag.store(true, Ordering::SeqCst);
            Ok(())
        });
        
        let result = resource_manager.cleanup_all();
        assert!(result.is_ok());
        assert!(cleanup_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_wait_for_shutdown_notify() {
        let shutdown = GracefulShutdown::new(Duration::from_secs(1));

        let (tx, rx) = oneshot::channel();
        let s = shutdown.clone();
        tokio::spawn(async move {
            let result = s.wait_for_shutdown().await;
            let _ = tx.send(result);
        });

        // Give spawned task a moment to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown.initiate_shutdown();

        let recv = tokio::time::timeout(Duration::from_millis(200), rx).await;
        assert!(recv.is_ok(), "wait_for_shutdown did not notify in time");
        assert_eq!(recv.unwrap().unwrap(), ShutdownResult::Requested);
    }

    #[tokio::test]
    async fn test_task_manager_timeout_and_abort() {
        // Short timeout to trigger abort on long running task
        let shutdown = GracefulShutdown::new(Duration::from_millis(50));
        let task_manager = TaskManager::new(shutdown);

        // Task that completes quickly
        let quick = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
        task_manager.register_task("quick_task".to_string(), quick);

        // Task that takes longer than timeout
        let long = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        task_manager.register_task("long_task".to_string(), long);

        let result = task_manager.shutdown_all_tasks().await;
        assert!(result.is_err(), "Expected timeout error for long task");
        let errs = result.err().unwrap();
        assert!(errs.iter().any(|e| e.contains("timeout")), "Timeout not reported: {:?}", errs);
    }

    #[tokio::test]
    async fn test_end_to_end_graceful_shutdown_flow() {
        let shutdown = GracefulShutdown::new(Duration::from_millis(100));
        let task_manager = TaskManager::new(shutdown.clone());
        let resource_manager = ResourceManager::new();

        // Tasks: one quick, one slow (to force timeout)
        let quick = tokio::spawn(async { tokio::time::sleep(Duration::from_millis(10)).await; });
        task_manager.register_task("quick".into(), quick);

        let slow = tokio::spawn(async { tokio::time::sleep(Duration::from_secs(2)).await; });
        task_manager.register_task("slow".into(), slow);

        // Cleanup callbacks: one ok, one error
        resource_manager.register_cleanup(|| Ok(()));
        resource_manager.register_cleanup(|| Err("cleanup failed".to_string()));

        // Initiate shutdown path and waiters would be notified
        shutdown.initiate_shutdown();

        // Tasks should attempt to finish within timeout; slow should time out
        let tasks_result = task_manager.shutdown_all_tasks().await;
        assert!(tasks_result.is_err());

        // Cleanup should report aggregated error(s)
        let cleanup_result = resource_manager.cleanup_all();
        assert!(cleanup_result.is_err());
        let cleanup_errs = cleanup_result.err().unwrap();
        assert_eq!(cleanup_errs.len(), 1);

        // Mark completion and verify wait_for_completion observes Completed
        shutdown.mark_shutdown_completed();
        let complete = shutdown.wait_for_completion().await;
        assert_eq!(complete, ShutdownResult::Completed);
    }
}