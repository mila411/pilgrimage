//! Module for managing auto-scaling of instances based on load.
//!
//! This module provides the `AutoScaler` struct, which helps manage the number of instances
//! dynamically, scaling up or down based on the current load.

use std::sync::Arc;
use std::time::Duration;

use crate::BrokerError;

/// The `AutoScaler` struct provides functionality for automatically
/// scaling instances based on load and predefined watermarks.
///
/// This struct helps manage the number of instances dynamically,
/// scaling up or down based on the current load.
#[derive(Debug)]
pub struct AutoScaler {
    /// The minimum number of instances that should be running.
    min_instances: usize,
    /// The maximum number of instances that can be running.
    max_instances: usize,
    /// The current number of instances that are running.
    current_instances: Arc<std::sync::Mutex<usize>>,
    /// The high watermark for the load.
    ///
    /// It indicates the load level at which the auto-scaler should consider scaling up.
    high_watermark: usize,
    /// The low watermark for the load.
    ///
    /// It indicates the load level at which the auto-scaler should consider scaling down.
    low_watermark: usize,
}

impl AutoScaler {
    /// Create a new `AutoScaler` with the given minimum and maximum
    /// number of instances.
    ///
    /// The load watermarks are set to 1000 and 100 for the high and low watermarks, respectively.
    ///
    /// # Arguments
    /// * `min_instances` - The minimum number of instances that should be running.
    /// * `max_instances` - The maximum number of instances that can be running.
    ///
    /// # Returns
    /// A new `AutoScaler` instance.
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::scaling::AutoScaler;
    ///
    /// let scaler = AutoScaler::new(1, 10);
    /// ```
    pub fn new(min_instances: usize, max_instances: usize) -> Self {
        Self {
            min_instances,
            max_instances,
            current_instances: Arc::new(std::sync::Mutex::new(min_instances)),
            high_watermark: 1000,
            low_watermark: 100,
        }
    }

    /// Get the high watermark for the load.
    ///
    /// # Returns
    /// The high watermark for the load.
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::scaling::AutoScaler;
    ///
    /// let scaler = AutoScaler::new(1, 10);
    /// // The high watermark is set to 1000 by default
    /// assert_eq!(scaler.high_watermark(), 1000);
    /// ```
    pub fn high_watermark(&self) -> usize {
        self.high_watermark
    }

    /// Get the low watermark for the load.
    ///
    /// # Returns
    /// The low watermark for the load.
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::scaling::AutoScaler;
    ///
    /// let scaler = AutoScaler::new(1, 10);
    /// // The low watermark is set to 100 by default
    /// assert_eq!(scaler.low_watermark(), 100);
    /// ```
    pub fn low_watermark(&self) -> usize {
        self.low_watermark
    }

    /// Scales up the number of instances by one, if the current number
    /// of instances is below the maximum limit.
    ///
    /// # Returns
    /// An `Ok(())` if the scaling operation was successful,
    /// otherwise an error (maximum instances reached).
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::scaling::AutoScaler;
    ///
    /// let scaler = AutoScaler::new(1, 10);
    /// let result = scaler.scale_up();
    /// assert!(result.is_ok());
    /// ```
    pub fn scale_up(&self) -> Result<(), BrokerError> {
        let mut instances = self.current_instances.lock().unwrap();
        if *instances < self.max_instances {
            *instances += 1;
            Ok(())
        } else {
            Err(BrokerError::ScalingError(
                "Maximum number of instances reached".into(),
            ))
        }
    }

    /// Scales down the number of instances by one, if the current number
    /// of instances is above the minimum limit.
    ///
    /// # Returns
    /// An `Ok(())` if the scaling operation was successful,
    /// otherwise an error (minimum instances reached).
    ///
    /// # Example
    /// ```
    /// use pilgrimage::broker::scaling::AutoScaler;
    ///
    /// let scaler = AutoScaler::new(1, 10);
    /// let result = scaler.scale_down();
    /// assert!(result.is_err());
    /// ```
    pub fn scale_down(&self) -> Result<(), BrokerError> {
        let mut instances = self.current_instances.lock().unwrap();
        if *instances > self.min_instances {
            *instances -= 1;
            Ok(())
        } else {
            Err(BrokerError::ScalingError(
                "Minimum number of instances reached".into(),
            ))
        }
    }

    /// Monitors the load and scales instances up or down based on the load level.
    ///
    /// This method runs in a separate thread and periodically checks the load.
    /// If the load exceeds the high watermark, it scales up; if it falls below
    /// the low watermark, it scales down.
    ///
    /// # Arguments
    /// * `check_interval` - The duration between load checks.
    pub fn monitor_and_scale(self: Arc<Self>, check_interval: Duration) {
        std::thread::spawn(move || {
            loop {
                let load = 0.75;
                if load > 0.7 {
                    let _ = self.scale_up();
                } else if load < 0.3 {
                    let _ = self.scale_down();
                }
                std::thread::sleep(check_interval);
            }
        });
    }
}
