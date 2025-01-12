use std::sync::Arc;
use std::time::Duration;

use crate::BrokerError;

#[derive(Debug)]
pub struct AutoScaler {
    min_instances: usize,
    max_instances: usize,
    current_instances: Arc<std::sync::Mutex<usize>>,
    high_watermark: usize,
    low_watermark: usize,
}

impl AutoScaler {
    pub fn new(min_instances: usize, max_instances: usize) -> Self {
        Self {
            min_instances,
            max_instances,
            current_instances: Arc::new(std::sync::Mutex::new(min_instances)),
            high_watermark: 1000,
            low_watermark: 100,
        }
    }

    pub fn high_watermark(&self) -> usize {
        self.high_watermark
    }

    pub fn low_watermark(&self) -> usize {
        self.low_watermark
    }

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
