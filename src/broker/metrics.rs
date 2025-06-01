use std::time::Instant;

#[derive(Debug, Clone)]
pub struct SystemMetrics {
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub network_io: f32,
    pub timestamp: Instant,
}

impl SystemMetrics {
    pub fn new(cpu_usage: f32, memory_usage: f32, network_io: f32) -> Self {
        Self {
            cpu_usage,
            memory_usage,
            network_io,
            timestamp: Instant::now(),
        }
    }

    pub fn is_overloaded(&self, threshold: f32) -> bool {
        self.cpu_usage > threshold || self.memory_usage > threshold
    }

    pub fn is_underutilized(&self, threshold: f32) -> bool {
        self.cpu_usage < threshold && self.memory_usage < threshold
    }
}
