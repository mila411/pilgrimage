/// Advanced metrics collection, alert management, and performance monitoring
pub mod advanced_metrics;

pub use advanced_metrics::{
    AdvancedMetricsSystem, Alert, AlertManager, AlertSeverity, AlertThresholds, AlertType,
    ApplicationMetrics, ExportConfig, MetricsCollector, MetricsConfig, MetricsSummary,
    SystemMetrics,
};
