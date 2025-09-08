//! Dashboard unit tests

#[cfg(test)]
mod dashboard_tests {
    use pilgrimage::monitoring::enhanced_dashboard::{
        EnhancedDashboard, EnhancedDashboardConfig, WidgetType, Widget, DashboardLayout
    };
    use pilgrimage::monitoring::enhanced_alerts::{EnhancedAlertManager, AlertConfig};
    use pilgrimage::monitoring::metrics_collector::{MetricsCollector, MetricsConfig};
    use std::sync::Arc;
    use std::time::Duration;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_dashboard_creation() {
        let config = EnhancedDashboardConfig::default();
        let metrics_config = MetricsConfig {
            enabled: true,
            collection_interval_seconds: 10,
            retention_days: 1,
            export_interval_seconds: 60,
            prometheus_enabled: false,
            prometheus_port: 9092,
            custom_metrics_enabled: true,
        };

        let alert_config = AlertConfig {
            enabled: true,
            evaluation_interval: Duration::from_secs(30),
            max_concurrent_evaluations: 5,
            alert_history_retention: Duration::from_secs(24 * 60 * 60),
            enable_anomaly_detection: false,
            enable_correlation: false,
            enable_auto_resolution: false,
            notification_timeout: Duration::from_secs(30),
            max_alert_age: Duration::from_secs(60 * 60),
            suppression_enabled: false,
            escalation_enabled: false,
        };

        let metrics_collector = Arc::new(MetricsCollector::new(
            metrics_config,
            "test-node".to_string(),
        ));

        let alert_manager = Arc::new(EnhancedAlertManager::new(
            alert_config,
            "test-node".to_string(),
            Arc::new(pilgrimage::network::transport::NetworkTransport::new("127.0.0.1:0".parse().unwrap())),
        ));

        let dashboard = EnhancedDashboard::new(
            config,
            "test-node".to_string(),
            alert_manager,
            metrics_collector,
        );

        // Test dashboard creation
        assert_eq!(dashboard.node_id, "test-node");
        assert!(dashboard.config.enabled);
    }

    #[test]
    fn test_widget_creation() {
        let widget = Widget {
            id: "test-widget".to_string(),
            widget_type: WidgetType::MetricChart,
            title: "Test Widget".to_string(),
            position: (0, 0),
            size: (400, 300),
            config: HashMap::new(),
            refresh_rate: Duration::from_secs(5),
        };

        assert_eq!(widget.id, "test-widget");
        assert_eq!(widget.title, "Test Widget");
        assert_eq!(widget.position, (0, 0));
        assert_eq!(widget.size, (400, 300));
    }

    #[test]
    fn test_dashboard_config_defaults() {
        let config = EnhancedDashboardConfig::default();

        assert!(config.enabled);
        assert_eq!(config.port, 8080);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.refresh_interval_seconds, 5);
        assert_eq!(config.max_data_points, 1000);
        assert!(config.enable_real_time);
        assert!(config.enable_alerts);
        assert!(config.enable_tracing);
        assert!(config.enable_logs);
        assert!(config.enable_metrics);
        assert!(config.websocket_enabled);
        assert!(!config.authentication_enabled);
        assert!(!config.ssl_enabled);
        assert!(config.cors_enabled);
    }

    #[tokio::test]
    async fn test_dashboard_metrics() {
        let config = EnhancedDashboardConfig::default();
        let metrics_config = MetricsConfig {
            enabled: true,
            collection_interval_seconds: 10,
            retention_days: 1,
            export_interval_seconds: 60,
            prometheus_enabled: false,
            prometheus_port: 9093,
            custom_metrics_enabled: true,
        };

        let alert_config = AlertConfig {
            enabled: true,
            evaluation_interval: Duration::from_secs(30),
            max_concurrent_evaluations: 5,
            alert_history_retention: Duration::from_secs(24 * 60 * 60),
            enable_anomaly_detection: false,
            enable_correlation: false,
            enable_auto_resolution: false,
            notification_timeout: Duration::from_secs(30),
            max_alert_age: Duration::from_secs(60 * 60),
            suppression_enabled: false,
            escalation_enabled: false,
        };

        let metrics_collector = Arc::new(MetricsCollector::new(
            metrics_config,
            "test-node".to_string(),
        ));

        let alert_manager = Arc::new(EnhancedAlertManager::new(
            alert_config,
            "test-node".to_string(),
            Arc::new(pilgrimage::network::transport::NetworkTransport::new("127.0.0.1:0".parse().unwrap())),
        ));

        let dashboard = EnhancedDashboard::new(
            config,
            "test-node".to_string(),
            alert_manager,
            metrics_collector,
        );

        let metrics = dashboard.get_dashboard_metrics();

        // Initial metrics should be zero
        assert_eq!(metrics.page_views, 0);
        assert_eq!(metrics.active_sessions, 0);
        assert_eq!(metrics.data_queries, 0);
        assert_eq!(metrics.real_time_updates, 0);
        assert_eq!(metrics.error_count, 0);
        assert_eq!(metrics.cache_hits, 0);
        assert_eq!(metrics.cache_misses, 0);
    }
}
