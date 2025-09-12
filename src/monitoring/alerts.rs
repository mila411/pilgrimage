//! Alert system for anomaly detection and notification
//!
//! Provides comprehensive alerting capabilities for monitoring
//! system health and detecting anomalous conditions.

use crate::network::error::NetworkResult;
use crate::network::transport::NetworkTransport;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;
use uuid::Uuid;
use log::{debug, error, info};

/// Alert severity levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "INFO"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
            AlertSeverity::Emergency => write!(f, "EMERGENCY"),
        }
    }
}

/// Alert state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertState {
    Pending,
    Firing,
    Resolved,
    Suppressed,
}

impl std::fmt::Display for AlertState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertState::Pending => write!(f, "PENDING"),
            AlertState::Firing => write!(f, "FIRING"),
            AlertState::Resolved => write!(f, "RESOLVED"),
            AlertState::Suppressed => write!(f, "SUPPRESSED"),
        }
    }
}

/// Alert condition types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    Threshold {
        metric: String,
        operator: ComparisonOperator,
        value: f64,
        duration: Duration,
    },
    RateChange {
        metric: String,
        threshold_percent: f64,
        window: Duration,
    },
    Absence {
        metric: String,
        duration: Duration,
    },
    ErrorRate {
        error_metric: String,
        total_metric: String,
        threshold_percent: f64,
        min_samples: u64,
    },
    Custom {
        name: String,
        expression: String,
    },
}

/// Comparison operators for threshold conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOperator {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
}

/// Alert rule configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub enabled: bool,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub notification_targets: Vec<String>,
    pub suppression_rules: Vec<SuppressionRule>,
}

/// Suppression rules to prevent alert spam
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuppressionRule {
    pub name: String,
    pub condition: String, // Simple expression for suppression
    pub duration: Duration,
}

/// Active alert instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub rule_name: String,
    pub severity: AlertSeverity,
    pub state: AlertState,
    pub message: String,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub started_at: u64,
    pub fired_at: Option<u64>,
    pub resolved_at: Option<u64>,
    pub node_id: String,
    pub metric_values: HashMap<String, f64>,
}

/// Notification channel configuration
#[derive(Debug, Clone)]
pub struct NotificationChannel {
    pub id: String,
    pub name: String,
    pub channel_type: NotificationChannelType,
    pub config: NotificationConfig,
    pub enabled: bool,
}

/// Notification channel types
#[derive(Debug, Clone)]
pub enum NotificationChannelType {
    Email,
    Slack,
    Webhook,
    PagerDuty,
    Log,
}

/// Notification configuration
#[derive(Debug, Clone)]
pub enum NotificationConfig {
    Email {
        smtp_server: String,
        from_address: String,
        to_addresses: Vec<String>,
    },
    Slack {
        webhook_url: String,
        channel: String,
    },
    Webhook {
        url: String,
        headers: HashMap<String, String>,
    },
    PagerDuty {
        integration_key: String,
    },
    Log {
        level: String,
    },
}

/// Alert manager configuration
#[derive(Debug, Clone)]
pub struct AlertManagerConfig {
    pub enabled: bool,
    pub evaluation_interval: Duration,
    pub retention_period: Duration,
    pub max_alerts: usize,
    pub grouping_enabled: bool,
    pub group_wait: Duration,
    pub group_interval: Duration,
    pub repeat_interval: Duration,
}

impl Default for AlertManagerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(30),
            retention_period: Duration::from_secs(24 * 3600), // 24 hours
            max_alerts: 10000,
            grouping_enabled: true,
            group_wait: Duration::from_secs(30),
            group_interval: Duration::from_secs(300), // 5 minutes
            repeat_interval: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// Alert manager for anomaly detection and notification
pub struct AlertManager {
    config: AlertManagerConfig,
    node_id: String,
    transport: Arc<NetworkTransport>,
    alert_rules: Arc<RwLock<HashMap<Uuid, AlertRule>>>,
    active_alerts: Arc<RwLock<HashMap<Uuid, Alert>>>,
    alert_history: Arc<Mutex<VecDeque<Alert>>>,
    notification_channels: Arc<RwLock<HashMap<String, NotificationChannel>>>,
    metrics_cache: Arc<Mutex<HashMap<String, MetricSeries>>>,
    alert_metrics: Arc<Mutex<AlertMetrics>>,
    suppressed_alerts: Arc<RwLock<HashSet<Uuid>>>,
}

/// Metric time series for evaluation
#[derive(Debug, Clone)]
struct MetricSeries {
    name: String,
    values: VecDeque<MetricPoint>,
    last_updated: Instant,
}

/// Single metric point
#[derive(Debug, Clone)]
struct MetricPoint {
    timestamp: u64,
    value: f64,
}

/// Alert system metrics
#[derive(Debug, Default)]
pub struct AlertMetrics {
    total_alerts_created: u64,
    alerts_fired: u64,
    alerts_resolved: u64,
    notifications_sent: u64,
    notification_failures: u64,
    rules_evaluated: u64,
    evaluation_duration_micros: u64,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new(
        config: AlertManagerConfig,
        node_id: String,
        transport: Arc<NetworkTransport>,
    ) -> Self {
        Self {
            config,
            node_id,
            transport,
            alert_rules: Arc::new(RwLock::new(HashMap::new())),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            alert_history: Arc::new(Mutex::new(VecDeque::new())),
            notification_channels: Arc::new(RwLock::new(HashMap::new())),
            metrics_cache: Arc::new(Mutex::new(HashMap::new())),
            alert_metrics: Arc::new(Mutex::new(AlertMetrics::default())),
            suppressed_alerts: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Start the alert manager service
    pub async fn start(&self) -> NetworkResult<()> {
        if !self.config.enabled {
            info!("Alert manager is disabled");
            return Ok(());
        }

        info!("Starting alert manager for node {}", self.node_id);

        // Start rule evaluation task
        self.start_evaluation_task().await;

        // Start notification task
        self.start_notification_task().await;

        // Start cleanup task
        self.start_cleanup_task().await;

        // Add default alert rules
        self.add_default_alert_rules().await?;

        Ok(())
    }

    /// Add an alert rule
    pub async fn add_alert_rule(&self, rule: AlertRule) -> NetworkResult<()> {
        let mut rules = self.alert_rules.write().await;
        rules.insert(rule.id, rule.clone());
        info!("Added alert rule: {} ({})", rule.name, rule.id);
        Ok(())
    }

    /// Remove an alert rule
    pub async fn remove_alert_rule(&self, rule_id: Uuid) -> NetworkResult<()> {
        let mut rules = self.alert_rules.write().await;
        if let Some(rule) = rules.remove(&rule_id) {
            info!("Removed alert rule: {} ({})", rule.name, rule_id);
        }
        Ok(())
    }

    /// Add a notification channel
    pub async fn add_notification_channel(&self, channel: NotificationChannel) -> NetworkResult<()> {
        let mut channels = self.notification_channels.write().await;
        channels.insert(channel.id.clone(), channel.clone());
        info!("Added notification channel: {} ({})", channel.name, channel.id);
        Ok(())
    }

    /// Update metric value for alert evaluation
    pub async fn update_metric(&self, name: &str, value: f64) -> NetworkResult<()> {
        let mut cache = self.metrics_cache.lock().await;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metric = cache.entry(name.to_string()).or_insert_with(|| MetricSeries {
            name: name.to_string(),
            values: VecDeque::new(),
            last_updated: Instant::now(),
        });

        metric.values.push_back(MetricPoint { timestamp, value });
        metric.last_updated = Instant::now();

        // Keep only recent values (last hour)
        let cutoff_time = timestamp - 3600;
        while let Some(front) = metric.values.front() {
            if front.timestamp < cutoff_time {
                metric.values.pop_front();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Get active alerts
    pub async fn get_active_alerts(&self) -> Vec<Alert> {
        let alerts = self.active_alerts.read().await;
        alerts.values().cloned().collect()
    }

    /// Get alert history
    pub async fn get_alert_history(&self, limit: Option<usize>) -> Vec<Alert> {
        let history = self.alert_history.lock().await;
        let limit = limit.unwrap_or(100);
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get alert metrics
    pub async fn get_metrics(&self) -> AlertMetrics {
        let metrics = self.alert_metrics.lock().await;
        AlertMetrics {
            total_alerts_created: metrics.total_alerts_created,
            alerts_fired: metrics.alerts_fired,
            alerts_resolved: metrics.alerts_resolved,
            notifications_sent: metrics.notifications_sent,
            notification_failures: metrics.notification_failures,
            rules_evaluated: metrics.rules_evaluated,
            evaluation_duration_micros: metrics.evaluation_duration_micros,
        }
    }

    /// Start rule evaluation task
    async fn start_evaluation_task(&self) {
        let rules = self.alert_rules.clone();
        let active_alerts = self.active_alerts.clone();
        let metrics_cache = self.metrics_cache.clone();
        let alert_metrics = self.alert_metrics.clone();
        let node_id = self.node_id.clone();
        let interval_duration = self.config.evaluation_interval;

        tokio::task::spawn(async move {
            let mut timer = interval(interval_duration);
            loop {
                timer.tick().await;

                let start_time = Instant::now();
                if let Err(e) = Self::evaluate_rules(
                    &rules,
                    &active_alerts,
                    &metrics_cache,
                    &alert_metrics,
                    &node_id,
                ).await {
                    error!("Rule evaluation failed: {}", e);
                }

                let duration = start_time.elapsed();
                let mut metrics = alert_metrics.lock().await;
                metrics.evaluation_duration_micros = duration.as_micros() as u64;
            }
        });
    }

    /// Evaluate alert rules
    async fn evaluate_rules(
        rules: &Arc<RwLock<HashMap<Uuid, AlertRule>>>,
        active_alerts: &Arc<RwLock<HashMap<Uuid, Alert>>>,
        metrics_cache: &Arc<Mutex<HashMap<String, MetricSeries>>>,
        alert_metrics: &Arc<Mutex<AlertMetrics>>,
        node_id: &str,
    ) -> NetworkResult<()> {
        let rules = rules.read().await;
        let mut metrics = alert_metrics.lock().await;

        for rule in rules.values() {
            if !rule.enabled {
                continue;
            }

            metrics.rules_evaluated += 1;

            // Evaluate rule condition
            let should_fire = Self::evaluate_condition(&rule.condition, metrics_cache).await?;

            let mut active_alerts = active_alerts.write().await;
            let existing_alert_id = active_alerts.values()
                .find(|alert| alert.rule_id == rule.id)
                .map(|alert| alert.id);

            match (should_fire, existing_alert_id) {
                (true, None) => {
                    // Create new alert
                    let alert = Alert {
                        id: Uuid::new_v4(),
                        rule_id: rule.id,
                        rule_name: rule.name.clone(),
                        severity: rule.severity.clone(),
                        state: AlertState::Pending,
                        message: Self::format_alert_message(rule, metrics_cache).await,
                        labels: rule.labels.clone(),
                        annotations: rule.annotations.clone(),
                        started_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                        fired_at: None,
                        resolved_at: None,
                        node_id: node_id.to_string(),
                        metric_values: Self::get_current_metric_values(&rule.condition, metrics_cache).await,
                    };

                    active_alerts.insert(alert.id, alert.clone());
                    metrics.total_alerts_created += 1;
                    info!("Created alert: {} for rule: {}", alert.id, rule.name);
                }
                (false, Some(alert_id)) => {
                    // Resolve existing alert
                    if let Some(alert) = active_alerts.get(&alert_id) {
                        let mut resolved_alert = alert.clone();
                        resolved_alert.state = AlertState::Resolved;
                        resolved_alert.resolved_at = Some(
                            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
                        );

                        active_alerts.remove(&alert_id);
                        metrics.alerts_resolved += 1;
                        info!("Resolved alert: {} for rule: {}", alert_id, rule.name);
                    }
                }
                _ => {
                    // No state change
                }
            }
        }

        Ok(())
    }

    /// Evaluate a single condition
    async fn evaluate_condition(
        condition: &AlertCondition,
        metrics_cache: &Arc<Mutex<HashMap<String, MetricSeries>>>,
    ) -> NetworkResult<bool> {
        let cache = metrics_cache.lock().await;

        match condition {
            AlertCondition::Threshold { metric, operator, value, duration: _ } => {
                if let Some(series) = cache.get(metric) {
                    if let Some(latest) = series.values.back() {
                        return Ok(Self::compare_values(latest.value, *value, operator));
                    }
                }
                Ok(false)
            }
            AlertCondition::RateChange { metric, threshold_percent, window } => {
                if let Some(series) = cache.get(metric) {
                    let window_secs = window.as_secs();
                    let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    let cutoff_time = current_time - window_secs;

                    let recent_values: Vec<f64> = series.values
                        .iter()
                        .filter(|point| point.timestamp >= cutoff_time)
                        .map(|point| point.value)
                        .collect();

                    if recent_values.len() >= 2 {
                        let first = recent_values[0];
                        let last = recent_values[recent_values.len() - 1];
                        let change_percent = if first != 0.0 {
                            ((last - first) / first) * 100.0
                        } else {
                            0.0
                        };

                        return Ok(change_percent.abs() > *threshold_percent);
                    }
                }
                Ok(false)
            }
            AlertCondition::Absence { metric, duration } => {
                if let Some(series) = cache.get(metric) {
                    let absence_threshold = Instant::now() - *duration;
                    Ok(series.last_updated < absence_threshold)
                } else {
                    Ok(true)
                }
            }
            AlertCondition::ErrorRate { error_metric, total_metric, threshold_percent, min_samples } => {
                if let (Some(error_series), Some(total_series)) =
                    (cache.get(error_metric), cache.get(total_metric)) {

                    if let (Some(error_point), Some(total_point)) =
                        (error_series.values.back(), total_series.values.back()) {

                        if total_point.value >= *min_samples as f64 {
                            let error_rate = (error_point.value / total_point.value) * 100.0;
                            return Ok(error_rate > *threshold_percent);
                        }
                    }
                }
                Ok(false)
            }
            AlertCondition::Custom { name: _, expression: _ } => {
                // Custom expression evaluation would go here
                debug!("Custom condition evaluation not implemented");
                Ok(false)
            }
        }
    }

    /// Compare values using operator
    fn compare_values(left: f64, right: f64, operator: &ComparisonOperator) -> bool {
        match operator {
            ComparisonOperator::GreaterThan => left > right,
            ComparisonOperator::GreaterThanOrEqual => left >= right,
            ComparisonOperator::LessThan => left < right,
            ComparisonOperator::LessThanOrEqual => left <= right,
            ComparisonOperator::Equal => (left - right).abs() < f64::EPSILON,
            ComparisonOperator::NotEqual => (left - right).abs() >= f64::EPSILON,
        }
    }

    /// Format alert message
    async fn format_alert_message(
        rule: &AlertRule,
        metrics_cache: &Arc<Mutex<HashMap<String, MetricSeries>>>,
    ) -> String {
        let metric_values = Self::get_current_metric_values(&rule.condition, metrics_cache).await;

        match &rule.condition {
            AlertCondition::Threshold { metric, operator, value, .. } => {
                let current_value = metric_values.get(metric).cloned().unwrap_or(0.0);
                format!("{}: {} {} {} (current: {})",
                    rule.name, metric, format!("{:?}", operator), value, current_value)
            }
            AlertCondition::RateChange { metric, threshold_percent, .. } => {
                format!("{}: {} rate change exceeds {}%",
                    rule.name, metric, threshold_percent)
            }
            AlertCondition::Absence { metric, duration } => {
                format!("{}: {} absent for {:?}",
                    rule.name, metric, duration)
            }
            AlertCondition::ErrorRate { error_metric, total_metric, threshold_percent, .. } => {
                format!("{}: error rate ({}/{}) exceeds {}%",
                    rule.name, error_metric, total_metric, threshold_percent)
            }
            AlertCondition::Custom { name, .. } => {
                format!("{}: custom condition '{}' triggered", rule.name, name)
            }
        }
    }

    /// Get current metric values for condition
    async fn get_current_metric_values(
        condition: &AlertCondition,
        metrics_cache: &Arc<Mutex<HashMap<String, MetricSeries>>>,
    ) -> HashMap<String, f64> {
        let cache = metrics_cache.lock().await;
        let mut values = HashMap::new();

        match condition {
            AlertCondition::Threshold { metric, .. } => {
                if let Some(series) = cache.get(metric) {
                    if let Some(latest) = series.values.back() {
                        values.insert(metric.clone(), latest.value);
                    }
                }
            }
            AlertCondition::RateChange { metric, .. } => {
                if let Some(series) = cache.get(metric) {
                    if let Some(latest) = series.values.back() {
                        values.insert(metric.clone(), latest.value);
                    }
                }
            }
            AlertCondition::ErrorRate { error_metric, total_metric, .. } => {
                if let Some(series) = cache.get(error_metric) {
                    if let Some(latest) = series.values.back() {
                        values.insert(error_metric.clone(), latest.value);
                    }
                }
                if let Some(series) = cache.get(total_metric) {
                    if let Some(latest) = series.values.back() {
                        values.insert(total_metric.clone(), latest.value);
                    }
                }
            }
            _ => {}
        }

        values
    }

    /// Start notification task
    async fn start_notification_task(&self) {
        let active_alerts = self.active_alerts.clone();
        let notification_channels = self.notification_channels.clone();
        let alert_metrics = self.alert_metrics.clone();

        tokio::task::spawn(async move {
            let mut timer = interval(Duration::from_secs(10));
            loop {
                timer.tick().await;

                if let Err(e) = Self::process_notifications(
                    &active_alerts,
                    &notification_channels,
                    &alert_metrics,
                ).await {
                    error!("Notification processing failed: {}", e);
                }
            }
        });
    }

    /// Process alert notifications
    async fn process_notifications(
        active_alerts: &Arc<RwLock<HashMap<Uuid, Alert>>>,
        notification_channels: &Arc<RwLock<HashMap<String, NotificationChannel>>>,
        alert_metrics: &Arc<Mutex<AlertMetrics>>,
    ) -> NetworkResult<()> {
        let alerts = active_alerts.read().await;
        let channels = notification_channels.read().await;

        for alert in alerts.values() {
            if alert.state == AlertState::Pending && alert.fired_at.is_none() {
                // Send notifications for new alerts
                for channel_id in &alert.annotations.get("notification_channels").unwrap_or(&String::new()).split(',').collect::<Vec<&str>>() {
                    if let Some(channel) = channels.get(*channel_id) {
                        if let Err(e) = Self::send_notification(channel, alert).await {
                            error!("Failed to send notification: {}", e);
                            let mut metrics = alert_metrics.lock().await;
                            metrics.notification_failures += 1;
                        } else {
                            let mut metrics = alert_metrics.lock().await;
                            metrics.notifications_sent += 1;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Send notification through channel
    async fn send_notification(
        channel: &NotificationChannel,
        alert: &Alert,
    ) -> NetworkResult<()> {
        if !channel.enabled {
            return Ok(());
        }

        match &channel.channel_type {
            NotificationChannelType::Log => {
                info!("ALERT: {} - {} ({})", alert.severity, alert.rule_name, alert.message);
            }
            NotificationChannelType::Email => {
                debug!("Sending email notification for alert: {}", alert.id);
                // Email implementation would go here
            }
            NotificationChannelType::Slack => {
                debug!("Sending Slack notification for alert: {}", alert.id);
                // Slack implementation would go here
            }
            NotificationChannelType::Webhook => {
                debug!("Sending webhook notification for alert: {}", alert.id);
                // Webhook implementation would go here
            }
            NotificationChannelType::PagerDuty => {
                debug!("Sending PagerDuty notification for alert: {}", alert.id);
                // PagerDuty implementation would go here
            }
        }

        Ok(())
    }

    /// Start cleanup task
    async fn start_cleanup_task(&self) {
        let alert_history = self.alert_history.clone();
        let retention_period = self.config.retention_period;

        tokio::task::spawn(async move {
            let mut timer = interval(Duration::from_secs(3600)); // Every hour
            loop {
                timer.tick().await;

                let cutoff_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() - retention_period.as_secs();

                let mut history = alert_history.lock().await;
                while let Some(front) = history.front() {
                    if front.started_at < cutoff_time {
                        history.pop_front();
                    } else {
                        break;
                    }
                }
            }
        });
    }

    /// Add default alert rules
    async fn add_default_alert_rules(&self) -> NetworkResult<()> {
        // High memory usage
        let memory_rule = AlertRule {
            id: Uuid::new_v4(),
            name: "High Memory Usage".to_string(),
            description: "Memory usage is above 80%".to_string(),
            condition: AlertCondition::Threshold {
                metric: "memory_usage_percent".to_string(),
                operator: ComparisonOperator::GreaterThan,
                value: 80.0,
                duration: Duration::from_secs(300),
            },
            severity: AlertSeverity::Warning,
            enabled: true,
            labels: [("component".to_string(), "system".to_string())].iter().cloned().collect(),
            annotations: [("description".to_string(), "Memory usage is critically high".to_string())].iter().cloned().collect(),
            notification_targets: vec!["default".to_string()],
            suppression_rules: vec![],
        };

        // High error rate
        let error_rate_rule = AlertRule {
            id: Uuid::new_v4(),
            name: "High Error Rate".to_string(),
            description: "Error rate exceeds 5%".to_string(),
            condition: AlertCondition::ErrorRate {
                error_metric: "errors_total".to_string(),
                total_metric: "requests_total".to_string(),
                threshold_percent: 5.0,
                min_samples: 100,
            },
            severity: AlertSeverity::Critical,
            enabled: true,
            labels: [("component".to_string(), "broker".to_string())].iter().cloned().collect(),
            annotations: [("description".to_string(), "Error rate is above acceptable threshold".to_string())].iter().cloned().collect(),
            notification_targets: vec!["default".to_string()],
            suppression_rules: vec![],
        };

        // Node unreachable
        let node_unreachable_rule = AlertRule {
            id: Uuid::new_v4(),
            name: "Node Unreachable".to_string(),
            description: "Node has not sent heartbeat in 60 seconds".to_string(),
            condition: AlertCondition::Absence {
                metric: "node_heartbeat".to_string(),
                duration: Duration::from_secs(60),
            },
            severity: AlertSeverity::Critical,
            enabled: true,
            labels: [("component".to_string(), "cluster".to_string())].iter().cloned().collect(),
            annotations: [("description".to_string(), "Node appears to be unreachable".to_string())].iter().cloned().collect(),
            notification_targets: vec!["default".to_string()],
            suppression_rules: vec![],
        };

        // Add default notification channel
        let log_channel = NotificationChannel {
            id: "default".to_string(),
            name: "Default Log Channel".to_string(),
            channel_type: NotificationChannelType::Log,
            config: NotificationConfig::Log {
                level: "info".to_string(),
            },
            enabled: true,
        };

        self.add_notification_channel(log_channel).await?;
        self.add_alert_rule(memory_rule).await?;
        self.add_alert_rule(error_rate_rule).await?;
        self.add_alert_rule(node_unreachable_rule).await?;

        info!("Added default alert rules and notification channels");
        Ok(())
    }
}
