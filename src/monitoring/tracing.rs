//! Distributed tracing system for request tracking across nodes
//!
//! Provides comprehensive tracing capabilities to track requests
//! and operations across the distributed messaging system.

use crate::network::error::NetworkResult;
use crate::network::transport::NetworkTransport;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use uuid::Uuid;
use log::{debug, error, info, warn};

/// Distributed trace context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    pub trace_id: Uuid,
    pub span_id: Uuid,
    pub parent_span_id: Option<Uuid>,
    pub operation_name: String,
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub duration_micros: Option<u64>,
    pub node_id: String,
    pub component: String,
    pub tags: HashMap<String, String>,
    pub logs: Vec<SpanLog>,
    pub status: SpanStatus,
}

/// Span log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLog {
    pub timestamp: u64,
    pub level: String,
    pub message: String,
    pub fields: HashMap<String, String>,
}

/// Span status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanStatus {
    Ok,
    Error,
    Timeout,
    Cancelled,
}

/// Trace sampling configuration
#[derive(Debug, Clone)]
pub struct TracingSamplingConfig {
    pub enabled: bool,
    pub sample_rate: f64, // 0.0 to 1.0
    pub max_traces_per_second: usize,
    pub force_sample_operations: Vec<String>,
}

impl Default for TracingSamplingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sample_rate: 0.1, // 10% sampling
            max_traces_per_second: 1000,
            force_sample_operations: vec![
                "message_send".to_string(),
                "message_receive".to_string(),
                "replication".to_string(),
                "consensus".to_string(),
            ],
        }
    }
}

/// Distributed tracing configuration
#[derive(Debug, Clone)]
pub struct TracingConfig {
    pub enabled: bool,
    pub service_name: String,
    pub sampling: TracingSamplingConfig,
    pub retention_period: Duration,
    pub export_interval: Duration,
    pub jaeger_endpoint: Option<String>,
    pub otlp_endpoint: Option<String>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            service_name: "pilgrimage".to_string(),
            sampling: TracingSamplingConfig::default(),
            retention_period: Duration::from_secs(24 * 3600), // 24 hours
            export_interval: Duration::from_secs(10),
            jaeger_endpoint: None,
            otlp_endpoint: None,
        }
    }
}

/// Distributed tracer
pub struct DistributedTracer {
    config: TracingConfig,
    node_id: String,
    transport: Arc<NetworkTransport>,
    active_spans: Arc<RwLock<HashMap<Uuid, TraceContext>>>,
    completed_spans: Arc<Mutex<VecDeque<TraceContext>>>,
    trace_metrics: Arc<Mutex<TracingMetrics>>,
    sampling_state: Arc<Mutex<SamplingState>>,
}

/// Tracing metrics
#[derive(Debug, Default)]
pub struct TracingMetrics {
    total_spans_created: u64,
    total_spans_finished: u64,
    spans_sampled: u64,
    spans_dropped: u64,
    traces_exported: u64,
    export_errors: u64,
    avg_span_duration_micros: u64,
}

/// Sampling state tracker
#[derive(Debug)]
struct SamplingState {
    traces_this_second: usize,
    last_second: u64,
    random_state: u64,
}

impl Default for SamplingState {
    fn default() -> Self {
        Self {
            traces_this_second: 0,
            last_second: 0,
            random_state: 12345, // Simple PRNG state
        }
    }
}

/// Trace span builder
pub struct SpanBuilder {
    trace_id: Uuid,
    parent_span_id: Option<Uuid>,
    operation_name: String,
    component: String,
    tags: HashMap<String, String>,
}

impl DistributedTracer {
    /// Create a new distributed tracer
    pub fn new(
        config: TracingConfig,
        node_id: String,
        transport: Arc<NetworkTransport>,
    ) -> Self {
        Self {
            config,
            node_id,
            transport,
            active_spans: Arc::new(RwLock::new(HashMap::new())),
            completed_spans: Arc::new(Mutex::new(VecDeque::new())),
            trace_metrics: Arc::new(Mutex::new(TracingMetrics::default())),
            sampling_state: Arc::new(Mutex::new(SamplingState::default())),
        }
    }

    /// Start the tracing service
    pub async fn start(&self) -> NetworkResult<()> {
        if !self.config.enabled {
            info!("Distributed tracing is disabled");
            return Ok(());
        }

        info!("Starting distributed tracing service for node {}", self.node_id);

        // Start span export task
        self.start_export_task().await;

        // Start cleanup task
        self.start_cleanup_task().await;

        Ok(())
    }

    /// Start a new trace span
    pub async fn start_span(&self, builder: SpanBuilder) -> NetworkResult<TraceContext> {
        let span_id = Uuid::new_v4();

        // Check sampling decision
        if !self.should_sample(&builder.operation_name).await {
            let mut metrics = self.trace_metrics.lock().unwrap();
            metrics.spans_dropped += 1;
            // Return a non-sampled span context
            return Ok(TraceContext {
                trace_id: builder.trace_id,
                span_id,
                parent_span_id: builder.parent_span_id,
                operation_name: builder.operation_name,
                start_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64,
                end_time: None,
                duration_micros: None,
                node_id: self.node_id.clone(),
                component: builder.component,
                tags: builder.tags,
                logs: Vec::new(),
                status: SpanStatus::Ok,
            });
        }

        let span = TraceContext {
            trace_id: builder.trace_id,
            span_id,
            parent_span_id: builder.parent_span_id,
            operation_name: builder.operation_name,
            start_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64,
            end_time: None,
            duration_micros: None,
            node_id: self.node_id.clone(),
            component: builder.component,
            tags: builder.tags,
            logs: Vec::new(),
            status: SpanStatus::Ok,
        };

        // Store active span
        {
            let mut active_spans = self.active_spans.write().await;
            active_spans.insert(span_id, span.clone());
        }

        // Update metrics
        {
            let mut metrics = self.trace_metrics.lock().unwrap();
            metrics.total_spans_created += 1;
            metrics.spans_sampled += 1;
        }

        debug!("Started span: {} for operation: {}", span_id, span.operation_name);
        Ok(span)
    }

    /// Finish a trace span
    pub async fn finish_span(&self, span_id: Uuid, status: SpanStatus) -> NetworkResult<()> {
        let span = {
            let mut active_spans = self.active_spans.write().await;
            active_spans.remove(&span_id)
        };

        if let Some(mut span) = span {
            let end_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
            span.end_time = Some(end_time);
            span.duration_micros = Some(end_time - span.start_time);
            span.status = status;

            // Store completed span
            {
                let mut completed_spans = self.completed_spans.lock().unwrap();
                completed_spans.push_back(span.clone());

                // Limit completed spans in memory
                if completed_spans.len() > 10000 {
                    completed_spans.pop_front();
                }
            }

            // Update metrics
            {
                let mut metrics = self.trace_metrics.lock().unwrap();
                metrics.total_spans_finished += 1;
                if let Some(duration) = span.duration_micros {
                    // Simple running average
                    metrics.avg_span_duration_micros =
                        (metrics.avg_span_duration_micros + duration) / 2;
                }
            }

            debug!("Finished span: {} with status: {:?}", span_id, span.status);
        }

        Ok(())
    }

    /// Add a log to an active span
    pub async fn add_span_log(
        &self,
        span_id: Uuid,
        level: &str,
        message: &str,
        fields: Option<HashMap<String, String>>,
    ) -> NetworkResult<()> {
        let mut active_spans = self.active_spans.write().await;
        if let Some(span) = active_spans.get_mut(&span_id) {
            span.logs.push(SpanLog {
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64,
                level: level.to_string(),
                message: message.to_string(),
                fields: fields.unwrap_or_default(),
            });
        }
        Ok(())
    }

    /// Add tags to an active span
    pub async fn add_span_tags(
        &self,
        span_id: Uuid,
        tags: HashMap<String, String>,
    ) -> NetworkResult<()> {
        let mut active_spans = self.active_spans.write().await;
        if let Some(span) = active_spans.get_mut(&span_id) {
            span.tags.extend(tags);
        }
        Ok(())
    }

    /// Get trace by ID
    pub async fn get_trace(&self, trace_id: Uuid) -> NetworkResult<Vec<TraceContext>> {
        let completed_spans = self.completed_spans.lock().unwrap();
        let trace_spans: Vec<TraceContext> = completed_spans
            .iter()
            .filter(|span| span.trace_id == trace_id)
            .cloned()
            .collect();

        Ok(trace_spans)
    }

    /// Get tracing metrics
    pub fn get_metrics(&self) -> TracingMetrics {
        let metrics = self.trace_metrics.lock().unwrap();
        TracingMetrics {
            total_spans_created: metrics.total_spans_created,
            total_spans_finished: metrics.total_spans_finished,
            spans_sampled: metrics.spans_sampled,
            spans_dropped: metrics.spans_dropped,
            traces_exported: metrics.traces_exported,
            export_errors: metrics.export_errors,
            avg_span_duration_micros: metrics.avg_span_duration_micros,
        }
    }

    /// Create a span builder
    pub fn span_builder(&self, operation_name: &str, component: &str) -> SpanBuilder {
        SpanBuilder::new(operation_name, component)
    }

    /// Extract trace context from headers/metadata
    pub fn extract_trace_context(&self, headers: &HashMap<String, String>) -> Option<(Uuid, Uuid)> {
        if let (Some(trace_id_str), Some(span_id_str)) =
            (headers.get("trace-id"), headers.get("span-id")) {
            if let (Ok(trace_id), Ok(span_id)) =
                (Uuid::parse_str(trace_id_str), Uuid::parse_str(span_id_str)) {
                return Some((trace_id, span_id));
            }
        }
        None
    }

    /// Inject trace context into headers/metadata
    pub fn inject_trace_context(
        &self,
        headers: &mut HashMap<String, String>,
        trace_id: Uuid,
        span_id: Uuid,
    ) {
        headers.insert("trace-id".to_string(), trace_id.to_string());
        headers.insert("span-id".to_string(), span_id.to_string());
    }

    /// Check if operation should be sampled
    async fn should_sample(&self, operation_name: &str) -> bool {
        if !self.config.sampling.enabled {
            return true;
        }

        // Force sample certain operations
        if self.config.sampling.force_sample_operations.contains(&operation_name.to_string()) {
            return true;
        }

        let mut sampling_state = self.sampling_state.lock().unwrap();
        let current_second = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Reset counter for new second
        if current_second != sampling_state.last_second {
            sampling_state.traces_this_second = 0;
            sampling_state.last_second = current_second;
        }

        // Check rate limit
        if sampling_state.traces_this_second >= self.config.sampling.max_traces_per_second {
            return false;
        }

        // Simple PRNG for sampling decision
        sampling_state.random_state = sampling_state.random_state.wrapping_mul(1103515245).wrapping_add(12345);
        let random_value = (sampling_state.random_state >> 16) as f64 / 65535.0;

        if random_value < self.config.sampling.sample_rate {
            sampling_state.traces_this_second += 1;
            true
        } else {
            false
        }
    }

    /// Start span export task
    async fn start_export_task(&self) {
        let completed_spans = self.completed_spans.clone();
        let metrics = self.trace_metrics.clone();
        let interval_duration = self.config.export_interval;
        let jaeger_endpoint = self.config.jaeger_endpoint.clone();
        let otlp_endpoint = self.config.otlp_endpoint.clone();

        tokio::task::spawn(async move {
            let mut timer = interval(interval_duration);
            loop {
                timer.tick().await;

                let spans_to_export = {
                    let mut spans = completed_spans.lock().unwrap();
                    let mut export_spans = Vec::new();
                    while let Some(span) = spans.pop_front() {
                        export_spans.push(span);
                        if export_spans.len() >= 100 { // Batch size
                            break;
                        }
                    }
                    export_spans
                };

                if !spans_to_export.is_empty() {
                    let export_result = Self::export_spans(
                        &spans_to_export,
                        &jaeger_endpoint,
                        &otlp_endpoint,
                    ).await;

                    let mut metrics = metrics.lock().unwrap();
                    match export_result {
                        Ok(_) => {
                            metrics.traces_exported += spans_to_export.len() as u64;
                            debug!("Exported {} spans", spans_to_export.len());
                        }
                        Err(e) => {
                            metrics.export_errors += 1;
                            error!("Failed to export spans: {}", e);
                        }
                    }
                }
            }
        });
    }

    /// Export spans to external systems
    async fn export_spans(
        spans: &[TraceContext],
        _jaeger_endpoint: &Option<String>,
        _otlp_endpoint: &Option<String>,
    ) -> NetworkResult<()> {
        // Implementation would export to Jaeger/OTLP
        debug!("Exporting {} spans to external tracing systems", spans.len());

        // For now, just log the spans
        for span in spans {
            debug!("Span: {} - {} ({}μs)",
                span.operation_name,
                span.span_id,
                span.duration_micros.unwrap_or(0)
            );
        }

        Ok(())
    }

    /// Start cleanup task
    async fn start_cleanup_task(&self) {
        let active_spans = self.active_spans.clone();
        let retention_period = self.config.retention_period;

        tokio::task::spawn(async move {
            let mut timer = interval(Duration::from_secs(300)); // Every 5 minutes
            loop {
                timer.tick().await;

                let cutoff_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64 - retention_period.as_micros() as u64;

                let mut active_spans = active_spans.write().await;
                let mut expired_spans = Vec::new();

                for (span_id, span) in active_spans.iter() {
                    if span.start_time < cutoff_time {
                        expired_spans.push(*span_id);
                    }
                }

                for span_id in expired_spans {
                    active_spans.remove(&span_id);
                    warn!("Removed expired active span: {}", span_id);
                }
            }
        });
    }
}

impl SpanBuilder {
    /// Create a new span builder
    pub fn new(operation_name: &str, component: &str) -> Self {
        Self {
            trace_id: Uuid::new_v4(),
            parent_span_id: None,
            operation_name: operation_name.to_string(),
            component: component.to_string(),
            tags: HashMap::new(),
        }
    }

    /// Set trace ID (for continuing existing trace)
    pub fn with_trace_id(mut self, trace_id: Uuid) -> Self {
        self.trace_id = trace_id;
        self
    }

    /// Set parent span ID
    pub fn with_parent(mut self, parent_span_id: Uuid) -> Self {
        self.parent_span_id = Some(parent_span_id);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags.extend(tags);
        self
    }
}

/// Tracing utilities and macros
pub mod utils {
    use super::*;

    /// Convenience function to create trace context headers
    pub fn create_trace_headers(trace_id: Uuid, span_id: Uuid) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("trace-id".to_string(), trace_id.to_string());
        headers.insert("span-id".to_string(), span_id.to_string());
        headers
    }

    /// Extract operation name from function name
    pub fn operation_name_from_fn(function_name: &str) -> String {
        function_name.split("::").last().unwrap_or(function_name).to_string()
    }
}
