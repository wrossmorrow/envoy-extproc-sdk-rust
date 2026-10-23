//! Prometheus metrics for the ext_proc server.

use prometheus::{Counter, Gauge, Histogram, Registry};

/// Metrics for the ext_proc server, mirroring the Go SDK's
/// `extproc.Metrics`.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// Current number of active streams.
    pub active_streams: Gauge,
    /// Total number of streams that errored, counted once per stream.
    pub errored_streams: Counter,
    /// Total number of phase processing errors, which may repeat within one stream.
    pub phase_errors: Counter,
    /// Total number of streams processed.
    pub total_streams: Counter,
    /// Stream duration in seconds (native histogram).
    pub stream_duration_seconds: Histogram,
    /// Total number of request headers processed.
    pub total_request_headers: Counter,
    /// Total number of request body messages processed.
    pub total_request_body: Counter,
    /// Total number of request trailer phases processed.
    pub total_request_trailers: Counter,
    /// Total number of response headers processed.
    pub total_response_headers: Counter,
    /// Total number of response body messages processed.
    pub total_response_body: Counter,
    /// Total number of response trailer phases processed.
    pub total_response_trailers: Counter,
    /// Total number of phases that produced no response.
    pub total_empty_responses: Counter,
    /// Total number of request body bytes received.
    pub request_body_bytes: Counter,
    /// Total number of response body bytes returned.
    pub response_body_bytes: Counter,
    /// Failures sending responses to envoy.
    pub response_send_errors: Counter,
}

impl Metrics {
    /// Create an empty [`Metrics`] with all collectors uninitialized.
    pub fn new() -> Self {
        Self {
            active_streams: prometheus::Gauge::new(
                "extproc_active_streams",
                "Current number of active streams.",
            )
            .unwrap(),
            errored_streams: prometheus::Counter::new(
                "extproc_errored_streams",
                "Total number of streams that errored, counted once per stream.",
            )
            .unwrap(),
            phase_errors: prometheus::Counter::new(
                "extproc_phase_errors_total",
                "Total number of phase processing errors, which may repeat within one stream.",
            )
            .unwrap(),
            total_streams: prometheus::Counter::new(
                "extproc_total_streams",
                "Total number of streams processed.",
            )
            .unwrap(),
            stream_duration_seconds: prometheus::Histogram::with_opts(
                prometheus::HistogramOpts::new(
                    "extproc_stream_duration_seconds",
                    "Stream duration in seconds (native histogram).",
                ),
            )
            .unwrap(),
            total_request_headers: prometheus::Counter::new(
                "extproc_total_request_headers",
                "Total number of request headers processed.",
            )
            .unwrap(),
            total_request_body: prometheus::Counter::new(
                "extproc_request_body_messages_total",
                "Total number of request bodies processed.",
            )
            .unwrap(),
            total_request_trailers: prometheus::Counter::new(
                "extproc_total_request_trailers",
                "Total number of request trailer phases processed.",
            )
            .unwrap(),
            total_response_headers: prometheus::Counter::new(
                "extproc_total_response_headers",
                "Total number of response headers processed.",
            )
            .unwrap(),
            total_response_body: prometheus::Counter::new(
                "extproc_response_body_messages_total",
                "Total number of response bodies processed.",
            )
            .unwrap(),
            total_response_trailers: prometheus::Counter::new(
                "extproc_total_response_trailers",
                "Total number of response trailer phases processed.",
            )
            .unwrap(),
            total_empty_responses: prometheus::Counter::new(
                "extproc_empty_responses",
                "Total number of phases that produced no response.",
            )
            .unwrap(),
            request_body_bytes: prometheus::Counter::new(
                "extproc_request_body_bytes_total",
                "Total number of body bytes received.",
            )
            .unwrap(),
            response_body_bytes: prometheus::Counter::new(
                "extproc_response_body_bytes_total",
                "Total number of body bytes returned.",
            )
            .unwrap(),
            response_send_errors: prometheus::Counter::new(
                "extproc_response_send_errors",
                "Failures sending responses to envoy.",
            )
            .unwrap(),
        }
    }

    /// Register all collectors to a registry.
    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.active_streams.clone()))?;
        registry.register(Box::new(self.errored_streams.clone()))?;
        registry.register(Box::new(self.phase_errors.clone()))?;
        registry.register(Box::new(self.total_streams.clone()))?;
        registry.register(Box::new(self.stream_duration_seconds.clone()))?;
        registry.register(Box::new(self.total_request_headers.clone()))?;
        registry.register(Box::new(self.total_request_body.clone()))?;
        registry.register(Box::new(self.total_request_trailers.clone()))?;
        registry.register(Box::new(self.total_response_headers.clone()))?;
        registry.register(Box::new(self.total_response_body.clone()))?;
        registry.register(Box::new(self.total_response_trailers.clone()))?;
        registry.register(Box::new(self.total_empty_responses.clone()))?;
        registry.register(Box::new(self.request_body_bytes.clone()))?;
        registry.register(Box::new(self.response_body_bytes.clone()))?;
        registry.register(Box::new(self.response_send_errors.clone()))?;
        Ok(())
    }

    /// The collectors exposed by this [`Metrics`].
    pub fn collectors(&self) -> Vec<&dyn prometheus::core::Collector> {
        vec![
            &self.active_streams,
            &self.errored_streams,
            &self.phase_errors,
            &self.total_streams,
            &self.stream_duration_seconds,
            &self.total_request_headers,
            &self.total_request_body,
            &self.total_request_trailers,
            &self.total_response_headers,
            &self.total_response_body,
            &self.total_response_trailers,
            &self.total_empty_responses,
            &self.request_body_bytes,
            &self.response_body_bytes,
            &self.response_send_errors,
        ]
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::proto::MetricType;
    use prometheus::Registry;

    #[test]
    fn test_metrics_collectors_cover_every_field() {
        let metrics = Metrics::new();
        assert_eq!(metrics.collectors().len(), 15);
        assert_eq!(
            metrics.collectors().len(),
            Metrics::new().collectors().len()
        );
    }

    #[test]
    fn test_metrics_register_and_gather() {
        let metrics = Metrics::new();
        let registry = Registry::new();
        metrics.register(&registry).unwrap();

        metrics.response_body_bytes.inc_by(3.0);
        metrics.total_request_headers.inc();

        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_response_body_bytes_total")
            .expect("expected response body bytes family");
        assert_eq!(family.type_(), MetricType::COUNTER);
        assert_eq!(family.metric.len(), 1);
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 3.0);

        let family = families
            .iter()
            .find(|f| f.name() == "extproc_total_request_headers")
            .expect("expected total request headers family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 1.0);
    }
}
