//! Conformance processor that dispatches phase callbacks by case ID.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use envoy_extproc_sdk::{
    AllHeaders, Error, GenericExtProcServer, Metrics, ProcessingOptions, RequestContext,
    RequestPhase, RequestProcessor,
};
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;
use prometheus::Registry;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// Header used by conformance scenarios to route a request to its registered
/// case handler.
pub const CASE_HEADER: &str = "x-conformance-case";

/// The typed custom-value key used to carry the active conformance case ID
/// between phases of a single request/response exchange.
/// Optional per-phase callbacks for a single conformance test case. Phase
/// methods that are not overridden default to continuing the request, mirroring
/// the Go SDK's nil-handler defaults.
pub trait CaseHandler: Send + Sync {
    fn request_headers(&self, ctx: &mut RequestContext, headers: &AllHeaders) -> Result<(), Error> {
        let _ = headers;
        ctx.continue_request()
    }

    fn request_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
        let _ = body;
        ctx.continue_request()
    }

    fn request_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        let _ = trailers;
        ctx.continue_request()
    }

    fn response_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error> {
        let _ = headers;
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
        let _ = body;
        ctx.continue_request()
    }

    fn response_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        let _ = trailers;
        ctx.continue_request()
    }

    fn on_error(&self, ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        let _ = (phase, err);
        let _ = ctx;
    }
}

/// [`RequestProcessor`] that dispatches phase callbacks to a registered
/// conformance case handler via the [`CASE_HEADER`] header.
#[derive(Clone)]
pub struct ConformanceProcessor {
    name: String,
    options: Arc<ProcessingOptions>,
    cases: Arc<RwLock<HashMap<String, Arc<dyn CaseHandler>>>>,
}

impl ConformanceProcessor {
    /// Create a conformance processor named `conformance` with the supplied
    /// processing options.
    pub fn new(opts: &ProcessingOptions) -> Self {
        Self::with_name("conformance", opts)
    }

    /// Create a conformance processor with an explicit name and processing
    /// options.
    pub fn with_name(name: &str, opts: &ProcessingOptions) -> Self {
        Self {
            name: name.to_string(),
            options: Arc::new(opts.clone()),
            cases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a case handler under `id`.
    pub fn register(&self, id: &str, handler: Arc<dyn CaseHandler>) {
        self.cases.write().unwrap().insert(id.to_string(), handler);
    }

    /// Unregister the case handler registered under `id`.
    pub fn unregister(&self, id: &str) {
        self.cases.write().unwrap().remove(id);
    }

    /// Look up the handler registered under `id`.
    pub(crate) fn lookup_by_id(&self, id: &str) -> Option<Arc<dyn CaseHandler>> {
        if id.is_empty() {
            return None;
        }
        self.cases.read().unwrap().get(id).cloned()
    }

    /// Look up the handler for the case ID carried on the context.
    fn lookup(&self, ctx: &RequestContext) -> Option<Arc<dyn CaseHandler>> {
        let id = ctx.get_value::<String>().ok()?.clone();
        if id.is_empty() {
            return None;
        }
        self.lookup_by_id(&id)
    }
}

#[async_trait]
impl RequestProcessor for ConformanceProcessor {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.options
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error> {
        if let Some(id) = headers.get(CASE_HEADER) {
            if !id.is_empty() {
                let _ = ctx.set_value(id);
            }
        }
        if let Some(h) = self.lookup(ctx) {
            return h.request_headers(ctx, headers);
        }
        ctx.continue_request()
    }

    async fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        if let Some(h) = self.lookup(ctx) {
            return h.request_trailers(ctx, trailers);
        }
        ctx.continue_request()
    }

    async fn process_request_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error> {
        if let Some(h) = self.lookup(ctx) {
            return h.request_body(ctx, body);
        }
        ctx.continue_request()
    }

    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error> {
        // Under request_header_mode: SKIP, ProcessRequestHeaders never ran;
        // resolve the case ID echoed back by the upstream in the response
        // headers instead.
        if self.lookup(ctx).is_none() {
            if let Some(id) = headers.get(CASE_HEADER) {
                if !id.is_empty() {
                    let _ = ctx.set_value(id);
                }
            }
        }
        if let Some(h) = self.lookup(ctx) {
            return h.response_headers(ctx, headers);
        }
        ctx.continue_request()
    }

    async fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error> {
        if let Some(h) = self.lookup(ctx) {
            return h.response_body(ctx, body);
        }
        ctx.continue_request()
    }

    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        if let Some(h) = self.lookup(ctx) {
            return h.response_trailers(ctx, trailers);
        }
        ctx.continue_request()
    }

    async fn error_handler(&self, ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        if let Some(h) = self.lookup(ctx) {
            h.on_error(ctx, phase, err);
        }
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}

/// The in-process ext_proc server harness handle: wraps a
/// [`GenericExtProcServer`](envoy_extproc_sdk::GenericExtProcServer) plus its
/// [`Metrics`](envoy_extproc_sdk::Metrics) registered on a private registry for
/// conformance scenario assertions.
pub struct ConformanceExtProcServer {
    server: GenericExtProcServer,
    metrics: Metrics,
    registry: Registry,
}

impl ConformanceExtProcServer {
    /// Create a conformance ext_proc server harness handle.
    pub fn new(server: GenericExtProcServer, metrics: Metrics) -> Self {
        let registry = Registry::new();
        let _ = metrics.register(&registry);
        Self {
            server,
            metrics,
            registry,
        }
    }

    /// The wrapped generic ext_proc server.
    pub fn server(&self) -> &GenericExtProcServer {
        &self.server
    }

    /// The metrics collected by the server.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Read the current value of a registered counter by name.
    pub fn counter_value(&self, metric_name: &str) -> Result<f64, Error> {
        let families = self.registry.gather();
        for family in families {
            if family.name() != metric_name {
                continue;
            }
            for metric in family.get_metric() {
                if let Some(gauge) = metric.get_gauge().as_ref() {
                    let value = gauge.value();
                    if value.is_finite() {
                        return Ok(value);
                    }
                }
                if let Some(counter) = metric.get_counter().as_ref() {
                    let value = counter.value();
                    if value.is_finite() {
                        return Ok(value);
                    }
                }
            }
        }
        Err(format!("metric {metric_name} not found").into())
    }

    /// Read the current value of a registered gauge by name.
    pub fn gauge_value(&self, metric_name: &str) -> Result<f64, Error> {
        let families = self.registry.gather();
        for family in families {
            if family.name() != metric_name {
                continue;
            }
            for metric in family.get_metric() {
                if let Some(gauge) = metric.get_gauge().as_ref() {
                    let value = gauge.value();
                    if value.is_finite() {
                        return Ok(value);
                    }
                }
            }
        }
        Err(format!("metric {metric_name} not found").into())
    }

    /// Shut the wrapped generic ext_proc server down.
    pub fn close(&self, grace_period_seconds: i32) -> Result<(), Error> {
        self.server.close(grace_period_seconds)
    }
}

/// Start an in-process gRPC `ExternalProcessor` server for `processor` on an
/// ephemeral `0.0.0.0:0` port and return the server handle plus bound port.
pub async fn start_conformance_extproc_server(
    processor: ConformanceProcessor,
) -> Result<(ConformanceExtProcServer, u16), Error> {
    let metrics = Metrics::new();
    let opts = processor.options().clone();
    let server = GenericExtProcServer::new_with_processor(
        Some(Arc::new(processor)),
        Some(opts),
        metrics.clone(),
    )?;
    let handle = ConformanceExtProcServer::new(server.clone(), metrics);

    let listener = TcpListener::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("bind conformance ext_proc server: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("conformance ext_proc server local addr: {e}"))?
        .port();

    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .max_concurrent_streams(1024)
            .add_service(ExternalProcessorServer::new(server))
            .serve_with_incoming(incoming)
            .await
        {
            tracing::warn!("conformance ext_proc server stopped: {e}");
        }
    });

    Ok((handle, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingHandler {
        phases: Arc<Mutex<Vec<String>>>,
    }

    impl CaseHandler for RecordingHandler {
        fn request_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push("request_headers".to_string());
            ctx.continue_request()
        }

        fn response_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push("response_headers".to_string());
            ctx.continue_request()
        }
    }

    fn all_headers_with_case(case_id: &str) -> AllHeaders {
        let mut headers = AllHeaders::default();
        headers
            .headers
            .insert("x-conformance-case".to_string(), vec![case_id.to_string()]);
        headers
    }

    #[tokio::test]
    async fn new_defaults_to_conformance_name_and_default_options() {
        let processor = ConformanceProcessor::new(&ProcessingOptions::new());
        assert_eq!(processor.name(), "conformance");
        assert!(!processor.options().abort_on_processor_failure);
    }

    #[tokio::test]
    async fn dispatches_request_headers_phase_to_registered_case() {
        let processor = ConformanceProcessor::new(&ProcessingOptions::new());
        let phases = Arc::new(Mutex::new(Vec::<String>::new()));
        let handler: Arc<dyn CaseHandler> = Arc::new(RecordingHandler {
            phases: phases.clone(),
        });
        processor.register("case-1", handler);

        let mut ctx = RequestContext::new();
        ctx.set_phase(RequestPhase::RequestHeaders);
        let _ = ctx.set_value("case-1".to_string());
        let headers = all_headers_with_case("case-1");

        processor
            .process_request_headers(&mut ctx, &headers)
            .await
            .expect("request headers phase should continue");
        assert_eq!(*phases.lock().unwrap(), vec!["request_headers".to_string()]);
    }

    #[tokio::test]
    async fn response_headers_phase_resolves_case_from_echoed_header() {
        let processor = ConformanceProcessor::new(&ProcessingOptions::new());
        let phases = Arc::new(Mutex::new(Vec::<String>::new()));
        let handler: Arc<dyn CaseHandler> = Arc::new(RecordingHandler {
            phases: phases.clone(),
        });
        processor.register("case-2", handler);

        // No context value set (e.g. the request headers phase was skipped via
        // request_header_mode: SKIP), so the response phase resolves the case
        // from the echoed `x-conformance-case` header.
        let mut ctx = RequestContext::new();
        ctx.set_phase(RequestPhase::ResponseHeaders);
        let headers = all_headers_with_case("case-2");

        processor
            .process_response_headers(&mut ctx, &headers)
            .await
            .expect("response headers phase should continue");
        assert_eq!(
            *phases.lock().unwrap(),
            vec!["response_headers".to_string()]
        );
    }
}
