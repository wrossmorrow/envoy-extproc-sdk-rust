//! The gRPC server implementation: [`RequestProcessor`] trait and
//! [`GenericExtProcServer`].

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessor;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;
use envoy_types::pb::envoy::service::ext_proc::v3::processing_request::Request as ProcessingRequestOneof;
use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest;
use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingResponse;

use crate::context::{HeaderValue, RequestContext};
use crate::headers::{gen_headers, AllHeaders};
use crate::metrics::Metrics;
use crate::options::ProcessingOptions;
use crate::Error;
use crate::REQUEST_PHASE_REQUEST_BODY;
use crate::REQUEST_PHASE_REQUEST_HEADERS;
use crate::REQUEST_PHASE_REQUEST_TRAILERS;
use crate::REQUEST_PHASE_RESPONSE_BODY;
use crate::REQUEST_PHASE_RESPONSE_HEADERS;
use crate::REQUEST_PHASE_RESPONSE_TRAILERS;
use crate::REQUEST_PHASE_UNDETERMINED;
use prometheus::Encoder;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::sleep;
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Errors that can occur while processing a phase. Implemented by processors
/// to return from phase methods; errors flow to the processor's
/// [`error_handler`](RequestProcessor::error_handler).
pub trait RequestProcessor: Send + Sync + 'static {
    /// The name of the processor, reported to `envoy` via the
    /// `x-extproc-names` response header.
    fn get_name(&self) -> String;

    /// The processor's [`ProcessingOptions`](crate::options::ProcessingOptions).
    fn get_options(&self) -> &ProcessingOptions;

    /// Process the request headers phase.
    fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the request trailers phase.
    fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the request body phase.
    fn process_request_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error>;

    /// Process the response headers phase.
    fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the response body phase.
    fn process_response_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error>;

    /// Process the response trailers phase.
    fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Called when the SDK hits an error during request processing. An
    /// implemented handler can implement any logic it likes.
    fn error_handler(&self, ctx: &mut RequestContext, phase: u32, err: Error);

    /// Called on shutdown, after in-flight streams have drained.
    fn close(&self, grace_period_seconds: i32) -> Result<(), Error>;
}

/// The generic ext_proc server: holds a [`RequestProcessor`] and handles the
/// bidirectional gRPC stream, parsing the processing phase and calling the
/// right phase method.
#[derive(Clone)]
pub struct GenericExtProcServer {
    /// The name of the processor, as reported to `envoy`.
    pub name: String,
    /// The processor handling phases.
    pub processor: Arc<dyn RequestProcessor>,
    /// The options controlling SDK behavior for the current phase, if set.
    pub options: Option<ProcessingOptions>,
    /// Metrics collected during processing.
    pub metrics: Metrics,
}

impl GenericExtProcServer {
    pub fn new(
        name: impl Into<String>,
        processor: Arc<dyn RequestProcessor>,
        options: Option<ProcessingOptions>,
        metrics: Metrics,
    ) -> Self {
        Self {
            name: name.into(),
            processor,
            options,
            metrics,
        }
    }

    /// The name of the processor.
    pub fn get_name(&self) -> String {
        self.processor.get_name()
    }

    /// Called on shutdown, after in-flight streams have drained.
    pub fn close(&self, grace_period_seconds: i32) -> Result<(), Error> {
        self.processor.close(grace_period_seconds)
    }

    /// Handle one phase of a request/response exchange, building the
    /// [`ProcessingResponse`] to send back to `envoy`.
    pub fn process_phase(
        &self,
        proc_req: &ProcessingRequest,
        rc: &mut RequestContext,
    ) -> Result<(Option<ProcessingResponse>, u32), Error> {
        let phase_start = Instant::now();
        let mut phase = REQUEST_PHASE_UNDETERMINED;
        let outcome = self.dispatch_phase(proc_req, rc, &mut phase);
        rc.duration += phase_start.elapsed();
        outcome
    }

    fn dispatch_phase(
        &self,
        proc_req: &ProcessingRequest,
        rc: &mut RequestContext,
        phase: &mut u32,
    ) -> Result<(Option<ProcessingResponse>, u32), Error> {
        let metrics = &self.metrics;
        let options = &self.options;

        let outcome = (|| -> Result<(Option<ProcessingResponse>, u32), Error> {
            match &proc_req.request {
                Some(ProcessingRequestOneof::RequestHeaders(h)) => {
                    *phase = REQUEST_PHASE_REQUEST_HEADERS;
                    metrics.total_request_headers.inc();
                    let init = init_req_ctx(rc, h);
                    if let Err(e) = init {
                        self.processor.error_handler(rc, *phase, e);
                        return Err("request context initialization failed".into());
                    }
                    rc.end_of_stream = h.end_of_stream;
                    rc.set_phase(*phase);
                    let headers = rc.all_headers.clone();
                    self.processor.process_request_headers(rc, &headers)?;
                }
                Some(ProcessingRequestOneof::RequestBody(b)) => {
                    *phase = REQUEST_PHASE_REQUEST_BODY;
                    rc.set_phase(*phase);
                    metrics.total_request_body.inc();
                    metrics.request_body_bytes.inc_by(b.body.len() as f64);
                    rc.end_of_stream = b.end_of_stream;
                    self.processor.process_request_body(rc, &b.body)?;
                }
                Some(ProcessingRequestOneof::RequestTrailers(t)) => {
                    *phase = REQUEST_PHASE_REQUEST_TRAILERS;
                    rc.set_phase(*phase);
                    metrics.total_request_trailers.inc();
                    let trailers = match gen_headers(
                        t.trailers
                            .as_ref()
                            .ok_or_else(|| "missing trailers".to_string())?,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            self.processor.error_handler(rc, *phase, e);
                            return Err("failed to generate request trailers".into());
                        }
                    };
                    self.processor.process_request_trailers(rc, &trailers)?;
                }
                Some(ProcessingRequestOneof::ResponseHeaders(h)) => {
                    *phase = REQUEST_PHASE_RESPONSE_HEADERS;
                    rc.set_phase(*phase);
                    metrics.total_response_headers.inc();
                    let headers = match gen_headers(
                        h.headers
                            .as_ref()
                            .ok_or_else(|| "missing headers".to_string())?,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            self.processor.error_handler(rc, *phase, e);
                            return Err("failed to generate response headers".into());
                        }
                    };
                    rc.end_of_stream = h.end_of_stream;
                    self.processor.process_response_headers(rc, &headers)?;
                    if options.as_ref().is_some_and(|o| o.update_ext_proc_header) {
                        rc.append_header(
                            "x-extproc-names",
                            &HeaderValue {
                                value: String::new(),
                                raw_value: Some(self.name.as_bytes().to_vec()),
                            },
                        )?;
                    }
                    if rc.end_of_stream
                        && options.as_ref().is_some_and(|o| o.update_duration_header)
                    {
                        rc.append_header(
                            "x-extproc-duration-ns",
                            &HeaderValue {
                                value: String::new(),
                                raw_value: Some(rc.duration.as_nanos().to_string().into_bytes()),
                            },
                        )?;
                    }
                }
                Some(ProcessingRequestOneof::ResponseBody(b)) => {
                    *phase = REQUEST_PHASE_RESPONSE_BODY;
                    rc.set_phase(*phase);
                    metrics.total_response_body.inc();
                    metrics.response_body_bytes.inc_by(b.body.len() as f64);
                    rc.end_of_stream = b.end_of_stream;
                    self.processor.process_response_body(rc, &b.body)?;
                    if rc.end_of_stream
                        && options.as_ref().is_some_and(|o| o.update_duration_header)
                    {
                        rc.append_header(
                            "x-extproc-duration-ns",
                            &HeaderValue {
                                value: String::new(),
                                raw_value: Some(rc.duration.as_nanos().to_string().into_bytes()),
                            },
                        )?;
                    }
                }
                Some(ProcessingRequestOneof::ResponseTrailers(t)) => {
                    *phase = REQUEST_PHASE_RESPONSE_TRAILERS;
                    rc.set_phase(*phase);
                    metrics.total_response_trailers.inc();
                    let trailers = match gen_headers(
                        t.trailers
                            .as_ref()
                            .ok_or_else(|| "missing trailers".to_string())?,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            self.processor.error_handler(rc, *phase, e);
                            return Err("failed to generate response trailers".into());
                        }
                    };
                    self.processor.process_response_trailers(rc, &trailers)?;
                }
                None => {
                    rc.set_phase(*phase);
                    return Err("unknown request type".into());
                }
            }

            let response = rc.get_response(*phase)?;
            Ok((response, *phase))
        })();

        if outcome.is_err() {
            metrics.phase_errors.inc_by(1.0);
        }

        outcome
    }

    /// Set up and serve the ext_proc gRPC server, metrics endpoint, and
    /// health service.
    pub async fn serve(&self, server_options: crate::options::ServerOptions) -> Result<(), Error> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // metrics
        let registry = prometheus::Registry::new();
        self.metrics.register(&registry)?;
        let metrics_state = MetricsState { registry };
        let metrics_router = axum::Router::new()
            .route("/metrics", axum::routing::get(metrics_handler))
            .with_state(metrics_state);
        let metrics_listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from((
            [0, 0, 0, 0],
            server_options.metrics_http_port,
        )))
        .await
        .map_err(|e| format!("failed to bind metrics HTTP server: {}", e))?;
        let metrics_server =
            axum::serve(metrics_listener, metrics_router).with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            });
        let metrics_task = tokio::spawn(async move {
            if let Err(e) = metrics_server.await {
                tracing::error!("metrics HTTP server task failed: {:?}", e);
            }
        });

        // grpc server
        let health = crate::health::HealthServer::new_ready();
        let health_for_shutdown = health.clone();
        let grpc_server = Server::builder()
            .max_concurrent_streams(server_options.max_concurrent_streams)
            .add_service(ExternalProcessorServer::new(self.clone()))
            .add_service(health.health_service());
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], server_options.ext_proc_port));

        // Shutdown signal: on SIGINT/SIGTERM mark the health service unready and
        // wait out the unready propagation delay; the graceful shutdown then runs.
        let sigint = signal(SignalKind::interrupt())
            .map_err(|e| format!("failed to listen for SIGINT: {}", e))?;
        let sigterm = signal(SignalKind::terminate())
            .map_err(|e| format!("failed to listen for SIGTERM: {}", e))?;
        let unready_delay_seconds = server_options.unready_propagation_delay_seconds;

        let signal = async move {
            let mut sigint = sigint;
            let mut sigterm = sigterm;
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
            let mut health = health_for_shutdown;
            health.mark_not_ready().await;
            sleep(Duration::from_secs(unready_delay_seconds as u64)).await;
            let _ = shutdown_tx.send(());
        };
        let grpc_fut = grpc_server.serve_with_shutdown(addr, signal);
        let grpc_result = grpc_fut
            .await
            .map_err(|e| format!("failed to serve extproc gRPC server: {}", e));

        if let Err(e) = grpc_result {
            metrics_task.abort();
            return Err(e.into());
        }

        // graceful shutdown of the metrics HTTP server
        let _ = metrics_task.await;

        // notify processor
        self.close(server_options.termination_grace_period_seconds)?;

        Ok(())
    }
}

/// Start the ext_proc server, rejecting a nil processor. Convenience wrapper
/// around [`GenericExtProcServer::serve`](GenericExtProcServer::serve).
pub async fn serve(
    server_options: crate::options::ServerOptions,
    processor: Option<Arc<dyn RequestProcessor>>,
) -> Result<(), Error> {
    let processor = match processor {
        Some(p) => p,
        None => return Err("processor is nil".into()),
    };
    let name = processor.get_name();
    let options = processor.get_options().clone();
    let server = GenericExtProcServer::new(name, processor, Some(options), Metrics::new());
    server.serve(server_options).await
}

/// Start the ext_proc server and exit on failure. Convenience wrapper around
/// [`serve`](serve).
pub async fn must_serve(
    server_options: crate::options::ServerOptions,
    processor: Option<Arc<dyn RequestProcessor>>,
) {
    if let Err(e) = serve(server_options, processor).await {
        panic!("extproc server failed: {}", e);
    }
}

/// Shared state for the metrics handler.
#[derive(Clone)]
struct MetricsState {
    registry: prometheus::Registry,
}

async fn metrics_handler(state: axum::extract::State<MetricsState>) -> axum::response::Response {
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let families = state.registry.gather();
    if let Err(e) = encoder.encode(&families, &mut buffer) {
        return axum::response::Response::builder()
            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::from(e.to_string()))
            .unwrap();
    }
    // Match the Go SDK's `EnableOpenMetrics` output: the OpenMetrics text
    // format requires a final `# EOF` line and is served with the OpenMetrics
    // content type.
    buffer.extend_from_slice(b"# EOF\n");
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(
            "content-type",
            "application/openmetrics-text; version=0.0.4; charset=utf-8",
        )
        .body(axum::body::Body::from(buffer))
        .unwrap()
}

/// Initialize a [`RequestContext`] from an envoy
/// [`HttpHeaders`](envoy_types::pb::envoy::service::ext_proc::v3::HttpHeaders),
/// resetting phase state and populating the parsed context fields.
fn init_req_ctx(
    rc: &mut RequestContext,
    headers: &envoy_types::pb::envoy::service::ext_proc::v3::HttpHeaders,
) -> Result<(), Error> {
    rc.started = Instant::now();
    rc.duration = std::time::Duration::ZERO;

    rc.all_headers = gen_headers(
        headers
            .headers
            .as_ref()
            .ok_or_else(|| "missing headers".to_string())?,
    )?;

    let empty: Vec<envoy_types::pb::envoy::config::core::v3::HeaderValue> = Vec::new();
    let hs: &[envoy_types::pb::envoy::config::core::v3::HeaderValue] = headers
        .headers
        .as_ref()
        .map(|m| &m.headers)
        .unwrap_or(&empty);
    for h in hs {
        let v = if !h.raw_value.is_empty() {
            String::from_utf8_lossy(&h.raw_value).into_owned()
        } else {
            h.value.clone()
        };
        match h.key.as_str() {
            ":scheme" => rc.scheme = v,
            ":authority" => rc.authority = v,
            ":method" => rc.method = v,
            ":path" => {
                rc.full_path = v.clone();
                rc.path = v.split('?').next().unwrap_or(&v).to_string();
            }
            "x-request-id" => rc.request_id = v,
            _ => {}
        }
    }

    Ok(())
}

/// The response stream driving one bidirectional ext_proc stream.
pub struct ProcessStream {
    server: Arc<GenericExtProcServer>,
    rc: RequestContext,
    incoming: std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ProcessingRequest, tonic::Status>> + Send>,
    >,
    pending: Option<Result<ProcessingResponse, tonic::Status>>,
}

impl Stream for ProcessStream {
    type Item = Result<ProcessingResponse, tonic::Status>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(resp) = this.pending.take() {
            return Poll::Ready(Some(resp));
        }
        let poll = Stream::poll_next(std::pin::Pin::new(&mut this.incoming), cx);
        match poll {
            Poll::Ready(Some(Ok(req))) => {
                let outcome = this.server.process_phase(&req, &mut this.rc);
                match outcome {
                    Ok((resp, _phase)) => this.pending = resp.map(Ok),
                    Err(e) => {
                        this.pending = Some(Err(Status::internal(format!(
                            "failed to process phase: {}",
                            e
                        ))));
                    }
                }
                Poll::Ready(Some(this.pending.take().unwrap()))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[tonic::async_trait]
impl ExternalProcessor for GenericExtProcServer {
    type ProcessStream = ProcessStream;

    async fn process(
        &self,
        request: Request<Streaming<ProcessingRequest>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        let incoming = Box::pin(request.into_inner());
        Ok(Response::new(ProcessStream {
            server: Arc::new(self.clone()),
            rc: RequestContext::new(),
            incoming,
            pending: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::metrics::Metrics;
    use crate::options::{ProcessingOptions, ServerOptions};
    use envoy_types::pb::envoy::service::ext_proc::v3::processing_request::Request as ProcessingRequestOneof;
    use envoy_types::pb::envoy::service::ext_proc::v3::processing_response::Response as ResponseOneof;
    use envoy_types::pb::envoy::service::ext_proc::v3::HttpBody;
    use envoy_types::pb::envoy::service::ext_proc::v3::HttpHeaders;
    use envoy_types::pb::envoy::service::ext_proc::v3::HttpTrailers;
    use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest;

    use envoy_types::pb::envoy::config::core::v3::HeaderMap as EnvoyHeaderMap;
    use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;

    use prometheus::Registry;

    use crate::context::{default_phase_if_invalid, request_phase_to_string, RequestContext};
    use crate::headers::AllHeaders;

    fn processing_request(request: Option<ProcessingRequestOneof>) -> ProcessingRequest {
        ProcessingRequest {
            metadata_context: None,
            attributes: HashMap::new(),
            observability_mode: false,
            protocol_config: None,
            flow_control_init: None,
            client_window_update: None,
            request,
        }
    }

    fn http_headers(headers: Option<&EnvoyHeaderMap>, end_of_stream: bool) -> HttpHeaders {
        HttpHeaders {
            headers: headers.cloned(),
            attributes: HashMap::new(),
            end_of_stream,
        }
    }

    fn envoy_header_map(pairs: &[(&str, &str)]) -> EnvoyHeaderMap {
        EnvoyHeaderMap {
            headers: pairs
                .iter()
                .map(|(k, v)| EnvoyHeaderValue {
                    key: k.to_string(),
                    value: String::new(),
                    raw_value: v.as_bytes().to_vec(),
                })
                .collect(),
        }
    }

    #[derive(Clone)]
    struct TestProcessor {
        name: String,
        opts: Option<ProcessingOptions>,
        phases: Arc<Mutex<Vec<u32>>>,
        bodies: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        errors: Arc<Mutex<Vec<u32>>>,
    }

    impl TestProcessor {
        fn new(name: &str, opts: Option<ProcessingOptions>) -> Self {
            Self {
                name: name.to_string(),
                opts,
                phases: Arc::new(Mutex::new(Vec::new())),
                bodies: Arc::new(Mutex::new(HashMap::new())),
                errors: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn recorded_phases(&self) -> Vec<u32> {
            self.phases.lock().unwrap().clone()
        }

        fn recorded_bodies(&self) -> HashMap<String, Vec<u8>> {
            self.bodies.lock().unwrap().clone()
        }

        fn recorded_errors(&self) -> Vec<u32> {
            self.errors.lock().unwrap().clone()
        }
    }

    impl RequestProcessor for TestProcessor {
        fn get_name(&self) -> String {
            self.name.clone()
        }
        fn get_options(&self) -> &ProcessingOptions {
            self.opts.as_ref().unwrap()
        }
        fn process_request_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(REQUEST_PHASE_REQUEST_HEADERS);
            ctx.continue_request()
        }
        fn process_request_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(REQUEST_PHASE_REQUEST_TRAILERS);
            ctx.continue_request()
        }
        fn process_request_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
            self.phases.lock().unwrap().push(REQUEST_PHASE_REQUEST_BODY);
            self.bodies
                .lock()
                .unwrap()
                .insert("request".to_string(), body.to_vec());
            ctx.continue_request()
        }
        fn process_response_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(REQUEST_PHASE_RESPONSE_HEADERS);
            ctx.continue_request()
        }
        fn process_response_body(
            &self,
            ctx: &mut RequestContext,
            body: &[u8],
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(REQUEST_PHASE_RESPONSE_BODY);
            self.bodies
                .lock()
                .unwrap()
                .insert("response".to_string(), body.to_vec());
            ctx.continue_request()
        }
        fn process_response_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(REQUEST_PHASE_RESPONSE_TRAILERS);
            ctx.continue_request()
        }
        fn error_handler(&self, _ctx: &mut RequestContext, phase: u32, _err: Error) {
            self.errors.lock().unwrap().push(phase);
        }
        fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
            Ok(())
        }
    }

    struct ErroringProcessor {
        opts: ProcessingOptions,
    }

    impl RequestProcessor for ErroringProcessor {
        fn get_name(&self) -> String {
            "test".to_string()
        }
        fn get_options(&self) -> &ProcessingOptions {
            &self.opts
        }
        fn process_request_headers(
            &self,
            _ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            Err("processor exploded".into())
        }
        fn process_request_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        fn process_request_body(
            &self,
            ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        fn process_response_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        fn process_response_body(
            &self,
            _ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            Err("processor exploded".into())
        }
        fn process_response_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        fn error_handler(&self, _ctx: &mut RequestContext, _phase: u32, _err: Error) {}
        fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_serve_rejects_nil_processor() {
        let err = serve(ServerOptions::new(), None).await.unwrap_err();
        assert_eq!(err.to_string(), "processor is nil");
    }

    #[test]
    fn test_process_phase_request_headers() {
        let proc = Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new())));
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();

        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&envoy_header_map(&[(":method", "POST")])),
            false,
        ))));
        let (resp, phase) = server.process_phase(&req, &mut rc).unwrap();
        assert_eq!(phase, REQUEST_PHASE_REQUEST_HEADERS);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::RequestHeaders(_))
        ));
        assert_eq!(rc.method, "POST");
        assert!(!rc.end_of_stream);
        assert_eq!(rc.get_phase(), REQUEST_PHASE_REQUEST_HEADERS);
        assert_eq!(proc.recorded_phases(), vec![REQUEST_PHASE_REQUEST_HEADERS]);
    }

    #[test]
    fn test_process_phase_request_body() {
        let proc = Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new())));
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::RequestBody(HttpBody {
            body: b"{\"hello\":\"world\"}".to_vec(),
            end_of_stream: true,
            end_of_stream_without_message: false,
            grpc_message_compressed: false,
            drain_complete: false,
        })));
        let (resp, phase) = server.process_phase(&req, &mut rc).unwrap();
        assert_eq!(phase, REQUEST_PHASE_REQUEST_BODY);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::RequestBody(_))
        ));
        assert!(rc.end_of_stream);
        assert_eq!(rc.get_phase(), REQUEST_PHASE_REQUEST_BODY);
        assert_eq!(
            proc.recorded_bodies().get("request").unwrap(),
            &b"{\"hello\":\"world\"}".to_vec()
        );
    }

    #[test]
    fn test_process_phase_response_headers_updates_context() {
        let options = ProcessingOptions {
            update_ext_proc_header: true,
            update_duration_header: true,
            abort_on_processor_failure: false,
        };
        let proc = Arc::new(TestProcessor::new("myproc", Some(options.clone())));
        let server = GenericExtProcServer::new(
            "myproc".to_string(),
            proc.clone(),
            Some(options),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.duration = std::time::Duration::from_secs(5);
        let req = processing_request(Some(ProcessingRequestOneof::ResponseHeaders(http_headers(
            Some(&envoy_header_map(&[("x-foo", "bar")])),
            true,
        ))));
        let (resp, phase) = server.process_phase(&req, &mut rc).unwrap();
        assert_eq!(phase, REQUEST_PHASE_RESPONSE_HEADERS);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::ResponseHeaders(_))
        ));
        assert!(rc.end_of_stream);

        let mutation = rc.response.header_mutation.as_ref().unwrap();
        assert_eq!(mutation.set_headers.len(), 2);
        assert_eq!(
            mutation.set_headers[0].header.as_ref().unwrap().key,
            "x-extproc-names"
        );
        assert_eq!(
            mutation.set_headers[0].header.as_ref().unwrap().raw_value,
            b"myproc".to_vec()
        );
        assert_eq!(
            mutation.set_headers[1].header.as_ref().unwrap().key,
            "x-extproc-duration-ns"
        );
        let dur_str =
            String::from_utf8_lossy(&mutation.set_headers[1].header.as_ref().unwrap().raw_value)
                .to_string();
        let dur = dur_str.parse::<i64>().unwrap();
        assert!(dur > 0);
    }

    #[test]
    fn test_process_phase_response_body_does_not_add_extproc_name() {
        let options = ProcessingOptions {
            update_ext_proc_header: true,
            update_duration_header: true,
            abort_on_processor_failure: false,
        };
        let proc = Arc::new(TestProcessor::new("myproc", Some(options.clone())));
        let server = GenericExtProcServer::new(
            "myproc".to_string(),
            proc.clone(),
            Some(options),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::ResponseBody(HttpBody {
            body: b"hello".to_vec(),
            end_of_stream: true,
            end_of_stream_without_message: false,
            grpc_message_compressed: false,
            drain_complete: false,
        })));
        let (resp, phase) = server.process_phase(&req, &mut rc).unwrap();
        assert_eq!(phase, REQUEST_PHASE_RESPONSE_BODY);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::ResponseBody(_))
        ));
        assert!(rc.end_of_stream);

        let mutation = rc.response.header_mutation.as_ref().unwrap();
        assert_eq!(mutation.set_headers.len(), 1);
        assert_eq!(
            mutation.set_headers[0].header.as_ref().unwrap().key,
            "x-extproc-duration-ns"
        );
    }

    #[test]
    fn test_process_phase_response_trailers() {
        let proc = Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new())));
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::ResponseTrailers(
            HttpTrailers {
                trailers: Some(envoy_header_map(&[("x-trailer", "yes")])),
            },
        )));
        let (resp, phase) = server.process_phase(&req, &mut rc).unwrap();
        assert_eq!(phase, REQUEST_PHASE_RESPONSE_TRAILERS);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::ResponseTrailers(_))
        ));
    }

    #[test]
    fn test_process_phase_processor_error_propagates() {
        let server = GenericExtProcServer::new(
            "test".to_string(),
            Arc::new(ErroringProcessor {
                opts: ProcessingOptions::new(),
            }),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&envoy_header_map(&[(":method", "GET")])),
            false,
        ))));
        let err = server.process_phase(&req, &mut rc).unwrap_err();
        assert!(err.to_string().contains("processor exploded"));
        assert_eq!(rc.get_phase(), REQUEST_PHASE_REQUEST_HEADERS);
    }

    #[test]
    fn test_process_phase_init_req_ctx_error_invokes_error_handler() {
        let proc = Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new())));
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let bad = EnvoyHeaderValue {
            key: "x-bad".to_string(),
            value: "a".to_string(),
            raw_value: b"b".to_vec(),
        };
        let hm = EnvoyHeaderMap { headers: vec![bad] };
        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&hm),
            false,
        ))));
        let err = server.process_phase(&req, &mut rc).unwrap_err();
        assert!(err
            .to_string()
            .contains("request context initialization failed"));
        assert_eq!(rc.get_phase(), REQUEST_PHASE_UNDETERMINED);
        assert_eq!(proc.recorded_errors(), vec![REQUEST_PHASE_REQUEST_HEADERS]);
    }

    #[test]
    fn test_process_phase_unknown_request_type() {
        let proc = Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new())));
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let err = server
            .process_phase(&processing_request(None), &mut rc)
            .unwrap_err();
        assert!(err.to_string().contains("unknown request type"));
        assert_eq!(rc.get_phase(), REQUEST_PHASE_UNDETERMINED);
    }

    #[test]
    fn test_process_phase_metrics_increment() {
        let proc = Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new())));
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::ResponseBody(HttpBody {
            body: b"abc".to_vec(),
            end_of_stream: true,
            end_of_stream_without_message: false,
            grpc_message_compressed: false,
            drain_complete: false,
        })));
        let (resp, phase) = server.process_phase(&req, &mut rc).unwrap();
        assert_eq!(phase, REQUEST_PHASE_RESPONSE_BODY);
        assert!(resp.is_some());

        let registry = Registry::new();
        server.metrics.register(&registry).unwrap();
        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_response_body_bytes_total")
            .unwrap();
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 3.0);
    }

    #[test]
    fn test_phase_helpers() {
        assert_eq!(default_phase_if_invalid(42), REQUEST_PHASE_UNDETERMINED);
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_RESPONSE_BODY),
            "RESPONSE_BODY"
        );
        assert_eq!(REQUEST_PHASE_RESPONSE_BODY, 5);
        assert_eq!(REQUEST_PHASE_RESPONSE_TRAILERS, 6);
        assert_eq!(REQUEST_PHASE_RESPONSE_HEADERS, 4);
    }
}
