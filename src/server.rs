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

use crate::context::{HeaderValue, RequestContext, RequestPhase};
use crate::headers::{gen_headers, AllHeaders};
use crate::metrics::Metrics;
use crate::options::ProcessingOptions;
use crate::Error;
use async_trait::async_trait;
use prometheus::Encoder;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Errors that can occur while processing a phase. Implemented by processors
/// to return from phase methods; errors flow to the processor's
/// [`error_handler`](RequestProcessor::error_handler).
///
/// Phase processing methods are async so that processor implementations can
/// perform asynchronous work (e.g. downstream HTTP calls, database lookups)
/// without blocking the stream task. Phase messages within a single stream are
/// still processed sequentially, matching the envoy ext_proc protocol's
/// ordered message flow.
#[async_trait]
pub trait RequestProcessor: Send + Sync + 'static {
    /// The name of the processor, reported to `envoy` via the
    /// `x-extproc-names` response header.
    fn name(&self) -> String;

    /// The processor's [`ProcessingOptions`](crate::options::ProcessingOptions).
    fn options(&self) -> &ProcessingOptions;

    /// Process the request headers phase. May perform asynchronous work.
    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the request trailers phase. May perform asynchronous work.
    async fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the request body phase. May perform asynchronous work.
    async fn process_request_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error>;

    /// Process the response headers phase. May perform asynchronous work.
    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the response body phase. May perform asynchronous work.
    async fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error>;

    /// Process the response trailers phase. May perform asynchronous work.
    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Called when the SDK hits an error during request processing. May
    /// perform asynchronous work. An implemented handler can implement any
    /// logic it likes.
    async fn error_handler(&self, ctx: &mut RequestContext, phase: RequestPhase, err: Error);

    /// Called on shutdown, after in-flight streams have completed, including
    /// in-flight phase processing. Implementations may perform blocking
    /// cleanup during close; the server awaits in-flight streams (via graceful
    /// shutdown) before invoking this.
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
    /// Create a new generic ext_proc server holding the given processor, its
    /// options and the metrics to collect.
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
    pub fn name(&self) -> String {
        self.processor.name()
    }

    /// The processor's [`ProcessingOptions`](crate::options::ProcessingOptions).
    pub fn options(&self) -> &ProcessingOptions {
        self.processor.options()
    }

    /// Called on shutdown, after in-flight streams have completed, including
    /// in-flight phase processing; the graceful shutdown awaits in-flight
    /// streams before invoking the processor's close.
    pub fn close(&self, grace_period_seconds: i32) -> Result<(), Error> {
        self.processor.close(grace_period_seconds)
    }

    /// Handle one phase of a request/response exchange, building the
    /// [`ProcessingResponse`] to send back to `envoy`.
    ///
    /// Phase processing is asynchronous: the processor's phase methods are
    /// async, and this drives them. Called from the spawned phase task that
    /// backs the [`ProcessStream`], which owns the inbound stream and context
    /// and sends responses over a channel.
    pub async fn process_phase(
        &self,
        proc_req: &ProcessingRequest,
        rc: &mut RequestContext,
    ) -> Result<(Option<ProcessingResponse>, RequestPhase), Error> {
        // Reset phase-scoped state at the start of every phase, mirroring the
        // Go SDK's per-message reset (and `initReqCtx`'s reset for the
        // request-headers phase).
        rc.reset_phase()?;
        let mut phase = RequestPhase::Undetermined;
        let outcome = self.dispatch_phase(proc_req, rc, &mut phase).await;
        if let Err(e) = outcome {
            if self
                .options
                .as_ref()
                .is_some_and(|o| o.abort_on_processor_failure)
            {
                // Abort requested: propagate the phase error, which the
                // ProcessStream turns into a stream error, closing the stream.
                return Err(e);
            }
            // Without abort: mirror the Go SDK — send an empty continue
            // response and keep the stream alive.
            let response = rc.empty_continue_response(phase)?;
            return Ok((response, phase));
        }

        outcome
    }

    async fn dispatch_phase(
        &self,
        proc_req: &ProcessingRequest,
        rc: &mut RequestContext,
        phase: &mut RequestPhase,
    ) -> Result<(Option<ProcessingResponse>, RequestPhase), Error> {
        let phase_start = Instant::now();
        let metrics = &self.metrics;
        let options = &self.options;

        let outcome = async {
            match &proc_req.request {
                Some(ProcessingRequestOneof::RequestHeaders(h)) => {
                    *phase = RequestPhase::RequestHeaders;
                    rc.set_phase(*phase);
                    metrics.total_request_headers.inc();
                    let init = init_req_ctx(rc, h);
                    if let Err(e) = init {
                        self.processor.error_handler(rc, *phase, e).await;
                        return Err("request context initialization failed".into());
                    }
                    rc.end_of_stream = h.end_of_stream;
                    rc.set_phase(*phase);
                    let headers = rc.all_headers.clone();
                    rc.duration += phase_start.elapsed();
                    self.processor.process_request_headers(rc, &headers).await?;
                }
                Some(ProcessingRequestOneof::RequestBody(b)) => {
                    *phase = RequestPhase::RequestBody;
                    rc.set_phase(*phase);
                    metrics.total_request_body.inc();
                    metrics.request_body_bytes.inc_by(b.body.len() as f64);
                    rc.end_of_stream = b.end_of_stream;
                    rc.duration += phase_start.elapsed();
                    self.processor.process_request_body(rc, &b.body).await?;
                }
                Some(ProcessingRequestOneof::RequestTrailers(t)) => {
                    *phase = RequestPhase::RequestTrailers;
                    rc.set_phase(*phase);
                    metrics.total_request_trailers.inc();
                    let trailers = match gen_headers(
                        t.trailers
                            .as_ref()
                            .ok_or_else(|| "missing trailers".to_string())?,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            self.processor.error_handler(rc, *phase, e).await;
                            return Err("failed to generate request trailers".into());
                        }
                    };
                    rc.duration += phase_start.elapsed();
                    self.processor
                        .process_request_trailers(rc, &trailers)
                        .await?;
                }
                Some(ProcessingRequestOneof::ResponseHeaders(h)) => {
                    *phase = RequestPhase::ResponseHeaders;
                    rc.set_phase(*phase);
                    metrics.total_response_headers.inc();
                    let headers = match gen_headers(
                        h.headers
                            .as_ref()
                            .ok_or_else(|| "missing headers".to_string())?,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            self.processor.error_handler(rc, *phase, e).await;
                            return Err("failed to generate response headers".into());
                        }
                    };
                    rc.end_of_stream = h.end_of_stream;
                    self.processor
                        .process_response_headers(rc, &headers)
                        .await?;
                    rc.duration += phase_start.elapsed();
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
                    *phase = RequestPhase::ResponseBody;
                    rc.set_phase(*phase);
                    metrics.total_response_body.inc();
                    metrics.response_body_bytes.inc_by(b.body.len() as f64);
                    rc.end_of_stream = b.end_of_stream;
                    self.processor.process_response_body(rc, &b.body).await?;
                    rc.duration += phase_start.elapsed();
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
                    *phase = RequestPhase::ResponseTrailers;
                    rc.set_phase(*phase);
                    metrics.total_response_trailers.inc();
                    let trailers = match gen_headers(
                        t.trailers
                            .as_ref()
                            .ok_or_else(|| "missing trailers".to_string())?,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            self.processor.error_handler(rc, *phase, e).await;
                            return Err("failed to generate response trailers".into());
                        }
                    };
                    rc.duration += phase_start.elapsed();
                    self.processor
                        .process_response_trailers(rc, &trailers)
                        .await?;
                }
                None => {
                    rc.set_phase(*phase);
                    return Err("unknown request type".into());
                }
            }

            let response = rc.get_response(*phase)?;
            Ok((response, *phase))
        }
        .await;

        if outcome.is_err() {
            metrics.phase_errors.inc_by(1.0);
        }

        outcome
    }

    /// Set up and serve the ext_proc gRPC server, metrics endpoint, and
    /// health service.
    pub async fn serve(&self, server_options: crate::options::ServerOptions) -> Result<(), Error> {
        // Install a default tracing subscriber so startup logs (e.g. the bound
        // ports below) are emitted.
        let _ = tracing_subscriber::fmt().try_init();

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
        tracing::info!(
            port = server_options.metrics_http_port,
            "extproc server bound metrics HTTP listener"
        );
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
        tracing::info!(
            port = server_options.ext_proc_port,
            "extproc server bound gRPC listener"
        );

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
            // Still attempt processor shutdown before exiting, so close is
            // invoked even when the graceful shutdown path errors.
            self.close(server_options.termination_grace_period_seconds)?;
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
    let name = processor.name();
    let options = processor.options().clone();
    let server = GenericExtProcServer::new(name, processor, Some(options), Metrics::new());
    server.serve(server_options).await
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
///
/// Phase processing runs on a spawned task that owns the inbound stream and the
/// [`RequestContext`](crate::context::RequestContext); responses (and terminal
/// errors) are sent through an unbounded channel and relayed by
/// [`Stream::poll_next`](tokio_stream::Stream::poll_next). The task sends each
/// phase's response before pulling the next phase, so the relay preserves
/// envoy's ordered phase flow; a closed channel means the task has finished and
/// the stream is done.
pub struct ProcessStream {
    server: Arc<GenericExtProcServer>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Result<ProcessingResponse, tonic::Status>>,
    /// Whether the stream was ended by a processing error, for metrics.
    errored: bool,
}

impl Drop for ProcessStream {
    fn drop(&mut self) {
        self.server.metrics.active_streams.dec();
        if self.errored {
            self.server.metrics.errored_streams.inc();
        }
    }
}

impl Stream for ProcessStream {
    type Item = Result<ProcessingResponse, tonic::Status>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Relay phase responses produced by the spawned task over the channel.
        // The task sends each phase's response (or a terminal error) in order,
        // so relaying preserves envoy's ordered phase flow. A closed channel
        // means the task has finished and the stream is done.
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(resp)) => {
                return match resp {
                    Ok(resp) => Poll::Ready(Some(Ok(resp))),
                    Err(e) => {
                        this.errored = true;
                        Poll::Ready(Some(Err(e)))
                    }
                };
            }
            // The task has finished and dropped the sender: the exchange is
            // complete, so end the stream.
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }
        Poll::Pending
    }
}

/// Spawn the task that drives one bidirectional ext_proc stream: pull inbound
/// phase messages, run phase processing, and send responses/errors through the
/// channel. The task owns the inbound stream and the
/// [`RequestContext`](crate::context::RequestContext), so phase processing is
/// fully async. Observes the stream duration before exiting.
async fn run_phase_task(
    server: Arc<GenericExtProcServer>,
    incoming: std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ProcessingRequest, tonic::Status>> + Send>,
    >,
    tx: tokio::sync::mpsc::UnboundedSender<Result<ProcessingResponse, tonic::Status>>,
) {
    let mut incoming = incoming;
    let mut rc = RequestContext::new();
    loop {
        let Some(req) = incoming.next().await else {
            break;
        };
        let req = match req {
            Ok(req) => req,
            Err(e) => {
                // Inbound stream error: forward it to the client and end the
                // exchange (a cancelled stream is a clean end).
                if e.code() != tonic::Code::Cancelled && tx.send(Err(e)).is_err() {
                    break;
                }
                break;
            }
        };
        let outcome = server.process_phase(&req, &mut rc).await;
        match outcome {
            Ok((resp, _phase)) => {
                if let Some(resp) = resp {
                    if tx.send(Ok(resp)).is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                if tx
                    .send(Err(Status::internal(format!(
                        "failed to process phase: {}",
                        e
                    ))))
                    .is_err()
                {
                    break;
                }
            }
        }
    }
    server
        .metrics
        .stream_duration_seconds
        .observe((Instant::now() - rc.started).as_secs_f64());
}

#[tonic::async_trait]
impl ExternalProcessor for GenericExtProcServer {
    type ProcessStream = ProcessStream;

    async fn process(
        &self,
        request: Request<Streaming<ProcessingRequest>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        self.metrics.total_streams.inc();
        self.metrics.active_streams.inc();
        let incoming = Box::pin(request.into_inner());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let server = Arc::new(self.clone());
        let task_server = server.clone();
        tokio::spawn(async move {
            run_phase_task(task_server, incoming, tx).await;
        });
        Ok(Response::new(ProcessStream {
            server,
            rx,
            errored: false,
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
    use envoy_types::pb::envoy::service::ext_proc::v3::CommonResponse;
    use envoy_types::pb::envoy::service::ext_proc::v3::HttpBody;
    use envoy_types::pb::envoy::service::ext_proc::v3::HttpHeaders;
    use envoy_types::pb::envoy::service::ext_proc::v3::HttpTrailers;
    use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest;

    use envoy_types::pb::envoy::config::core::v3::HeaderMap as EnvoyHeaderMap;
    use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;

    use prometheus::Registry;

    use crate::context::{
        default_phase_if_invalid, request_phase_to_string, RequestContext, RequestPhase,
    };
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
        phases: Arc<Mutex<Vec<RequestPhase>>>,
        bodies: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        errors: Arc<Mutex<Vec<RequestPhase>>>,
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

        fn recorded_phases(&self) -> Vec<RequestPhase> {
            self.phases.lock().unwrap().clone()
        }

        fn recorded_bodies(&self) -> HashMap<String, Vec<u8>> {
            self.bodies.lock().unwrap().clone()
        }

        fn recorded_errors(&self) -> Vec<RequestPhase> {
            self.errors.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl RequestProcessor for TestProcessor {
        fn name(&self) -> String {
            self.name.clone()
        }
        fn options(&self) -> &ProcessingOptions {
            self.opts.as_ref().unwrap()
        }
        async fn process_request_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(RequestPhase::RequestHeaders);
            ctx.continue_request()
        }
        async fn process_request_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(RequestPhase::RequestTrailers);
            ctx.continue_request()
        }
        async fn process_request_body(
            &self,
            ctx: &mut RequestContext,
            body: &[u8],
        ) -> Result<(), Error> {
            self.phases.lock().unwrap().push(RequestPhase::RequestBody);
            self.bodies
                .lock()
                .unwrap()
                .insert("request".to_string(), body.to_vec());
            ctx.continue_request()
        }
        async fn process_response_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(RequestPhase::ResponseHeaders);
            ctx.continue_request()
        }
        async fn process_response_body(
            &self,
            ctx: &mut RequestContext,
            body: &[u8],
        ) -> Result<(), Error> {
            self.phases.lock().unwrap().push(RequestPhase::ResponseBody);
            self.bodies
                .lock()
                .unwrap()
                .insert("response".to_string(), body.to_vec());
            ctx.continue_request()
        }
        async fn process_response_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            self.phases
                .lock()
                .unwrap()
                .push(RequestPhase::ResponseTrailers);
            ctx.continue_request()
        }
        async fn error_handler(&self, _ctx: &mut RequestContext, phase: RequestPhase, _err: Error) {
            self.errors.lock().unwrap().push(phase);
        }
        fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
            Ok(())
        }
    }

    struct ErroringProcessor {
        opts: ProcessingOptions,
    }

    #[async_trait]
    impl RequestProcessor for ErroringProcessor {
        fn name(&self) -> String {
            "test".to_string()
        }
        fn options(&self) -> &ProcessingOptions {
            &self.opts
        }
        async fn process_request_headers(
            &self,
            _ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            Err("processor exploded".into())
        }
        async fn process_request_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn process_request_body(
            &self,
            ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn process_response_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn process_response_body(
            &self,
            _ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            Err("processor exploded".into())
        }
        async fn process_response_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn error_handler(
            &self,
            _ctx: &mut RequestContext,
            _phase: RequestPhase,
            _err: Error,
        ) {
        }
        fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
            Ok(())
        }
    }

    fn response_common(resp: &ProcessingResponse) -> Option<&CommonResponse> {
        match &resp.response {
            Some(ResponseOneof::RequestHeaders(h)) => h.response.as_ref(),
            Some(ResponseOneof::ResponseHeaders(h)) => h.response.as_ref(),
            Some(ResponseOneof::RequestBody(h)) => h.response.as_ref(),
            Some(ResponseOneof::ResponseBody(h)) => h.response.as_ref(),
            _ => None,
        }
    }

    // clearRouteProcessor is a minimal external processor that clears the route
    // cache, as an upstream-changing processor does when it rewrites a routed
    // header such as `:authority`.
    struct ClearRouteCacheProcessor {
        name: String,
        opts: Option<ProcessingOptions>,
        clear: bool,
    }

    impl ClearRouteCacheProcessor {
        fn new(name: &str, clear: bool) -> Self {
            Self {
                name: name.to_string(),
                opts: Some(ProcessingOptions::new()),
                clear,
            }
        }
    }

    #[async_trait]
    impl RequestProcessor for ClearRouteCacheProcessor {
        fn name(&self) -> String {
            self.name.clone()
        }
        fn options(&self) -> &ProcessingOptions {
            self.opts.as_ref().unwrap()
        }
        async fn process_request_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            if self.clear {
                ctx.clear_route_cache();
            }
            ctx.continue_request()
        }
        async fn process_request_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn process_request_body(
            &self,
            ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn process_response_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn process_response_body(
            &self,
            ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn process_response_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            ctx.continue_request()
        }
        async fn error_handler(
            &self,
            _ctx: &mut RequestContext,
            _phase: RequestPhase,
            _err: Error,
        ) {
        }
        fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_process_phase_clear_route_cache_clears_envoy_response() {
        // An external processor that clears the route cache (i.e. is changing the
        // upstream, typically by rewriting a routed header such as `:authority`)
        // must surface that decision in the envoy-facing response, or envoy will
        // keep serving from its cached route.
        let proc = Arc::new(ClearRouteCacheProcessor::new("router", true));
        let server = GenericExtProcServer::new(
            "router".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&envoy_header_map(&[(":method", "GET")])),
            false,
        ))));
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_some());
        let common = response_common(resp.as_ref().unwrap())
            .expect("expected a CommonResponse in the request headers response");
        assert!(common.clear_route_cache);
    }

    #[tokio::test]
    async fn test_process_phase_clear_route_cache_defaults_to_false() {
        // Without a processor clearing the route cache, the envoy-facing response
        // must not clear it (the default).
        let proc = Arc::new(ClearRouteCacheProcessor::new("router", false));
        let server = GenericExtProcServer::new(
            "router".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&envoy_header_map(&[(":method", "GET")])),
            false,
        ))));
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_some());
        let common = response_common(resp.as_ref().unwrap())
            .expect("expected a CommonResponse in the request headers response");
        assert!(!common.clear_route_cache);
    }

    #[tokio::test]
    async fn test_serve_rejects_nil_processor() {
        let err = serve(ServerOptions::new(), None).await.unwrap_err();
        assert_eq!(err.to_string(), "processor is nil");
    }

    #[tokio::test]
    async fn test_process_phase_request_headers() {
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
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::RequestHeaders(_))
        ));
        assert_eq!(rc.method, "POST");
        assert!(!rc.end_of_stream);
        assert_eq!(rc.get_phase(), RequestPhase::RequestHeaders);
        assert_eq!(proc.recorded_phases(), vec![RequestPhase::RequestHeaders]);
    }

    #[tokio::test]
    async fn test_process_phase_request_body() {
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
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestBody);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::RequestBody(_))
        ));
        assert!(rc.end_of_stream);
        assert_eq!(rc.get_phase(), RequestPhase::RequestBody);
        assert_eq!(
            proc.recorded_bodies().get("request").unwrap(),
            &b"{\"hello\":\"world\"}".to_vec()
        );
    }

    #[tokio::test]
    async fn test_process_phase_response_headers_updates_context() {
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
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseHeaders);
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

    #[tokio::test]
    async fn test_process_phase_response_body_does_not_add_extproc_name() {
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
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseBody);
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

    #[tokio::test]
    async fn test_process_phase_response_trailers() {
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
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseTrailers);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::ResponseTrailers(_))
        ));
    }

    #[tokio::test]
    async fn test_process_phase_response_headers_mutation_without_explicit_reset() {
        // A fresh RequestContext must already carry a phase response mutation:
        // the server appends `x-extproc-names` during the response-headers phase
        // without any explicit reset, so a fresh context must accept mutations.
        // (Go resets via rc.ResetPhase() per phase message; the Rust port must do
        // the same internally.)
        let options = ProcessingOptions {
            update_ext_proc_header: true,
            update_duration_header: false,
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
        let req = processing_request(Some(ProcessingRequestOneof::ResponseHeaders(http_headers(
            Some(&envoy_header_map(&[("x-foo", "bar")])),
            false,
        ))));
        let (_resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseHeaders);
        let mutation = rc.response.header_mutation.as_ref().unwrap();
        assert_eq!(mutation.set_headers.len(), 1);
        assert_eq!(
            mutation.set_headers[0].header.as_ref().unwrap().key,
            "x-extproc-names"
        );
    }

    #[tokio::test]
    async fn test_process_phase_resets_mutation_between_phases() {
        // Mutations must not accumulate across phases within one stream: the
        // response-headers phase appends `x-extproc-names`, and the following
        // response-body phase must start from a clean mutation.
        let options = ProcessingOptions {
            update_ext_proc_header: true,
            update_duration_header: false,
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

        let req = processing_request(Some(ProcessingRequestOneof::ResponseHeaders(http_headers(
            Some(&envoy_header_map(&[("x-foo", "bar")])),
            false,
        ))));
        let (_resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseHeaders);
        assert_eq!(
            rc.response
                .header_mutation
                .as_ref()
                .unwrap()
                .set_headers
                .len(),
            1
        );

        let req = processing_request(Some(ProcessingRequestOneof::ResponseBody(HttpBody {
            body: b"hello".to_vec(),
            end_of_stream: true,
            end_of_stream_without_message: false,
            grpc_message_compressed: false,
            drain_complete: false,
        })));
        let (_resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseBody);
        assert_eq!(
            rc.response
                .header_mutation
                .as_ref()
                .unwrap()
                .set_headers
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn test_process_phase_processor_error_with_abort_propagates() {
        let server = GenericExtProcServer::new(
            "test".to_string(),
            Arc::new(ErroringProcessor {
                opts: ProcessingOptions {
                    abort_on_processor_failure: true,
                    ..ProcessingOptions::new()
                },
            }),
            Some(ProcessingOptions {
                abort_on_processor_failure: true,
                ..ProcessingOptions::new()
            }),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&envoy_header_map(&[(":method", "GET")])),
            false,
        ))));
        let err = server.process_phase(&req, &mut rc).await.unwrap_err();
        assert!(err.to_string().contains("processor exploded"));
        assert_eq!(rc.get_phase(), RequestPhase::RequestHeaders);
    }

    #[tokio::test]
    async fn test_process_phase_processor_error_without_abort_sends_empty_continue() {
        // Without AbortOnProcessorFailure, a phase error must send an empty
        // continue response and keep the stream alive (Go semantics).
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
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_some());
        assert!(matches!(
            resp.as_ref().unwrap().response,
            Some(ResponseOneof::RequestHeaders(_))
        ));

        // The phase error was counted by the server metrics.
        let registry = Registry::new();
        server.metrics.register(&registry).unwrap();
        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_phase_errors_total")
            .expect("expected phase errors family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 1.0);
    }

    #[tokio::test]
    async fn test_process_phase_init_req_ctx_error_invokes_error_handler() {
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
        // Without AbortOnProcessorFailure, an init error sends an empty
        // continue response (Go semantics) while the error handler still runs.
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_some());
        assert_eq!(rc.get_phase(), RequestPhase::RequestHeaders);
        assert_eq!(proc.recorded_errors(), vec![RequestPhase::RequestHeaders]);
    }

    #[tokio::test]
    async fn test_process_phase_unknown_request_type() {
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
            .await
            .unwrap_err();
        assert!(!err.to_string().is_empty());
        assert_eq!(rc.get_phase(), RequestPhase::Undetermined);
    }

    #[tokio::test]
    async fn test_process_phase_metrics_increment() {
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
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseBody);
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

    #[tokio::test]
    async fn test_process_phase_async_processor_awaits() {
        // Phase methods are async: an implementation may await asynchronous
        // work during a phase and still drive the phase to a response.
        struct AsyncTestProcessor {
            name: String,
            opts: Option<ProcessingOptions>,
        }

        #[async_trait]
        impl RequestProcessor for AsyncTestProcessor {
            fn name(&self) -> String {
                self.name.clone()
            }
            fn options(&self) -> &ProcessingOptions {
                self.opts.as_ref().unwrap()
            }
            async fn process_request_headers(
                &self,
                ctx: &mut RequestContext,
                _headers: &AllHeaders,
            ) -> Result<(), Error> {
                // Simulate an async phase by awaiting an already-completed
                // oneshot signal, then continue the request.
                let (tx, rx) = tokio::sync::oneshot::channel();
                let _ = tx.send(());
                let _ = rx.await;
                ctx.continue_request()
            }
            async fn process_request_trailers(
                &self,
                ctx: &mut RequestContext,
                _trailers: &AllHeaders,
            ) -> Result<(), Error> {
                ctx.continue_request()
            }
            async fn process_request_body(
                &self,
                ctx: &mut RequestContext,
                _body: &[u8],
            ) -> Result<(), Error> {
                ctx.continue_request()
            }
            async fn process_response_headers(
                &self,
                ctx: &mut RequestContext,
                _headers: &AllHeaders,
            ) -> Result<(), Error> {
                ctx.continue_request()
            }
            async fn process_response_body(
                &self,
                ctx: &mut RequestContext,
                _body: &[u8],
            ) -> Result<(), Error> {
                ctx.continue_request()
            }
            async fn process_response_trailers(
                &self,
                ctx: &mut RequestContext,
                _trailers: &AllHeaders,
            ) -> Result<(), Error> {
                ctx.continue_request()
            }
            async fn error_handler(
                &self,
                _ctx: &mut RequestContext,
                _phase: RequestPhase,
                _err: Error,
            ) {
            }
            fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
                Ok(())
            }
        }

        let proc = Arc::new(AsyncTestProcessor {
            name: "async".to_string(),
            opts: Some(ProcessingOptions::new()),
        });
        let server = GenericExtProcServer::new(
            "async".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&envoy_header_map(&[(":method", "GET")])),
            false,
        ))));
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_some());
        assert_eq!(rc.get_phase(), RequestPhase::RequestHeaders);
    }

    #[tokio::test]
    async fn test_stream_phase_processing_with_sleeping_processor() {
        // Drive a full ext_proc stream in-process: spawn the phase task over an
        // inbound channel, relay responses through an outbound channel, and poll
        // the stream — with a processor that awaits a real timer in each phase.
        // The whole exchange must complete within a timeout.
        struct SleepingProcessor {
            opts: ProcessingOptions,
        }

        impl SleepingProcessor {
            fn new() -> Self {
                Self {
                    opts: ProcessingOptions::new(),
                }
            }
        }

        #[async_trait]
        impl RequestProcessor for SleepingProcessor {
            fn name(&self) -> String {
                "sleepy".to_string()
            }
            fn options(&self) -> &ProcessingOptions {
                &self.opts
            }
            async fn process_request_headers(
                &self,
                ctx: &mut RequestContext,
                _headers: &AllHeaders,
            ) -> Result<(), Error> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ctx.continue_request()
            }
            async fn process_request_trailers(
                &self,
                ctx: &mut RequestContext,
                _trailers: &AllHeaders,
            ) -> Result<(), Error> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ctx.continue_request()
            }
            async fn process_request_body(
                &self,
                ctx: &mut RequestContext,
                _body: &[u8],
            ) -> Result<(), Error> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ctx.continue_request()
            }
            async fn process_response_headers(
                &self,
                ctx: &mut RequestContext,
                _headers: &AllHeaders,
            ) -> Result<(), Error> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ctx.continue_request()
            }
            async fn process_response_body(
                &self,
                ctx: &mut RequestContext,
                _body: &[u8],
            ) -> Result<(), Error> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ctx.continue_request()
            }
            async fn process_response_trailers(
                &self,
                ctx: &mut RequestContext,
                _trailers: &AllHeaders,
            ) -> Result<(), Error> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ctx.continue_request()
            }
            async fn error_handler(
                &self,
                _ctx: &mut RequestContext,
                _phase: RequestPhase,
                _err: Error,
            ) {
            }
            fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
                Ok(())
            }
        }

        let server = Arc::new(GenericExtProcServer::new(
            "sleepy".to_string(),
            Arc::new(SleepingProcessor::new()),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        ));

        let (in_tx, in_rx) =
            tokio::sync::mpsc::channel::<Result<ProcessingRequest, tonic::Status>>(1);
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        let task_server = server.clone();
        tokio::spawn(async move {
            run_phase_task(
                task_server,
                Box::pin(tokio_stream::wrappers::ReceiverStream::new(in_rx)),
                out_tx,
            )
            .await;
        });

        let mut stream = ProcessStream {
            server,
            rx: out_rx,
            errored: false,
        };

        let phase_req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(
            http_headers(Some(&envoy_header_map(&[(":method", "GET")])), false),
        )));
        in_tx.send(Ok(phase_req)).await.unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for phase response")
            .expect("stream ended before a phase response was produced");
        assert!(response.is_ok());
    }

    #[test]
    fn test_phase_helpers() {
        assert_eq!(default_phase_if_invalid(42), RequestPhase::Undetermined);
        assert_eq!(
            request_phase_to_string(RequestPhase::ResponseBody),
            "RESPONSE_BODY"
        );
        assert_eq!(RequestPhase::ResponseBody.as_u32(), 5);
        assert_eq!(RequestPhase::ResponseTrailers.as_u32(), 6);
        assert_eq!(RequestPhase::ResponseHeaders.as_u32(), 4);
    }
}
