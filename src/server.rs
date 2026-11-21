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

use crate::context::{header_key_set, HeaderValue, ProtocolConfig, RequestContext, RequestPhase};
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
            // In observability mode no responses are sent at all, whether the
            // phase errored or not.
            if proc_req.observability_mode {
                return Ok((None, phase));
            }
            return Ok((response, phase));
        }
        if proc_req.observability_mode {
            // Observability mode: callbacks run and metrics are recorded, but
            // no ProcessingResponse is sent (spurious_msgs_received == 0).
            return Ok((None, phase));
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
        let obs = proc_req.observability_mode;
        rc.observability_mode = obs;
        // Envoy encodes the protocol configuration only on the first message
        // of a processing stream; capture it when present and retain it across
        // the phases of the stream.
        if let Some(pc) = &proc_req.protocol_config {
            rc.protocol_config = Some(ProtocolConfig::from_proto(pc)?);
        }
        // Envoy forwards dynamic metadata (via `metadata_options
        // .forwarding_namespaces`) and evaluates CEL attributes (via
        // `request_attributes` / `response_attributes`) on each processing
        // message; merge them into the context, across phases of the stream.
        if let Some(md) = &proc_req.metadata_context {
            rc.merge_metadata_context(md);
        }
        if !proc_req.attributes.is_empty() {
            rc.merge_attributes(&proc_req.attributes);
        }

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
                    rc.phase_header_keys = Some(rc.req_header_keys.clone());
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
                    rc.phase_header_keys = Some(header_key_set(&trailers));
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
                    rc.resp_header_keys = header_key_set(&headers);
                    rc.phase_header_keys = Some(rc.resp_header_keys.clone());
                    rc.end_of_stream = h.end_of_stream;
                    self.processor
                        .process_response_headers(rc, &headers)
                        .await?;
                    rc.duration += phase_start.elapsed();
                    if !obs && options.as_ref().is_some_and(|o| o.update_ext_proc_header) {
                        rc.append_header(
                            "x-extproc-names",
                            &HeaderValue {
                                value: String::new(),
                                raw_value: Some(self.name.as_bytes().to_vec()),
                            },
                        )?;
                    }
                    if !obs
                        && rc.end_of_stream
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
                    rc.phase_header_keys = Some(rc.resp_header_keys.clone());
                    metrics.total_response_body.inc();
                    metrics.response_body_bytes.inc_by(b.body.len() as f64);
                    rc.end_of_stream = b.end_of_stream;
                    self.processor.process_response_body(rc, &b.body).await?;
                    rc.duration += phase_start.elapsed();
                    if !obs
                        && rc.end_of_stream
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
                    rc.phase_header_keys = Some(header_key_set(&trailers));
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

            // Resolve the phase's header mutation against the phase header key
            // set, then build the envoy-facing response. The post-mutation key
            // set is carried into the next phase of the same kind (e.g. mutations
            // made during the request headers phase inform the request body
            // phase).
            let (_resolved, next_keys) = rc.resolved_header_mutation();
            let response = rc.get_response(*phase)?;
            if let Some(next) = next_keys {
                match *phase {
                    RequestPhase::RequestHeaders | RequestPhase::RequestBody => {
                        rc.req_header_keys = next;
                    }
                    RequestPhase::ResponseHeaders | RequestPhase::ResponseBody => {
                        rc.resp_header_keys = next;
                    }
                    _ => {}
                }
            }
            Ok((response, *phase))
        }
        .await;

        if outcome.is_err() {
            metrics.phase_errors.inc_by(1.0);
        }

        outcome
    }

    /// Create a new generic ext_proc server wrapping a processor, resolving
    /// the processor name and defaults for options and metrics. Returns an
    /// error when the processor is not supplied, mirroring the Go SDK's
    /// `NewGenericExtProcServer`. Use this when registering the server on a
    /// custom `tonic` `Server` instead of via
    /// [`GenericExtProcServer::serve`](GenericExtProcServer::serve).
    pub fn new_with_processor(
        processor: Option<Arc<dyn RequestProcessor>>,
        options: Option<ProcessingOptions>,
        metrics: Metrics,
    ) -> Result<Self, Error> {
        let processor = processor.ok_or_else(|| "processor is nil".to_string())?;
        let name = processor.name();
        let resolved_options = options.unwrap_or_else(|| processor.options().clone());
        Ok(Self {
            name,
            processor,
            options: Some(resolved_options),
            metrics,
        })
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

/// Reports whether an error is a stream cancellation rather than a processing
/// failure. Cancellations occur when the client goes away mid-stream or envoy
/// aborts the stream; they are a normal end of stream, not a processing error,
/// and must not be surfaced through the error handler or error metrics.
///
/// Mirrors the Go SDK's `isStreamCancelled`: a `tonic::Status` carrying the gRPC
/// `Canceled` code is a cancellation, and a raw context cancellation error is
/// too. Context cancellation is normalized into a `Canceled` status by the
/// transport, so checking the status covers both cases.
pub fn is_stream_cancelled(err: Option<&Error>) -> bool {
    let Some(err) = err else {
        return false;
    };
    err.downcast_ref::<tonic::Status>()
        .is_some_and(|status| status.code() == tonic::Code::Cancelled)
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

    rc.req_header_keys = rc.all_headers.headers.keys().cloned().collect();
    rc.phase_header_keys = Some(rc.req_header_keys.clone());

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
                // exchange, unless it is a stream cancellation, which is a clean
                // end that still notifies the error handler so the processor can
                // clean up.
                let err: Error = e.clone().into();
                if is_stream_cancelled(Some(&err)) {
                    // the stream was cancelled; notify the handler so it can clean
                    // up, but this is not a processing error
                    let phase = rc.get_phase();
                    server.processor.error_handler(&mut rc, phase, err).await;
                    tracing::debug!("Processing stream cancelled: {}", e.message());
                    break;
                }
                if tx.send(Err(e)).is_err() {
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
    server
        .metrics
        .external_processing_seconds
        .observe(rc.duration.as_secs_f64());
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
    use prost::Message;
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
    use envoy_types::pb::envoy::service::ext_proc::v3::ProtocolConfiguration;

    use envoy_types::pb::envoy::config::core::v3::HeaderMap as EnvoyHeaderMap;
    use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;
    use envoy_types::pb::envoy::config::core::v3::Metadata;
    use envoy_types::pb::google::protobuf::value::Kind as ValueKind;
    use envoy_types::pb::google::protobuf::{Any, Struct, Value};

    use prometheus::Registry;

    use crate::context::{
        default_phase_if_invalid, request_phase_to_string, BodySendMode, HeaderSendMode,
        ModeOverride, ProtocolConfig, RequestContext, RequestPhase,
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

    fn http_body(body: &[u8], end_of_stream: bool) -> HttpBody {
        HttpBody {
            body: body.to_vec(),
            end_of_stream,
            end_of_stream_without_message: false,
            grpc_message_compressed: false,
            drain_complete: false,
        }
    }

    fn string_value(s: &str) -> Value {
        Value {
            kind: Some(ValueKind::StringValue(s.to_string())),
        }
    }

    fn bool_value(b: bool) -> Value {
        Value {
            kind: Some(ValueKind::BoolValue(b)),
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
        error_handler_calls: Arc<Mutex<u32>>,
        last_error: Arc<Mutex<Option<Error>>>,
    }

    impl TestProcessor {
        fn new(name: &str, opts: Option<ProcessingOptions>) -> Self {
            Self {
                name: name.to_string(),
                opts,
                phases: Arc::new(Mutex::new(Vec::new())),
                bodies: Arc::new(Mutex::new(HashMap::new())),
                errors: Arc::new(Mutex::new(Vec::new())),
                error_handler_calls: Arc::new(Mutex::new(0)),
                last_error: Arc::new(Mutex::new(None)),
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
        async fn error_handler(&self, _ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
            self.errors.lock().unwrap().push(phase);
            *self.error_handler_calls.lock().unwrap() += 1;
            *self.last_error.lock().unwrap() = Some(err);
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

    // Test processor that expresses a dynamic mode override (and optional
    // header mutations) during the request headers phase, and may error during
    // the request body phase.
    struct ModeOverrideProcessor {
        name: String,
        opts: Option<ProcessingOptions>,
        override_mode: Option<ModeOverride>,
        added_headers: Vec<(String, Vec<u8>)>,
        error_in_request_body: bool,
    }

    impl ModeOverrideProcessor {
        fn new(
            name: &str,
            opts: Option<ProcessingOptions>,
            override_mode: Option<ModeOverride>,
            added_headers: Vec<(String, Vec<u8>)>,
            error_in_request_body: bool,
        ) -> Self {
            Self {
                name: name.to_string(),
                opts,
                override_mode,
                added_headers,
                error_in_request_body,
            }
        }
    }

    #[async_trait]
    impl RequestProcessor for ModeOverrideProcessor {
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
            for (n, v) in &self.added_headers {
                ctx.add_header(
                    n,
                    &HeaderValue {
                        value: String::new(),
                        raw_value: Some(v.clone()),
                    },
                )?;
            }
            if let Some(mode) = &self.override_mode {
                ctx.override_mode(*mode)?;
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
            if self.error_in_request_body {
                return Err("mode override rejected in body phase".into());
            }
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
    async fn test_process_phase_mode_override_propagates_and_headers() {
        // An external processor that overrides the mode during the request
        // headers phase must surface it on the envoy-facing response, alongside
        // the header mutations it accumulated.
        let proc = Arc::new(ModeOverrideProcessor::new(
            "mode-override",
            Some(ProcessingOptions::new()),
            Some(ModeOverride {
                response_header_mode: HeaderSendMode::Default,
                request_body_mode: BodySendMode::Buffered,
                response_body_mode: BodySendMode::Streamed,
                request_trailer_mode: HeaderSendMode::Send,
                response_trailer_mode: HeaderSendMode::Skip,
            }),
            vec![("x-checked".to_string(), b"true".to_vec())],
            false,
        ));
        let server = GenericExtProcServer::new(
            "mode-override".to_string(),
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
        let response = resp.as_ref().unwrap();
        let mo = response
            .mode_override
            .as_ref()
            .expect("expected mode override on request headers response");
        assert_eq!(mo.response_header_mode, i32::from(HeaderSendMode::Default));
        assert_eq!(mo.request_body_mode, i32::from(BodySendMode::Buffered));
        assert_eq!(mo.response_body_mode, i32::from(BodySendMode::Streamed));
        assert_eq!(mo.request_trailer_mode, i32::from(HeaderSendMode::Send));
        assert_eq!(mo.response_trailer_mode, i32::from(HeaderSendMode::Skip));

        let (hm, _) = rc.resolved_header_mutation();
        let hm = hm.expect("expected a resolved header mutation");
        assert_eq!(hm.set_headers.len(), 1);
        assert_eq!(hm.set_headers[0].header.as_ref().unwrap().key, "x-checked");
        assert_eq!(
            hm.set_headers[0].header.as_ref().unwrap().raw_value,
            b"true".to_vec()
        );
    }

    #[tokio::test]
    async fn test_process_phase_mode_override_rejected_in_body_phase() {
        // A mode override expressed during the request body phase must be
        // rejected; with abort-on-failure the phase error propagates and is
        // counted, and the exchange ends errored.
        let server = Arc::new(GenericExtProcServer::new(
            "mode-override".to_string(),
            Arc::new(ModeOverrideProcessor::new(
                "mode-override",
                Some(ProcessingOptions::new()),
                Some(ModeOverride {
                    response_header_mode: HeaderSendMode::Default,
                    request_body_mode: BodySendMode::Buffered,
                    response_body_mode: BodySendMode::Streamed,
                    request_trailer_mode: HeaderSendMode::Send,
                    response_trailer_mode: HeaderSendMode::Skip,
                }),
                vec![],
                true,
            )),
            Some(ProcessingOptions {
                abort_on_processor_failure: true,
                ..ProcessingOptions::new()
            }),
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
            server: server.clone(),
            rx: out_rx,
            errored: false,
        };

        in_tx
            .send(Ok(processing_request(Some(
                ProcessingRequestOneof::RequestBody(HttpBody {
                    body: b"payload".to_vec(),
                    end_of_stream: true,
                    end_of_stream_without_message: false,
                    grpc_message_compressed: false,
                    drain_complete: false,
                }),
            ))))
            .await
            .unwrap();
        in_tx
            .send(Ok(processing_request(Some(
                ProcessingRequestOneof::RequestHeaders(http_headers(
                    Some(&envoy_header_map(&[(":method", "POST")])),
                    false,
                )),
            ))))
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for the phase response")
            .expect("stream ended before a phase response was produced");
        assert!(response.is_err());

        drop(in_tx);
        drop(stream);

        let registry = Registry::new();
        server.metrics.register(&registry).unwrap();
        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_phase_errors_total")
            .expect("expected phase errors family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 1.0);
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_errored_streams")
            .expect("expected errored streams family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 1.0);
    }

    // Inbound dynamic metadata observed during a phase, keyed by namespace and
    // then metadata key.
    #[derive(Default, Clone)]
    struct ObservedMetadata {
        namespaces: HashMap<String, HashMap<String, Value>>,
    }

    // Test processor that emits outbound dynamic metadata during the request
    // body phase and records the inbound metadata seen during the response body
    // phase.
    struct DynamicMetadataProcessor {
        name: String,
        opts: Option<ProcessingOptions>,
        emit_dynamic_metadata: bool,
        inbound_metadata: Arc<Mutex<Option<ObservedMetadata>>>,
    }

    impl DynamicMetadataProcessor {
        fn new(
            name: &str,
            opts: Option<ProcessingOptions>,
            emit_dynamic_metadata: bool,
            inbound_metadata: Arc<Mutex<Option<ObservedMetadata>>>,
        ) -> Self {
            Self {
                name: name.to_string(),
                opts,
                emit_dynamic_metadata,
                inbound_metadata,
            }
        }
    }

    #[async_trait]
    impl RequestProcessor for DynamicMetadataProcessor {
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
            if self.emit_dynamic_metadata {
                ctx.set_dynamic_metadata("com.example.out", "checked", bool_value(true))?;
            }
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
            *self.inbound_metadata.lock().unwrap() = Some(ObservedMetadata {
                namespaces: ctx.metadata.clone(),
            });
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
    async fn test_process_phase_dynamic_metadata_and_attributes() {
        // Inbound envoy dynamic metadata and evaluated CEL attributes are
        // merged into the context and persist across phases of the stream;
        // outbound dynamic metadata set during a phase is attached to that
        // phase's envoy-facing response, and cleared on the next phase.
        let inbound_metadata = Arc::new(Mutex::new(None));
        let server = GenericExtProcServer::new(
            "dynamic-metadata".to_string(),
            Arc::new(DynamicMetadataProcessor::new(
                "dynamic-metadata",
                Some(ProcessingOptions::new()),
                true,
                inbound_metadata.clone(),
            )),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );

        // Phase 1: inbound metadata_context and attributes, merged into context.
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let mut req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(
            http_headers(Some(&envoy_header_map(&[(":method", "POST")])), false),
        )));
        let mut fields = HashMap::new();
        fields.insert("sub".to_string(), string_value("alice"));
        let mut filter_metadata = HashMap::new();
        filter_metadata.insert(
            "com.example.auth".to_string(),
            Struct {
                fields: fields.clone(),
            },
        );
        let mut typed_filter_metadata = HashMap::new();
        typed_filter_metadata.insert(
            "com.example.typed".to_string(),
            Any {
                type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
                value: b"typed-v1".to_vec(),
            },
        );
        req.metadata_context = Some(Metadata {
            filter_metadata,
            typed_filter_metadata,
        });
        let mut attr_fields = HashMap::new();
        attr_fields.insert("request.protocol".to_string(), string_value("HTTP/2"));
        let mut attrs = HashMap::new();
        attrs.insert(
            "envoy.filters.http.ext_proc".to_string(),
            Struct {
                fields: attr_fields,
            },
        );
        req.attributes = attrs;
        server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(
            rc.metadata
                .get("com.example.auth")
                .and_then(|m| m.get("sub")),
            Some(&string_value("alice"))
        );
        assert_eq!(
            rc.typed_metadata
                .get("com.example.typed")
                .map(|a| a.type_url.clone()),
            Some("type.googleapis.com/google.protobuf.StringValue".to_string())
        );
        assert_eq!(
            rc.attributes.get("request.protocol"),
            Some(&string_value("HTTP/2"))
        );

        // Phase 2: request body — the processor emits outbound dynamic metadata,
        // which the phase response carries.
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::RequestBody(HttpBody {
            body: b"payload".to_vec(),
            end_of_stream: true,
            end_of_stream_without_message: false,
            grpc_message_compressed: false,
            drain_complete: false,
        })));
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestBody);
        let resp = resp.expect("expected a continue response");
        assert!(resp.dynamic_metadata.is_some());
        let envoy_struct = resp.dynamic_metadata.as_ref().unwrap();
        let emitted = envoy_struct
            .fields
            .get("com.example.out")
            .and_then(|v| v.kind.as_ref())
            .and_then(|k| match k {
                ValueKind::StructValue(s) => Some(s),
                _ => None,
            })
            .expect("expected a struct value for the emitted namespace");
        assert_eq!(emitted.fields.get("checked"), Some(&bool_value(true)));

        // Phase 3: response body — the inbound metadata persists across phases.
        rc.reset_phase().unwrap();
        let req = processing_request(Some(ProcessingRequestOneof::ResponseBody(HttpBody {
            body: b"resp".to_vec(),
            end_of_stream: true,
            end_of_stream_without_message: false,
            grpc_message_compressed: false,
            drain_complete: false,
        })));
        server.process_phase(&req, &mut rc).await.unwrap();
        let seen = inbound_metadata.lock().unwrap().take();
        let seen = seen.expect("expected inbound metadata during response body phase");
        assert_eq!(
            seen.namespaces
                .get("com.example.auth")
                .and_then(|m| m.get("sub")),
            Some(&string_value("alice"))
        );
    }

    #[tokio::test]
    async fn test_process_phase_protocol_config_captured_and_retained_across_phases() {
        // The envoy protocol configuration arrives with the first message of
        // the stream and must be captured, then retained across the phases of
        // that stream (phase resets must not clear it).
        let server = GenericExtProcServer::new(
            "test".to_string(),
            Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new()))),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();

        // No protocol config on the wire: the context keeps none.
        let req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
            Some(&envoy_header_map(&[(":method", "POST")])),
            false,
        ))));
        server.process_phase(&req, &mut rc).await.unwrap();
        assert!(rc.protocol_config.is_none());

        // Protocol configuration arrives with the first message: captured, and
        // retained across the phases of the stream.
        let mut req = processing_request(Some(ProcessingRequestOneof::RequestHeaders(
            http_headers(Some(&envoy_header_map(&[(":method", "POST")])), false),
        )));
        req.protocol_config = Some(ProtocolConfiguration {
            request_body_mode: i32::from(BodySendMode::Buffered),
            response_body_mode: i32::from(BodySendMode::Streamed),
            send_body_without_waiting_for_header_response: true,
        });
        server.process_phase(&req, &mut rc).await.unwrap();
        let pc = rc
            .protocol_config
            .as_ref()
            .expect("expected protocol config");
        assert_eq!(pc.request_body_mode, BodySendMode::Buffered);
        assert_eq!(pc.response_body_mode, BodySendMode::Streamed);
        assert!(pc.send_body_without_waiting_for_header_response);

        for phase_req in [
            ProcessingRequestOneof::RequestBody(HttpBody {
                body: b"payload".to_vec(),
                end_of_stream: true,
                end_of_stream_without_message: false,
                grpc_message_compressed: false,
                drain_complete: false,
            }),
            ProcessingRequestOneof::ResponseHeaders(HttpHeaders {
                headers: Some(envoy_header_map(&[])),
                attributes: HashMap::new(),
                end_of_stream: true,
            }),
            ProcessingRequestOneof::ResponseBody(HttpBody {
                body: b"resp".to_vec(),
                end_of_stream: true,
                end_of_stream_without_message: false,
                grpc_message_compressed: false,
                drain_complete: false,
            }),
        ] {
            let req = processing_request(Some(phase_req));
            server.process_phase(&req, &mut rc).await.unwrap();
            let pc = rc
                .protocol_config
                .as_ref()
                .expect("expected protocol config");
            assert_eq!(pc.request_body_mode, BodySendMode::Buffered);
            assert_eq!(pc.response_body_mode, BodySendMode::Streamed);
            assert!(pc.send_body_without_waiting_for_header_response);
        }

        // The protocol config is captured even when the request headers phase
        // was skipped (i.e. it arrives with the response headers phase).
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let mut req =
            processing_request(Some(ProcessingRequestOneof::ResponseHeaders(HttpHeaders {
                headers: Some(envoy_header_map(&[])),
                attributes: HashMap::new(),
                end_of_stream: true,
            })));
        req.protocol_config = Some(ProtocolConfiguration {
            request_body_mode: i32::from(BodySendMode::None),
            response_body_mode: i32::from(BodySendMode::Buffered),
            send_body_without_waiting_for_header_response: false,
        });
        server.process_phase(&req, &mut rc).await.unwrap();
        let pc = rc
            .protocol_config
            .as_ref()
            .expect("expected protocol config");
        assert_eq!(pc.request_body_mode, BodySendMode::None);
        assert_eq!(pc.response_body_mode, BodySendMode::Buffered);
        assert!(!pc.send_body_without_waiting_for_header_response);
    }

    #[tokio::test]
    async fn test_process_phase_default_mode_override_uses_protocol_config() {
        // A processor that starts from the context's default mode override
        // preserves envoy's configured response body mode until it overrides.
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.protocol_config = Some(ProtocolConfig {
            request_body_mode: BodySendMode::None,
            response_body_mode: BodySendMode::Buffered,
            send_body_without_waiting_for_header_response: false,
        });

        let mode = rc.default_mode_override();
        assert_eq!(mode.response_header_mode, HeaderSendMode::Default);
        assert_eq!(mode.request_body_mode, BodySendMode::None);
        assert_eq!(mode.response_body_mode, BodySendMode::Buffered);
        assert_eq!(mode.request_trailer_mode, HeaderSendMode::Default);
        assert_eq!(mode.response_trailer_mode, HeaderSendMode::Default);
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
    async fn test_process_phase_outbound_response_is_wire_valid() {
        // Fast protocol invariant check: every outbound `ProcessingResponse`
        // emitted by the SDK must be structurally wire-valid. Encode the
        // response and decode it back (prost message round-trip) so malformed
        // outbound frames surface here instead of as spurious envoy messages.
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
        let response = resp.as_ref().unwrap();
        let bytes = response.encode_to_vec();
        let decoded = ProcessingResponse::decode(&bytes[..])
            .expect("outbound ProcessingResponse must decode");
        assert_eq!(decoded, *response);
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
            server: server.clone(),
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

        // End the inbound stream so the phase task completes and observes its
        // histograms; the external processing histogram records the time spent
        // in phase processing (here, the sleeping processor's phase).
        drop(in_tx);
        let end = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for stream end");
        assert!(end.is_none());

        let registry = Registry::new();
        server.metrics.register(&registry).unwrap();
        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_external_processing_seconds")
            .expect("expected external processing seconds family");
        let histogram = family.metric[0].histogram.as_ref().unwrap();
        assert!(histogram.sample_sum.unwrap() > 0.0);
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

    #[test]
    fn test_is_stream_cancelled() {
        assert!(!is_stream_cancelled(None));
        let cancelled: Error = tonic::Status::cancelled("context canceled").into();
        assert!(is_stream_cancelled(Some(&cancelled)));
        let failed: Error = tonic::Status::internal("failed to send response to envoy").into();
        assert!(!is_stream_cancelled(Some(&failed)));
    }

    #[tokio::test]
    async fn test_stream_context_cancelled_while_idle_is_graceful() {
        // the stream context transitioning to done between phases is handled
        // like a stream cancellation: notify the error handler, end gracefully
        let proc = Arc::new(TestProcessor::new("test", Some(ProcessingOptions::new())));
        let server = Arc::new(GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
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
            server: server.clone(),
            rx: out_rx,
            errored: false,
        };

        // The stream is cancelled before any phase message arrives: the task
        // observes the cancellation error and must end the stream gracefully.
        in_tx
            .send(Err(tonic::Status::cancelled("context canceled")))
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for the stream to end");
        assert!(response.is_none());

        assert_eq!(*proc.error_handler_calls.lock().unwrap(), 1);
        let last_error = proc.last_error.lock().unwrap();
        let last_error = last_error
            .as_ref()
            .expect("expected the error handler to record an error");
        assert!(is_stream_cancelled(Some(last_error)));
        assert_eq!(
            proc.errors.lock().unwrap().last(),
            Some(&RequestPhase::Undetermined)
        );

        let registry = Registry::new();
        server.metrics.register(&registry).unwrap();
        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_phase_errors_total")
            .expect("expected phase errors family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 0.0);
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_errored_streams")
            .expect("expected errored streams family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 0.0);
    }
    // -------------------------------------------------------------------------
    // Observability mode (envoy `observability_mode: true`)
    // -------------------------------------------------------------------------

    struct ObservabilityProcessor {
        name: String,
        opts: Option<ProcessingOptions>,
        phases: Arc<Mutex<Vec<(RequestPhase, bool)>>>,
    }

    impl ObservabilityProcessor {
        fn new(name: &str, opts: Option<ProcessingOptions>) -> Self {
            Self {
                name: name.to_string(),
                opts,
                phases: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn record(&self, ctx: &RequestContext) {
            self.phases
                .lock()
                .unwrap()
                .push((ctx.get_phase(), ctx.observability_mode));
        }
    }

    #[async_trait]
    impl RequestProcessor for ObservabilityProcessor {
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
            self.record(ctx);
            ctx.continue_request()
        }
        async fn process_request_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            self.record(ctx);
            ctx.continue_request()
        }
        async fn process_request_body(
            &self,
            ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            self.record(ctx);
            ctx.continue_request()
        }
        async fn process_response_headers(
            &self,
            ctx: &mut RequestContext,
            _headers: &AllHeaders,
        ) -> Result<(), Error> {
            self.record(ctx);
            ctx.continue_request()
        }
        async fn process_response_body(
            &self,
            ctx: &mut RequestContext,
            _body: &[u8],
        ) -> Result<(), Error> {
            self.record(ctx);
            ctx.continue_request()
        }
        async fn process_response_trailers(
            &self,
            ctx: &mut RequestContext,
            _trailers: &AllHeaders,
        ) -> Result<(), Error> {
            self.record(ctx);
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

    fn processing_request_with_mode(
        request: Option<ProcessingRequestOneof>,
        observability_mode: bool,
    ) -> ProcessingRequest {
        ProcessingRequest {
            metadata_context: None,
            attributes: HashMap::new(),
            observability_mode,
            protocol_config: None,
            flow_control_init: None,
            client_window_update: None,
            request,
        }
    }

    #[tokio::test]
    async fn test_process_phase_observability_mode_suppresses_responses() {
        // In envoy `observability_mode: true`, processor callbacks run and the
        // SDK metrics are recorded, but no ProcessingResponse is sent (keeping
        // envoy's spurious_msgs_received == 0).
        let proc = Arc::new(ObservabilityProcessor::new(
            "obs",
            Some(ProcessingOptions {
                update_ext_proc_header: true,
                update_duration_header: true,
                abort_on_processor_failure: false,
            }),
        ));
        let server = GenericExtProcServer::new(
            "obs".to_string(),
            proc.clone(),
            Some(ProcessingOptions {
                update_ext_proc_header: true,
                update_duration_header: true,
                abort_on_processor_failure: false,
            }),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();

        let req_hdrs = processing_request_with_mode(
            Some(ProcessingRequestOneof::RequestHeaders(http_headers(
                Some(&envoy_header_map(&[(":method", "POST")])),
                false,
            ))),
            true,
        );
        let (resp, phase) = server.process_phase(&req_hdrs, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_none());

        let req_body = processing_request_with_mode(
            Some(ProcessingRequestOneof::RequestBody(http_body(
                b"hello", true,
            ))),
            true,
        );
        let (resp, phase) = server.process_phase(&req_body, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestBody);
        assert!(resp.is_none());

        let resp_hdrs = processing_request_with_mode(
            Some(ProcessingRequestOneof::ResponseHeaders(http_headers(
                Some(&envoy_header_map(&[(":status", "200")])),
                false,
            ))),
            true,
        );
        let (resp, phase) = server.process_phase(&resp_hdrs, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseHeaders);
        assert!(resp.is_none());

        let resp_body = processing_request_with_mode(
            Some(ProcessingRequestOneof::ResponseBody(http_body(
                b"world", true,
            ))),
            true,
        );
        let (resp, phase) = server.process_phase(&resp_body, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::ResponseBody);
        assert!(resp.is_none());

        // The processor saw the phases with observability_mode set, and no
        // response was ever sent (no spurious messages).
        let recorded = proc.phases.lock().unwrap().clone();
        assert_eq!(recorded.len(), 4);
        assert_eq!(
            recorded[0],
            (RequestPhase::RequestHeaders, true),
            "expected the request headers phase to run with observability_mode"
        );
        assert_eq!(
            recorded[3],
            (RequestPhase::ResponseBody, true),
            "expected the response body phase to run with observability_mode"
        );

        // Metrics: no phase errors, no empty responses, body bytes still
        // counted, no send errors. (Total streams is incremented by the
        // `ExternalProcessor::process` entrypoint, not by `process_phase`.)
        let registry = Registry::new();
        server.metrics.register(&registry).unwrap();
        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_phase_errors_total")
            .expect("expected phase errors family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 0.0);
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_empty_responses")
            .expect("expected empty responses family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 0.0);
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_response_send_errors")
            .expect("expected response send errors family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 0.0);
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_request_body_bytes_total")
            .expect("expected request body bytes family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 5.0);
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_response_body_bytes_total")
            .expect("expected response body bytes family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 5.0);
    }

    #[tokio::test]
    async fn test_process_phase_observability_mode_processor_error_counts_phase_error() {
        // Without AbortOnProcessorFailure, a phase error under observability
        // mode counts the phase error but sends no response and no empty
        // response.
        let proc = Arc::new(ErroringProcessor {
            opts: ProcessingOptions {
                abort_on_processor_failure: false,
                ..ProcessingOptions::new()
            },
        });
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request_with_mode(
            Some(ProcessingRequestOneof::RequestHeaders(http_headers(
                Some(&envoy_header_map(&[(":method", "GET")])),
                false,
            ))),
            true,
        );
        let (resp, phase) = server.process_phase(&req, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_none());

        let registry = Registry::new();
        server.metrics.register(&registry).unwrap();
        let families = registry.gather();
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_phase_errors_total")
            .expect("expected phase errors family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 1.0);
        let family = families
            .iter()
            .find(|f| f.name() == "extproc_empty_responses")
            .expect("expected empty responses family");
        assert_eq!(family.metric[0].counter.as_ref().unwrap().value(), 0.0);
    }

    #[tokio::test]
    async fn test_process_phase_observability_mode_abort_on_failure_propagates() {
        let proc = Arc::new(ErroringProcessor {
            opts: ProcessingOptions {
                abort_on_processor_failure: true,
                ..ProcessingOptions::new()
            },
        });
        let server = GenericExtProcServer::new(
            "test".to_string(),
            proc.clone(),
            Some(ProcessingOptions {
                abort_on_processor_failure: true,
                ..ProcessingOptions::new()
            }),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        let req = processing_request_with_mode(
            Some(ProcessingRequestOneof::RequestHeaders(http_headers(
                Some(&envoy_header_map(&[(":method", "GET")])),
                false,
            ))),
            true,
        );
        let err = server.process_phase(&req, &mut rc).await.unwrap_err();
        assert!(err.to_string().contains("processor exploded"));
    }

    #[tokio::test]
    async fn test_process_phase_header_existence_carries_into_body_phase() {
        // Mutations from an earlier phase of the same kind inform later phases:
        // the request headers phase adds a header (ADD_IF_ABSENT), and the
        // request body phase must resolve its adds against the carried key set
        // rather than an empty set.
        struct AddHeadersProcessor {
            opts: ProcessingOptions,
        }

        impl AddHeadersProcessor {
            fn new() -> Self {
                Self {
                    opts: ProcessingOptions::new(),
                }
            }
        }

        #[async_trait]
        impl RequestProcessor for AddHeadersProcessor {
            fn name(&self) -> String {
                "adder".to_string()
            }
            fn options(&self) -> &ProcessingOptions {
                &self.opts
            }
            async fn process_request_headers(
                &self,
                ctx: &mut RequestContext,
                _headers: &AllHeaders,
            ) -> Result<(), Error> {
                ctx.add_header(
                    "x-added-in-hdrs",
                    &HeaderValue {
                        value: String::new(),
                        raw_value: Some(b"v1".to_vec()),
                    },
                )?;
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
                // x-added-in-hdrs was added in the request headers phase, so
                // ADD_IF_ABSENT here must skip it; x-added-in-body is new and is
                // added.
                ctx.add_header(
                    "x-added-in-hdrs",
                    &HeaderValue {
                        value: String::new(),
                        raw_value: Some(b"v2".to_vec()),
                    },
                )?;
                ctx.add_header(
                    "x-added-in-body",
                    &HeaderValue {
                        value: String::new(),
                        raw_value: Some(b"v3".to_vec()),
                    },
                )?;
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

        let server = GenericExtProcServer::new(
            "adder".to_string(),
            Arc::new(AddHeadersProcessor::new()),
            Some(ProcessingOptions::new()),
            Metrics::new(),
        );
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();

        let req_hdrs = processing_request(Some(ProcessingRequestOneof::RequestHeaders(
            http_headers(Some(&envoy_header_map(&[(":method", "GET")])), false),
        )));
        let (resp, phase) = server.process_phase(&req_hdrs, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestHeaders);
        assert!(resp.is_some());
        let mutation = rc.response.header_mutation.as_ref().unwrap();
        assert_eq!(mutation.set_headers.len(), 1);
        assert_eq!(
            mutation.set_headers[0].header.as_ref().unwrap().key,
            "x-added-in-hdrs"
        );

        let req_body = processing_request(Some(ProcessingRequestOneof::RequestBody(http_body(
            b"body", true,
        ))));
        let (resp, phase) = server.process_phase(&req_body, &mut rc).await.unwrap();
        assert_eq!(phase, RequestPhase::RequestBody);
        assert!(resp.is_some());
        // The resolved mutation for the request body phase is filtered against
        // the carried phase header key set: the re-add of `x-added-in-hdrs`
        // (ADD_IF_ABSENT) is skipped because the request headers phase added it,
        // and only the new `x-added-in-body` add survives.
        let (hm, _) = rc.resolved_header_mutation();
        let set = hm
            .clone()
            .expect("expected a resolved mutation")
            .set_headers;
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].header.as_ref().unwrap().key, "x-added-in-body");
    }
}
