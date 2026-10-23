//! The gRPC server implementation: [`RequestProcessor`] trait and
//! [`GenericExtProcServer`].

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

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
                    init_req_ctx(rc, h)?;
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
                    let trailers = gen_headers(
                        t.trailers
                            .as_ref()
                            .ok_or_else(|| "missing trailers".to_string())?,
                    )?;
                    self.processor.process_request_trailers(rc, &trailers)?;
                }
                Some(ProcessingRequestOneof::ResponseHeaders(h)) => {
                    *phase = REQUEST_PHASE_RESPONSE_HEADERS;
                    rc.set_phase(*phase);
                    metrics.total_response_headers.inc();
                    let headers = gen_headers(
                        h.headers
                            .as_ref()
                            .ok_or_else(|| "missing headers".to_string())?,
                    )?;
                    rc.end_of_stream = h.end_of_stream;
                    self.processor.process_response_headers(rc, &headers)?;
                    if options.as_ref().map_or(false, |o| o.update_ext_proc_header) {
                        rc.append_header(
                            "x-extproc-names",
                            &HeaderValue {
                                value: String::new(),
                                raw_value: Some(self.name.as_bytes().to_vec()),
                            },
                        )?;
                    }
                    if rc.end_of_stream
                        && options.as_ref().map_or(false, |o| o.update_duration_header)
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
                        && options.as_ref().map_or(false, |o| o.update_duration_header)
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
                    let trailers = gen_headers(
                        t.trailers
                            .as_ref()
                            .ok_or_else(|| "missing trailers".to_string())?,
                    )?;
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

        outcome
    }

    /// Set up and serve the ext_proc gRPC server, metrics endpoint, and
    /// health service.
    pub async fn serve(&self, server_options: crate::options::ServerOptions) -> Result<(), Error> {
        // metrics
        let registry = prometheus::Registry::new();
        let metrics = Metrics::new();
        metrics.register(&registry)?;

        let _metrics_router =
            axum::Router::new().route("/metrics", axum::routing::get(metrics_handler));

        // grpc server
        let health = crate::health::HealthServer::new_ready();
        let grpc_server = Server::builder()
            .max_concurrent_streams(server_options.max_concurrent_streams)
            .add_service(ExternalProcessorServer::new(self.clone()))
            .add_service(health.health_service());
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], server_options.ext_proc_port));
        let signal = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        let grpc_fut = grpc_server.serve_with_shutdown(addr, signal);
        grpc_fut
            .await
            .map_err(|e| format!("failed to serve extproc gRPC server: {}", e))?;

        // notify processor
        let _ = self.close(server_options.termination_grace_period_seconds);

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
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; encoding=utf-8")
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
