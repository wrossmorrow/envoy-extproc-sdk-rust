//! End-to-end integration tests: drive a real [`GenericExtProcServer`] over a
//! real gRPC stream, using the generated tonic client the way envoy does, and
//! assert on the [`ProcessingResponse`]s the server produces.

#![allow(clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use envoy_types::pb::envoy::config::core::v3::HeaderMap as EnvoyHeaderMap;
use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;
use envoy_types::pb::envoy::service::ext_proc::v3::body_mutation::Mutation as BodyMutationMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_client::ExternalProcessorClient;
use envoy_types::pb::envoy::service::ext_proc::v3::processing_request::Request as ProcessingRequestOneof;
use envoy_types::pb::envoy::service::ext_proc::v3::processing_response::Response as ResponseOneof;
use envoy_types::pb::envoy::service::ext_proc::v3::HttpBody;
use envoy_types::pb::envoy::service::ext_proc::v3::HttpHeaders;
use envoy_types::pb::envoy::service::ext_proc::v3::{
    CommonResponse, ImmediateResponse, ProcessingRequest, ProcessingResponse,
};

use extproc::{
    AllHeaders, Error, GenericExtProcServer, Metrics, ProcessingOptions, RequestContext,
    RequestProcessor, ServerOptions, TesterRequestProcessor,
};
use http_body_util::Full;
use hyper::body::Bytes as HyperBytes;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A free TCP port, for binding the server under test.
/// A spawned [`GenericExtProcServer`] serving via tonic on a real listener, plus
/// the addresses it bound and the task handle for shutdown.
struct RunningServer {
    grpc_addr: std::net::SocketAddr,
    metrics_addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<Result<(), extproc::Error>>,
}

impl RunningServer {
    async fn shutdown(self) {
        self.task.abort();
    }
}

async fn spawn_server(processor: Arc<dyn RequestProcessor>) -> RunningServer {
    let server = GenericExtProcServer::new(
        "testing".to_string(),
        processor,
        Some(ProcessingOptions::default()),
        Metrics::new(),
    );
    let task = tokio::spawn(async move { server.serve(ServerOptions::default()).await });
    let grpc_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 50051));
    let metrics_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 9090));

    // Wait for the gRPC server to accept connections.
    let deadline = Duration::from_secs(15);
    let start = std::time::Instant::now();
    loop {
        if ExternalProcessorClient::connect(format!("http://{}", grpc_addr))
            .await
            .is_ok()
        {
            break;
        }
        if start.elapsed() > deadline {
            panic!("extproc server did not become ready in time");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    RunningServer {
        grpc_addr,
        metrics_addr,
        task,
    }
}

// ---------------------------------------------------------------------------
// Wire-format helpers (mirrors the server unit tests)
// ---------------------------------------------------------------------------

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

fn envoy_request_headers(end_of_stream: bool, pairs: &[(&str, &str)]) -> ProcessingRequest {
    processing_request(Some(ProcessingRequestOneof::RequestHeaders(http_headers(
        Some(&envoy_header_map(pairs)),
        end_of_stream,
    ))))
}

fn envoy_response_headers(end_of_stream: bool) -> ProcessingRequest {
    processing_request(Some(ProcessingRequestOneof::ResponseHeaders(http_headers(
        Some(&envoy_header_map(&[])),
        end_of_stream,
    ))))
}

fn envoy_request_body(body: &[u8], end_of_stream: bool) -> ProcessingRequest {
    processing_request(Some(ProcessingRequestOneof::RequestBody(http_body(
        body,
        end_of_stream,
    ))))
}

fn envoy_response_body(body: &[u8], end_of_stream: bool) -> ProcessingRequest {
    processing_request(Some(ProcessingRequestOneof::ResponseBody(http_body(
        body,
        end_of_stream,
    ))))
}

// ---------------------------------------------------------------------------
// Response introspection helpers
// ---------------------------------------------------------------------------

fn response_common(resp: &ProcessingResponse) -> Option<&CommonResponse> {
    match &resp.response {
        Some(ResponseOneof::RequestHeaders(r)) => r.response.as_ref(),
        Some(ResponseOneof::ResponseHeaders(r)) => r.response.as_ref(),
        Some(ResponseOneof::RequestBody(r)) => r.response.as_ref(),
        Some(ResponseOneof::ResponseBody(r)) => r.response.as_ref(),
        _ => None,
    }
}

fn response_immediate(resp: &ProcessingResponse) -> Option<&ImmediateResponse> {
    match &resp.response {
        Some(ResponseOneof::ImmediateResponse(r)) => Some(r),
        _ => None,
    }
}

fn continue_status(resp: &ProcessingResponse) -> Option<i32> {
    response_common(resp).map(|c| c.status)
}

// ---------------------------------------------------------------------------
// Stream driver
// ---------------------------------------------------------------------------

/// Send one processing request and wait for its response.
async fn send_recv(
    tx: &mut mpsc::Sender<ProcessingRequest>,
    stream: &mut tonic::codec::Streaming<ProcessingResponse>,
    req: ProcessingRequest,
) -> Result<ProcessingResponse, tonic::Status> {
    tx.send(req)
        .await
        .expect("failed to send processing request");
    stream
        .next()
        .await
        .expect("failed to receive processing response")
}

// ---------------------------------------------------------------------------
// Metrics fetch over the axum metrics server
// ---------------------------------------------------------------------------

async fn fetch_metrics_text(metrics_addr: std::net::SocketAddr) -> String {
    let client: Client<HttpConnector, Full<HyperBytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let uri: hyper::http::Uri = format!("http://{}", metrics_addr).parse().unwrap();
    let resp = client
        .get(uri)
        .await
        .expect("failed to get metrics response");
    assert_eq!(resp.status(), 200, "metrics endpoint should return 200");
    let bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .expect("failed to collect metrics body")
        .to_bytes();
    String::from_utf8_lossy(&bytes).to_string()
}

fn metrics_counter_value(text: &str, name: &str) -> Option<f64> {
    text.lines().find_map(|l| {
        l.strip_prefix(name)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|v| v.parse::<f64>().ok())
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn integration_noop_stream() {
    // Spawn the real server binary as a subprocess.
    let bin = std::env::var("CARGO_BIN_EXE_extproc-tester").expect("CARGO_BIN_EXE_extproc-tester");
    let child = std::process::Command::new(bin)
        .spawn()
        .expect("failed to spawn server binary");
    // Ensure the spawned server is cleaned up even if the test fails.
    let _child_guard = ChildGuard { child };

    let grpc_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 50051));
    let deadline = Duration::from_secs(15);
    let start = std::time::Instant::now();
    let mut client = loop {
        if let Ok(c) = ExternalProcessorClient::connect(format!("http://{}", grpc_addr)).await {
            break c;
        }
        if start.elapsed() > deadline {
            panic!("extproc server did not become ready in time");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let (tx, rx) = mpsc::channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .expect("failed to open process stream")
        .into_inner();
    let mut tx = tx;

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "POST"), (":path", "/test")]),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));
    assert!(response_common(&resp)
        .expect("common response")
        .header_mutation
        .is_none());

    let resp = send_recv(&mut tx, &mut stream, envoy_request_body(b"{}", true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(&mut tx, &mut stream, envoy_response_headers(true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    // Clean end of stream.
    drop(tx);
    assert!(stream.next().await.is_none());
}

/// Kills and reaps a spawned server child when dropped, so a failed test
/// cannot leak a running server process.
struct ChildGuard {
    child: std::process::Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn integration_request_body_actions() {
    let actions = r#"{
        "add_request_headers": {"x-add-me": "add_value"},
        "overwrite_request_headers": {"x-overwrite": "new value"},
        "remove_request_headers": ["x-remove-me"],
        "replace_request_body": "brand new body"
    }"#;
    let server = spawn_server(Arc::new(TesterRequestProcessor::default())).await;
    let mut client = ExternalProcessorClient::connect(format!("http://{}", server.grpc_addr))
        .await
        .expect("failed to connect");
    let (tx, rx) = mpsc::channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .expect("failed to open process stream")
        .into_inner();
    let mut tx = tx;

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "POST"), (":path", "/test")]),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_body(actions.as_bytes(), true),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));
    let common = response_common(&resp).expect("common response in response");
    let hm = common
        .header_mutation
        .as_ref()
        .expect("header mutation in response");
    assert_eq!(hm.set_headers.len(), 3);
    assert_eq!(hm.remove_headers, vec!["x-remove-me".to_string()]);
    let bm = common
        .body_mutation
        .as_ref()
        .expect("body mutation in response");
    assert!(matches!(bm.mutation, Some(BodyMutationMutation::Body(_))));

    let resp = send_recv(&mut tx, &mut stream, envoy_response_headers(true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    drop(tx);
    assert!(stream.next().await.is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn integration_response_header_actions() {
    let actions = r#"{
        "add_response_headers": {"x-add-me": "true"},
        "append_response_headers": {"x-append-me": "append_value"},
        "overwrite_response_headers": {"content-type": "test/plain"},
        "remove_response_headers": ["date"]
    }"#;
    let server = spawn_server(Arc::new(TesterRequestProcessor::default())).await;
    let mut client = ExternalProcessorClient::connect(format!("http://{}", server.grpc_addr))
        .await
        .expect("failed to connect");
    let (tx, rx) = mpsc::channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .expect("failed to open process stream")
        .into_inner();
    let mut tx = tx;

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "POST"), (":path", "/test")]),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_body(actions.as_bytes(), true),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(&mut tx, &mut stream, envoy_response_headers(false))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));
    let common = response_common(&resp).expect("common response in response");
    let hm = common
        .header_mutation
        .as_ref()
        .expect("header mutation in response");
    assert_eq!(hm.set_headers.len(), 3);
    assert_eq!(hm.remove_headers, vec!["date".to_string()]);

    let resp = send_recv(&mut tx, &mut stream, envoy_response_body(b"original", true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    drop(tx);
    assert!(stream.next().await.is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn integration_response_body_actions() {
    let actions = r#"{ "replace_response_body": "replaced body" }"#;
    let server = spawn_server(Arc::new(TesterRequestProcessor::default())).await;
    let mut client = ExternalProcessorClient::connect(format!("http://{}", server.grpc_addr))
        .await
        .expect("failed to connect");
    let (tx, rx) = mpsc::channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .expect("failed to open process stream")
        .into_inner();
    let mut tx = tx;

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "POST"), (":path", "/test")]),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_body(actions.as_bytes(), true),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(&mut tx, &mut stream, envoy_response_headers(true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(&mut tx, &mut stream, envoy_response_body(b"original", true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));
    let common = response_common(&resp).expect("common response in response");
    let bm = common
        .body_mutation
        .as_ref()
        .expect("body mutation in response");
    assert!(matches!(bm.mutation, Some(BodyMutationMutation::Body(_))));

    drop(tx);
    assert!(stream.next().await.is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn integration_cancel_request() {
    let actions = r#"{
        "cancel_request": true,
        "cancel_request_status": 400,
        "cancel_request_headers": {"x-custom-header": "true"},
        "cancel_request_body": "{\"error\":\"BadRequest\"}"
    }"#;
    let server = spawn_server(Arc::new(TesterRequestProcessor::default())).await;
    let mut client = ExternalProcessorClient::connect(format!("http://{}", server.grpc_addr))
        .await
        .expect("failed to connect");
    let (tx, rx) = mpsc::channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .expect("failed to open process stream")
        .into_inner();
    let mut tx = tx;

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "POST"), (":path", "/test")]),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_body(actions.as_bytes(), true),
    )
    .await
    .expect("expected immediate response");
    let immediate = response_immediate(&resp).expect("immediate response in response");
    assert_eq!(immediate.status.as_ref().map(|s| s.code), Some(400));
    let hm = immediate.headers.as_ref().expect("immediate headers");
    assert_eq!(hm.set_headers.len(), 1);
    assert_eq!(
        hm.set_headers[0].header.as_ref().unwrap().key,
        "x-custom-header"
    );
    assert_eq!(
        hm.set_headers[0].header.as_ref().unwrap().raw_value,
        b"true".to_vec()
    );
    assert_eq!(immediate.body, b"{\"error\":\"BadRequest\"}".to_vec());

    drop(tx);
    assert!(stream.next().await.is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn integration_error_path() {
    struct ErroringProcessor {
        opts: ProcessingOptions,
    }

    impl RequestProcessor for ErroringProcessor {
        fn get_name(&self) -> String {
            "erroring".to_string()
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

    let server = spawn_server(Arc::new(ErroringProcessor {
        opts: ProcessingOptions::default(),
    }))
    .await;
    let mut client = ExternalProcessorClient::connect(format!("http://{}", server.grpc_addr))
        .await
        .expect("failed to connect");
    let (tx, rx) = mpsc::channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .expect("failed to open process stream")
        .into_inner();
    let mut tx = tx;

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "GET"), (":path", "/test")]),
    )
    .await;
    let status = resp.expect_err("expected phase error");
    assert_eq!(status.code(), tonic::Code::Internal);

    drop(tx);
    assert!(stream.next().await.is_none());

    // The phase error was counted and the stream was marked as errored.
    let text = fetch_metrics_text(server.metrics_addr).await;
    assert_eq!(
        metrics_counter_value(&text, "extproc_phase_errors_total"),
        Some(1.0)
    );
    assert_eq!(
        metrics_counter_value(&text, "extproc_errored_streams"),
        Some(1.0)
    );
    assert_eq!(
        metrics_counter_value(&text, "extproc_total_streams"),
        Some(1.0)
    );

    server.shutdown().await;
}

#[tokio::test]
async fn integration_health() {
    let server = spawn_server(Arc::new(TesterRequestProcessor::default())).await;
    let channel = tonic::transport::Channel::from_shared(format!("http://{}", server.grpc_addr))
        .expect("valid channel address")
        .connect()
        .await
        .expect("failed to connect health client");
    let mut health_client = tonic_health::pb::health_client::HealthClient::new(channel);
    let resp = health_client
        .check(tonic::Request::new(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        }))
        .await
        .expect("health check failed");
    assert_eq!(resp.into_inner().status, 1); // Serving
    server.shutdown().await;
}

#[tokio::test]
async fn integration_metrics_endpoint() {
    let server = spawn_server(Arc::new(TesterRequestProcessor::default())).await;
    let mut client = ExternalProcessorClient::connect(format!("http://{}", server.grpc_addr))
        .await
        .expect("failed to connect");
    let (tx, rx) = mpsc::channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .expect("failed to open process stream")
        .into_inner();
    let mut tx = tx;

    let resp = send_recv(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "POST"), (":path", "/test")]),
    )
    .await
    .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(&mut tx, &mut stream, envoy_request_body(b"hello", true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    let resp = send_recv(&mut tx, &mut stream, envoy_response_headers(true))
        .await
        .expect("expected continue response");
    assert_eq!(continue_status(&resp), Some(0));

    drop(tx);
    assert!(stream.next().await.is_none());

    let text = fetch_metrics_text(server.metrics_addr).await;
    assert!(text.contains("# EOF"));
    assert_eq!(
        metrics_counter_value(&text, "extproc_request_body_bytes_total"),
        Some(5.0)
    );
    assert_eq!(
        metrics_counter_value(&text, "extproc_total_streams"),
        Some(1.0)
    );
    assert_eq!(
        metrics_counter_value(&text, "extproc_total_request_body"),
        Some(1.0)
    );

    server.shutdown().await;
}
