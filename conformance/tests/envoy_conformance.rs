//! Live Envoy conformance test suite for `envoy_extproc_sdk`.
//!
//! Each test in this file corresponds to an entry in `conformance/features.yaml`
//! (`envoy_conformance::<test_name>`). A single shared Envoy instance (via
//! `ENVOY_BIN` or Docker `envoyproxy/envoy:<ENVOY_VERSION>`) is booted once per
//! test binary invocation alongside in-process upstream and `ext_proc` servers.

#![allow(clippy::too_many_lines)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use envoy_conformance::{
    check_docker_available, mode_override_allowlist_denied_handler,
    mode_override_allowlist_ok_handler, mode_override_default_from_protocol_config_handler,
    mode_override_disabled_handler, mode_override_full_resp_handler, mode_override_handler,
    mode_override_resp_stream_handler, mode_override_skip_handler, protocol_config_handler,
    register_scenario_handlers, start_conformance_extproc_server, start_envoy_runner,
    upstream_host_for_runner, CaseHandler, ConformanceExtProcServer, ConformanceProcessor,
    EnvoyConfig, EnvoyRunner, UpstreamEcho, UpstreamServer,
};
use envoy_extproc_sdk::{
    AllHeaders, BodySendMode, Error, HeaderValue, ProcessingOptions, ProtocolConfig,
    RequestContext, RequestPhase,
};
use envoy_types::pb::envoy::config::core::v3::header_value_option::HeaderAppendAction;
use http_body::Frame;
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::body::Bytes as HyperBytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue as HyperHeaderValue};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio_stream::wrappers::ReceiverStream;

type HttpClient = Client<HttpConnector, BoxBody<HyperBytes, std::io::Error>>;

struct ConformanceHarness {
    rt: tokio::runtime::Runtime,
    _primary_upstream: UpstreamServer,
    _secondary_upstream: UpstreamServer,
    default_proc: ConformanceProcessor,
    abort_proc: ConformanceProcessor,
    secondary_proc: ConformanceProcessor,
    default_server: ConformanceExtProcServer,
    _abort_server: ConformanceExtProcServer,
    _secondary_server: ConformanceExtProcServer,
    metrics_lock: tokio::sync::Mutex<()>,
    envoy: Mutex<Option<EnvoyRunner>>,
    proxy_url: String,
    http1: HttpClient,
    h2c: HttpClient,
}

static HARNESS: OnceLock<Option<ConformanceHarness>> = OnceLock::new();

impl ConformanceHarness {
    /// The Envoy version this harness booted (or the resolved default when no
    /// Envoy was started, e.g. when the harness skipped the live run).
    fn booted_version(&self) -> String {
        self.envoy
            .lock()
            .unwrap()
            .as_ref()
            .map(|e| e.version().to_string())
            .unwrap_or_else(default_version)
    }
}

fn default_version() -> String {
    envoy_conformance::resolve_envoy_version(None).unwrap_or_else(|_| "v1.40-latest".to_string())
}

fn hv(s: &str) -> HeaderValue {
    HeaderValue {
        value: s.to_string(),
        raw_value: None,
    }
}

fn hv_raw(bytes: &[u8]) -> HeaderValue {
    HeaderValue {
        value: String::new(),
        raw_value: Some(bytes.to_vec()),
    }
}

fn require_envoy() -> bool {
    matches!(
        std::env::var("CONFORMANCE_REQUIRE_ENVOY").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn get_harness() -> Option<&'static ConformanceHarness> {
    HARNESS
        .get_or_init(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build conformance tokio runtime");
            let init = rt.block_on(async {
                let has_bin = std::env::var("ENVOY_BIN")
                    .map(|v| !v.is_empty())
                    .unwrap_or(false);
                if !has_bin && check_docker_available().await.is_err() {
                    if require_envoy() {
                        panic!("CONFORMANCE_REQUIRE_ENVOY=1 but neither ENVOY_BIN nor Docker is available");
                    }
                    eprintln!("skipping conformance test: neither ENVOY_BIN nor Docker is available");
                    return None;
                }

                let primary_upstream = UpstreamServer::start("primary")
                    .await
                    .expect("start primary upstream");
                let secondary_upstream = UpstreamServer::start("secondary")
                    .await
                    .expect("start secondary upstream");

                let default_proc = ConformanceProcessor::new(&ProcessingOptions::new());
                register_scenario_handlers(&default_proc).expect("register scenario handlers");

                let abort_opts = ProcessingOptions {
                    abort_on_processor_failure: true,
                    ..ProcessingOptions::new()
                };
                let abort_proc = ConformanceProcessor::with_name("conformance-abort", &abort_opts);
                let secondary_proc = ConformanceProcessor::with_name(
                    "conformance-secondary",
                    &ProcessingOptions::new(),
                );

                let (default_server, ext_proc_port) =
                    start_conformance_extproc_server(default_proc.clone())
                        .await
                        .expect("start default ext_proc server");
                let (abort_server, abort_ext_proc_port) =
                    start_conformance_extproc_server(abort_proc.clone())
                        .await
                        .expect("start abort ext_proc server");
                let (secondary_server, secondary_ext_proc_port) =
                    start_conformance_extproc_server(secondary_proc.clone())
                        .await
                        .expect("start secondary ext_proc server");

                let cfg = EnvoyConfig {
                    version: String::new(),
                    upstream_host: upstream_host_for_runner(),
                    primary_upstream_port: primary_upstream.port(),
                    secondary_upstream_port: secondary_upstream.port(),
                    ext_proc_port,
                    abort_ext_proc_port,
                    secondary_ext_proc_port,
                    listener_port: 0,
                    admin_port: 0,
                    bootstrap_json: String::new(),
                };

                let envoy = start_envoy_runner(&cfg)
                    .await
                    .expect("start live envoy runner");
                let proxy_url = envoy.proxy_url();

                let http1 = Client::builder(TokioExecutor::new()).build_http();
                let h2c = Client::builder(TokioExecutor::new())
                    .http2_only(true)
                    .build_http();

                Some((
                    primary_upstream,
                    secondary_upstream,
                    default_proc,
                    abort_proc,
                    secondary_proc,
                    default_server,
                    abort_server,
                    secondary_server,
                    envoy,
                    proxy_url,
                    http1,
                    h2c,
                ))
            });
            init.map(
                |(
                    primary_upstream,
                    secondary_upstream,
                    default_proc,
                    abort_proc,
                    secondary_proc,
                    default_server,
                    abort_server,
                    secondary_server,
                    envoy,
                    proxy_url,
                    http1,
                    h2c,
                )| ConformanceHarness {
                    rt,
                    _primary_upstream: primary_upstream,
                    _secondary_upstream: secondary_upstream,
                    default_proc,
                    abort_proc,
                    secondary_proc,
                    default_server,
                    _abort_server: abort_server,
                    _secondary_server: secondary_server,
                    metrics_lock: tokio::sync::Mutex::new(()),
                    envoy: Mutex::new(Some(envoy)),
                    proxy_url,
                    http1,
                    h2c,
                },
            )
        })
        .as_ref()
}

fn test_key() -> String {
    let name = std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_string();
    if name.contains("::") {
        name
    } else {
        format!("{}::{}", env!("CARGO_CRATE_NAME"), name)
    }
}

fn outcomes() -> &'static Mutex<HashMap<String, envoy_conformance::report::TestOutcome>> {
    static OUTCOMES: OnceLock<Mutex<HashMap<String, envoy_conformance::report::TestOutcome>>> =
        OnceLock::new();
    OUTCOMES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn record_outcome(test: &str, outcome: envoy_conformance::report::TestOutcome) {
    outcomes().lock().unwrap().insert(test.to_string(), outcome);
}

/// Write the accumulated conformance outcomes to `results-<version>.json` as
/// JSON-lines, in the `go test -json`-style shape consumed by the report
/// aggregator (`conformance report`).
fn write_conformance_results(version: &str) -> Result<(), String> {
    let outcomes = outcomes().lock().unwrap();
    let mut tests: Vec<(&String, &envoy_conformance::report::TestOutcome)> =
        outcomes.iter().collect();
    tests.sort_by(|a, b| a.0.cmp(b.0));
    let mut content = String::new();
    for (test, outcome) in tests {
        let action = match outcome {
            envoy_conformance::report::TestOutcome::Pass => "pass",
            envoy_conformance::report::TestOutcome::Fail => "fail",
            envoy_conformance::report::TestOutcome::Skip => "skip",
        };
        let line = serde_json::to_string(&serde_json::json!({
            "Action": action,
            "Package": "envoy-conformance",
            "Test": test,
        }))
        .map_err(|e| format!("marshal conformance outcome: {e}"))?;
        content.push_str(&line);
        content.push('\n');
    }
    if !content.is_empty() {
        std::fs::write(format!("results-{version}.json"), content)
            .map_err(|e| format!("write conformance results file: {e}"))?;
    }
    Ok(())
}

fn run_conformance<F, Fut>(f: F)
where
    F: FnOnce(&'static ConformanceHarness) -> Fut,
    Fut: Future<Output = ()>,
{
    let test = test_key();
    let Some(h) = get_harness() else {
        record_outcome(&test, envoy_conformance::report::TestOutcome::Skip);
        if let Err(e) = write_conformance_results(&default_version()) {
            panic!("failed to write conformance results: {e}");
        }
        return;
    };
    h.rt.block_on(f(h));
    record_outcome(&test, envoy_conformance::report::TestOutcome::Pass);
    if let Err(e) = write_conformance_results(&h.booted_version()) {
        panic!("failed to write conformance results: {e}");
    }
}

struct ResponseData {
    status: StatusCode,
    headers: HeaderMap,
    trailers: HeaderMap,
    body: Vec<u8>,
}

async fn send_request(
    client: &HttpClient,
    base_url: &str,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: BoxBody<HyperBytes, std::io::Error>,
) -> ResponseData {
    let uri: hyper::http::Uri = format!("{base_url}{path}").parse().expect("valid uri");
    let mut builder = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let req = builder.body(body).expect("build request");
    let resp = client.request(req).await.expect("send request to envoy");
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let collected = resp.into_body().collect().await.expect("collect response");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let body = collected.to_bytes().to_vec();
    ResponseData {
        status,
        headers: resp_headers,
        trailers,
        body,
    }
}

fn full_body(data: &[u8]) -> BoxBody<HyperBytes, std::io::Error> {
    Full::new(HyperBytes::from(data.to_vec()))
        .map_err(|never| match never {})
        .boxed()
}

fn chunked_body_with_trailers(
    chunks: Vec<Vec<u8>>,
    trailers: Vec<(&'static str, &'static str)>,
) -> BoxBody<HyperBytes, std::io::Error> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<HyperBytes>, std::io::Error>>(16);
    tokio::spawn(async move {
        for chunk in chunks {
            if tx
                .send(Ok(Frame::data(HyperBytes::from(chunk))))
                .await
                .is_err()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        if !trailers.is_empty() {
            let mut map = HeaderMap::new();
            for (k, v) in trailers {
                map.insert(HeaderName::from_static(k), HyperHeaderValue::from_static(v));
            }
            let _ = tx.send(Ok(Frame::trailers(map))).await;
        }
    });
    StreamBody::new(ReceiverStream::new(rx)).boxed()
}

fn decode_echo(body: &[u8]) -> UpstreamEcho {
    serde_json::from_slice(body).unwrap_or_else(|e| {
        panic!(
            "failed to decode UpstreamEcho: {e}\nbody: {}",
            String::from_utf8_lossy(body)
        )
    })
}

async fn assert_no_spurious(h: &ConformanceHarness) {
    let stats = {
        let guard = h.envoy.lock().unwrap();
        let runner = guard.as_ref().expect("envoy runner present");
        runner.admin_url()
    };
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<HyperBytes>>();
    let uri: hyper::http::Uri = format!("{stats}/stats?filter=ext_proc&format=json")
        .parse()
        .unwrap();
    let resp = client.get(uri).await.expect("query envoy stats");
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    if let Some(arr) = json.get("stats").and_then(|v| v.as_array()) {
        for entry in arr {
            let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let val = entry.get("value").and_then(|v| v.as_u64()).unwrap_or(0);
            if name.ends_with(".spurious_msgs_received") {
                assert_eq!(val, 0, "Envoy stat {name} = {val}, want 0");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 1–4. Headers & Skip Request Headers
// ---------------------------------------------------------------------------

struct HeaderMutationHandler;
impl CaseHandler for HeaderMutationHandler {
    fn request_headers(&self, ctx: &mut RequestContext, headers: &AllHeaders) -> Result<(), Error> {
        if headers.has("x-remove-me") {
            ctx.remove_header("x-remove-me")?;
        }
        ctx.add_header("x-added-req", &hv("added-val"))?;
        ctx.overwrite_header("x-overwrite-req", &hv("new-val"))?;
        ctx.continue_request()
    }

    fn response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.add_header("x-added-resp", &hv("resp-val"))?;
        ctx.overwrite_header("content-type", &hv("application/json; charset=utf-8"))?;
        ctx.continue_request()
    }
}

async fn run_headers_mutations(h: &ConformanceHarness, client: &HttpClient, case_id: &str) {
    h.default_proc
        .register(case_id, Arc::new(HeaderMutationHandler));
    let resp = send_request(
        client,
        &h.proxy_url,
        Method::POST,
        "/mode/buffered/headers",
        &[
            ("x-conformance-case", case_id),
            ("x-remove-me", "gone"),
            ("x-overwrite-req", "old-val"),
        ],
        full_body(b"hello"),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(
        resp.headers
            .get("x-added-resp")
            .and_then(|v| v.to_str().ok()),
        Some("resp-val")
    );
    let echo = decode_echo(&resp.body);
    assert!(!echo.headers.contains_key("x-remove-me"));
    assert_eq!(
        echo.headers.get("x-added-req"),
        Some(&vec!["added-val".to_string()])
    );
    assert_eq!(
        echo.headers.get("x-overwrite-req"),
        Some(&vec!["new-val".to_string()])
    );
    assert_no_spurious(h).await;
}

#[test]
fn headers_mutations_http1() {
    run_conformance(|h| async move {
        run_headers_mutations(h, &h.http1, "headers_mutations_http1").await;
    });
}

#[test]
fn headers_mutations_h2c() {
    run_conformance(|h| async move {
        run_headers_mutations(h, &h.h2c, "headers_mutations_h2c").await;
    });
}

struct MultiValueStateHandler {
    observed_raw: Mutex<Vec<Vec<u8>>>,
    observed_split: Mutex<Vec<String>>,
    cross_phase_seen: AtomicBool,
}
impl CaseHandler for MultiValueStateHandler {
    fn request_headers(&self, ctx: &mut RequestContext, headers: &AllHeaders) -> Result<(), Error> {
        *self.observed_raw.lock().unwrap() = headers.raw_values("x-multi");
        *self.observed_split.lock().unwrap() = headers.split_list("x-list");
        let _ = ctx.set_value(MultiState {
            case_id: "headers_multivalue_and_context_state".to_string(),
            tag: "from-req-headers".to_string(),
        });
        ctx.continue_request()
    }

    fn response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        // Cross-phase state via header or context
        self.cross_phase_seen.store(true, Ordering::SeqCst);
        ctx.add_header("x-resp-multi", &hv("one"))?;
        ctx.append_header("x-resp-multi", &hv("two"))?;
        ctx.continue_request()
    }
}

#[derive(Clone)]
struct MultiState {
    #[allow(dead_code)]
    case_id: String,
    #[allow(dead_code)]
    tag: String,
}

#[test]
fn headers_multivalue_and_context_state() {
    run_conformance(|h| async move {
        let case_id = "headers_multivalue_and_context_state";
        let handler = Arc::new(MultiValueStateHandler {
            observed_raw: Mutex::new(Vec::new()),
            observed_split: Mutex::new(Vec::new()),
            cross_phase_seen: AtomicBool::new(false),
        });
        h.default_proc.register(case_id, handler.clone());

        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/none/multi",
            &[
                ("x-conformance-case", case_id),
                ("x-multi", "alpha"),
                ("x-multi", "beta"),
                ("x-list", "a, b , c"),
            ],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let vals: Vec<&str> = resp
            .headers
            .get_all("x-resp-multi")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect();
        assert_eq!(vals, vec!["one", "two"]);
        assert!(!handler.observed_raw.lock().unwrap().is_empty());
        assert_eq!(
            *handler.observed_split.lock().unwrap(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(handler.cross_phase_seen.load(Ordering::SeqCst));
        assert_no_spurious(h).await;
    });
}

struct SkipReqHeadersHandler {
    req_headers_called: AtomicBool,
}
impl CaseHandler for SkipReqHeadersHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        self.req_headers_called.store(true, Ordering::SeqCst);
        ctx.continue_request()
    }

    fn response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.add_header("x-skip-req-resp", &hv("ok"))?;
        ctx.continue_request()
    }
}

#[test]
fn skip_request_headers_response_mutation() {
    run_conformance(|h| async move {
        let case_id = "skip_request_headers_response_mutation";
        let handler = Arc::new(SkipReqHeadersHandler {
            req_headers_called: AtomicBool::new(false),
        });
        h.default_proc.register(case_id, handler.clone());

        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/skip-req-headers/test",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert!(!handler.req_headers_called.load(Ordering::SeqCst));
        assert_eq!(
            resp.headers
                .get("x-skip-req-resp")
                .and_then(|v| v.to_str().ok()),
            Some("ok")
        );
        assert_no_spurious(h).await;
    });
}

struct AppendActionHandler;
impl CaseHandler for AppendActionHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        // Already present on request -> ADD_IF_ABSENT must NOT overwrite
        ctx.add_header("x-existing", &hv("should-not-overwrite"))?;
        // Already present on request -> APPEND_IF_EXISTS_OR_ADD must append
        ctx.append_header("x-append-me", &hv("second"))?;
        // Already present on request -> OVERWRITE_IF_EXISTS must overwrite
        ctx.update_header(
            "x-overwrite-existing",
            &hv("updated"),
            HeaderAppendAction::OverwriteIfExists,
        )?;
        // Absent on request -> OVERWRITE_IF_EXISTS must NOT add
        ctx.update_header(
            "x-absent-no-add",
            &hv("nope"),
            HeaderAppendAction::OverwriteIfExists,
        )?;
        ctx.continue_request()
    }
}

#[test]
fn header_append_action_envoy_gap() {
    run_conformance(|h| async move {
        let case_id = "header_append_action_envoy_gap";
        h.default_proc
            .register(case_id, Arc::new(AppendActionHandler));

        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/none/append-action",
            &[
                ("x-conformance-case", case_id),
                ("x-existing", "original"),
                ("x-append-me", "first"),
                ("x-overwrite-existing", "stale"),
            ],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(
            echo.headers.get("x-existing"),
            Some(&vec!["original".to_string()])
        );
        assert_eq!(
            echo.headers.get("x-append-me"),
            Some(&vec!["first".to_string(), "second".to_string()])
        );
        assert_eq!(
            echo.headers.get("x-overwrite-existing"),
            Some(&vec!["updated".to_string()])
        );
        assert!(!echo.headers.contains_key("x-absent-no-add"));
        assert_no_spurious(h).await;
    });
}

// ---------------------------------------------------------------------------
// 5–9. Body Modes (NONE, BUFFERED, STREAMED, BUFFERED_PARTIAL)
// ---------------------------------------------------------------------------

#[test]
fn get_nobody_mode_none() {
    run_conformance(|h| async move {
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/none/get",
            &[("x-conformance-case", "get_nobody_mode_none")],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(echo.method, "GET");
        assert_eq!(echo.body_bytes, 0);
        assert_no_spurious(h).await;
    });
}

#[test]
fn post_with_body_mode_none() {
    run_conformance(|h| async move {
        let payload = b"passthrough-body-123";
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/none/post",
            &[("x-conformance-case", "post_with_body_mode_none")],
            full_body(payload),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(echo.body, "passthrough-body-123");
        assert_eq!(echo.body_bytes, payload.len());
        assert_no_spurious(h).await;
    });
}

struct BufferedGrowShrinkHandler {
    req_replacement: Vec<u8>,
    resp_replacement: Vec<u8>,
}
impl CaseHandler for BufferedGrowShrinkHandler {
    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.replace_body(&self.req_replacement)?;
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.replace_body(&self.resp_replacement)?;
        ctx.continue_request()
    }
}

#[test]
fn buffered_body_replace_grow_and_shrink() {
    run_conformance(|h| async move {
        let grow_id = "buffered_body_grow";
        let grown = b"this-is-a-much-longer-replaced-request-body-than-the-input";
        h.default_proc.register(
            grow_id,
            Arc::new(BufferedGrowShrinkHandler {
                req_replacement: grown.to_vec(),
                resp_replacement: b"grown-resp".to_vec(),
            }),
        );
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered/grow",
            &[
                ("x-conformance-case", grow_id),
                ("x-upstream-raw-body", "hi"),
            ],
            full_body(b"tiny"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.body, b"grown-resp");

        let shrink_id = "buffered_body_shrink";
        h.default_proc.register(
            shrink_id,
            Arc::new(BufferedGrowShrinkHandler {
                req_replacement: b"sm".to_vec(),
                resp_replacement: b"ok".to_vec(),
            }),
        );
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered/shrink",
            &[
                ("x-conformance-case", shrink_id),
                (
                    "x-upstream-raw-body",
                    "a-very-long-upstream-response-body-that-will-be-shrunk",
                ),
            ],
            full_body(b"a-very-long-request-body-that-will-be-shrunk"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.body, b"ok");
        assert_no_spurious(h).await;
    });
}

struct BufferedClearHandler;
impl CaseHandler for BufferedClearHandler {
    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.clear_body()?;
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.clear_body()?;
        ctx.continue_request()
    }
}

#[test]
fn buffered_body_clear() {
    run_conformance(|h| async move {
        let case_id = "buffered_body_clear";
        h.default_proc
            .register(case_id, Arc::new(BufferedClearHandler));
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered/clear",
            &[("x-conformance-case", case_id)],
            full_body(b"to-be-cleared"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert!(resp.body.is_empty());
        assert_no_spurious(h).await;
    });
}

#[test]
fn http1_chunked_response_buffered_replace_and_clear() {
    run_conformance(|h| async move {
        let case_id = "http1_chunked_replace";
        h.default_proc.register(
            case_id,
            Arc::new(BufferedGrowShrinkHandler {
                req_replacement: b"req-ok".to_vec(),
                resp_replacement: b"chunked-replaced".to_vec(),
            }),
        );
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered/chunked",
            &[
                ("x-conformance-case", case_id),
                ("x-upstream-raw-body", "chunk1-chunk2-chunk3-chunk4"),
                ("x-upstream-chunks", "4"),
            ],
            full_body(b"req"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.body, b"chunked-replaced");

        let clear_id = "http1_chunked_clear";
        h.default_proc
            .register(clear_id, Arc::new(BufferedClearHandler));
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered/chunked-clear",
            &[
                ("x-conformance-case", clear_id),
                ("x-upstream-raw-body", "chunk1-chunk2-chunk3"),
                ("x-upstream-chunks", "3"),
            ],
            full_body(b"req"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert!(resp.body.is_empty());
        assert_no_spurious(h).await;
    });
}

struct StreamedReplaceHandler;
impl CaseHandler for StreamedReplaceHandler {
    fn request_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
        let upper = String::from_utf8_lossy(body).to_ascii_uppercase();
        ctx.replace_body_chunk(upper.as_bytes())?;
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
        let upper = String::from_utf8_lossy(body).to_ascii_uppercase();
        ctx.replace_body_chunk(upper.as_bytes())?;
        ctx.continue_request()
    }
}

#[test]
fn streamed_body_chunks() {
    run_conformance(|h| async move {
        let case_id = "streamed_body_chunks";
        h.default_proc
            .register(case_id, Arc::new(StreamedReplaceHandler));
        let body = chunked_body_with_trailers(
            vec![b"hello ".to_vec(), b"streamed ".to_vec(), b"world".to_vec()],
            Vec::new(),
        );
        let resp = send_request(
            &h.h2c,
            &h.proxy_url,
            Method::POST,
            "/mode/streamed/chunks",
            &[
                ("x-conformance-case", case_id),
                ("x-upstream-raw-body", "resp chunk one resp chunk two"),
                ("x-upstream-chunks", "3"),
            ],
            body,
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.body, b"RESP CHUNK ONE RESP CHUNK TWO");
        assert_no_spurious(h).await;
    });
}

struct StreamedClearHandler;
impl CaseHandler for StreamedClearHandler {
    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.clear_body_chunk()?;
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.clear_body_chunk()?;
        ctx.continue_request()
    }
}

#[test]
fn streamed_request_and_response_clear_chunk() {
    run_conformance(|h| async move {
        let case_id = "streamed_request_and_response_clear_chunk";
        h.default_proc
            .register(case_id, Arc::new(StreamedClearHandler));
        let body =
            chunked_body_with_trailers(vec![b"part1".to_vec(), b"part2".to_vec()], Vec::new());
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/streamed/clear",
            &[
                ("x-conformance-case", case_id),
                ("x-upstream-raw-body", "upstream-body-to-clear"),
                ("x-upstream-chunks", "2"),
            ],
            body,
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert!(resp.body.is_empty());
        assert_no_spurious(h).await;
    });
}

struct BufferedPartialHandler {
    req_chunks: AtomicUsize,
}
impl CaseHandler for BufferedPartialHandler {
    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        self.req_chunks.fetch_add(1, Ordering::SeqCst);
        ctx.continue_request()
    }
}

#[test]
fn buffered_partial_mode() {
    run_conformance(|h| async move {
        let case_id = "buffered_partial_mode";
        let handler = Arc::new(BufferedPartialHandler {
            req_chunks: AtomicUsize::new(0),
        });
        h.default_proc.register(case_id, handler.clone());

        // Within buffer limit (1024 bytes)
        let small = vec![b'a'; 128];
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered-partial-small/within",
            &[("x-conformance-case", case_id)],
            full_body(&small),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(echo.body_bytes, 128);

        // Exceeding buffer limit (1024 bytes)
        let large = vec![b'b'; 4096];
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered-partial-small/exceed",
            &[("x-conformance-case", case_id)],
            full_body(&large),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(echo.body_bytes, 4096);
        assert!(handler.req_chunks.load(Ordering::SeqCst) >= 2);
        assert_no_spurious(h).await;
    });
}

// ---------------------------------------------------------------------------
// 10. Trailers (HTTP/1.1 & h2c)
// ---------------------------------------------------------------------------

struct TrailersHandler {
    saw_req_trailer: AtomicBool,
    saw_resp_trailer: AtomicBool,
}
impl CaseHandler for TrailersHandler {
    fn request_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        if trailers.get("x-req-trailer").as_deref() == Some("req-tval") {
            self.saw_req_trailer.store(true, Ordering::SeqCst);
        }
        ctx.add_header("x-added-req-trailer", &hv("added-rt"))?;
        ctx.continue_request()
    }

    fn response_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        if trailers.get("x-up-trailer").as_deref() == Some("up-tval") {
            self.saw_resp_trailer.store(true, Ordering::SeqCst);
        }
        ctx.add_header("x-added-resp-trailer", &hv("added-respt"))?;
        ctx.continue_request()
    }
}

async fn run_trailers_roundtrip(
    h: &ConformanceHarness,
    client: &HttpClient,
    case_id: &str,
    is_http1: bool,
) {
    let handler = Arc::new(TrailersHandler {
        saw_req_trailer: AtomicBool::new(false),
        saw_resp_trailer: AtomicBool::new(false),
    });
    h.default_proc.register(case_id, handler.clone());
    let body = chunked_body_with_trailers(
        vec![b"trailer-body".to_vec()],
        vec![("x-req-trailer", "req-tval")],
    );
    let mut headers = vec![
        ("x-conformance-case", case_id),
        ("x-upstream-resp-trailer", "x-up-trailer=up-tval"),
    ];
    if is_http1 {
        headers.push(("trailer", "x-req-trailer"));
        headers.push(("te", "trailers"));
    }
    let resp = send_request(
        client,
        &h.proxy_url,
        Method::POST,
        "/mode/trailers/roundtrip",
        &headers,
        body,
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(handler.saw_req_trailer.load(Ordering::SeqCst));
    assert!(
        handler.saw_resp_trailer.load(Ordering::SeqCst),
        "ext_proc did not see response trailer x-up-trailer"
    );
    assert_eq!(
        resp.trailers
            .get("x-up-trailer")
            .and_then(|v| v.to_str().ok()),
        Some("up-tval"),
        "missing x-up-trailer; trailers={:?}, headers={:?}",
        resp.trailers,
        resp.headers
    );
    assert_eq!(
        resp.trailers
            .get("x-added-resp-trailer")
            .and_then(|v| v.to_str().ok()),
        Some("added-respt")
    );
    assert_no_spurious(h).await;
}

#[test]
fn http1_trailers_roundtrip() {
    run_conformance(|h| async move {
        run_trailers_roundtrip(h, &h.http1, "http1_trailers_roundtrip", true).await;
    });
}

#[test]
fn h2c_trailers_roundtrip() {
    run_conformance(|h| async move {
        run_trailers_roundtrip(h, &h.h2c, "h2c_trailers_roundtrip", false).await;
    });
}

// ---------------------------------------------------------------------------
// 10. Dynamic mode override (mode_override)
// ---------------------------------------------------------------------------

#[test]
fn mode_override() {
    run_conformance(|h| async move {
        // Sub-scenario: opt in to BUFFERED bodies and trailers for a full
        // exchange via a dynamic mode override during the request headers phase,
        // then replace the bodies and trailers.
        {
            let case_id = "mode-override-full-req";
            h.default_proc.register(case_id, mode_override_handler());
            let body = chunked_body_with_trailers(
                vec![b"raw-req".to_vec()],
                vec![("x-req-tr", "orig-req")],
            );
            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/profile/mode-override/full",
                &[
                    ("x-conformance-case", case_id),
                    ("x-upstream-resp-trailer", "x-resp-tr=orig-resp"),
                    ("trailer", "x-req-tr"),
                    ("te", "trailers"),
                    ("content-type", "application/json"),
                ],
                body,
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);
            let echo = decode_echo(&resp.body);
            assert_eq!(echo.body, "mutated-req");
            assert_eq!(
                resp.trailers.get("x-resp-tr").and_then(|v| v.to_str().ok()),
                Some("mutated-resp-tr"),
                "unexpected response trailers: {:?}",
                resp.trailers
            );
            assert_eq!(
                resp.trailers
                    .get("x-resp-added-tr")
                    .and_then(|v| v.to_str().ok()),
                Some("added")
            );
            assert_no_spurious(h).await;
        }

        // Sub-scenario: replace the response body via a BUFFERED mode override
        // expressed during the request headers phase.
        {
            let case_id = "mode-override-full-resp";
            h.default_proc
                .register(case_id, mode_override_full_resp_handler());

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/profile/mode-override/full",
                &[
                    ("x-conformance-case", case_id),
                    ("x-upstream-raw-body", "raw-resp"),
                ],
                full_body(b"raw-req"),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);
            assert_eq!(resp.body, b"mutated-resp");
            assert_no_spurious(h).await;
        }

        // Sub-scenario: skip all remaining phases via SkipRemainingPhases,
        // after adding a header during the request headers phase.
        {
            let case_id = "mode-override-skip";
            h.default_proc
                .register(case_id, mode_override_skip_handler());

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/profile/mode-override/skip",
                &[("x-conformance-case", case_id)],
                full_body(b"untouched"),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);
            let echo = decode_echo(&resp.body);
            assert_eq!(echo.body, "untouched");
            assert_eq!(
                echo.headers
                    .get("x-fast-path")
                    .and_then(|v| v.first())
                    .map(|v| v.as_str()),
                Some("yes"),
                "unexpected echo headers: {:?}",
                echo.headers
            );
            assert_no_spurious(h).await;
        }

        // Sub-scenario: opt in to a STREAMED response body mode during the
        // response headers phase and replace the streamed chunks.
        {
            let case_id = "mode-override-resp-stream";
            h.default_proc
                .register(case_id, mode_override_resp_stream_handler());

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::GET,
                "/profile/mode-override/resp-stream",
                &[
                    ("x-conformance-case", case_id),
                    ("x-upstream-raw-body", "part1-part2-part3"),
                    ("x-upstream-chunks", "3"),
                ],
                full_body(b""),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);
            assert_eq!(resp.body, b"PART1-PART2-PART3");
            assert_no_spurious(h).await;
        }

        // Sub-scenario: when the filter is configured without allow_mode_override
        // (baseline NONE body mode), the override is ignored and the exchange
        // continues on the baseline.
        {
            let case_id = "mode-override-disabled";
            h.default_proc
                .register(case_id, mode_override_disabled_handler());

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/mode/none/override-disabled",
                &[("x-conformance-case", case_id)],
                full_body(b"no-override"),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);
            let echo = decode_echo(&resp.body);
            assert_eq!(echo.body, "no-override");
            assert_no_spurious(h).await;
        }

        // Sub-scenario: allowed_override_modes accepts the override and the
        // body phase runs; a disallowed override is dropped and the body phase
        // does not run.
        {
            let allowed_case = "mode-override-allowlist-ok";
            let allowed_saw_body = Arc::new(AtomicBool::new(false));
            h.default_proc.register(
                allowed_case,
                mode_override_allowlist_ok_handler(allowed_saw_body.clone()),
            );
            let allowed_resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/profile/mode-override-allowlist/check",
                &[("x-conformance-case", allowed_case)],
                full_body(b"probe"),
            )
            .await;
            assert_eq!(allowed_resp.status, StatusCode::OK);
            assert!(allowed_saw_body.load(Ordering::SeqCst));

            let denied_case = "mode-override-allowlist-denied";
            let denied_saw_body = Arc::new(AtomicBool::new(false));
            h.default_proc.register(
                denied_case,
                mode_override_allowlist_denied_handler(denied_saw_body.clone()),
            );
            let denied_resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/profile/mode-override-allowlist/check",
                &[("x-conformance-case", denied_case)],
                full_body(b"probe"),
            )
            .await;
            assert_eq!(denied_resp.status, StatusCode::OK);
            assert!(!denied_saw_body.load(Ordering::SeqCst));
            assert_no_spurious(h).await;
        }
    });
}

// ---------------------------------------------------------------------------
// 10b. Protocol configuration (protocol_config)
// ---------------------------------------------------------------------------

#[test]
fn protocol_config() {
    run_conformance(|h| async move {
        // Sub-scenario: the envoy protocol configuration is captured from the
        // first message of the stream and retained across the phases, and
        // reflects the filter's configured body modes for the probed paths.
        let cases = [
            (
                "none",
                ProtocolConfig {
                    request_body_mode: BodySendMode::None,
                    response_body_mode: BodySendMode::None,
                    send_body_without_waiting_for_header_response: false,
                },
                false,
            ),
            (
                "buffered",
                ProtocolConfig {
                    request_body_mode: BodySendMode::Buffered,
                    response_body_mode: BodySendMode::Buffered,
                    send_body_without_waiting_for_header_response: false,
                },
                true,
            ),
            (
                "streamed",
                ProtocolConfig {
                    request_body_mode: BodySendMode::Streamed,
                    response_body_mode: BodySendMode::Streamed,
                    send_body_without_waiting_for_header_response: false,
                },
                true,
            ),
            (
                "buffered-partial",
                ProtocolConfig {
                    request_body_mode: BodySendMode::BufferedPartial,
                    response_body_mode: BodySendMode::BufferedPartial,
                    send_body_without_waiting_for_header_response: false,
                },
                true,
            ),
        ];

        for (name, want, expect_bodies) in cases {
            let case_id = format!("proto-cfg-{name}");
            let recorder = Arc::new(Mutex::new(HashMap::new()));
            h.default_proc
                .register(&case_id, protocol_config_handler(recorder.clone()));

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                &format!("/mode/{name}/protocol-config"),
                &[("x-conformance-case", &case_id)],
                full_body(b"probe-body"),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);

            let expected_phases: &[&str] = if expect_bodies {
                &["req_headers", "resp_headers", "req_body", "resp_body"]
            } else {
                &["req_headers", "resp_headers"]
            };
            {
                let observed = recorder.lock().unwrap();
                for phase in expected_phases.iter().copied() {
                    let got = observed.get(phase);
                    assert_eq!(
                        got,
                        Some(&want),
                        "phase {phase} ProtocolConfig = {got:?}, want {want:?}"
                    );
                }
                if !expect_bodies {
                    assert!(!observed.contains_key("req_body"));
                    assert!(!observed.contains_key("resp_body"));
                }
            }
            assert_no_spurious(h).await;
        }

        // Sub-scenario: with `request_header_mode: SKIP`, the protocol
        // configuration arrives with the response headers phase and must be
        // captured there (the request headers phase never runs).
        {
            let case_id = "proto-cfg-skip-req-hdrs";
            let recorder = Arc::new(Mutex::new(HashMap::new()));
            h.default_proc
                .register(case_id, protocol_config_handler(recorder.clone()));

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::GET,
                "/mode/skip-req-headers/protocol-config",
                &[("x-conformance-case", case_id)],
                full_body(b""),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);

            {
                let observed = recorder.lock().unwrap();
                assert_eq!(
                    observed.get("resp_headers"),
                    Some(&ProtocolConfig {
                        request_body_mode: BodySendMode::None,
                        response_body_mode: BodySendMode::Buffered,
                        send_body_without_waiting_for_header_response: false,
                    })
                );
                assert_eq!(
                    observed.get("resp_body"),
                    Some(&ProtocolConfig {
                        request_body_mode: BodySendMode::None,
                        response_body_mode: BodySendMode::Buffered,
                        send_body_without_waiting_for_header_response: false,
                    })
                );
                assert!(observed.get("req_headers").is_none());
            }
            assert_no_spurious(h).await;
        }

        // Sub-scenario: `send_body_without_waiting_for_header_response` is
        // surfaced on the captured protocol configuration, alongside the
        // streamed body modes of the `send-body-without-waiting` profile.
        {
            let case_id = "proto-cfg-no-wait";
            let recorder = Arc::new(Mutex::new(HashMap::new()));
            h.default_proc
                .register(case_id, protocol_config_handler(recorder.clone()));

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/profile/send-body-without-waiting/check",
                &[("x-conformance-case", case_id)],
                full_body(b"fast-stream-body"),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);
            let echo = decode_echo(&resp.body);
            assert_eq!(echo.body, "fast-stream-body");

            {
                let observed = recorder.lock().unwrap();
                let want = ProtocolConfig {
                    request_body_mode: BodySendMode::Streamed,
                    response_body_mode: BodySendMode::Streamed,
                    send_body_without_waiting_for_header_response: true,
                };
                for phase in ["req_headers", "req_body", "resp_headers", "resp_body"] {
                    assert_eq!(
                        observed.get(phase),
                        Some(&want),
                        "phase {phase} ProtocolConfig = {:?}, want {want:?}",
                        observed.get(phase)
                    );
                }
            }
            assert_no_spurious(h).await;
        }

        // Sub-scenario: `DefaultModeOverride` pre-populates body modes from
        // the captured protocol configuration, preserving the response body
        // mode until the processor overrides it.
        {
            let case_id = "proto-cfg-default-override";
            let saw_response_body = Arc::new(AtomicBool::new(false));
            h.default_proc.register(
                case_id,
                mode_override_default_from_protocol_config_handler(saw_response_body.clone()),
            );

            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/profile/mode-override-resp-buffered/check",
                &[("x-conformance-case", case_id)],
                full_body(b"raw-req"),
            )
            .await;
            assert_eq!(resp.status, StatusCode::OK);
            let echo = decode_echo(&resp.body);
            assert_eq!(echo.body, "mutated-req");
            assert!(saw_response_body.load(Ordering::SeqCst));
            assert_no_spurious(h).await;
        }
    });
}

// ---------------------------------------------------------------------------
// 11–13. Routing & Local Reply
// ---------------------------------------------------------------------------

struct ClearRouteCacheHeaderHandler;
impl CaseHandler for ClearRouteCacheHeaderHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.overwrite_header(":authority", &hv("secondary.local"))?;
        ctx.clear_route_cache();
        ctx.continue_request()
    }
}

#[test]
fn clear_route_cache_upstream_switch() {
    run_conformance(|h| async move {
        let case_id = "clear_route_cache_upstream_switch";
        h.default_proc
            .register(case_id, Arc::new(ClearRouteCacheHeaderHandler));
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/buffered/reroute",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(echo.server, "secondary");
        assert_no_spurious(h).await;
    });
}

struct ClearRouteCacheBodyHandler;
impl CaseHandler for ClearRouteCacheBodyHandler {
    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.overwrite_header(":authority", &hv("secondary.local"))?;
        ctx.clear_route_cache();
        ctx.continue_request()
    }
}

#[test]
fn clear_route_cache_from_request_body() {
    run_conformance(|h| async move {
        let case_id = "clear_route_cache_from_request_body";
        h.default_proc
            .register(case_id, Arc::new(ClearRouteCacheBodyHandler));
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/mode/buffered/reroute-body",
            &[("x-conformance-case", case_id)],
            full_body(b"trigger-reroute"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(echo.server, "secondary");
        assert_no_spurious(h).await;
    });
}

#[test]
fn immediate_response_request_headers() {
    run_conformance(|h| async move {
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/none/immediate",
            &[("x-conformance-case", "immediate_response_request_headers")],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert!(resp.body.is_empty());
        assert_no_spurious(h).await;
    });
}

struct ImmediatePhaseHandler {
    target_phase: RequestPhase,
}
impl CaseHandler for ImmediatePhaseHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        if self.target_phase == RequestPhase::RequestHeaders {
            let mut hdrs = HashMap::new();
            hdrs.insert("x-imm-phase".to_string(), hv("req-headers"));
            return ctx.cancel_request(403, &hdrs, b"blocked-req-headers");
        }
        ctx.continue_request()
    }

    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        if self.target_phase == RequestPhase::RequestBody {
            let mut hdrs = HashMap::new();
            hdrs.insert("x-imm-phase".to_string(), hv("req-body"));
            return ctx.cancel_request(400, &hdrs, b"blocked-req-body");
        }
        ctx.continue_request()
    }

    fn response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        if self.target_phase == RequestPhase::ResponseHeaders {
            let mut hdrs = HashMap::new();
            hdrs.insert("x-imm-phase".to_string(), hv("resp-headers"));
            return ctx.cancel_request(502, &hdrs, b"blocked-resp-headers");
        }
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        if self.target_phase == RequestPhase::ResponseBody {
            let mut hdrs = HashMap::new();
            hdrs.insert("x-imm-phase".to_string(), hv("resp-body"));
            return ctx.cancel_request(503, &hdrs, b"blocked-resp-body");
        }
        ctx.continue_request()
    }
}

#[test]
fn immediate_response_all_phases_and_trailers_fallback() {
    run_conformance(|h| async move {
        let cases = [
            (
                "imm_req_headers",
                RequestPhase::RequestHeaders,
                403,
                b"blocked-req-headers".as_slice(),
            ),
            (
                "imm_req_body",
                RequestPhase::RequestBody,
                400,
                b"blocked-req-body".as_slice(),
            ),
            (
                "imm_resp_headers",
                RequestPhase::ResponseHeaders,
                502,
                b"blocked-resp-headers".as_slice(),
            ),
            (
                "imm_resp_body",
                RequestPhase::ResponseBody,
                503,
                b"blocked-resp-body".as_slice(),
            ),
        ];
        for (id, phase, expected_status, expected_body) in cases {
            h.default_proc.register(
                id,
                Arc::new(ImmediatePhaseHandler {
                    target_phase: phase,
                }),
            );
            let resp = send_request(
                &h.http1,
                &h.proxy_url,
                Method::POST,
                "/mode/buffered/imm",
                &[("x-conformance-case", id)],
                full_body(b"payload"),
            )
            .await;
            assert_eq!(resp.status.as_u16(), expected_status, "case {id}");
            assert_eq!(resp.body, expected_body, "case {id}");
        }
        assert_no_spurious(h).await;
    });
}

struct FailIfCalledHandler {
    called: AtomicBool,
}
impl CaseHandler for FailIfCalledHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        self.called.store(true, Ordering::SeqCst);
        ctx.continue_request()
    }
}

#[test]
fn per_route_disabled_bypass() {
    run_conformance(|h| async move {
        let case_id = "per_route_disabled_bypass";
        let handler = Arc::new(FailIfCalledHandler {
            called: AtomicBool::new(false),
        });
        h.default_proc.register(case_id, handler.clone());
        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/disabled/bypass",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert!(!handler.called.load(Ordering::SeqCst));
        assert_no_spurious(h).await;
    });
}

// ---------------------------------------------------------------------------
// 14–18. Resilience, Chaining & Observability
// ---------------------------------------------------------------------------

struct SlowHandler {
    delay: Duration,
}
impl CaseHandler for SlowHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        std::thread::sleep(self.delay);
        ctx.continue_request()
    }
}

#[test]
fn downstream_client_cancel_graceful() {
    run_conformance(|h| async move {
        let _guard = h.metrics_lock.lock().await;
        let case_id = "downstream_client_cancel_graceful";
        h.default_proc.register(
            case_id,
            Arc::new(SlowHandler {
                delay: Duration::from_millis(200),
            }),
        );

        let err_before = h
            .default_server
            .counter_value("extproc_phase_errors_total")
            .unwrap_or(0.0);

        let uri: hyper::http::Uri = format!("{}/mode/none/cancel", h.proxy_url).parse().unwrap();
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header("x-conformance-case", case_id)
            .body(full_body(b""))
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_millis(40), h.http1.request(req)).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        let err_after = h
            .default_server
            .counter_value("extproc_phase_errors_total")
            .unwrap_or(0.0);
        assert_eq!(err_before, err_after);
        assert_no_spurious(h).await;
    });
}

#[test]
fn message_timeout_fail_closed_and_fail_open() {
    run_conformance(|h| async move {
        let case_id = "message_timeout_sleep";
        h.default_proc.register(
            case_id,
            Arc::new(SlowHandler {
                delay: Duration::from_millis(400),
            }),
        );

        let closed = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/profile/timeout-fail-closed/test",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(closed.status, StatusCode::GATEWAY_TIMEOUT);

        let open = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/profile/timeout-fail-open/test",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(open.status, StatusCode::OK);
        assert_no_spurious(h).await;
    });
}

struct ErroringHandler;
impl CaseHandler for ErroringHandler {
    fn request_headers(
        &self,
        _ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        Err("intentional processor failure".into())
    }
}

#[test]
fn processor_error_continue_vs_abort() {
    run_conformance(|h| async move {
        let _guard = h.metrics_lock.lock().await;
        let case_id = "processor_error_continue_vs_abort";
        h.default_proc.register(case_id, Arc::new(ErroringHandler));
        h.abort_proc.register(case_id, Arc::new(ErroringHandler));

        // Continue-on-error (default processor)
        let cont = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/mode/none/err",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(cont.status, StatusCode::OK);

        // Abort fail-closed
        let abort_closed = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/profile/abort-fail-closed/err",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(abort_closed.status, StatusCode::INTERNAL_SERVER_ERROR);

        // Abort fail-open
        let abort_open = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/profile/abort-fail-open/err",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(abort_open.status, StatusCode::OK);
        assert_no_spurious(h).await;
    });
}

struct ChainFirstHandler;
impl CaseHandler for ChainFirstHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.add_header("x-chain-step", &hv("first"))?;
        ctx.add_header("x-raw-bytes", &hv_raw(&[0x61, 0x62, 0x63]))?;
        ctx.continue_request()
    }
}

struct ChainSecondHandler;
impl CaseHandler for ChainSecondHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.append_header("x-chain-step", &hv("second"))?;
        ctx.continue_request()
    }
}

#[test]
fn multi_filter_chaining() {
    run_conformance(|h| async move {
        let case_id = "multi_filter_chaining";
        h.default_proc
            .register(case_id, Arc::new(ChainFirstHandler));
        h.secondary_proc
            .register(case_id, Arc::new(ChainSecondHandler));

        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::GET,
            "/profile/chained/test",
            &[("x-conformance-case", case_id)],
            full_body(b""),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert_eq!(
            echo.headers.get("x-chain-step"),
            Some(&vec!["first".to_string(), "second".to_string()])
        );
        assert_eq!(
            echo.headers.get("x-raw-bytes"),
            Some(&vec!["abc".to_string()])
        );
        assert_no_spurious(h).await;
    });
}

struct ObservabilityHandler {
    saw_observability: AtomicBool,
}
impl CaseHandler for ObservabilityHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        if ctx.observability_mode {
            self.saw_observability.store(true, Ordering::SeqCst);
        }
        // Mutating in observability mode must be suppressed by the SDK so Envoy
        // records zero spurious_msgs_received.
        let _ = ctx.add_header("x-should-not-appear", &hv("bad"));
        ctx.continue_request()
    }
}

#[test]
fn observability_mode_envoy_gap() {
    run_conformance(|h| async move {
        let case_id = "observability_mode_envoy_gap";
        let handler = Arc::new(ObservabilityHandler {
            saw_observability: AtomicBool::new(false),
        });
        h.default_proc.register(case_id, handler.clone());

        let resp = send_request(
            &h.http1,
            &h.proxy_url,
            Method::POST,
            "/profile/observability/test",
            &[("x-conformance-case", case_id)],
            full_body(b"observed-body"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let echo = decode_echo(&resp.body);
        assert!(!echo.headers.contains_key("x-should-not-appear"));
        assert!(handler.saw_observability.load(Ordering::SeqCst));
        assert_no_spurious(h).await;
    });
}
