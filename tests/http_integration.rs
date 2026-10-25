//! Plain-HTTP end-to-end integration tests: drive a running `envoy` (through
//! the docker compose harness in `tests/`) via the ext_proc filter using plain
//! HTTP tester requests, and assert on the responses the gateway returns.
//!
//! These are the Rust ports of the Go SDK's plain-HTTP integration tests
//! (`test/tester_test.go` and `test/cases.yaml`): the tester request, carrying
//! its actions as JSON, is POSTed to the gateway, envoy runs it through the
//! ext_proc filter and the upstream echo server, and the resulting response is
//! checked. They speak plain HTTP only, unlike the gRPC stream tests in
//! `tests/extproc_integration.rs`.

use std::collections::HashMap;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Bytes as HyperBytes;
use hyper::header::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use envoy_extproc_sdk::TesterRequestBody;

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// The gateway under test (envoy), from the `EXTO_PROC_ECHO_URL` environment
/// variable and defaulting to the docker compose harness address.
const DEFAULT_BASE_URL: &str = "http://localhost:8080";

/// The path the tester requests are POSTed to, mirroring the Go SDK's
/// `urlPrefix`.
const TEST_PATH: &str = "/test";

/// The path that bypasses the ext_proc filter entirely, mirroring the Go SDK's
/// `upstream`.
const BYPASS_PATH: &str = "/no-extproc";

/// How long to wait for the gateway to accept connections before failing.
const GATEWAY_READY_TIMEOUT: Duration = Duration::from_secs(60);

fn base_url() -> String {
    std::env::var("EXTO_PROC_ECHO_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.to_string())
}

fn http_client() -> Client<HttpConnector, Full<HyperBytes>> {
    Client::builder(TokioExecutor::new()).build_http()
}

/// Wait until the gateway answers a bypass request with a status under 500,
/// mirroring the Go SDK's `TestMain` readiness poll.
async fn wait_for_gateway() -> String {
    let base = base_url();
    let client = http_client();
    let deadline = GATEWAY_READY_TIMEOUT;
    let start = std::time::Instant::now();
    loop {
        let uri: hyper::http::Uri = format!("{}{}", base.trim_end_matches('/'), BYPASS_PATH)
            .parse()
            .expect("gateway bypass uri should be valid");
        let ready = match client.get(uri).await {
            Ok(resp) => resp.status() < StatusCode::INTERNAL_SERVER_ERROR,
            Err(_) => false,
        };
        if ready {
            return base;
        }
        if start.elapsed() > deadline {
            panic!("gateway never became ready");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ---------------------------------------------------------------------------
// Wire-format helpers
// ---------------------------------------------------------------------------

/// The JSON echo response the upstream returns for a tester request, matching
/// the Go SDK's `TesterResponseBody`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[allow(dead_code)]
struct EchoResponse {
    datetime: Option<String>,
    server: Option<String>,
    hostname: Option<String>,
    method: Option<String>,
    path: Option<String>,
    query: Option<HashMap<String, Vec<String>>>,
    headers: Option<HashMap<String, Vec<String>>>,
    body: Option<String>,
    duration: Option<i64>,
    status: Option<i64>,
}

/// A plain-HTTP response to a tester request.
struct TesterResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

/// Send a tester request to the gateway and collect the full response.
async fn send_tester_request(
    client: &Client<HttpConnector, Full<HyperBytes>>,
    base_url: &str,
    extra_headers: &HashMap<String, String>,
    actions: &TesterRequestBody,
) -> TesterResponse {
    let uri: hyper::http::Uri = format!("{}{}", base_url.trim_end_matches('/'), TEST_PATH)
        .parse()
        .expect("tester request uri should be valid");
    let json = serde_json::to_vec(actions).expect("tester actions should serialize");
    let mut builder = hyper::Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .header("accept", "application/json");
    for (name, value) in extra_headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let req = builder
        .body(Full::new(HyperBytes::from(json)))
        .expect("tester request should build");
    let resp = client
        .request(req)
        .await
        .expect("failed to send tester request");
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let body = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .expect("failed to collect tester response body")
        .to_bytes()
        .to_vec();
    TesterResponse {
        status,
        headers: resp_headers,
        body,
    }
}

/// Parse the echo response of a tester request, or `None` when the response
/// body is empty (mirroring the Go SDK's `EmptyBody` handling).
fn parse_echo_response(body: &[u8]) -> Option<EchoResponse> {
    let text = std::str::from_utf8(body).ok()?;
    if text.trim().is_empty() {
        return None;
    }
    serde_json::from_str(text).ok()
}

/// Look up the (possibly multi-valued) echo header by name, case-insensitively.
fn echo_header_values<'a>(
    headers: &'a HashMap<String, Vec<String>>,
    name: &str,
) -> Option<&'a [String]> {
    let target = name.to_ascii_lowercase();
    headers
        .iter()
        .find(|kv| kv.0.to_ascii_lowercase() == target)
        .map(|kv| kv.1.as_slice())
}

/// Look up the (possibly multi-valued) response header by name, case-insensitively.
fn response_header_values(headers: &HeaderMap, name: &str) -> Option<Vec<String>> {
    let target = name.to_ascii_lowercase();
    let mut values = Vec::new();
    for (key, value) in headers {
        if key.as_str().to_ascii_lowercase() == target {
            if let Ok(v) = value.to_str() {
                values.push(v.to_string());
            }
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_noop() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let resp = send_tester_request(
        &client,
        &gateway,
        &HashMap::new(),
        &TesterRequestBody::default(),
    )
    .await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "noop tester request should succeed"
    );
    let echo = parse_echo_response(&resp.body).expect("noop response should carry an echo");
    assert_eq!(echo.method.as_deref(), Some("POST"));
    assert_eq!(echo.path.as_deref(), Some("/test"));
    assert_eq!(echo.status, Some(200));
}

#[tokio::test]
async fn http_clear_request_body() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let actions = TesterRequestBody {
        clear_request_body: Some(true),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &HashMap::new(), &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "clearing the request body should not fail"
    );
    let echo = parse_echo_response(&resp.body).expect("echo should still respond");
    assert_eq!(
        echo.body.as_deref(),
        Some(""),
        "cleared request body should be empty"
    );
}

#[tokio::test]
async fn http_clear_response_body() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let actions = TesterRequestBody {
        clear_response_body: Some(true),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &HashMap::new(), &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "clearing the response body should not fail"
    );
    assert!(
        parse_echo_response(&resp.body).is_none(),
        "cleared response body should not carry an echo"
    );
}

#[tokio::test]
async fn http_add_request_header() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let actions = TesterRequestBody {
        add_request_headers: Some(HashMap::from([(
            "x-add-me".to_string(),
            "add_value".to_string(),
        )])),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &HashMap::new(), &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "adding a request header should not fail"
    );
    let echo = parse_echo_response(&resp.body).expect("echo should still respond");
    let headers = echo.headers.as_ref().expect("echo should echo headers");
    let values = echo_header_values(headers, "x-add-me")
        .expect("added request header should be echoed back");
    assert!(
        values.contains(&"add_value".to_string()),
        "added request header should be echoed back with the added value"
    );
}

#[tokio::test]
async fn http_remove_request_header() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let extra_headers = HashMap::from([("x-remove-me".to_string(), "true".to_string())]);
    let actions = TesterRequestBody {
        remove_request_headers: Some(vec!["x-remove-me".to_string()]),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &extra_headers, &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "removing a request header should not fail"
    );
    let echo = parse_echo_response(&resp.body).expect("echo should still respond");
    let headers = echo.headers.as_ref().expect("echo should echo headers");
    assert!(
        echo_header_values(headers, "x-remove-me").is_none(),
        "removed request header should not be echoed back"
    );
}

#[tokio::test]
async fn http_overwrite_request_header() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let extra_headers = HashMap::from([("x-overwrite-me".to_string(), "old value".to_string())]);
    let actions = TesterRequestBody {
        overwrite_request_headers: Some(HashMap::from([(
            "x-overwrite-me".to_string(),
            "new value".to_string(),
        )])),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &extra_headers, &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "overwriting a request header should not fail"
    );
    let echo = parse_echo_response(&resp.body).expect("echo should still respond");
    let headers = echo.headers.as_ref().expect("echo should echo headers");
    let values = echo_header_values(headers, "x-overwrite-me")
        .expect("overwritten request header should be echoed back");
    assert_eq!(
        values.len(),
        1,
        "overwritten request header should have one value"
    );
    assert_eq!(values[0].as_str(), "new value");
}

#[tokio::test]
async fn http_add_response_header() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let actions = TesterRequestBody {
        add_response_headers: Some(HashMap::from([(
            "x-add-me".to_string(),
            "true".to_string(),
        )])),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &HashMap::new(), &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "adding a response header should not fail"
    );
    let values = response_header_values(&resp.headers, "x-add-me")
        .expect("added response header should be returned");
    assert!(
        values.contains(&"true".to_string()),
        "added response header should be returned with the added value"
    );
}

#[tokio::test]
async fn http_remove_response_header() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let actions = TesterRequestBody {
        add_response_headers: Some(HashMap::from([(
            "x-add-me".to_string(),
            "true".to_string(),
        )])),
        remove_response_headers: Some(vec!["x-echo-server-received".to_string()]),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &HashMap::new(), &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "removing a response header should not fail"
    );
    assert!(
        response_header_values(&resp.headers, "x-echo-server-received").is_none(),
        "removed response header should not be returned"
    );
}

#[tokio::test]
async fn http_overwrite_response_header() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let actions = TesterRequestBody {
        overwrite_response_headers: Some(HashMap::from([(
            "content-type".to_string(),
            "test/plain".to_string(),
        )])),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &HashMap::new(), &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "overwriting a response header should not fail"
    );
    let values = response_header_values(&resp.headers, "content-type")
        .expect("overwritten response header should be returned");
    assert_eq!(
        values.len(),
        1,
        "overwritten response header should have one value"
    );
    assert_eq!(values[0].as_str(), "test/plain");
}

#[tokio::test]
async fn http_cancel_request() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let actions = TesterRequestBody {
        cancel_request: Some(true),
        cancel_request_status: Some(400),
        cancel_request_headers: Some(HashMap::from([(
            "x-custom-header".to_string(),
            "true".to_string(),
        )])),
        cancel_request_body: Some(
            r#"{"error":"BadRequest","detail":"Testing bad requests"}"#.to_string(),
        ),
        ..Default::default()
    };
    let resp = send_tester_request(&client, &gateway, &HashMap::new(), &actions).await;

    assert_eq!(
        resp.status.as_u16(),
        400,
        "cancelled request should return the declared status"
    );
    assert_eq!(
        resp.body,
        br#"{"error":"BadRequest","detail":"Testing bad requests"}"#
    );
}

#[tokio::test]
async fn http_header_parsing() {
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let extra_headers = HashMap::from([
        ("x-probe-list".to_string(), "a,b,c".to_string()),
        (
            "x-probe-date".to_string(),
            "Mon, 22 Aug 2026 17:34:26 GMT".to_string(),
        ),
    ]);
    let actions = TesterRequestBody::default();
    let resp = send_tester_request(&client, &gateway, &extra_headers, &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "header parsing probe should not fail"
    );
    let echo = parse_echo_response(&resp.body).expect("echo should respond");
    let headers = echo.headers.as_ref().expect("echo should echo headers");
    let str_count = echo_header_values(headers, "x-parsed-str-count").expect("str count header");
    let raw_count = echo_header_values(headers, "x-parsed-raw-count").expect("raw count header");
    assert_eq!(
        str_count, raw_count,
        "string and raw header counts should agree"
    );
    assert_ne!(str_count[0].as_str(), "0", "expected headers to be parsed");

    let list_value = echo_header_values(headers, "x-parsed-list-value").expect("list value header");
    assert_eq!(
        list_value[0].as_str(),
        "a,b,c",
        "list header value should not be split"
    );
    let list_count = echo_header_values(headers, "x-parsed-list-count").expect("list count header");
    assert_eq!(
        list_count[0].as_str(),
        "1",
        "single list header should count once"
    );
    let list_split = echo_header_values(headers, "x-parsed-list-split").expect("list split header");
    assert_eq!(
        list_split[0].as_str(),
        "a|b|c",
        "list header should be split on commas"
    );
}

#[tokio::test]
async fn http_change_upstream_via_clear_route_cache() {
    // The clear-route-cache driven upstream change: the tester rewrites the
    // `:authority` to the value of the `x-route-to-upstream` header and clears
    // the route cache, so envoy recomputes the route and routes to the
    // "upstream-copy" upstream.
    let gateway = wait_for_gateway().await;
    let client = http_client();
    let extra_headers = HashMap::from([(
        "x-route-to-upstream".to_string(),
        "upstream-copy".to_string(),
    )]);
    let actions = TesterRequestBody::default();
    let resp = send_tester_request(&client, &gateway, &extra_headers, &actions).await;

    assert_eq!(
        resp.status,
        StatusCode::OK,
        "clear-route-cache driven upstream change should not fail"
    );
    let echo = parse_echo_response(&resp.body).expect("echo should still respond");
    assert_eq!(echo.status, Some(200));
    assert_eq!(
        echo.hostname.as_deref(),
        Some("upstream-copy"),
        "clear-route-cache driven upstream change should route to the copy upstream"
    );
}
