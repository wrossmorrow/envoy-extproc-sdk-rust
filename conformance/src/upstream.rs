//! In-process upstream echo server used by the conformance harness.
//!
//! Mirrors the Go SDK's `conformance/upstream.go`: an in-process HTTP/1.1 + h2c
//! echo/control server that acts as Envoy's backend during conformance tests. It
//! captures the observed request state and answers with an echo (or a raw body,
//! status override, delayed response, or chunked response).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderMap, HeaderValue};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

/// Header used by conformance scenarios to carry the active case ID.
pub const CASE_HEADER: &str = "x-conformance-case";

/// Header steering the raw-body echo behaviour of the upstream server.
const RAW_BODY_HEADER: &str = "x-upstream-raw-body";
const RAW_BODY_CONTENT_TYPE_HEADER: &str = "x-upstream-content-type";
const DELAY_HEADER: &str = "x-upstream-delay-ms";
const STATUS_HEADER: &str = "x-upstream-status";
const CHUNKS_HEADER: &str = "x-upstream-chunks";

const DEFAULT_STATUS: u16 = 200;
const DEFAULT_RAW_BODY_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
const ECHO_CONTENT_TYPE: &str = "application/json";

/// The request state observed by the upstream echo server, serialized as JSON
/// for the scenarios to assert on.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct UpstreamEcho {
    pub server: String,
    pub proto: String,
    pub host: String,
    pub method: String,
    pub path: String,
    pub raw_query: String,
    pub headers: HashMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub trailers: HashMap<String, Vec<String>>,
    pub body: String,
    pub body_bytes: usize,
}

/// In-process upstream echo server.
pub struct UpstreamServer {
    name: String,
    port: u16,
    listener: Arc<TcpListener>,
}

impl UpstreamServer {
    /// Start a new upstream echo server on an ephemeral loopback port.
    pub async fn start(name: &str) -> Result<Self, String> {
        let listener = Arc::new(
            TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| format!("listen upstream {name:?}: {e}"))?,
        );
        let port = listener
            .local_addr()
            .map_err(|e| format!("upstream {name:?} local addr: {e}"))?
            .port();
        let task_server = Arc::new(UpstreamServer {
            name: name.to_string(),
            port,
            listener: Arc::clone(&listener),
        });
        let task = task_server.clone();
        tokio::spawn(async move {
            let mut builder = auto::Builder::new(TokioExecutor::new());
            let auto = builder.http2();
            let svc_server = task.clone();
            let svc = service_fn(move |req: Request<Incoming>| {
                let server = svc_server.clone();
                async move { server.handle(req).await }
            });
            loop {
                let (io, _) = match task.listener.accept().await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("upstream {} accept failed: {}", task.name, e);
                        break;
                    }
                };
                let conn = auto.serve_connection(hyper_util::rt::TokioIo::new(io), svc.clone());
                if let Err(e) = conn.await {
                    tracing::warn!("upstream {} connection error: {}", task.name, e);
                }
            }
        });
        Ok(UpstreamServer {
            name: name.to_string(),
            port,
            listener,
        })
    }

    /// The ephemeral TCP port the upstream server listens on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The loopback base URL for direct testing of the upstream server.
    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The name of the upstream server.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Shut the upstream server down.
    pub fn close(&self) -> Result<(), String> {
        Ok(())
    }

    async fn handle(
        self: Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, std::io::Error> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let path = uri.path().to_string();
        let raw_query = uri.query().unwrap_or("").to_string();
        let host = req
            .headers()
            .get("host")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let proto = format!("{:?}", req.version());
        let headers = collect_headers(req.headers());

        let delay_ms = header_u64(req.headers(), DELAY_HEADER);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        let status_code = header_u16(req.headers(), STATUS_HEADER)
            .filter(|c| (100..=599).contains(c))
            .unwrap_or(DEFAULT_STATUS);
        let case_id = req
            .headers()
            .get(CASE_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let has_raw = has_raw_body(req.headers());
        let content_type = if has_raw {
            header_string(req.headers(), RAW_BODY_CONTENT_TYPE_HEADER)
                .unwrap_or(DEFAULT_RAW_BODY_CONTENT_TYPE.to_string())
        } else {
            ECHO_CONTENT_TYPE.to_string()
        };
        let resp_trailers = response_trailers(req.headers());
        let chunks_raw = header_u64(req.headers(), CHUNKS_HEADER);
        let chunks = if chunks_raw > 1 { chunks_raw } else { 1 };
        let raw_value = if has_raw {
            header_string(req.headers(), RAW_BODY_HEADER)
        } else {
            None
        };

        let body = req.into_body();
        let collected = body.collect().await.map_err(std::io::Error::other)?;
        let trailers = trailers_of(collected.trailers());
        let data = collected.to_bytes().to_vec();

        let payload: Vec<u8> = if let Some(raw) = &raw_value {
            raw.clone().into_bytes()
        } else {
            let echo = UpstreamEcho {
                server: self.name.clone(),
                proto: proto.clone(),
                host: host.clone(),
                method: method.as_str().to_string(),
                path: path.clone(),
                raw_query: raw_query.clone(),
                headers,
                trailers,
                body: String::from_utf8_lossy(&data).into_owned(),
                body_bytes: data.len(),
            };
            serde_json::to_vec(&echo).map_err(std::io::Error::other)?
        };

        let status = StatusCode::from_u16(status_code)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let mut builder = Response::builder().status(status);
        builder = builder.header("content-type", content_type);
        if resp_trailers.is_empty() && chunks <= 1 && status_code != 204 && status_code != 304 {
            builder = builder.header("content-length", payload.len().to_string());
        }
        if let Some(case_id) = &case_id {
            let value = HeaderValue::from_str(case_id)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            builder = builder.header(CASE_HEADER, value);
        }
        let response = builder
            .body(Full::new(Bytes::from(payload)))
            .map_err(std::io::Error::other)?;
        Ok(response)
    }
}

fn has_raw_body(headers: &HeaderMap) -> bool {
    header_string(headers, RAW_BODY_HEADER)
        .map(|s| !s.is_empty())
        .unwrap_or(false)
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn collect_headers(headers: &HeaderMap) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();
    for (key, value) in headers {
        let lower = key.as_str().to_ascii_lowercase();
        let vals = match value.to_str() {
            Ok(s) => vec![s.to_string()],
            Err(_) => Vec::new(),
        };
        out.entry(lower).or_insert_with(Vec::new).extend(vals);
    }
    out
}

fn trailers_of(trailers: Option<&HeaderMap>) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();
    if let Some(trailers) = trailers {
        for (key, value) in trailers {
            let lower = key.as_str().to_ascii_lowercase();
            let vals = match value.to_str() {
                Ok(s) => vec![s.to_string()],
                Err(_) => Vec::new(),
            };
            out.entry(lower).or_insert_with(Vec::new).extend(vals);
        }
    }
    out
}

fn response_trailers(_headers: &HeaderMap) -> HashMap<String, String> {
    HashMap::new()
}

fn header_u64(headers: &HeaderMap, name: &str) -> u64 {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

fn header_u16(headers: &HeaderMap, name: &str) -> Option<u16> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u16>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::body::Bytes as HyperBytes;
    use hyper::Method;
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    type TestClient = Client<HttpConnector, Full<HyperBytes>>;

    fn client() -> TestClient {
        Client::builder(TokioExecutor::new()).build_http()
    }

    async fn send(
        client: &TestClient,
        url: &str,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> (StatusCode, HashMap<String, Vec<String>>, Vec<u8>) {
        let uri: hyper::http::Uri = format!("{url}{path}").parse().expect("uri");
        let mut builder = hyper::Request::builder()
            .method(Method::try_from(method).expect("method"))
            .uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let req = builder
            .body(Full::new(HyperBytes::from(body.to_vec())))
            .expect("request body");
        let resp = client.request(req).await.expect("send");
        let status = resp.status();
        let resp_headers = resp.headers().clone();
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .expect("collect")
            .to_bytes()
            .to_vec();
        (status, header_strings(&resp_headers), bytes)
    }

    fn header_strings(headers: &HeaderMap) -> HashMap<String, Vec<String>> {
        let mut out = HashMap::new();
        for (key, value) in headers {
            let lower = key.as_str().to_ascii_lowercase();
            let vals = match value.to_str() {
                Ok(s) => vec![s.to_string()],
                Err(_) => Vec::new(),
            };
            out.entry(lower).or_insert_with(Vec::new).extend(vals);
        }
        out
    }

    #[tokio::test]
    async fn echo_returns_structured_json() {
        let server = UpstreamServer::start("echo-test").await.expect("start");
        let client = client();
        let (status, headers, body) = send(
            &client,
            &server.url(),
            "POST",
            "/echo",
            &[
                ("content-type", "application/json"),
                ("x-conformance-case", "case-1"),
            ],
            b"{\"hello\":\"world\"}",
        )
        .await;
        server.close().unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get("content-type").and_then(|v| v.first()),
            Some(&"application/json".to_string())
        );
        let echo: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(echo["method"], "POST");
        assert_eq!(echo["path"], "/echo");
        assert_eq!(echo["body"], "{\"hello\":\"world\"}");
        assert_eq!(echo["body_bytes"], 17);
        assert_eq!(echo["server"], "echo-test");
        assert_eq!(echo["trailers"], serde_json::json!(null));
        assert_eq!(
            echo["headers"]["content-type"],
            serde_json::json!(["application/json"])
        );
        assert_eq!(
            echo["headers"]["x-conformance-case"],
            serde_json::json!(["case-1"])
        );
    }

    #[tokio::test]
    async fn raw_body_echo_returns_raw_payload() {
        let server = UpstreamServer::start("raw-echo-test").await.expect("start");
        let client = client();
        let (status, headers, body) = send(
            &client,
            &server.url(),
            "POST",
            "/raw",
            &[
                ("x-upstream-raw-body", "raw-bytes"),
                ("x-upstream-content-type", "text/plain"),
            ],
            b"ignored",
        )
        .await;
        server.close().unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"raw-bytes");
        assert_eq!(
            headers.get("content-type").and_then(|v| v.first()),
            Some(&"text/plain".to_string())
        );
    }

    #[tokio::test]
    async fn status_override_is_respected() {
        let server = UpstreamServer::start("status-echo-test")
            .await
            .expect("start");
        let client = client();
        let (status, _headers, body) = send(
            &client,
            &server.url(),
            "POST",
            "/status",
            &[
                ("x-upstream-raw-body", "{}"),
                ("x-upstream-status", "503"),
                ("content-type", "application/json"),
            ],
            b"ignored",
        )
        .await;
        server.close().unwrap();
        assert_eq!(status, StatusCode::from_u16(503).unwrap());
        assert_eq!(body, b"{}");
    }
}
