//! Plain gRPC load generator for the envoy ext_proc SDK (Rust).
//!
//! Drives full ext_proc exchanges over a raw bidirectional gRPC stream to the
//! SDK's ext_proc server
//! (`envoy.service.ext_proc.v3.ExternalProcessor/Process`), without an envoy
//! in the path, and reports per-exchange latency and errors.
//!
//! Each exchange mirrors the phase sequence the envoy ext_proc filter drives for
//! a buffered request/response: request headers -> request body ->
//! response headers -> response body, then the stream is closed. Because envoy
//! (and the HTTP machinery in between) are absent, the reported latency is the
//! processor-side cost of the SDK's gRPC server path, complementing the
//! containerized k6 load tests in [`loadtest/`](../loadtest) that drive the full
//! envoy -> extproc -> upstream chain.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use envoy_types::pb::envoy::config::core::v3::HeaderMap as EnvoyHeaderMap;
use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_client::ExternalProcessorClient;
use envoy_types::pb::envoy::service::ext_proc::v3::processing_request::Request as ProcessingRequestOneof;
use envoy_types::pb::envoy::service::ext_proc::v3::processing_response::Response as ResponseOneof;
use envoy_types::pb::envoy::service::ext_proc::v3::HttpBody;
use envoy_types::pb::envoy::service::ext_proc::v3::HttpHeaders;
use envoy_types::pb::envoy::service::ext_proc::v3::{ProcessingRequest, ProcessingResponse};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

/// The response header the server may set (via the
/// `ProcessingOptions.update_duration_header` option) carrying the accumulated
/// extproc processing time in nanoseconds.
const DURATION_HEADER: &str = "x-extproc-duration-ns";

const DEFAULT_ADDR: &str = "127.0.0.1:50051";
const DEFAULT_ITERATIONS: usize = 100;
const DEFAULT_CONCURRENCY: usize = 10;

/// One full exchange: latency of the exchange, and the server-reported extproc
/// processing duration in nanoseconds (when the server set the duration header).
struct ExchangeOutcome {
    latency: Duration,
    extproc_duration_ns: Option<i64>,
}

/// The error that aborted one exchange, if any.
struct ExchangeError {
    index: usize,
    error: String,
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

/// Send one processing phase message and wait for the response it produces.
async fn send_phase(
    tx: &mut Sender<ProcessingRequest>,
    stream: &mut tonic::codec::Streaming<ProcessingResponse>,
    req: ProcessingRequest,
) -> Result<ProcessingResponse, String> {
    tx.send(req)
        .await
        .map_err(|e| format!("failed to send processing request: {}", e))?;
    match stream.next().await {
        Some(Ok(resp)) => Ok(resp),
        Some(Err(e)) => Err(format!("processing response stream error: {}", e)),
        None => Err("processing response stream closed before a response arrived".into()),
    }
}

/// Extract the `x-extproc-duration-ns` processing duration (nanoseconds) from a
/// response, if the server set it (via the `ProcessingOptions.update_duration_header`
/// option) and the phase ended the stream.
fn extract_duration_ns(resp: &ProcessingResponse) -> Option<i64> {
    let common = match &resp.response {
        Some(ResponseOneof::ResponseHeaders(r)) => r.response.as_ref(),
        Some(ResponseOneof::ResponseBody(r)) => r.response.as_ref(),
        _ => None,
    };
    let mutation = common?.header_mutation.as_ref()?;
    mutation.set_headers.iter().find_map(|option| {
        option
            .header
            .as_ref()
            .filter(|h| h.key == DURATION_HEADER)
            .and_then(|h| String::from_utf8_lossy(&h.raw_value).parse::<i64>().ok())
    })
}

/// Drive one full ext_proc exchange over a single bidirectional gRPC stream and
/// measure it.
async fn run_exchange(
    client: &mut ExternalProcessorClient<tonic::transport::Channel>,
) -> Result<ExchangeOutcome, String> {
    let start = Instant::now();
    let (tx, rx) = channel::<ProcessingRequest>(8);
    let mut stream = client
        .process(ReceiverStream::new(rx))
        .await
        .map_err(|e| format!("failed to open process stream: {}", e))?
        .into_inner();
    let mut tx = tx;

    // Request headers phase (not yet end of stream).
    send_phase(
        &mut tx,
        &mut stream,
        envoy_request_headers(false, &[(":method", "POST"), (":path", "/test")]),
    )
    .await?;
    // Request body phase (end of stream).
    send_phase(&mut tx, &mut stream, envoy_request_body(b"{}", true)).await?;
    // Response headers phase (end of stream).
    send_phase(&mut tx, &mut stream, envoy_response_headers(true)).await?;
    // Response body phase (end of stream).
    let resp = send_phase(&mut tx, &mut stream, envoy_response_body(b"{}", true)).await?;

    // The exchange is complete: the data plane closes the stream here.
    drop(tx);
    let _ = stream.next().await;

    Ok(ExchangeOutcome {
        latency: start.elapsed(),
        extproc_duration_ns: extract_duration_ns(&resp),
    })
}

/// The outcome of one worker task: the latencies / durations it observed, and
/// the exchanges it failed on.
struct WorkerResult {
    latencies: Vec<Duration>,
    extproc_durations_ns: Vec<Option<i64>>,
    errors: Vec<ExchangeError>,
}

/// Run the exchanges assigned to one worker task: indices `base`,
/// `base + stride`, ... below `iterations`, sequentially.
async fn run_worker(
    client: &mut ExternalProcessorClient<tonic::transport::Channel>,
    base: usize,
    stride: usize,
    iterations: usize,
) -> WorkerResult {
    let mut latencies = Vec::new();
    let mut extproc_durations_ns = Vec::new();
    let mut errors = Vec::new();
    let mut index = base;
    while index < iterations {
        match run_exchange(client).await {
            Ok(outcome) => {
                latencies.push(outcome.latency);
                extproc_durations_ns.push(outcome.extproc_duration_ns);
            }
            Err(error) => errors.push(ExchangeError {
                index,
                error: error.to_string(),
            }),
        }
        index += stride;
    }
    WorkerResult {
        latencies,
        extproc_durations_ns,
        errors,
    }
}

fn format_duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn percentile(sorted: &[Duration], percentile: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((sorted.len() as f64 - 1.0) * percentile).round() as usize;
    Some(sorted[rank].as_secs_f64() * 1e3)
}

/// Parse the load generator arguments: `--addr <addr>`, `--iterations <n>`,
/// `--concurrency <n>` (values may also use `--key=value`).
fn parse_args() -> (String, usize, usize) {
    let mut addr = DEFAULT_ADDR.to_string();
    let mut iterations = DEFAULT_ITERATIONS;
    let mut concurrency = DEFAULT_CONCURRENCY;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let arg = arg.trim_start_matches('-').to_string();
        let (key, value) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg, args.next()),
        };
        match key.as_str() {
            "addr" => {
                if let Some(v) = value {
                    addr = v;
                }
            }
            "iterations" => {
                if let Some(v) = value {
                    if let Ok(n) = v.parse::<usize>() {
                        iterations = n;
                    }
                }
            }
            "concurrency" => {
                if let Some(v) = value {
                    if let Ok(n) = v.parse::<usize>() {
                        concurrency = n;
                    }
                }
            }
            _ => {}
        }
    }
    (addr, iterations, concurrency)
}

#[tokio::main]
async fn main() {
    let (addr, iterations, concurrency) = parse_args();

    if iterations == 0 {
        eprintln!("extproc-loadgen: iterations must be greater than zero");
        std::process::exit(1);
    }

    println!(
        "extproc-loadgen: target extproc gRPC server: http://{}",
        addr
    );
    println!(
        "extproc-loadgen: running {} exchanges with concurrency {}",
        iterations, concurrency
    );

    let client = match ExternalProcessorClient::connect(format!("http://{}", addr)).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "extproc-loadgen: failed to connect to extproc server at {}: {}",
                addr, e
            );
            std::process::exit(1);
        }
    };

    let mut handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let mut client = client.clone();
        handles.push(tokio::spawn(async move {
            run_worker(&mut client, i, concurrency, iterations).await
        }));
    }
    let mut latencies = Vec::new();
    let mut extproc_durations_ns = Vec::new();
    let mut errors = Vec::new();
    for handle in handles {
        let result = handle.await.expect("extproc-loadgen worker task panicked");
        latencies.extend(result.latencies);
        extproc_durations_ns.extend(result.extproc_durations_ns);
        errors.extend(result.errors);
    }

    latencies.sort_by_key(|d| d.as_nanos());
    let avg_ms = if latencies.is_empty() {
        0.0
    } else {
        latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / latencies.len() as f64 * 1e3
    };
    let p50_ms = percentile(&latencies, 0.50).unwrap_or(0.0);
    let p95_ms = percentile(&latencies, 0.95).unwrap_or(0.0);
    let max_ms = latencies
        .last()
        .map(|d| format_duration_ms(*d))
        .unwrap_or(0.0);

    let extproc_duration_ns: Vec<i64> = extproc_durations_ns.into_iter().flatten().collect();
    let avg_duration_ms = if extproc_duration_ns.is_empty() {
        0.0
    } else {
        extproc_duration_ns
            .iter()
            .map(|ns| *ns as f64 / 1e6)
            .sum::<f64>()
            / extproc_duration_ns.len() as f64
    };

    for error in &errors {
        println!(
            "extproc-loadgen: exchange {} failed: {}",
            error.index, error.error
        );
    }
    println!(
        "extproc-loadgen: finished: {} exchanges, {} errors",
        latencies.len(),
        errors.len()
    );
    println!(
        "extproc-loadgen: latency avg={}ms p50={}ms p95={}ms max={}ms",
        avg_ms, p50_ms, p95_ms, max_ms
    );
    println!(
        "extproc-loadgen: extproc duration avg={}ms ({} samples)",
        avg_duration_ms,
        extproc_duration_ns.len()
    );

    if !errors.is_empty() {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use envoy_types::pb::envoy::config::core::v3::HeaderValueOption;
    use envoy_types::pb::envoy::service::ext_proc::v3::processing_response::Response as ResponseOneof;
    use envoy_types::pb::envoy::service::ext_proc::v3::{
        BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, ProcessingResponse,
    };

    #[allow(deprecated)]
    fn duration_header_option(ns: i64) -> HeaderValueOption {
        HeaderValueOption {
            header: Some(EnvoyHeaderValue {
                key: DURATION_HEADER.to_string(),
                value: String::new(),
                raw_value: ns.to_string().into_bytes(),
            }),
            append: None,
            append_action: 0,
            keep_empty_value: false,
        }
    }

    fn common_response_with_duration(ns: i64) -> CommonResponse {
        CommonResponse {
            status: 0,
            header_mutation: Some(HeaderMutation {
                set_headers: vec![duration_header_option(ns)],
                remove_headers: Vec::new(),
            }),
            body_mutation: None,
            trailers: None,
            clear_route_cache: false,
        }
    }

    fn processing_response_with(kind: &str, ns: i64) -> ProcessingResponse {
        let response = match kind {
            "response_headers" => ResponseOneof::ResponseHeaders(HeadersResponse {
                response: Some(common_response_with_duration(ns)),
            }),
            _ => ResponseOneof::ResponseBody(BodyResponse {
                response: Some(common_response_with_duration(ns)),
            }),
        };
        ProcessingResponse {
            response: Some(response),
            ..Default::default()
        }
    }

    #[test]
    fn test_extract_duration_ns_from_response_headers() {
        let resp = processing_response_with("response_headers", 12_345_678);
        assert_eq!(extract_duration_ns(&resp), Some(12_345_678));
    }

    #[test]
    fn test_extract_duration_ns_from_response_body() {
        let resp = processing_response_with("response_body", 1_000_000_000);
        assert_eq!(extract_duration_ns(&resp), Some(1_000_000_000));
    }

    #[allow(deprecated)]
    #[test]
    fn test_extract_duration_ns_absent_without_duration_header() {
        // A mutation that carries a different header, but no duration header.
        let resp = ProcessingResponse {
            response: Some(ResponseOneof::ResponseHeaders(HeadersResponse {
                response: Some(CommonResponse {
                    status: 0,
                    header_mutation: Some(HeaderMutation {
                        set_headers: vec![HeaderValueOption {
                            header: Some(EnvoyHeaderValue {
                                key: "x-extproc-names".to_string(),
                                value: String::new(),
                                raw_value: b"tester".to_vec(),
                            }),
                            append: None,
                            append_action: 0,
                            keep_empty_value: false,
                        }],
                        remove_headers: Vec::new(),
                    }),
                    body_mutation: None,
                    trailers: None,
                    clear_route_cache: false,
                }),
            })),
            ..Default::default()
        };
        assert_eq!(extract_duration_ns(&resp), None);
    }

    #[test]
    fn test_extract_duration_ns_absent_without_mutation() {
        let resp = ProcessingResponse {
            response: Some(ResponseOneof::ResponseHeaders(HeadersResponse {
                response: None,
            })),
            ..Default::default()
        };
        assert_eq!(extract_duration_ns(&resp), None);
    }

    #[test]
    fn test_percentile_empty() {
        assert_eq!(percentile(&[], 0.95), None);
    }

    #[test]
    fn test_percentile_single() {
        let sorted = vec![Duration::from_millis(3)];
        assert_eq!(percentile(&sorted, 0.95), Some(3.0));
    }

    #[test]
    fn test_percentile_sorted() {
        let sorted = vec![
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(4),
        ];
        // Nearest-rank percentile: index = (len - 1) * percentile, rounded.
        assert_eq!(percentile(&sorted, 0.50), Some(3.0));
        assert_eq!(percentile(&sorted, 0.95), Some(4.0));
    }

    #[test]
    fn test_format_duration_ms() {
        assert_eq!(format_duration_ms(Duration::from_millis(2)), 2.0);
        assert_eq!(format_duration_ms(Duration::from_secs(1)), 1000.0);
    }
}
