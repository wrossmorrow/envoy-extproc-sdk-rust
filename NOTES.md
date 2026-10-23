# Envoy External Processor SDK — Rust Port: Working Notes

## Plan

Port github.com/wrossmorrow/envoy-extproc-sdk-go to Rust using tonic/tonic-prost
for gRPC and the `envoy-types` crate for compiled protobuf types.

Suggested stage breakdown (adjust as reality hits):

1. **Stage 1** — Proto/types layer: `ProcessingRequest/Response`, `AllHeaders`,
   `HeaderValue`, `PhaseResponse`, `RequestContext`, phase constants, helpers.
2. **Stage 2** — Options/health/metrics: `ProcessingOptions`, `ServerOptions`,
   `HealthServer`, `Metrics`.
3. **Stage 3** — gRPC server: `RequestProcessor` trait, `GenericExtProcServer`,
   `Serve`, graceful shutdown.
4. **Stage 4** — Tests replicating the Go test suite (context, headers, options,
   phase, metrics, health, server).

## Progress

### Environment

- Rust toolchain at `/usr/local/cargo` (rustc/cargo 1.99.1); must run cargo with
  `export CARGO_HOME=/work/.cargo` (default CARGO_HOME has permission issues).
- Running cargo from `/work` dirs.

### Dependencies (all resolved via crates.io)

- `envoy-types = "0.7.7"` (github.com/flemosr/envoy-types) — compiled Envoy
  protobuf types for ext_proc. Generated trait path:
  `envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessor`;
  messages in the same parent module (`ProcessingRequest`, `ProcessingResponse`,
  `ExternalProcessor`, `ExternalProcessorClient/Server`, `HeadersResponse`,
  `BodyResponse`, `TrailersResponse`, `CommonResponse`, `ImmediateResponse`,
  `HeaderMutation`, `BodyMutation`, ...).
- `tonic = "0.14"` (server/transport/codegen features) + `tonic-prost = "0.14"` +
  `tokio` full + `tokio-stream` (for the `ProcessStream` boxed-stream type).
- `tonic-health = "0.14"` — health service helpers:
  - `ServingStatus` at `tonic_health::ServingStatus`
  - `HealthReporter`, `HealthService`, `WatchStream` at `tonic_health::server::{...}`
  - trait `Health` at `tonic_health::server::Health`
  - messages at `tonic_health::pb::HealthCheckRequest/HealthCheckResponse/ServingStatus`
  (verify exact paths via compiler errors).
- `prometheus = "0.14"` — `Registry`, `register`, `gather`, `Counter`, `Gauge`,
  `Histogram`, `HistogramOpts`, `Encoder`, `TextEncoder`; raw proto model at
  `prometheus::proto::{MetricFamily, Metric, MetricType, ...}`.
- `axum = "0.8"` — metrics HTTP server (Go uses net/http; axum wraps it).
- `serde`, `serde_json`, `serde_yaml = "0.9"`, `tracing`, `tracing-subscriber` —
  planned logger/options wiring.

### Rust API learnings

- envoy-types generated trait impl must be via
  `#[tonic::async_trait] impl ExternalProcessor for MyProcessor { type ProcessStream = ...; async fn process(...) }`.
- `ProcessingResponseResponse` is a oneof: variants
  `ImmediateResponse(ImmediateResponse)`, `ResponseHeaders(HeadersResponse)`,
  `RequestBody(BodyResponse)`, `ResponseBody(BodyResponse)`,
  `RequestTrailers(TrailersResponse)`, etc. — the active arm is obtained via
  `.response` (field/method returning the oneof enum).
- `CommonResponse { status, header_mutation, body_mutation, trailers, clear_route_cache, ... }`.
- `BodyMutation { mutation: Option<body_mutation::Mutation> }`; `Mutation` has
  variants like `Body(Vec<u8>)`, `ClearBody(bool)` (+ possibly `Streamed`).
- `ImmediateResponse { status: Option<HttpStatus>, headers, body, grpc_status, details }`;
  `HttpStatus { code: i32 }` from `envoy_types::pb::envoy::r#type::v3::HttpStatus`.
- `HeaderMutation { set_headers: Vec<HeaderValueOption>, remove_headers: Vec<String> }`.
- `HeaderValueOption` has fields `HeaderValue { value, raw_value }` and action
  `HeaderAppendAction`; action variants and constructor names may differ from
  the Go ones — check generated source before implementing.

### Current state (post green gate — 2026-10-23)

The port now compiles and the full test suite passes:

- `cargo check --all-targets` ✓, `cargo fmt --check` ✓, `cargo clippy --all-targets -- -D warnings` ✓, `cargo test` ✓ (33 tests passing).

- Implemented modules: `context`, `headers`, `options`, `metrics`, `health`, `server`,
  plus `lib.rs` re-exports and `Error` alias.

- Server behavior ported from Go `processPhase`:
  - phase dispatch via oneof arms (`RequestHeaders`, `ResponseHeaders`, `RequestBody`,
    `ResponseBody`, `RequestTrailers`, `ResponseTrailers`).
  - metrics incremented per phase; phase errors counted via `phase_errors`.
  - init errors / gen-headers errors invoke the processor `error_handler` and propagate.
  - processor errors propagate out of `process_phase`.

### Remaining known gaps (draft)

- Metrics HTTP endpoint wiring in `GenericExtProcServer::serve` (metrics registry +
  axum router are set up but the router is currently dropped/unused).
- Graceful shutdown wiring in `serve` (signal handling, `serve_with_shutdown`, close
  notification) — partially scaffolded, not finalized.
- Envoy `FullDuplexStreamed` body flow-control handling is marked not-implemented in
  envoy-types (`HttpBody.drain_complete`, flow-control fields).
- Local response streaming (`StreamedImmediateResponse`) not yet implemented.
- `Serve`/`MustServe` wrappers and `close` wiring finalized at the time of the green gate,
  but the end-to-end server lifecycle still needs a final review pass.

### Next steps

1. Finalize metrics HTTP endpoint wiring and graceful shutdown in `server.rs`.
2. Implement full-duplex streaming response handling if needed.
3. Re-run full gate: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
   `cargo test`.
4. Add the `Justfile` targets (`fmt`, `build`, `test`, …) — done.

## Go test inventory (ported so far)

- context_test.go: phase constants values/names, `default_phase_if_invalid`,
  `request_phase_to_string`, HeaderValue isValid/to_envoy conversions, PhaseResponse,
  RequestContext helpers (`newRequestContext`, `Get...`).
- headers_test.go: AllHeaders add/values/get/has/split_list/normalize behavior,
  gen_headers from HeaderMap (lowercase keys, multiple values, comma-splitting).
- health_test.go: NewHealthServer/NewReadyHealthServer status, MarkReady/MarkUnready,
  Check, Watch-unimplemented.
- metrics_test.go: TestMetricsCollectorsCoverEveryField, TestMetricsRegisterAndGather
  (registry gather produces expected families), TestMetricsDefault maybe.
- options_test.go: defaults of ServerOptions/ProcessingOptions, ApplyOptions helpers.
- phase_test.go: phase name strings for all 7 phases, errors like "processor is nil".
- server_test.go: Serve error when processor nil, processPhase dispatch, error handler.

## Go source inventory

- /tmp/envoy-extproc-sdk-go/*.go — context.go, extproc.go (RequestProcessor trait),
  headers.go, health.go, options.go, server.go, metrics.go, tests.
