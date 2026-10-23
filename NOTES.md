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
  - trait `Health` at `tonic_health::pb::grpc_health_v1::health_server::Health`
  - messages at `tonic_health::pb::grpc_health_v1::health::{HealthCheckRequest, HealthCheckResponse, ServingStatus}`
  (verify exact paths via compiler errors.)
- `prometheus = "0.14"` — `Registry`, `register`, `gather`, `Counter`, `Gauge`,
  `Histogram`, `HistogramOpts`, `Encoder`, `TextEncoder`; raw proto model at
  `prometheus::proto::{MetricFamily, Metric, MetricType, ...}`.
- `axum = "0.8"` — planned metrics HTTP server (Go uses net/http; axum can wrap it).
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
- prometheus `Histogram::observe(f64)`, `Counter::inc/inc_by`, `Gauge::set/inc/add`,
  `HistogramOpts::new(name, help)`.
- `prometheus::Registry::gather() -> Vec<MetricFamily>`; family name via
  `.name()`, type via `type_` (MetricType), values via message getters
  (`.counter.get_value()`, `.gauge.get_value()`, ...).
- `prometheus::Encoder::encode(&self, mfs: &[MetricFamily], w: &mut W)` and
  `TextEncoder::new().encode_to_string(&mfs)`.

### Current compile-error state (known, being fixed iteratively)

- `/work/repo/src/lib.rs` — re-exports and `Error` alias (`Box<dyn Error + Send + Sync>`)
  and module declarations. Was being reconciled with the other modules.
- `/work/repo/src/context.rs` — phase constants (u32, UNDETERMINED=0 .. 6),
  `default_phase_if_invalid`, `request_phase_to_string`, `HeaderValue`
  (with `is_valid`, `to_envoy_header_value`, `to_envoy_header_value_option`),
  `PhaseResponse`, `RequestContext`. Had duplicate `Error`, missing
  `use std::error::Error;`, stray text, `HeaderAppendAction::from_str_name`
  signature drift, `CommonResponse.trailers` required `None` field, brace
  syntax errors in `get_response`/`empty_continue_response` (fixed via scripted
  replacement). `RequestContext` should NOT derive Clone.
- `/work/repo/src/headers.rs` — `AllHeaders` (HashMap-based,
  `headers: HashMap<String, Vec<String>>`, `raw_headers: HashMap<String, Vec<Vec<u8>>>`,
  add/values/get/has/split_list, `gen_headers(&HeaderMap) -> Result<AllHeaders, Error>`).
  Had an error: `return Err(format!(...).into())` missing error conversion.
- `/work/repo/src/options.rs` — `ServerOptions` (ports u16/u32, defaults
  50051/9090/1000/5/10), `ProcessingOptions` (bools, defaults false).
- `/work/repo/src/metrics.rs` — full implementation written: `Metrics` with 14
  public collector fields (`active_streams: Gauge`, `errored_streams: Counter`,
  `phase_errors: Counter`, `total_streams: Counter`, `stream_duration_seconds:
  Histogram`, `total_request_headers`, `total_request_body`, `total_request_trailers`,
  `total_response_headers`, `total_response_body`, `total_response_trailers`,
  `total_empty_responses`, `request_body_bytes`, `response_body_bytes`,
  `response_send_errors`), `new()`, `register(&Registry) -> Result<(), Error>`,
  `collectors() -> Vec<&dyn Collector>`. Stubbed in an earlier draft, now full.
- `/work/repo/src/health.rs` — rewritten: `HealthStatus` enum, `HealthServer`
  (fields `reporter: HealthReporter`, `status: HealthStatus`; `new() -> NotServing`,
  `new_ready() -> Serving`, `with_status`, `get_status`, `mark_ready/mark_not_ready`),
  `health_service() -> HealthService`. Trait-impl sketch used tentative paths
  (`tonic_health::pb::grpc_health_v1::health_server::Health`, etc.) — paths to be
  validated by the compiler. **NOT yet compiled** (module was just written).
- `/work/repo/src/server.rs` — **rewrite in progress**. Current content has the
  trait and struct, plus a deliberately broken placeholder
  (`if rc == ... // placeholder`). Needs a full rewrite:
  - `trait RequestProcessor: Send + Sync + 'static` (object-safe; methods:
    `get_name`, `get_options`, `process_request_headers/trailers/body`,
    `process_response_headers/body/trailers`, `error_handler`, `close`).
  - `struct GenericExtProcServer<P: RequestProcessor>` (fields `name`, `processor: Arc<P>`,
    `options: Option<ProcessingOptions>`, `metrics: Metrics`).
  - `process_phase(&self, proc_req: &ProcessingRequest, processor: &dyn RequestProcessor, rc: &mut RequestContext) -> Result<(Option<ProcessingResponse>, u32), Error>`
    — port of Go `processPhase`.
  - `#[tonic::async_trait] impl<P: RequestProcessor> ExternalProcessor for P` with
    `type ProcessStream = Pin<Box<dyn Stream<Item = Result<ProcessingResponse, tonic::Status>> + Send + 'static>>;`
  - `process` handler implementing the trait, wiring phases to the right methods.
  - `Serve`, graceful shutdown, health registration (pending).
- **Not yet created**: `src/lib.rs` module wiring finalized, `src/options.rs` finalized,
  `src/server.rs` finalized, tests.

### Next steps

1. Rewrite `/work/repo/src/server.rs` completely and carefully.
2. Finish `lib.rs`, `options.rs`; make `cargo check` pass.
3. Add missing deps to `Cargo.toml`: `tokio-stream`, maybe `prost-types`.
4. Implement `Serve` (grpc server, health, metrics HTTP via axum, signal handling,
   graceful stop) — likely in `server.rs` or a separate `grpc_server.rs`.
5. Write tests replicating Go tests (context, headers, options, phase, metrics, health, server).
6. `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.

## Go test inventory (to port)

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
