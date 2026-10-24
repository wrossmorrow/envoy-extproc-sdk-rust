# Envoy External Processor SDK — Rust Port: Working Notes

Working notes for the Rust port of
[`envoy-extproc-sdk-go`](https://github.com/wrossmorrow/envoy-extproc-sdk-go).
The port mirrors the Go SDK's behavior; the sections below track the plan, the
current state, and the known gaps.

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

Remaining plan items:

5. **Stage 5** — Implement integration tests. **In progress**: the `tests/` harness
   now has `extproc_integration.rs` (gRPC) and `http_integration.rs` (plain HTTP;
   HTTP suite at 10/11 passing, one red), plus `Dockerfile`/`justfile`/`compose.yaml`/
   `envoy.yaml`. Not yet done: full green gate across all suites, remaining cases.
6. **Stage 6** — Implement examples. **Not started**: port the Go `examples/`
   processors (`noop`, `trivial`, `timer`, `data`, `digest`, `dedup`, `masker`,
   `echo`) to Rust.

## Environment

- Rust toolchain at `/usr/local/cargo` (rustc/cargo 1.99.1); must run cargo with
  `export CARGO_HOME=/work/.cargo` (default CARGO_HOME has permission issues).
- Running cargo from `/work` dirs.

## Dependencies

(all resolved via crates.io)

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
  options/logger wiring.

## Rust API learnings

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

## Current status

The port now compiles and the unit test suite passes:

- `cargo check --all-targets` ✓, `cargo fmt --check` ✓, `cargo clippy --all-targets -- -D warnings` ✓, `cargo test` ✓ (33 unit tests passing).

- Implemented modules: `context`, `headers`, `options`, `metrics`, `health`, `server`, plus `lib.rs` re-exports and the `Error` alias. (`tester` module backs the integration harness.)

- Server behavior ported from Go `processPhase`:
  - Phase dispatch via oneof arms (`RequestHeaders`, `ResponseHeaders`, `RequestBody`,
    `ResponseBody`, `RequestTrailers`, `ResponseTrailers`).
  - Phase response state is reset via `rc.reset_phase()` at the start of phase
    processing (mirroring Go's per-message reset, including the request-headers
    `initReqCtx` reset).
  - Metrics incremented per phase; phase errors counted via `phase_errors`; init
    errors / gen-headers errors invoke the processor `error_handler` and propagate.
  - Error semantics mirror Go: on a phase error, if the processor opted in via
    `abort_on_processor_failure`, the error propagates (stream errors); otherwise
    an empty continue response is sent and the stream stays alive.

- Metrics HTTP endpoint wiring (**done**, Phase 1): the server registers the
  server-held `self.metrics` into a `prometheus::Registry`, serves `/metrics` via
  axum, and emits OpenMetrics output (with the mandatory `# EOF` terminator and
  the OpenMetrics content type) matching Go's `EnableOpenMetrics` behavior.

- Graceful shutdown wiring (**in progress**, Phase 2): SIGINT+SIGTERM signal
  future, health `MarkUnready`, unready propagation wait, tonic
  `serve_with_shutdown`, and `close` error propagation. Bounded force-stop after
  `termination_grace_period_seconds` still open (see Remaining known gaps).

- Stream lifecycle metrics, error semantics and abort (**done**, Phase 3).

## Remaining known gaps

- **Local response streaming** (`StreamedImmediateResponse`) — **NOT implemented**;
  Phase 4 deferred. envoy-types marks `FlowControlInit`, `ClientWindowUpdate`,
  `ServerWindowUpdate` and `HttpBody.drain_complete` as not-implemented. Completion
  requires mapping `PhaseResponse` onto the streamed arms and handling end-of-stream
  signaling. Scope decision recorded (accept-and-document vs implement where
  envoy-types permits).

- **Full-duplex body flow control** — **NOT implemented**; envoy-types marks the
  flow-control messages as not-implemented. Decision needed on scope (accept-and-
  document vs implement where envoy-types permits).

- **Bounded force-stop** after `termination_grace_period_seconds` — **NOT
  implemented**; tonic 0.14 `serve_with_shutdown` performs graceful shutdown
  without an externally-bounded force-stop. Design decision recorded: in
  Kubernetes the force stop happens automatically when the process is killed
  (kubelet sends SIGTERM, then SIGKILL per `terminationGracePeriodSeconds`), so
  the SDK may not need its own bounded force-stop; revisit if running outside k8s.

- **Response send errors** — `response_send_errors` still unwired (send errors are
  not observable at the stream level in the current design — accepted gap).

- Metrics histogram lacks Go's `NativeHistogramBucketFactor(1.1)`.

- `Cargo.toml` `repository` still points at the Go repo — copy/paste leftover.

## Completion plan

Ordered plan derived from the gaps above, sequenced against the existing green gate
(`cargo check --all-targets`, `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test` — all passing, 33 tests):

1. **Phase 1** — Metrics HTTP endpoint wiring (completes gap 1) — **DONE**.
   - Done via PR #3.
2. **Phase 2** — Graceful shutdown wiring (completes gap 2) — **IN PROGRESS**.
   - Done: SIGINT+SIGTERM signal future, health `MarkUnready`, unready propagation wait,
     graceful stop via tonic `serve_with_shutdown`, and `close` error propagation.
   - Still open: bounded force-stop after `termination_grace_period_seconds`.
3. **Phase 3** — Stream lifecycle metrics, error semantics and abort — **DONE**.
4. **Phase 4** — Streaming responses — **DEFERRED** (maintainer decision recorded).
5. **Phase 5** — Final gate and notes.
6. **Phase 6** — Integration tests (proposed next step).

## Examples Port (Go SDK → Rust SDK)

**Status:** in progress. Starting-point branch `feat/examples-port` (PR open).

Port of the Go SDK
[`examples/`](https://github.com/wrossmorrow/envoy-extproc-sdk-go/tree/main/examples)
(`noop`, `trivial`, `timer`, `data`, `digest`, `dedup`, `masker`, `echo`, `router`) to the
Rust SDK, as runnable example binaries built on the public `extproc` lib API.

Summary of the agreed port plan:

- Dispatcher binary: `src/bin/extproc-example.rs` with processor modules under
  `src/bin/processors/`. Takes the processor name as the first argument and mirrors the Go
  `examples/main.go` flags (`-port`, `-update-extproc-header`, `-update-duration-header`).
- The Rust `RequestProcessor` trait has no `Init`; options live in each processor struct and
  flow via `get_options`.
- Errors are `Box<dyn Error + Send + Sync>` (`extproc::Error`).
- Phase helpers: `continue_request`, `cancel_request(status, &HashMap<String, HeaderValue>, body: &[u8])`,
  `add/append/overwrite/remove_header`, `replace_body/clear_body/replace_body_chunk/clear_body_chunk`,
  `clear_route_cache`, `set_value/get_value` (values are `Box<dyn Any + Send + Sync>`, downcast via
  `DynAny = dyn Any + Send + Sync`), `end_of_stream`, `started: Instant`, `duration: Duration`, phase
  consts, `get_response`.
- Immediate responses are valid for request-headers/request-body/response-headers/response-body
  phases; trailers phases ignore them.
- Digest example needs interior mutability for the shared hasher across phases
  (`Mutex<Sha256>` via ctx data).
- Masker needs JSON flatten/unflatten (via `serde_json`) — decision: hand-rolled minimal helpers.
- Dedup cache: static `Mutex<HashMap<String, bool>>` (Go used a package var).
- Example-processor logging: `tracing` (server installs a subscriber).

Port progress on this branch (checked off as each example lands):

- [x] Port plan + status summary added here; starting-point dispatcher binary and `noop`
      processor committed.
- [ ] `trivial`
- [ ] `timer`
- [ ] `data`
- [ ] `digest`
- [ ] `dedup`
- [ ] `masker`
- [ ] `echo`
- [ ] `router`

## Go test inventory (ported so far)

- context_test.go: phase constants values/names, `default_phase_if_invalid`,
  `request_phase_to_string`, HeaderValue isValid/to_envoy conversions, PhaseResponse,
  RequestContext helpers (`newRequestContext`, `Get...`).
- headers_test.go: AllHeaders add/values/get/has/split_list/normalize behavior,
  gen_headers from HeaderMap (lowercase keys, multiple values, comma-splitting).
- health_test.go: NewHealthServer/NewReadyHealthServer status, MarkReady/MarkUnready,
  Check, Watch-unimplemented.
- metrics_test.go: TestMetricsCollectorsCoverEveryField, TestMetricsRegisterAndGather
  (registry gather produces expected families).
- options_test.go: defaults of ServerOptions/ProcessingOptions, options file loading
  helpers (`from_json`/`from_yaml`).
- phase_test.go: phase name strings for all 7 phases, errors like "processor is nil".
- server_test.go: Serve error when processor nil, processPhase dispatch, error handler.

**Not yet ported** (candidates for the upcoming integration tests):

- phase_test.go: ContinueRequest clears immediate / honors clear-route-cache, GetResponse
  typed responses per phase (incl. trailers), GetResponse unknown phase, EmptyContinueResponse
  per phase, GetResponse ignores immediate for trailers, ReplaceBody/SetContentLength/ClearBody
  content-length behavior, UpdateHeader actions, UpdateHeaders all-or-nothing.
- context_test.go: context value helpers (Set/Get/overwrite), CancelRequest invalid header
  value, ReplaceBodyChunk/ReplaceBody/ClearBody content-length cases, MetricsCollectors
  counter/gauge/register-exposes-every-collector cases.
- server_test.go: TestProcessPhaseMetricsErrorPath (processor error increments phase_errors
  and propagates).

## Integration tests

- Go-side fixtures: `test/cases.yaml` (parametrized cases: noop, clear/append/
  remove/overwrite request & response headers/body, cancel), `test/compose.yaml`
  (envoy + upstream echo + extproc), `test/envoy.yaml`, `test/justfile`,
  `test/Dockerfile` (golang:1.25-alpine builder → runtime, grpc-health-probe),
  `test/main.go` (tester binary), `test/tester.go` (`TesterRequestProcessor`),
  `test/tester_test.go`, `test/test.py`, `test/README.md`.
- Rust side: `tests/` holds `Dockerfile`, `justfile`, `compose.yaml`, `envoy.yaml`,
  `extproc_integration.rs` (gRPC harness: spawned server via `GenericExtProcServer`,
  tonic client, wire-format helpers, metrics fetch, 8 tests), and
  `http_integration.rs` (plain-HTTP end-to-end, 11 tests ported from the Go
  `tester_test.go`/`cases.yaml`; 10/11 passing, `http_remove_response_header` still
  red due to a body-mutation rewrite failure).
- Rust side plan: new `test/` directory mirroring the Go layout; Rust tester
  processor implementing the same `cases.yaml` action schema via the Rust
  `PhaseResponse` APIs.

## Go source inventory

- Go sources re-fetched from `wrossmorrow/envoy-extproc-sdk-go@main` into
  `/tmp/go_sdk` (context.go, extproc.go (RequestProcessor trait), headers.go,
  health.go, options.go, server.go, metrics.go, tests).
