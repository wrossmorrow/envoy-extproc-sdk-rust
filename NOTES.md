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

### Remaining known gaps (verified against the Go sources — 2026-10-24)

The items below are the open work. During this review each was re-checked against the
Go implementation (mirrored into `/tmp/go_*.go` from `wrossmorrow/envoy-extproc-sdk-go@main`)
and several additional gaps were found that were not yet listed.

1. **Metrics HTTP endpoint wiring in `GenericExtProcServer::serve`** — NOT done.
   `serve` builds a throwaway `prometheus::Registry` + `Metrics::new()`, creates an axum
   `Router` with the `/metrics` handler, binds it to `let _metrics_router` (immediately
   dropped), and never serves it. The server-held `self.metrics` is not used for the
   registry. Completion: register `self.metrics.clone()` into the served registry, run the
   axum server on the `metrics_http_port` listener (with graceful shutdown), and shut it
   down during the graceful shutdown sequence.

2. **Graceful shutdown wiring in `serve`** — NOT finalized. Current state: uses
   `serve_with_shutdown(addr, ctrl_c-only signal)`, then `let _ = self.close(...)`.
   Compared to Go `Serve`, missing: SIGTERM handling, health `MarkUnready`, unready
   propagation wait, graceful stop, bounded force-stop, metrics server shutdown, close error
   propagation. Completion: SIGINT+SIGTERM signal future; mark unready; wait
   `unready_propagation_delay_seconds`; graceful stop; force stop after
   `termination_grace_period_seconds`; shutdown metrics server; propagate `close` error.

3. **Local response streaming (`StreamedImmediateResponse`)** — NOT implemented. The
   envoy-types `ProcessingResponse` oneof includes `StreamedImmediateResponse` (oneof tag
   `11`) for streaming local responses (`FULL_DUPLEX_STREAMED` / streaming body modes).
   The Rust port only produces `ImmediateResponse` and plain phase responses via
   `PhaseResponse::get_response`. Completion: map `PhaseResponse` onto the streamed arms
   and handle end-of-stream signaling (`end_of_stream` on streamed headers/body responses,
   `TrailersResponse`).

4. **Full-duplex body flow control** — NOT implemented; envoy-types marks
   `FlowControlInit`, `ClientWindowUpdate`, `ServerWindowUpdate` and
   `HttpBody.drain_complete` as not-implemented. Decision needed on scope (accept-and-
   document vs. implement where envoy-types permits).

### Additional gaps found during the post-green-gate review

- Metrics stream-level collectors are never incremented by the server:
  `total_streams`, `errored_streams`, `active_streams`, `stream_duration_seconds`,
  `total_empty_responses`, `response_send_errors`. Only phase counters and body-byte
  counters are currently used. Go wires these in the stream handler via deferred stream
  accounting and send-error handling.
- `ProcessingOptions::abort_on_processor_failure` is never consulted by the server.
- `serve` creates a fresh `Metrics` + registry instead of using the server-held
  `self.metrics`.
- `ProcessStream` receive-error handling does not mirror Go: `codes.Canceled` should end
  the stream cleanly; other receive errors should route through the processor
  `error_handler` with phase `UNDETERMINED`.
- `GenericExtProcServer::close` error is discarded in `serve`.

### Completion plan for the remaining gaps

Ordered plan derived from the gaps above, sequenced against the existing green gate
(`cargo check --all-targets`, `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test` — all passing, 33 tests):

1. **Phase 1 — Metrics HTTP endpoint wiring** (completes gap 1) — **DONE (2026-10-24)**.
   - ✅ Done: `server.rs` registers the server-held `self.metrics` into a `prometheus::Registry`;
     passes it into the axum handler via `MetricsState`; binds a `TcpListener` on
     `metrics_http_port`; runs `axum::serve(...).with_graceful_shutdown(...)`; spawns the task;
     aborts it on gRPC serve error and gracefully awaits it during the shutdown sequence.
   - ⬜ Open decision: Go uses `promhttp.HandlerFor` with `EnableOpenMetrics: true` and
     promhttp's content type. Rust uses `prometheus::TextEncoder` + manual content-type
     header. Decide whether to switch to OpenMetrics output or keep TextEncoder.
   - Tests: handler/registry wiring unit tests; keep full gate green.

2. **Phase 2 — Graceful shutdown wiring** (completes gap 2) — **IN PROGRESS (2026-10-24)**.
   - ✅ Done: replaced the ctrl_c-only signal future with an SIGINT+SIGTERM future via
     `tokio::signal::unix::{signal, SignalKind}`; signal future marks the health service
     unready and waits out `unready_propagation_delay_seconds`; tonic `serve_with_shutdown`
     performs the graceful stop; processor `close` error is now propagated.
   - ⬜ Remaining: force stop after `termination_grace_period_seconds`. Design decision
     (recorded): tonic 0.14 `serve_with_shutdown` performs graceful shutdown without an
     externally-bounded force-stop. Either accept tonic's semantics or restructure around an
     incoming stream to implement the bounded force-stop. Metrics server shutdown deferred to
     Phase 1.
   - 📝 Note (2026-10-24): maintainer decision — in Kubernetes the force stop after the grace
     period happens automatically when the process is killed (kubelet sends SIGTERM, then
     SIGKILL per `terminationGracePeriodSeconds`). The SDK may not need its own bounded
     force-stop; graceful stop + close may be sufficient. Revisit if running outside k8s.

3. **Phase 3 — Stream lifecycle metrics, error semantics and abort** (completes the
   additional findings above).
   - Wire stream-level metrics: `total_streams`, `active_streams`, `errored_streams`,
     `stream_duration_seconds`, `response_send_errors`, `total_empty_responses`.
   - Wire `ProcessingOptions::abort_on_processor_failure`.
   - Fix `ProcessStream` receive-error handling (Canceled → clean end; other errors →
     `error_handler(phase = UNDETERMINED)` + error).
   - Propagate `close` error instead of discarding it.

4. **Phase 4 — Streaming responses** (completes gaps 3–4).
   - Implement `StreamedImmediateResponse` emission and end-of-stream signaling.
   - Resolve full-duplex flow-control scope decision.

5. **Phase 5 — Final gate and notes.**
   - Re-run full gate: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
     `cargo test`.
   - Update the Go test inventory to cover the newly ported tests.

Suggested order, updated per maintainer prioritization:

1. **Phase 2 — Graceful shutdown wiring** (most important; graceful shutdown is essential).
2. **Phase 1 — Metrics HTTP endpoint wiring** (important but not critical).
3. **Phase 3 — Stream lifecycle metrics, error semantics and abort** (third).
4. **Phase 5 — Final gate and notes** (last).
5. **Phase 4 — Streaming responses** — **DEFERRED/ignored for now** (maintainer decision;
   deemed not handled in the Go SDK and likely adds would be needed in both SDKs).

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
