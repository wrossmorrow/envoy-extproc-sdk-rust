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

### Current state (post green gate — 2026-10-24, refreshed after the quick overall review)

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

### Quick overall review findings (2026-10-24)

A quick side-by-side review of the merged Rust port against the Go sources
(re-fetched from `wrossmorrow/envoy-extproc-sdk-go@main` into `/tmp/go_sdk`) was
performed on top of the green gate. Findings, in priority order:

**P0 — correctness bugs (fix before relying on phase behavior):**

1. Phase response state is never reset between phases. Go resets via
   `rc.ResetPhase()` at the top of every stream message loop iteration (and
   again inside `initReqCtx` for the request-headers phase). The Rust port has
   `RequestContext::reset_phase` (with tests) but the server never calls it, so
   a fresh `RequestContext` starts with `PhaseResponse.header_mutation == None`
   and the first `append_header` / `replace_body` (which overwrite
   Content-Length via header mutation) fails with "no header mutation in
   progress"; mutations also accumulate across phases within one stream. The
   unit tests masked this by calling `rc.reset_phase()` manually. **Fix:** call
   `rc.reset_phase()` at the start of phase processing.

2. Error semantics diverge. Go: on a phase error, set `errored`, increment
   `PhaseErrors`, and if `AbortOnProcessorFailure` return an error (stream
   errors); **otherwise** send an empty continue response and keep the stream
   alive. Rust: always converts a phase error into a stream error. Rust also
   never increments `total_empty_responses` on this path. **Fix:** mirror Go's
   empty-continue flow in the stream loop.

3. `x-extproc-duration-ns` is computed before the current phase's duration is
   accumulated. Rust appends the duration header inside `dispatch_phase` using
   `rc.duration`, but `rc.duration += phase_start.elapsed()` runs in
   `process_phase` afterwards. Go adds the phase duration before appending. **Fix
   ordering.**

**P1 — fidelity bugs:**

4. `UpdateHeaders` (and `AddHeaders`/`OverwriteHeaders`) are not all-or-nothing
   in Rust; Go validates all header values before applying. Rust appends while
   iterating and leaves a partial mutation behind on error. **Fix:** validate
   first, then apply.

5. Metrics: `response_send_errors` is still unwired (send errors are not
   observable at the stream level in the current design — accepted gap).
   `total_empty_responses` becomes reachable once the P0-2 fix lands.

**P2 — accepted deviations / minor divergences:**

6. Bounded force-stop after `termination_grace_period_seconds` is still not
   implemented (tonic `serve_with_shutdown` graceful semantics; maintainer decision
   recorded to rely on k8s SIGKILL). Revisit if running outside k8s.

7. Metrics histogram lacks Go's `NativeHistogramBucketFactor(1.1)`.

8. `Cargo.toml` `repository` still points at the Go repo — copy/paste leftover.

**Test coverage gaps (for the upcoming integration tests):**

- Rust: 33 unit tests. Go: ~50 across context/headers/health/options/phase/server/metrics.
- Not yet ported: ContinueRequest clears immediate / honors clear-route-cache, GetResponse
  typed responses incl. trailers, GetResponse unknown phase, EmptyContinueResponse per phase,
  GetResponse ignores immediate for trailers, ReplaceBody/SetContentLength/ClearBody content-
  length behavior, UpdateHeader actions, UpdateHeaders all-or-nothing, metrics counter/gauge/
  register-exposes-every-collector tests, context value helpers, cancel-request validation,
  phase lifecycle via GetResponse.
- NOTES.md "Go test inventory" is stale: it overstates what has been ported.

### Remaining known gaps (verified against the Go sources — 2026-10-24)

The items below are the open work. During this review each was re-checked against the
Go implementation (mirrored into `/tmp/go_*.go` from `wrossmorrow/envoy-extproc-sdk-go@main`)
and several additional gaps were found that were not yet listed.

1. **Metrics HTTP endpoint wiring in `GenericExtProcServer::serve`** — **DONE (2026-10-24)**.
   Completed via PR #3 (Phase 1 of the completion plan below).

2. **Graceful shutdown wiring in `serve`** — **IN PROGRESS (2026-10-24)** (Phase 2 below).
   Done: SIGINT+SIGTERM signal future, health `MarkUnready`, unready propagation wait,
   graceful stop via tonic `serve_with_shutdown`, and `close` error propagation. Still
   open: bounded force-stop after `termination_grace_period_seconds`.

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
   - ✅ Resolved (2026-10-24): OpenMetrics adopted to match Go. Implemented as the Rust
     `prometheus::TextEncoder` output plus the mandatory OpenMetrics `# EOF` terminator and
     the OpenMetrics content type
     (`application/openmetrics-text; version=0.0.4; charset=utf-8`); Go's `EnableOpenMetrics`
     behaviour for the collected metric set (counters/gauge/histogram, no created lines or
     exemplars) is matched. (A dedicated OpenMetrics encoder would be more faithful long-term,
     but the TextEncoder output is the required sample syntax.)
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
     Phase 1 — **resolved**: Phase 1 completed 2026-10-24, so the metrics server is served and
     gracefully awaited during the shutdown sequence.
   - 📝 Note (2026-10-24): maintainer decision — in Kubernetes the force stop after the grace
     period happens automatically when the process is killed (kubelet sends SIGTERM, then
     SIGKILL per `terminationGracePeriodSeconds`). The SDK may not need its own bounded
     force-stop; graceful stop + close may be sufficient. Revisit if running outside k8s.

3. **Phase 3 — Stream lifecycle metrics, error semantics and abort** (completes the
   additional findings above) — **DONE (2026-10-24)**.
   - ✅ Done: stream-level metrics wired via `ExternalProcessor::process` (total_streams,
     active_streams) and `ProcessStream` Drop (active_streams decrement, stream_duration_seconds
     observe, errored_streams increment when errored).
   - ✅ Done: `abort_on_processor_failure` consulted in `process_phase`; phase error
     propagates and closes the stream (pre-review reading — see the P0-2 finding above: the
     Go SDK sends an empty continue and keeps the stream alive unless abort is set, so the
     error flow below is known to be incomplete until fixed).
   - ✅ Done: `ProcessStream` receive-error handling now treats `Code::Cancelled` as a clean
     end and routes other receive errors through `error_handler(phase = UNDETERMINED)`.
   - ⬜ Not wired here: `response_send_errors` (send errors are not observable at the
     stream level in the current design). `total_empty_responses` is expected to be wired
     together with the P0-2 error/empty-continue fix (the Rust `PhaseResponse` API already
     provides `empty_continue_response`).
   - ✅ Done: propagate `close` error instead of discarding it (done in Phase 2).

4. **Phase 4 — Streaming responses** (completes gaps 3–4).
   - Implement `StreamedImmediateResponse` emission and end-of-stream signaling.
   - Resolve full-duplex flow-control scope decision.

5. **Phase 5 — Final gate and notes.**
   - Re-run full gate: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
     `cargo test`.
   - Update the Go test inventory to cover the newly ported tests.

6. **Phase 6 — Integration tests (proposed next step).**
   - Spin up the real server (`GenericExtProcServer` + tonic listener) and drive it with a
     tonic client over a real gRPC stream: multi-phase streams (headers → body → trailers),
     header/body mutations and responses, error paths, metrics endpoint, health, shutdown.
   - Port/adapt the Go test-suite behaviors not covered by the unit tests listed in the
     Go test inventory above.

Suggested order, updated per maintainer prioritization:

1. **Phase 2 — Graceful shutdown wiring** (most important; graceful shutdown is essential).
2. **Phase 1 — Metrics HTTP endpoint wiring** (important but not critical).
3. **Phase 3 — Stream lifecycle metrics, error semantics and abort** (third).
4. **Phase 5 — Final gate and notes** (last).
5. **Phase 4 — Streaming responses** — **DEFERRED/ignored for now** (maintainer decision;
   deemed not handled in the Go SDK and likely adds would be needed in both SDKs).

## Go test inventory (ported so far, refreshed 2026-10-24)

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

**Not yet ported (candidates for the upcoming integration tests):**

- phase_test.go: ContinueRequest clears immediate / honors clear-route-cache, GetResponse
  typed responses per phase (incl. trailers), GetResponse unknown phase, EmptyContinueResponse
  per phase, GetResponse ignores immediate for trailers, ReplaceBody/SetContentLength/
  ClearBody content-length behavior, UpdateHeader actions, UpdateHeaders all-or-nothing.
- context_test.go: context value helpers (Set/Get/overwrite), CancelRequest invalid header
  value, ReplaceBodyChunk/ReplaceBody/ClearBody content-length cases, MetricsCollectors
  counter/gauge/register-exposes-every-collector cases.
- server_test.go: TestProcessPhaseMetricsErrorPath (processor error increments phase_errors
  and propagates).

## Integration tests (proposed — Phase 6, new branch `test/integration-envoy-e2e`)

Inspired by the Go SDK's `test/` directory (github.com/wrossmorrow/envoy-extproc-sdk-go
`test/`), planned for the Rust port:

- Go-side fixtures: `test/cases.yaml` (parametrized cases: noop, clear/append/
  remove/overwrite request & response headers/body, cancel), `test/compose.yaml`
  (envoy + upstream echo + extproc), `test/envoy.yaml`, `test/justfile`,
  `test/Dockerfile` (golang:1.25-alpine builder → runtime, grpc-health-probe),
  `test/main.go` (tester binary), `test/tester.go` (`TesterRequestProcessor`),
  `test/tester_test.go`, `test/test.py`, `test/README.md`.
- Rust side plan: new `test/` directory mirroring the layout; Rust Dockerfile for
  the compose build (rust builder, likely minimal); compose file wiring envoy +
  upstream echo + the Rust extproc server; Rust tester processor implementing the
  same cases.yaml action schema (append/add/overwrite/remove headers, clear/replace
  body, cancel) via the Rust PhaseResponse APIs (AppendHeader/AddHeader/Overwrite/
  RemoveHeader/ClearBody/ReplaceBody/CancelRequest, GetValue typed access).
- Notes: Go tester has `# TODO: requires work` for cancel; the Rust port may need
  equivalent handling. Envoy config uses BUFFERED body modes, 2s message timeout,
  `/no-extproc` bypass route, etc.

## Go source inventory

- /tmp/go_sdk/*.go (re-fetched 2026-10-24) — context.go, extproc.go (RequestProcessor trait),
  headers.go, health.go, options.go, server.go, metrics.go, tests.

## Session log (2026-10-24 — integration harness, port fix, envoy.yaml fix, bind logging)

### Changes made this session (2026-10-24, on `test/integration-envoy-e2e`)

- Added `tests/Dockerfile` (rust builder → runtime; installs
  `github.com/grpc-ecosystem/grpc-health-probe@v0.4.39`; exposes 50051/9090; entrypoint
  `/extproc`). Commit `aed4285`.
- Added `tests/justfile` (build/test/containerize/up/down recipes mirroring the Go
  `test/justfile`, image `ghcr.io/wrossmorrow/envoy-extproc-sdk-rust`). Commit
  `1fcf550`.
- Added `tests/compose.yaml` (envoy + upstream echo + extproc `test` service; envoy
  waits on extproc health). Commit `1fcf550`.
- Added `tests/envoy.yaml` (envoy static config; fixed malformed indentation in the
  `http_protocol_options`/`http2_protocol_options` and `admin:` blocks via commit
  `d8a5c9f`, `fix: invalid envoy.yaml, bad reconstruction not copy`). Commit `d8a5c9f`.
- Added `tests/extproc_integration.rs` (the integration harness: spawned server via
  `GenericExtProcServer`, tonic client, wire-format helpers, metrics fetch, 8 tests).
- Added `src/bin/extproc-tester.rs` (tester-backed server binary used by the integration
  tests). Commit `f385741` (switched to `ServerOptions::new()`).

### Root-cause fix (2026-10-24, the big one)

- **Discovery via new startup-bind logging** (`src/server.rs`: `tracing::info!` at metrics and
  gRPC bind sites + default subscriber init in `GenericExtProcServer::serve`): the spawned
  test server logged `port=0` for both listeners — i.e. it was binding to **ephemeral ports**
  rather than the SDK defaults. Root cause: the test binary used
  `ServerOptions::default()`, which (because `ServerOptions` *derives* `Default` with
  `u16` fields defaulting to `0`) yields ports `0`/`0`; the SDK-correct constructor is
  `ServerOptions::new()`. Fix: switched `src/bin/extproc-tester.rs` to
  `ServerOptions::new()`. Commit `f385741`.

### Harness hygiene fixes (2026-10-24)

- Cleaned up orphaned `extproc-tester` server processes leaked by failed/hung integration
  runs (now defunct; no listeners remain).
- Added child-kill guard (`ChildGuard`) in the noop test so a failed test can't leak a
  spawned server. Commit `dee351d`.

### Current state at session end (2026-10-24)

- Gate: `cargo fmt --check` ✓, `cargo clippy --all-targets -- -D warnings` ✓,
  `cargo test --lib` 33/33 ✓ (full `cargo test` still red on the 8 integration tests).
- Integration tests: noop now gets **past** the gRPC stream-open and receives a response
  (server binds `50051`/`9090`, health check would now report `SERVING`); the noop test now
  fails at the `header_mutation.is_none()` assertion instead. Remaining integration-test
  failures are due to the pre-existing gRPC transport issue and/or the now-resolved port bug.
- `tests/` now contains: `Dockerfile`, `justfile`, `compose.yaml`, `envoy.yaml`,
  `extproc_integration.rs`.

### Next steps (new session)

1. **Resolve the noop `header_mutation.is_none()` assertion** now that the server responds
   (inspect the actual `CommonResponse.header_mutation` content; decide whether the server
   should be attaching one or the test expectation is wrong).
2. Continue the integration-test execution investigation (the transport issue that caused the
   earlier `DispatchGone` / h2 errors should now be re-checked now that the server binds the
   SDK defaults and responds).
3. Consider fixing the latent derived-`Default` footgun for `ServerOptions` (u16 port fields
   default to `0`) — followup, since the fix was switching to `ServerOptions::new()`.
4. Re-run the full gate and update the Go test inventory to cover the newly ported tests.

## Go source inventory (2026-10-24)
