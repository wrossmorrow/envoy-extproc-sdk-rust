# Load Testing

Load testing for the Envoy External Processor SDK (Rust), containerized via the
stack in [`tests/`](../tests) (envoy -> extproc -> echo upstream).

## Layout

- `*.js` — k6 scripts (smoke, steady, burst, delayed).
- The stack under test lives in [`tests/`](../tests):
  - envoy (`envoyproxy/envoy`) listening on `:8080` (published to host), extproc
    gRPC on `:50051`, metrics on `:9090`, envoy admin on `:9901`;
  - upstream echo server (`ghcr.io/wrossmorrow/simple-echo-server`) on `:8000`.
  - The extproc server in the stack is the SDK's `TesterRequestProcessor`
    (via the `extproc-tester` binary), i.e. no dedicated load-test processor is
    needed; load behavior is driven via the tester action JSON / headers the
    generator sends. (`NoopRequestProcessor` is only used by the example stack.)

## Running locally

Bring up the full containerized stack (envoy + upstream echo + extproc):

```sh
just -f tests/justfile up
```

Run a load test (k6 runs in a container via the `grafana/k6` image):

```sh
just loadtest-smoke
just loadtest-steady
just loadtest-burst
just loadtest-delayed
```

Or directly, e.g. for a custom k6 invocation:

```sh
docker run --rm --network host -v "$(pwd)/loadtest:/scripts" grafana/k6 run /scripts/smoke.js
```

Stop the stack:

```sh
just -f tests/justfile down
```

## Scenarios

| script | scenario | purpose |
|---|---|---|
| `smoke.js` | `per-vu-iterations` (1 VU, 1 iteration) | single request through the full chain; sanity check that the whole containerized path works and meets basic latency/error thresholds. |
| `steady.js` | `constant-arrival-rate` | sustained load. Tune via `RPS` (default 25) and `DURATION` (default 60s). Thresholds: error rate < 1%, p95 < 2s. |
| `burst.js` | `ramping-vus` | ramp concurrency to `VUS` (default 100) over 10s, ramp down. Thresholds: error rate < 1%, p95 < 2s. |
| `delayed.js` | `constant-arrival-rate` | upstream latency injection via `X-Echo-Delay` and/or error injection via `X-Echo-Status`, through the full chain. |

## Pass/fail signal

The single generic signal for each k6 script is the **k6 exit code** (and the
final run status line):

- **Exit 0** = pass. All configured thresholds met.
- **Exit 1** = fail. One or more thresholds were violated (k6 logs
  `level=error msg="thresholds on metrics ... have been crossed"`).

The human-readable equivalent is the final status line: `✓ [ 100% ] ...` means
pass; `✗ [...] ...` means a threshold failed.

Each script encodes its pass criteria via thresholds:

- `http_req_failed: ['rate<0.01']` — error rate < 1% (unexpected statuses; the
  `http.setResponseCallback(http.expectedStatuses(...))` marks 4xx/5xx as failed
  requests).
- `http_req_duration: ['p(95)<2000']` — p95 latency under the script's budget (per
  script: smoke <1s, steady/burst <2s, delayed <5s).

So a green run = exit 0 = no threshold violations = the full chain responded
within the latency/error budget. A red run (exit 1) = a threshold was crossed
(e.g. error rate too high, or p95 latency over budget).

Without the containerized stack running, the scripts will fail on connection
refused and trip the `http_req_failed` threshold — that's expected; a passing
run implies the full envoy -> extproc -> upstream chain actually responded.

## Configuration

k6 scripts read their inputs from environment variables:

| var | default | used by | notes |
|---|---|---|---|
| `TEST_TARGET` | `http://localhost:8080` | all | base URL of the envoy listener; point at a staging deployment for cloud runs. |
| `RPS` | `25` (steady), `10` (delayed) | steady, delayed | requests per second. |
| `DURATION` | `60s` (steady), `30s` (delayed) | steady, delayed | test duration. |
| `VUS` | `100` | burst | target concurrency. |
| `ECHO_DELAY` | `2` | delayed | upstream delay in seconds (`"2"` or `"2s"`). |
| `ECHO_STATUS` | `` (empty) | delayed | optional upstream status to inject (e.g. `"503"`); empty = no injection. |

## Metrics

- **k6** (per-script thresholds): `http_req_failed` (error rate), `http_req_duration` (latency percentiles, e.g. `p(95)`).
- **extproc**: `curl localhost:9090/metrics` (OpenMetrics) — e.g. `extproc_total_streams`, `extproc_active_streams`, `extproc_phase_errors_total`, `extproc_stream_duration_seconds`, body-byte counters. Use deltas between test start/end to compute per-run numbers.
- **envoy**: `curl localhost:9901/stats` — envoy-side stats (active connections, upstream response times, circuit breakers, etc.).

## Envoy tuning

The envoy `ext_proc` `message_timeout` is set to `60s` (capped) in
[`tests/envoy.yaml`](../tests/envoy.yaml) to accommodate delayed phases /
asynchronous processor work during load testing. The previous value (`2s`) was
considered too tight for such scenarios.

## Cloud

The same scripts run against any reachable envoy deployment:

- Self-hosted: run the containerized stack in the cloud (e.g. k8s) and point
  `TEST_TARGET` at it; run the same k6 scripts (via the same docker image or a
  locally-installed k6).
- k6 Cloud: run the same scripts in k6 Cloud against the staging deployment.

Follow-up work: gRPC-level load tests to isolate the processor from envoy
overhead (see the original load-testing plan).
