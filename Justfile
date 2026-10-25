# Task runner for the envoy extproc SDK Rust port.

default:
    @just --list

# Format the Rust sources.
fmt:
    cargo fmt

# Check formatting without modifying files.
fmt-check:
    cargo fmt --check

# Type-check the crate and all targets.
check:
    cargo check --all-targets

# Build the crate.
build:
    cargo build

# Lint with clippy, denying warnings.
clippy:
    cargo clippy --all-targets -- -D warnings

# Run the unit test suite. Integration tests under tests/ drive a running
# server and are run separately via `just integration-test`.
test:
    cargo test --lib

# Run the integration test suite. Brings up the docker compose harness
# (envoy + upstream echo + extproc) and runs the tests against it; the harness
# must be up for the tests to pass. Single-threaded: the integration tests
# assert on cumulative metrics deltas against a shared server, which is not
# safe under parallel test execution.
integration-test:
    just -f tests/justfile up
    cargo test --tests -- --test-threads=1

# Run the full gate: format check, clippy, unit tests.
all: fmt-check clippy test

# Run the extproc server locally (gRPC on :50051, metrics on :9090).
run:
    cargo run

# Run a specific example processor (e.g. `just run example="noop"`, flags like
# `-port 50051 -update-extproc-header -update-duration-header`).
run-example example="noop" *flags="":
    cargo run --bin extproc-example -- {{example}} {{flags}}

# Build the example server binary.
build-example:
    cargo build --bin extproc-example

# Run a k6 load test script from loadtest/ against the containerized stack
# (envoy -> extproc -> echo upstream). Usage: just loadtest <script> [k6 args...]
loadtest *args="smoke.js":
    docker run --rm --network host -v "$(pwd)/loadtest:/scripts" ghcr.io/grafana/k6/k6 run /scripts/{{args}}

# Run the k6 smoke load test against the full containerized envoy chain.
loadtest-smoke:
    just loadtest smoke.js

# Run the k6 steady-state load test (RPS via $RPS, duration via $DURATION).
loadtest-steady:
    just loadtest steady.js

# Run the k6 burst load test (VUs via $VUS).
loadtest-burst:
    just loadtest burst.js

# Run the k6 delayed/upstream-latency load test (ECHO_DELAY/ECHO_STATUS).
loadtest-delayed:
    just loadtest delayed.js
