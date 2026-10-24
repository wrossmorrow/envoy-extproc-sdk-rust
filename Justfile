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
