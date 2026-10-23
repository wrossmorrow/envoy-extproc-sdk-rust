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

# Run the test suite.
test:
    cargo test

# Run the full gate: format check, clippy, tests.
all: fmt-check clippy test
