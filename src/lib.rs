//! # envoy-extproc-sdk
//!
//! A Rust SDK for implementing [Envoy](https://www.envoyproxy.io/) External
//! Processors (`ext_proc` HTTP gRPC filters).
//!
//! [`envoy`](https://www.envoyproxy.io/) can query an ExternalProcessor in its
//! filter chain. Such a processor is a gRPC service that streams messages back
//! and forth to modify HTTP requests being processed by `envoy`. This crate
//! makes developing those services easier by masking some of the details behind
//! the data structures `envoy` uses, which are effective but verbose and
//! idiosyncratic. Each request generates a bidirectional gRPC stream (with at
//! most 6 messages) and sends, in turn, data concerning request headers, request
//! body, request trailers, response headers, response body, and response
//! trailers (if `envoy` is configured to send all phases). The idea here is to
//! supply functions for each phase that operate on a context and more
//! generically typed data suitable for each phase.
//!
//! The [`RequestProcessor`] trait is implemented by processor authors and
//! handed to [`serve`](server::serve) / [`must_serve`](server::must_serve), which
//! handle the gRPC streaming and shared context, parsing the processing phase
//! in the gRPC stream and calling the right
//! [`RequestContext`](context::RequestContext) method. Phase processing methods
//! are async, so implementations may await asynchronous work (e.g. downstream
//! HTTP calls) without blocking the stream task.
//!
//! [`envoy-types`](https://crates.io/crates/envoy-types) provides the compiled
//! protobuf types for the `envoy.service.ext_proc.v3` service, and
//! [`tonic`](https://crates.io/crates/tonic) provides the gRPC transport and
//! server scaffolding.

/// An error type for SDK-level operations. Processors may return arbitrary
/// errors; they are passed through to the processor's error handler.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub mod context;
pub mod headers;
pub mod health;
pub mod metrics;
pub mod options;
pub mod server;
pub mod tester;

pub use context::HeaderAppendAction;
pub use context::{
    default_phase_if_invalid, request_phase_to_string, HeaderValue, PhaseResponse, RequestContext,
    RequestPhase,
};
pub use headers::AllHeaders;
pub use health::HealthServer;
pub use metrics::Metrics;
pub use options::{ProcessingOptions, ServerOptions};
pub use server::{must_serve, serve, GenericExtProcServer, RequestProcessor};
pub use tester::{TesterRequestBody, TesterRequestProcessor};
