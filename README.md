# An Envoy ExternalProcessor SDK (rust)

## Overview

[`envoy`](https://www.envoyproxy.io/), one of the most powerful and widely used reverse proxies, is able to query an [ExternalProcessor](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter) in it's filter chain. Such a processor is a gRPC service that streams messages back and forth to modify HTTP requests being processed by `envoy`. This functionality opens the door to quickly and robustly implemently customized functionality at the edge, instead of in targeted services. While powerful, implementing these services still requires dealing with complicated `envoy` specs, managing information sharing across request phases, and an understanding of gRPC, none of which are exactly straightforward.

**The purpose of this SDK is to make development of ExternalProcessors (more) easy**. This SDK _certainly_ won't supply the most _performant_ edge functions. Much better performance will come from eschewing the ease-of-use functionality here by using a [WASM plugin](https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/wasm/v3/wasm.proto) or registered [custom filter binary](https://github.com/envoyproxy/envoy-filter-example). Optimal performance isn't necessarily our goal; usability, maintainability, and low time-to-functionality is, and those aspects can often be more important than minimal request latency.

We attempt to achieve this ease largely by masking some of the details behind the datastructures `envoy` uses, which are effective but verbose and idiosyncratic. Each request generates a bidirectional gRPC stream (with at most 6 messages) and sends, in turn, data concerning request headers, request body, request trailers, response headers, response body, and response trailers (if `envoy` is configured to send all phases). The idea here is to supply functions for each phase that operate on a context and more generically typed data suitable for each phase. (See details below.)

The SDK is implemented as the `extproc` crate and uses the [`envoy-types`](https://crates.io/crates/envoy-types) crate for the compiled `envoy.service.ext_proc.v3` protobuf types and [`tonic`](https://crates.io/crates/tonic) for the gRPC transport and server scaffolding.

## Usage

### TL;DR

Implement the `extproc::RequestProcessor` trait, and pass an instance to the `extproc::serve` function.

### Details

This SDK uses a `struct`
```rust
use std::sync::Arc;

use extproc::{Metrics, ProcessingOptions, RequestProcessor};

pub struct GenericExtProcServer {
    /// The name of the processor, as reported to `envoy`.
    pub name: String,
    /// The processor handling phases.
    pub processor: Arc<dyn RequestProcessor>,
    /// The options controlling SDK behavior for the current phase, if set.
    pub options: Option<ProcessingOptions>,
    /// Metrics collected during processing.
    pub metrics: Metrics,
}
```
an interface
```rust
pub trait RequestProcessor: Send + Sync + 'static {
    /// The name of the processor, reported to `envoy` via the `x-extproc-names` response header.
    fn name(&self) -> String;

    /// The processor's `ProcessingOptions`.
    fn options(&self) -> &ProcessingOptions;

    /// Process the request headers phase. May perform asynchronous work.
    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the request trailers phase. May perform asynchronous work.
    async fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the request body phase. May perform asynchronous work.
    async fn process_request_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error>;

    /// Process the response headers phase. May perform asynchronous work.
    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Process the response body phase. May perform asynchronous work.
    async fn process_response_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error>;

    /// Process the response trailers phase. May perform asynchronous work.
    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error>;

    /// Called when the SDK hits an error during request processing. May
    /// perform asynchronous work. An implemented handler can implement any
    /// logic it likes.
    async fn error_handler(&self, ctx: &mut RequestContext, phase: RequestPhase, err: Error);

    /// Called on shutdown, after in-flight streams have completed, including
    /// in-flight phase processing. Implementations may perform blocking
    /// cleanup during close; the server awaits in-flight streams (via graceful
    /// shutdown) before invoking this.
    fn close(&self, grace_period_seconds: i32) -> Result<(), Error>;
}
```
and a context object
```rust
use std::collections::HashMap;
use std::time::{Duration, Instant};

use extproc::{AllHeaders, PhaseResponse, RequestContext, RequestPhase};

pub struct RequestContext {
    // parsed from headers
    pub scheme: String,
    pub authority: String,
    pub method: String,
    pub path: String,
    pub full_path: String,
    pub request_id: String,
    pub all_headers: AllHeaders,
    pub started: Instant,
    pub duration: Duration,
    pub end_of_stream: bool,
    pub data: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    pub phase: RequestPhase,
    pub response: PhaseResponse,
}
```
that work together to allow processing of requests and responses.

Phase processing methods are async (via the [`async_trait`](https://crates.io/crates/async-trait) crate): implementations may perform asynchronous work such as downstream HTTP calls or database lookups without blocking the stream task. Phase messages within a single request/response exchange are still processed sequentially, matching the ordered envoy ext_proc protocol flow; concurrency is available across concurrent streams.

An ExtProc service can be run as in

```rust
use std::sync::Arc;

use extproc::{GenericExtProcServer, Metrics, ProcessingOptions, ServerOptions};

// define serverOptions, setup your tracing subscriber...
// (ProcessingOptions are supplied by your processor's options method)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let processor = Arc::new(MyProcessor::default());
    GenericExtProcServer::new(
        "my-processor".to_string(),
        processor,
        Some(ProcessingOptions::default()),
        Metrics::new(),
    )
    .serve(ServerOptions::default())
    .await
}
```
Internally the `GenericExtProcServer` handles the gRPC streaming and shared context, parsing the processing phase in the gRPC stream and calling the right `RequestProcessor` method. The header and body messages can be responded to with either a "common" or "immediate" response object (or error); the trailer methods can only mutate headers. But that should be opaque to the user of this SDK; the `RequestContext` and `RequestProcessor` are more important.

### Context Data

The `RequestContext` is initialized with request data when request headers are received, implying that the `envoy` configuration should always have `processing_mode.request_header_mode: SEND`. Basic request data (method, path etc) are only available in this phase. As shown in the spec above, this data includes
* the HTTP `Scheme`
* the `Authority` (host)
* the HTTP `Method`
* the URL `Path`
* `envoy`'s `RequestId` (a UUID from the `x-request-id` header)
* the request processing stream start time in `Started`
* an accumulator `Duration` _for the time spent in external processing_
* a flag `EndOfStream` from gRPC messages for header and body phases

This context is carried through every request phase, meaning that data can be shared _across_ phases particularly in the generic slot `data` for arbitrary values. Data is stored in a type-keyed map ([`http::Extensions`](https://docs.rs/http/latest/http/struct.Extensions.html)) so values are strongly typed rather than string-keyed trait objects. Define and access data with the `RequestContext::set_value` and `RequestContext::get_value` methods, which take and return typed values. See the [data](#data) or [digest](#digest) examples.

### Forming Responses

We also provide some convenience routines for operating on process phase stream responses, so that users of this SDK need to learn less about the specifics of the `envoy` datastructures. The gRPC stream response datastructures are complicated, and our aim is to utilize the `RequestContext` to guard and simplify the construction of responses with a simpler user interface.

In particular, the methods
```rust
(rc: &mut RequestContext).continue_request() -> Result<(), Error>
(rc: &mut RequestContext).cancel_request(status: i32, headers: &HashMap<String, HeaderValue>, body: &[u8]) -> Result<(), Error>
```
define request phase responses for "continuing" and "responding immediately". Note that "cancelling" does not mean request failure; just "we know the response now, and don't need to process further". See the [echo](#echo) example for "OK" (200) responses from cancelling.

### Modifying Headers

You can add headers to a response with the convenience methods
```rust
(rc: &mut RequestContext).add_header(name: &str, hv: &HeaderValue) -> Result<(), Error>
(rc: &mut RequestContext).add_headers(headers: &HashMap<String, HeaderValue>) -> Result<(), Error>
(rc: &mut RequestContext).append_header(name: &str, hv: &HeaderValue) -> Result<(), Error>
(rc: &mut RequestContext).append_headers(headers: &HashMap<String, HeaderValue>) -> Result<(), Error>
(rc: &mut RequestContext).overwrite_header(name: &str, hv: &HeaderValue) -> Result<(), Error>
(rc: &mut RequestContext).overwrite_headers(headers: &HashMap<String, HeaderValue>) -> Result<(), Error>
(rc: &mut RequestContext).update_header(name: &str, hv: &HeaderValue, action: HeaderAppendAction) -> Result<(), Error>
(rc: &mut RequestContext).update_headers(headers: &HashMap<String, HeaderValue>, action: HeaderAppendAction) -> Result<(), Error>
```
where `append` adds header values if they exist, `add` adds a new value only if the header doesn't exist, and `overwrite` will add or overwrite if a header exists. The `RequestContext` should keep track of these headers and include them in a `continue_request` or `cancel_request` call. `update_header`/`update_headers` take a typed [`HeaderAppendAction`](https://docs.rs/envoy-types/latest/envoy_types/pb/envoy/config/core/v3/header_value_option/enum.HeaderAppendAction.html) from the envoy protocol, so an action outside the protocol's set is not representable and cannot produce a runtime error; invalid header values are still rejected.

Headers can be removed with the
```rust
(rc: &mut RequestContext).remove_header(name: &str) -> Result<(), Error>
(rc: &mut RequestContext).remove_headers(headers: &[String]) -> Result<(), Error>
```
methods, requiring only names of headers to remove.

### Modifying Bodies

Two methods help modify bodies:
```rust
(rc: &mut RequestContext).replace_body_chunk(body: &[u8]) -> Result<(), Error>
(rc: &mut RequestContext).clear_body_chunk() -> Result<(), Error>
```
These are the two options currently available in `envoy` ExtProcs: replace a chunk and clear the entire chunk. Note that with buffered bodies the "chunks" should be the entire body. See the [masker](#masker) example discussed below.

The Rust port also provides whole-body variants, [`replace_body`](https://docs.rs/extproc/latest/extproc/struct.RequestContext.html#method.replace_body) and [`clear_body`](https://docs.rs/extproc/latest/extproc/struct.RequestContext.html#method.clear_body), which replace (or clear) the entire body and update the `Content-Length` header accordingly.

## Examples

The examples from the Go SDK (`examples/noop.go`, `examples/trivial.go`, `examples/timer.go`, `examples/data.go`, `examples/digest.go`, `examples/dedup.go`, `examples/masker.go`, `examples/echo.go`, and `examples/router.go`) have been ported to Rust as example binaries. The example server binary is [`src/bin/extproc-example.rs`](src/bin/extproc-example.rs); run a specific example with `just run-example example="<name>"` or `cargo run --bin extproc-example -- <name>`.

The compose setup under [`examples/`](examples/) runs `envoy` (see [`examples/envoy.yaml`](examples/envoy.yaml)), a mock echo server, and the example processors, mirroring the Go SDK's `examples/` setup.
