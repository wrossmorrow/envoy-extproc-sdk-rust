//! Local binary entry point for the extproc server, for running the SDK
//! locally (e.g. to smoke-test against a real Envoy).

use std::sync::Arc;

use extproc::{
    AllHeaders, Error, GenericExtProcServer, Metrics, ProcessingOptions, RequestContext,
    RequestPhase, RequestProcessor, ServerOptions,
};

struct LocalProcessor {
    options: ProcessingOptions,
}

impl LocalProcessor {
    fn new() -> Self {
        Self {
            options: ProcessingOptions::default(),
        }
    }
}

impl RequestProcessor for LocalProcessor {
    fn name(&self) -> String {
        "local".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.options
    }

    fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.continue_request()
    }

    fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.continue_request()
    }

    fn process_request_body(
        &self,
        ctx: &mut RequestContext,
        _body: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.continue_request()
    }

    fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.continue_request()
    }

    fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        _body: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.continue_request()
    }

    fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.continue_request()
    }

    fn error_handler(&self, _ctx: &mut RequestContext, _phase: RequestPhase, _err: Error) {}

    fn close(
        &self,
        _grace_period_seconds: i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("extproc server failed: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let processor = Arc::new(LocalProcessor::new());
    let server = GenericExtProcServer::new(
        "local".to_string(),
        processor,
        Some(ProcessingOptions::default()),
        Metrics::new(),
    );
    server.serve(ServerOptions::default()).await
}
