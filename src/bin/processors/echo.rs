use async_trait::async_trait;
use std::collections::HashMap;

use regex::Regex;

use envoy_extproc_sdk::{
    AllHeaders, Error, HeaderValue, ProcessingOptions, RequestContext, RequestPhase,
    RequestProcessor,
};

fn join_headers(headers: &AllHeaders) -> HashMap<String, HeaderValue> {
    let mut out = HashMap::new();
    for (n, vs) in &headers.headers {
        let joined = vs.join(",");
        out.insert(
            n.clone(),
            HeaderValue {
                value: joined,
                raw_value: None,
            },
        );
    }
    out
}

fn echo_path_rx() -> Regex {
    Regex::new("/echo/.*").unwrap()
}

pub struct EchoRequestProcessor {
    pub opts: ProcessingOptions,
}

#[async_trait]
impl RequestProcessor for EchoRequestProcessor {
    fn name(&self) -> String {
        "echo".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        if echo_path_rx().is_match(&ctx.path) && ctx.end_of_stream {
            let headers = join_headers(&ctx.all_headers);
            return ctx.cancel_request(200, &headers, b"");
        }
        ctx.continue_request()
    }

    async fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn process_request_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error> {
        if echo_path_rx().is_match(&ctx.path) {
            let headers = join_headers(&ctx.all_headers);
            return ctx.cancel_request(200, &headers, body);
        }
        ctx.continue_request()
    }

    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        _body: &[u8],
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn error_handler(&self, _ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        tracing::error!("Error in phase {:?}: {:?}", phase, err);
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}
