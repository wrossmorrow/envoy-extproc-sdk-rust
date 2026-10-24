use std::time::SystemTime;

use extproc::{
    AllHeaders, Error, HeaderValue, ProcessingOptions, RequestContext, RequestPhase,
    RequestProcessor,
};

pub struct TimerRequestProcessor {
    pub opts: ProcessingOptions,
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn started_ns(ctx: &RequestContext) -> u128 {
    ctx.started.elapsed().as_nanos()
}

fn header_value(ns: u128) -> HeaderValue {
    HeaderValue {
        value: String::new(),
        raw_value: Some(ns.to_string().into_bytes()),
    }
}

impl RequestProcessor for TimerRequestProcessor {
    fn name(&self) -> String {
        "timer".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.overwrite_header("x-extproc-started-ns", &header_value(started_ns(ctx)))?;
        ctx.continue_request()
    }

    fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    fn process_request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.continue_request()
    }

    fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.add_header("x-extproc-started-ns", &header_value(started_ns(ctx)))?;
        ctx.add_header("x-extproc-finished-ns", &header_value(now_ns()))?;
        ctx.add_header(
            "x-upstream-duration-ns",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(ctx.started.elapsed().as_nanos().to_string().into_bytes()),
            },
        )?;
        ctx.continue_request()
    }

    fn process_response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.overwrite_header("x-extproc-started-ns", &header_value(started_ns(ctx)))?;
        ctx.overwrite_header("x-extproc-finished-ns", &header_value(now_ns()))?;
        ctx.overwrite_header(
            "x-upstream-duration-ns",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(ctx.started.elapsed().as_nanos().to_string().into_bytes()),
            },
        )?;
        ctx.continue_request()
    }

    fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    fn error_handler(&self, _ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        tracing::error!("Error in phase {:?}: {:?}", phase, err);
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}
