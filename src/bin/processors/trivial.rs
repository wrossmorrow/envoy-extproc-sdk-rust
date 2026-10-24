use extproc::{
    AllHeaders, Error, HeaderValue, ProcessingOptions, RequestContext, RequestPhase,
    RequestProcessor,
};

pub struct TrivialRequestProcessor {
    pub opts: ProcessingOptions,
}

impl RequestProcessor for TrivialRequestProcessor {
    fn name(&self) -> String {
        "trivial".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.add_header(
            "x-extproc-request",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"seen".to_vec()),
            },
        )?;
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
        ctx.add_header(
            "x-extproc-response",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"seen".to_vec()),
            },
        )?;
        ctx.continue_request()
    }

    fn process_response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
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
