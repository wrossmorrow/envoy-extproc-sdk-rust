use async_trait::async_trait;
use extproc::{
    AllHeaders, Error, ProcessingOptions, RequestContext, RequestPhase, RequestProcessor,
};

pub struct NoopRequestProcessor {
    pub opts: ProcessingOptions,
}

#[async_trait]
impl RequestProcessor for NoopRequestProcessor {
    fn name(&self) -> String {
        "noop".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
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
        _body: &[u8],
    ) -> Result<(), Error> {
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
