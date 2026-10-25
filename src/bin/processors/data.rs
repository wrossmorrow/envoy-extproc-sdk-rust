use async_trait::async_trait;

use extproc::{
    AllHeaders, Error, HeaderValue, ProcessingOptions, RequestContext, RequestPhase,
    RequestProcessor,
};

pub struct DataRequestProcessor {
    pub opts: ProcessingOptions,
}

#[async_trait]
impl RequestProcessor for DataRequestProcessor {
    fn name(&self) -> String {
        "data".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.set_value(uuid::Uuid::new_v4())?;
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
        if let Ok(id) = ctx.get_value::<uuid::Uuid>() {
            ctx.add_header(
                "x-extproc-custom-data",
                &HeaderValue {
                    value: String::new(),
                    raw_value: Some(id.to_string().as_bytes().to_vec()),
                },
            )?;
        }
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
