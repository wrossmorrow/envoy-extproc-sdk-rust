use async_trait::async_trait;
use std::any::Any;

use extproc::{
    AllHeaders, Error, HeaderValue, ProcessingOptions, RequestContext, RequestPhase,
    RequestProcessor,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct UpstreamBodySchema {
    upstream: String,
}

pub struct RoutingRequestProcessor {
    pub opts: ProcessingOptions,
}

#[async_trait]
impl RequestProcessor for RoutingRequestProcessor {
    fn name(&self) -> String {
        "router".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.set_value("routed", Box::new(false) as Box<dyn Any + Send + Sync>)?;
        // parse and "redirect" if there is a routing header
        // NOTE: mutation_rules.allow_all_routing is critical in the envoy config to modify :authority
        if let Some(upstream) = ctx.all_headers.get("x-route-to-upstream") {
            ctx.set_value("routed", Box::new(true) as Box<dyn Any + Send + Sync>)?;
            ctx.overwrite_header(
                ":authority",
                &HeaderValue {
                    value: upstream.clone(),
                    raw_value: Some(upstream.as_bytes().to_vec()),
                },
            )?;
            ctx.clear_route_cache();
        }

        let _response = ctx.get_response(ctx.get_phase());

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
        if ctx
            .get_value("routed")
            .ok()
            .and_then(|v| v.downcast_ref::<bool>().cloned())
            != Some(false)
        {
            // parse request body, assuming buffered and JSON in a specific schema declaring the upstream
            // NOTE: mutation_rules.allow_all_routing is critical in the envoy config to modify :authority
            let parsed_body: UpstreamBodySchema = serde_json::from_slice(body)
                .map_err(|e| format!("could not parse request schema: {}", e))?;
            ctx.set_value("routed", Box::new(true) as Box<dyn Any + Send + Sync>)?;
            let upstream = parsed_body.upstream;
            ctx.overwrite_header(
                ":authority",
                &HeaderValue {
                    value: upstream.clone(),
                    raw_value: Some(upstream.as_bytes().to_vec()),
                },
            )?;
            ctx.clear_route_cache();
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

    fn error_handler(&self, _ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        tracing::error!("Error in phase {:?}: {:?}", phase, err);
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}
