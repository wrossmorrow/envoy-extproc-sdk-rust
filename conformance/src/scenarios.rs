//! Conformance scenario layer: scenario handlers implementing processor-side
//! behavior for the envoy-driven conformance scenarios, and helpers to build
//! scenario handlers for the cases covered by the feature manifest.

use std::sync::Arc;

use envoy_extproc_sdk::{AllHeaders, Error, RequestContext};
use envoy_types::pb::envoy::r#type::v3::HttpStatus;
use envoy_types::pb::envoy::service::ext_proc::v3::body_mutation::Mutation as BodyMutationMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::BodyMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::HeaderMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::ImmediateResponse;

use crate::processor::{CaseHandler, ConformanceProcessor};

const SCENARIO_IMMEDIATE_RESPONSE_REQUEST_HEADERS: &str = "immediate_response_request_headers";
const SCENARIO_BUFFERED_BODY_REPLACE_GROW_AND_SHRINK: &str =
    "buffered_body_replace_grow_and_shrink";
const SCENARIO_GET_NOBODY_MODE_NONE: &str = "get_nobody_mode_none";
const SCENARIO_POST_WITH_BODY_MODE_NONE: &str = "post_with_body_mode_none";

/// Scenario handler for `immediate_response_request_headers`: responds with
/// an immediate response (status 200, `x-scenario-response` header, empty body)
/// during the request headers phase instead of continuing the exchange.
fn immediate_response_request_headers_handler() -> Arc<dyn CaseHandler> {
    Arc::new(ImmediateResponseScenarioHandler)
}

/// Scenario handler for `buffered_body_replace_grow_and_shrink`: replaces
/// the request body during the buffered body phase and clears the response
/// body during the response body phase.
fn buffered_body_replace_grow_and_shrink_handler() -> Arc<dyn CaseHandler> {
    Arc::new(BufferedBodyScenarioHandler)
}

/// Scenario handler for `get_nobody_mode_none`: with the `NONE` body mode envoy
/// does not invoke body callbacks, so the handler simply continues.
fn get_nobody_mode_none_handler() -> Arc<dyn CaseHandler> {
    Arc::new(NoopScenarioHandler)
}

/// Scenario handler for `post_with_body_mode_none`: with the `NONE` body mode
/// envoy does not invoke body callbacks, so the handler simply continues.
fn post_with_body_mode_none_handler() -> Arc<dyn CaseHandler> {
    Arc::new(NoopScenarioHandler)
}

struct ImmediateResponseScenarioHandler;

impl CaseHandler for ImmediateResponseScenarioHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.response.immediate_response = Some(ImmediateResponse {
            status: Some(HttpStatus { code: 200 }),
            headers: Some(HeaderMutation {
                set_headers: Vec::new(),
                remove_headers: Vec::new(),
            }),
            body: Vec::new(),
            grpc_status: None,
            details: "conformance scenario immediate response".to_string(),
        });
        Ok(())
    }
}

struct BufferedBodyScenarioHandler;

impl CaseHandler for BufferedBodyScenarioHandler {
    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.response.body_mutation = Some(BodyMutation {
            mutation: Some(BodyMutationMutation::Body(
                b"scenario-replaced-body".to_vec(),
            )),
        });
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.response.body_mutation = Some(BodyMutation {
            mutation: Some(BodyMutationMutation::ClearBody(true)),
        });
        ctx.continue_request()
    }
}

struct NoopScenarioHandler;

impl CaseHandler for NoopScenarioHandler {
    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        ctx.continue_request()
    }
}

/// Scenario handlers keyed by scenario id, for the cases covered by the
/// conformance feature manifest.
pub fn scenario_handlers() -> Vec<(String, Arc<dyn CaseHandler>)> {
    vec![
        (
            SCENARIO_IMMEDIATE_RESPONSE_REQUEST_HEADERS.to_string(),
            immediate_response_request_headers_handler(),
        ),
        (
            SCENARIO_BUFFERED_BODY_REPLACE_GROW_AND_SHRINK.to_string(),
            buffered_body_replace_grow_and_shrink_handler(),
        ),
        (
            SCENARIO_GET_NOBODY_MODE_NONE.to_string(),
            get_nobody_mode_none_handler(),
        ),
        (
            SCENARIO_POST_WITH_BODY_MODE_NONE.to_string(),
            post_with_body_mode_none_handler(),
        ),
    ]
}

/// Register all conformance scenario handlers on a processor.
pub fn register_scenario_handlers(processor: &ConformanceProcessor) -> Result<(), Error> {
    for (id, handler) in scenario_handlers() {
        processor.register(&id, handler);
    }
    Ok(())
}

/// Look up a conformance scenario handler by scenario id.
pub fn scenario_handler(
    processor: &ConformanceProcessor,
    scenario_id: &str,
) -> Option<Arc<dyn CaseHandler>> {
    processor.lookup_by_id(scenario_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use envoy_extproc_sdk::{ProcessingOptions, RequestPhase, RequestProcessor};

    fn processor() -> ConformanceProcessor {
        ConformanceProcessor::new(&ProcessingOptions::new())
    }

    fn all_headers_with_case(case_id: &str) -> AllHeaders {
        let mut headers = AllHeaders::default();
        headers
            .headers
            .insert("x-conformance-case".to_string(), vec![case_id.to_string()]);
        headers
    }

    fn set_phase(ctx: &mut RequestContext, phase: RequestPhase) {
        ctx.set_phase(phase);
    }

    #[tokio::test]
    async fn immediate_response_scenario_sets_immediate_response_during_request_headers() {
        let processor = processor();
        let handler = immediate_response_request_headers_handler();
        processor.register(SCENARIO_IMMEDIATE_RESPONSE_REQUEST_HEADERS, handler);

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestHeaders);
        let _ = ctx.set_value(SCENARIO_IMMEDIATE_RESPONSE_REQUEST_HEADERS.to_string());
        let headers = all_headers_with_case(SCENARIO_IMMEDIATE_RESPONSE_REQUEST_HEADERS);
        processor
            .process_request_headers(&mut ctx, &headers)
            .await
            .expect("immediate response scenario should continue");

        let immediate = ctx
            .response
            .immediate_response
            .as_ref()
            .expect("immediate response should be set");
        assert_eq!(immediate.status.as_ref().map(|s| s.code), Some(200));
        assert_eq!(immediate.details, "conformance scenario immediate response");
    }

    #[tokio::test]
    async fn buffered_body_scenario_replaces_request_body_and_clears_response_body() {
        let processor = processor();
        processor.register(
            SCENARIO_BUFFERED_BODY_REPLACE_GROW_AND_SHRINK,
            buffered_body_replace_grow_and_shrink_handler(),
        );

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestBody);
        let _ = ctx.set_value(SCENARIO_BUFFERED_BODY_REPLACE_GROW_AND_SHRINK.to_string());
        processor
            .process_request_body(&mut ctx, b"original-body")
            .await
            .expect("buffered body scenario should continue");
        assert_eq!(
            ctx.response
                .body_mutation
                .as_ref()
                .and_then(|m| m.mutation.as_ref())
                .and_then(|m| match m {
                    BodyMutationMutation::Body(b) => Some(b.clone()),
                    BodyMutationMutation::ClearBody(_) => None,
                    BodyMutationMutation::StreamedResponse(_) => None,
                }),
            Some(b"scenario-replaced-body".to_vec())
        );

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::ResponseBody);
        let _ = ctx.set_value(SCENARIO_BUFFERED_BODY_REPLACE_GROW_AND_SHRINK.to_string());
        processor
            .process_response_body(&mut ctx, b"original-body")
            .await
            .expect("buffered body scenario should continue");
        assert!(ctx
            .response
            .body_mutation
            .as_ref()
            .and_then(|m| m.mutation.as_ref())
            .is_some_and(|m| matches!(m, BodyMutationMutation::ClearBody(true))));
    }

    #[tokio::test]
    async fn noop_scenario_handlers_continue_the_exchange() {
        let processor = processor();
        processor.register(
            SCENARIO_GET_NOBODY_MODE_NONE,
            get_nobody_mode_none_handler(),
        );
        processor.register(
            SCENARIO_POST_WITH_BODY_MODE_NONE,
            post_with_body_mode_none_handler(),
        );

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestBody);
        let _ = ctx.set_value(SCENARIO_GET_NOBODY_MODE_NONE.to_string());
        processor
            .process_request_body(&mut ctx, b"body")
            .await
            .expect("noop scenario should continue");
        assert!(ctx.continue_request().is_ok());
    }
}
