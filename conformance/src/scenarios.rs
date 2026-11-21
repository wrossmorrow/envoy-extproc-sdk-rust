//! Conformance scenario layer: scenario handlers implementing processor-side
//! behavior for the envoy-driven conformance scenarios, and helpers to build
//! scenario handlers for the cases covered by the feature manifest.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use envoy_extproc_sdk::{
    AllHeaders, BodySendMode, Error, HeaderSendMode, HeaderValue, ModeOverride, RequestContext,
};
use envoy_types::pb::envoy::r#type::v3::HttpStatus;
use envoy_types::pb::envoy::service::ext_proc::v3::body_mutation::Mutation as BodyMutationMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::BodyMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::HeaderMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::ImmediateResponse;

use crate::processor::{CaseHandler, ConformanceProcessor};

pub const SCENARIO_IMMEDIATE_RESPONSE_REQUEST_HEADERS: &str = "immediate_response_request_headers";
pub const SCENARIO_BUFFERED_BODY_REPLACE_GROW_AND_SHRINK: &str =
    "buffered_body_replace_grow_and_shrink";
pub const SCENARIO_GET_NOBODY_MODE_NONE: &str = "get_nobody_mode_none";
pub const SCENARIO_POST_WITH_BODY_MODE_NONE: &str = "post_with_body_mode_none";
pub const SCENARIO_MODE_OVERRIDE: &str = "mode_override";
pub const SCENARIO_MODE_OVERRIDE_SKIP: &str = "mode_override_skip";
pub const SCENARIO_MODE_OVERRIDE_RESP_STREAM: &str = "mode_override_resp_stream";
pub const SCENARIO_MODE_OVERRIDE_DISABLED: &str = "mode_override_disabled";
pub const SCENARIO_MODE_OVERRIDE_FULL_RESP: &str = "mode-override-full-resp";
pub const SCENARIO_MODE_OVERRIDE_ALLOWLIST_OK: &str = "mode-override-allowlist-ok";
pub const SCENARIO_MODE_OVERRIDE_ALLOWLIST_DENIED: &str = "mode-override-allowlist-denied";

/// Scenario handler for `immediate_response_request_headers`: responds with
/// an immediate response (status 200, `x-scenario-response` header, empty body)
/// during the request headers phase instead of continuing the exchange.
pub fn immediate_response_request_headers_handler() -> Arc<dyn CaseHandler> {
    Arc::new(ImmediateResponseScenarioHandler)
}

/// Scenario handler for `buffered_body_replace_grow_and_shrink`: replaces
/// the request body during the buffered body phase and clears the response
/// body during the response body phase.
pub fn buffered_body_replace_grow_and_shrink_handler() -> Arc<dyn CaseHandler> {
    Arc::new(BufferedBodyScenarioHandler)
}

/// Scenario handler for `get_nobody_mode_none`: with the `NONE` body mode envoy
/// does not invoke body callbacks, so the handler simply continues.
pub fn get_nobody_mode_none_handler() -> Arc<dyn CaseHandler> {
    Arc::new(NoopScenarioHandler)
}

/// Scenario handler for `post_with_body_mode_none`: with the `NONE` body mode
/// envoy does not invoke body callbacks, so the handler simply continues.
pub fn post_with_body_mode_none_handler() -> Arc<dyn CaseHandler> {
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

/// Scenario handler for `mode-override-full-resp`: opts into a BUFFERED
/// response body mode during the request headers phase and replaces the
/// response body during the response body phase.
struct ModeOverrideFullRespScenarioHandler;

impl CaseHandler for ModeOverrideFullRespScenarioHandler {
    fn request_headers(&self, ctx: &mut RequestContext, _headers: &AllHeaders) -> Result<(), Error> {
        ctx.override_mode(ModeOverride {
            response_header_mode: HeaderSendMode::Send,
            request_body_mode: BodySendMode::Buffered,
            response_body_mode: BodySendMode::Buffered,
            request_trailer_mode: HeaderSendMode::Send,
            response_trailer_mode: HeaderSendMode::Send,
        })?;
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
        if body != b"raw-resp" {
            return Err(format!("unexpected response body {body:?}").into());
        }
        ctx.replace_body(b"mutated-resp")?;
        ctx.continue_request()
    }
}

/// Scenario handler for `mode_override`: opts into BUFFERED bodies and
/// trailers for a full request/response exchange via a dynamic mode override
/// expressed during the request headers phase, then replaces the bodies.
struct ModeOverrideScenarioHandler;

impl CaseHandler for ModeOverrideScenarioHandler {
    fn request_headers(&self, ctx: &mut RequestContext, _headers: &AllHeaders) -> Result<(), Error> {
        ctx.override_mode(ModeOverride {
            response_header_mode: HeaderSendMode::Send,
            request_body_mode: BodySendMode::Buffered,
            response_body_mode: BodySendMode::Buffered,
            request_trailer_mode: HeaderSendMode::Send,
            response_trailer_mode: HeaderSendMode::Send,
        })?;
        ctx.continue_request()
    }

    fn request_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
        if body != b"raw-req" {
            return Err(format!("unexpected request body {body:?}").into());
        }
        ctx.replace_body(b"mutated-req")?;
        ctx.continue_request()
    }

    fn request_trailers(&self, ctx: &mut RequestContext, trailers: &AllHeaders) -> Result<(), Error> {
        let got = trailers
            .get("x-req-tr")
            .ok_or_else(|| "missing x-req-tr trailer".to_string())?;
        if got != "orig-req" {
            return Err(format!("x-req-tr = {got}, want orig-req").into());
        }
        ctx.overwrite_header(
            "x-req-tr",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"mutated-req-tr".to_vec()),
            },
        )?;
        ctx.continue_request()
    }

    fn response_headers(&self, ctx: &mut RequestContext, _headers: &AllHeaders) -> Result<(), Error> {
        ctx.continue_request()
    }

    fn response_trailers(&self, ctx: &mut RequestContext, trailers: &AllHeaders) -> Result<(), Error> {
        let got = trailers
            .get("x-resp-tr")
            .ok_or_else(|| "missing x-resp-tr trailer".to_string())?;
        if got != "orig-resp" {
            return Err(format!("x-resp-tr = {got}, want orig-resp").into());
        }
        ctx.overwrite_header(
            "x-resp-tr",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"mutated-resp-tr".to_vec()),
            },
        )?;
        ctx.add_header(
            "x-resp-added-tr",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"added".to_vec()),
            },
        )?;
        ctx.continue_request()
    }
}

/// Scenario handler for `mode_override_skip`: skips all remaining phases of the
/// exchange via a dynamic mode override, after adding a header during the
/// request headers phase.
struct ModeOverrideSkipScenarioHandler;

impl CaseHandler for ModeOverrideSkipScenarioHandler {
    fn request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.add_header("x-fast-path", &HeaderValue {
            value: String::new(),
            raw_value: Some(b"yes".to_vec()),
        })?;
        ctx.skip_remaining_phases()
    }
}

/// Scenario handler for `mode_override_resp_stream`: opts into a STREAMED
/// response body mode during the response headers phase and replaces chunks.
struct ModeOverrideRespStreamScenarioHandler;

impl CaseHandler for ModeOverrideRespStreamScenarioHandler {
    fn response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.override_mode(ModeOverride {
            response_header_mode: HeaderSendMode::Send,
            request_body_mode: BodySendMode::None,
            response_body_mode: BodySendMode::Streamed,
            request_trailer_mode: HeaderSendMode::Skip,
            response_trailer_mode: HeaderSendMode::Skip,
        })?;
        ctx.continue_request()
    }

    fn response_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
        if body.is_empty() {
            return ctx.continue_request();
        }
        let mut upper = body.to_vec();
        for b in &mut upper {
            b.make_ascii_uppercase();
        }
        ctx.replace_body_chunk(&upper)?;
        ctx.continue_request()
    }
}

/// Scenario handler for `mode_override_disabled`: expresses a mode override
/// during the request headers phase, which the envoy `ext_proc` filter must
/// ignore when `allow_mode_override` is false (baseline `NONE` body mode means
/// envoy never sends the body to the processor).
struct ModeOverrideDisabledScenarioHandler;

impl CaseHandler for ModeOverrideDisabledScenarioHandler {
    fn request_headers(&self, ctx: &mut RequestContext, _headers: &AllHeaders) -> Result<(), Error> {
        ctx.override_mode(ModeOverride {
            response_header_mode: HeaderSendMode::Send,
            request_body_mode: BodySendMode::Buffered,
            response_body_mode: BodySendMode::None,
            request_trailer_mode: HeaderSendMode::Skip,
            response_trailer_mode: HeaderSendMode::Skip,
        })?;
        ctx.continue_request()
    }
}

/// Scenario handler for `mode_override_allowlist_ok`: opts into a STREAMED
/// body mode accepted by the envoy `allowed_override_modes` allowlist.
struct ModeOverrideAllowlistOkScenarioHandler {
    saw_body: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl CaseHandler for ModeOverrideAllowlistOkScenarioHandler {
    fn request_headers(&self, ctx: &mut RequestContext, _headers: &AllHeaders) -> Result<(), Error> {
        ctx.override_mode(ModeOverride {
            response_header_mode: HeaderSendMode::Send,
            request_body_mode: BodySendMode::Streamed,
            response_body_mode: BodySendMode::None,
            request_trailer_mode: HeaderSendMode::Skip,
            response_trailer_mode: HeaderSendMode::Skip,
        })?;
        ctx.continue_request()
    }

    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        self.saw_body.store(true, std::sync::atomic::Ordering::SeqCst);
        ctx.continue_request()
    }
}

/// Scenario handler for `mode_override_allowlist_denied`: opts into a
/// BUFFERED body mode outside the envoy `allowed_override_modes` allowlist, so
/// the override must be dropped and the exchange continue on the baseline.
struct ModeOverrideAllowlistDeniedScenarioHandler {
    saw_body: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl CaseHandler for ModeOverrideAllowlistDeniedScenarioHandler {
    fn request_headers(&self, ctx: &mut RequestContext, _headers: &AllHeaders) -> Result<(), Error> {
        ctx.override_mode(ModeOverride {
            response_header_mode: HeaderSendMode::Send,
            request_body_mode: BodySendMode::Buffered,
            response_body_mode: BodySendMode::None,
            request_trailer_mode: HeaderSendMode::Skip,
            response_trailer_mode: HeaderSendMode::Skip,
        })?;
        ctx.continue_request()
    }

    fn request_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        self.saw_body.store(true, std::sync::atomic::Ordering::SeqCst);
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
        (
            SCENARIO_MODE_OVERRIDE.to_string(),
            mode_override_handler(),
        ),
        (
            "mode-override-full-resp".to_string(),
            mode_override_full_resp_handler(),
        ),
        (
            SCENARIO_MODE_OVERRIDE_SKIP.to_string(),
            mode_override_skip_handler(),
        ),
        (
            SCENARIO_MODE_OVERRIDE_RESP_STREAM.to_string(),
            mode_override_resp_stream_handler(),
        ),
        (
            SCENARIO_MODE_OVERRIDE_DISABLED.to_string(),
            mode_override_disabled_handler(),
        ),
        (
            SCENARIO_MODE_OVERRIDE_ALLOWLIST_OK.to_string(),
            mode_override_allowlist_ok_handler(Arc::new(AtomicBool::new(false))),
        ),
        (
            SCENARIO_MODE_OVERRIDE_ALLOWLIST_DENIED.to_string(),
            mode_override_allowlist_denied_handler(Arc::new(AtomicBool::new(false))),
        ),
    ]
}

pub fn mode_override_handler() -> Arc<dyn CaseHandler> {
    Arc::new(ModeOverrideScenarioHandler)
}

/// Scenario handler for `mode-override-full-resp`: opts into a BUFFERED
/// response body mode during the request headers phase and replaces the
/// response body during the response body phase.
pub fn mode_override_full_resp_handler() -> Arc<dyn CaseHandler> {
    Arc::new(ModeOverrideFullRespScenarioHandler)
}

pub fn mode_override_skip_handler() -> Arc<dyn CaseHandler> {
    Arc::new(ModeOverrideSkipScenarioHandler)
}

pub fn mode_override_resp_stream_handler() -> Arc<dyn CaseHandler> {
    Arc::new(ModeOverrideRespStreamScenarioHandler)
}

pub fn mode_override_disabled_handler() -> Arc<dyn CaseHandler> {
    Arc::new(ModeOverrideDisabledScenarioHandler)
}

pub fn mode_override_allowlist_ok_handler(saw_body: Arc<std::sync::atomic::AtomicBool>) -> Arc<dyn CaseHandler> {
    Arc::new(ModeOverrideAllowlistOkScenarioHandler { saw_body })
}

pub fn mode_override_allowlist_denied_handler(saw_body: Arc<std::sync::atomic::AtomicBool>) -> Arc<dyn CaseHandler> {
    Arc::new(ModeOverrideAllowlistDeniedScenarioHandler { saw_body })
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

    #[tokio::test]
    async fn mode_override_skip_scenario_sets_skip_override() {
        let processor = processor();
        processor.register(SCENARIO_MODE_OVERRIDE_SKIP, mode_override_skip_handler());

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestHeaders);
        ctx.response.header_mutation = Some(HeaderMutation {
            set_headers: Vec::new(),
            remove_headers: Vec::new(),
        });
        let _ = ctx.set_value(SCENARIO_MODE_OVERRIDE_SKIP.to_string());
        processor
            .process_request_headers(&mut ctx, &all_headers_with_case(SCENARIO_MODE_OVERRIDE_SKIP))
            .await
            .expect("mode override skip scenario should continue");
        let mo = ctx.response.mode_override.as_ref().expect("expected skip override");
        assert_eq!(mo.response_header_mode, i32::from(HeaderSendMode::Skip));
        assert_eq!(mo.request_body_mode, i32::from(BodySendMode::None));
        assert_eq!(mo.response_body_mode, i32::from(BodySendMode::None));
        assert_eq!(mo.request_trailer_mode, i32::from(HeaderSendMode::Skip));
        assert_eq!(mo.response_trailer_mode, i32::from(HeaderSendMode::Skip));
    }

    #[tokio::test]
    async fn mode_override_disabled_scenario_sets_override() {
        let processor = processor();
        processor.register(SCENARIO_MODE_OVERRIDE_DISABLED, mode_override_disabled_handler());

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestHeaders);
        let _ = ctx.set_value(SCENARIO_MODE_OVERRIDE_DISABLED.to_string());
        processor
            .process_request_headers(&mut ctx, &all_headers_with_case(SCENARIO_MODE_OVERRIDE_DISABLED))
            .await
            .expect("mode override disabled scenario should continue");
        assert!(ctx.response.mode_override.is_some());
        assert_eq!(ctx.response.mode_override.as_ref().unwrap().request_body_mode, i32::from(BodySendMode::Buffered));
    }

    #[tokio::test]
    async fn mode_override_allowlist_handlers_set_overrides() {
        let processor = processor();
        processor.register(
            SCENARIO_MODE_OVERRIDE_ALLOWLIST_OK,
            mode_override_allowlist_ok_handler(Arc::new(AtomicBool::new(false))),
        );
        processor.register(
            SCENARIO_MODE_OVERRIDE_ALLOWLIST_DENIED,
            mode_override_allowlist_denied_handler(Arc::new(AtomicBool::new(false))),
        );

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestHeaders);
        let _ = ctx.set_value(SCENARIO_MODE_OVERRIDE_ALLOWLIST_OK.to_string());
        processor
            .process_request_headers(&mut ctx, &all_headers_with_case(SCENARIO_MODE_OVERRIDE_ALLOWLIST_OK))
            .await
            .expect("mode override allowlist ok scenario should continue");
        assert_eq!(
            ctx.response
                .mode_override
                .as_ref()
                .unwrap()
                .request_body_mode,
            i32::from(BodySendMode::Streamed)
        );

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestHeaders);
        let _ = ctx.set_value(SCENARIO_MODE_OVERRIDE_ALLOWLIST_DENIED.to_string());
        processor
            .process_request_headers(&mut ctx, &all_headers_with_case(SCENARIO_MODE_OVERRIDE_ALLOWLIST_DENIED))
            .await
            .expect("mode override allowlist denied scenario should continue");
        assert_eq!(
            ctx.response
                .mode_override
                .as_ref()
                .unwrap()
                .request_body_mode,
            i32::from(BodySendMode::Buffered)
        );
    }

    #[tokio::test]
    async fn mode_override_full_scenario_replaces_request_body() {
        let processor = processor();
        processor.register(SCENARIO_MODE_OVERRIDE, mode_override_handler());

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestHeaders);
        let _ = ctx.set_value(SCENARIO_MODE_OVERRIDE.to_string());
        processor
            .process_request_headers(&mut ctx, &all_headers_with_case(SCENARIO_MODE_OVERRIDE))
            .await
            .expect("mode override scenario should continue");
        assert!(ctx.response.mode_override.is_some());

        let mut ctx = RequestContext::new();
        set_phase(&mut ctx, RequestPhase::RequestBody);
        ctx.response.header_mutation = Some(HeaderMutation {
            set_headers: Vec::new(),
            remove_headers: Vec::new(),
        });
        let _ = ctx.set_value(SCENARIO_MODE_OVERRIDE.to_string());
        processor
            .process_request_body(&mut ctx, b"raw-req")
            .await
            .expect("mode override scenario should continue");
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
            Some(b"mutated-req".to_vec())
        );
    }
}
