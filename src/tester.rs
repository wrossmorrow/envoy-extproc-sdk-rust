//! The `TesterRequestProcessor`: an [`RequestProcessor`] used by the end-to-end
//! integration test harness against a running `envoy` (see the `test/` directory
//! plans in the notes, and the Go SDK's `test/` directory).
//!
//! The harness drives the ext_proc protocol like envoy does: the data plane
//! sends the phase messages, and the processor answers with mutations, a
//! "continue", or an immediate response. The parametrized cases (see
//! `cases.yaml` in the Go SDK) are expressed as actions carried in the JSON
//! request body during the request-body phase, and applied across the
//! response phases.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::context::{HeaderAppendAction, HeaderValue, RequestContext, RequestPhase};
use crate::headers::AllHeaders;
use crate::options::ProcessingOptions;
use crate::Error;
use crate::RequestProcessor;
use async_trait::async_trait;

/// The action schema of the parametrized integration test cases, mirroring the
/// Go SDK's `test/tester.go` `TesterRequestBody`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct TesterRequestBody {
    /// How to modify the request headers during the request phase:
    /// header names to append to, add only if absent, overwrite, or remove.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub append_request_headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_request_headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overwrite_request_headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_request_headers: Option<Vec<String>>,
    /// Whether to clear the request body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clear_request_body: Option<bool>,
    /// The full request body to replace the original with, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_request_body: Option<String>,

    // response processing options
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub append_response_headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_response_headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overwrite_response_headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_response_headers: Option<Vec<String>>,
    /// Whether to clear the response body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clear_response_body: Option<bool>,
    /// The full response body to replace the original with, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_response_body: Option<String>,

    // cancellation
    /// Whether to cancel (respond immediately to) the request instead of
    /// forwarding it upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_request: Option<bool>,
    /// The HTTP status to cancel the request with, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_request_status: Option<i32>,
    /// Headers to set on the cancellation response, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_request_headers: Option<HashMap<String, String>>,
    /// The body of the cancellation response, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_request_body: Option<String>,
}

/// The processor driven by the integration test cases: it parses the actions
/// carried in the request body, applies the request-phase actions, and applies
/// the response-phase actions, or answers with an immediate response when the
/// case cancels the request.
#[derive(Clone, Debug, Default)]
pub struct TesterRequestProcessor {
    opts: ProcessingOptions,
}

impl TesterRequestProcessor {
    /// Create a tester processor with the given options.
    pub fn new(opts: ProcessingOptions) -> Self {
        Self { opts }
    }

    /// Load the actions from a JSON-encoded request body, if any.
    fn load_actions(body: &[u8]) -> Option<TesterRequestBody> {
        serde_json::from_slice(body).ok()
    }

    /// Fetch the actions stored in the request context during the
    /// request-body phase, if any.
    fn tester_actions(ctx: &mut RequestContext) -> Option<TesterRequestBody> {
        ctx.get_value::<TesterRequestBody>().ok().cloned()
    }

    /// Store the actions in the request context for the later phases.
    fn set_actions(
        &self,
        ctx: &mut RequestContext,
        actions: TesterRequestBody,
    ) -> Result<(), Error> {
        ctx.set_value(actions)
    }

    /// Build the cancel headers exactly like the Go SDK's
    /// `BuildHeaderValuesFromMap`.
    fn cancel_headers(actions: &TesterRequestBody) -> HashMap<String, HeaderValue> {
        let mut headers = HashMap::new();
        if let Some(hv) = &actions.cancel_request_headers {
            for (n, v) in hv {
                headers.insert(
                    n.clone(),
                    HeaderValue {
                        value: String::new(),
                        raw_value: Some(v.as_bytes().to_vec()),
                    },
                );
            }
        }
        headers
    }

    /// Apply the request-phase actions to the context.
    fn apply_request_actions(&self, ctx: &mut RequestContext, actions: &TesterRequestBody) {
        if let Some(hv) = &actions.add_request_headers {
            for (n, v) in hv {
                let _ = ctx.add_header(
                    n,
                    &HeaderValue {
                        value: v.clone(),
                        raw_value: None,
                    },
                );
            }
        }
        if let Some(hv) = &actions.append_request_headers {
            for (n, v) in hv {
                let _ = ctx.append_header(
                    n,
                    &HeaderValue {
                        value: v.clone(),
                        raw_value: None,
                    },
                );
            }
        }
        if let Some(hv) = &actions.overwrite_request_headers {
            for (n, v) in hv {
                let _ = ctx.overwrite_header(
                    n,
                    &HeaderValue {
                        value: v.clone(),
                        raw_value: None,
                    },
                );
            }
        }
        if let Some(names) = &actions.remove_request_headers {
            for n in names {
                let _ = ctx.remove_header(n);
            }
        }
    }

    /// Apply the response-phase actions to the context.
    fn apply_response_actions(&self, ctx: &mut RequestContext, actions: &TesterRequestBody) {
        if let Some(hv) = &actions.add_response_headers {
            for (n, v) in hv {
                let _ = ctx.add_header(
                    n,
                    &HeaderValue {
                        value: v.clone(),
                        raw_value: None,
                    },
                );
            }
        }
        if let Some(hv) = &actions.append_response_headers {
            for (n, v) in hv {
                let _ = ctx.append_header(
                    n,
                    &HeaderValue {
                        value: v.clone(),
                        raw_value: None,
                    },
                );
            }
        }
        if let Some(hv) = &actions.overwrite_response_headers {
            for (n, v) in hv {
                let _ = ctx.overwrite_header(
                    n,
                    &HeaderValue {
                        value: v.clone(),
                        raw_value: None,
                    },
                );
            }
        }
        if let Some(names) = &actions.remove_response_headers {
            for n in names {
                let _ = ctx.remove_header(n);
            }
        }
    }
}

#[async_trait]
impl RequestProcessor for TesterRequestProcessor {
    fn name(&self) -> String {
        "testing".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error> {
        report_header_parsing(ctx, headers);

        // optional upstream override, exercising clear-route-cache driven upstream
        // changes: rewrite the authority and clear the route cache so envoy
        // recomputes the route
        if let Some(upstream) = headers.get("x-route-to-upstream") {
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
        let actions = Self::load_actions(body);

        if let Some(actions) = &actions {
            if actions.cancel_request.unwrap_or(false) {
                let status = actions.cancel_request_status.unwrap_or(0);
                let values = Self::cancel_headers(actions);
                return ctx.cancel_request(
                    status,
                    &values,
                    actions
                        .cancel_request_body
                        .as_deref()
                        .unwrap_or("")
                        .as_bytes(),
                );
            }

            self.set_actions(ctx, actions.clone())?;
            self.apply_request_actions(ctx, actions);
        }

        if actions
            .as_ref()
            .is_some_and(|a| a.clear_request_body.unwrap_or(false))
        {
            ctx.clear_body()?;
        } else if let Some(actions) = &actions {
            if let Some(replace) = &actions.replace_request_body {
                if !replace.is_empty() {
                    ctx.replace_body(replace.as_bytes())?;
                }
            }
        }

        ctx.continue_request()
    }

    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        if let Some(actions) = TesterRequestProcessor::tester_actions(ctx) {
            self.apply_response_actions(ctx, &actions);
        }
        ctx.continue_request()
    }

    async fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        _body: &[u8],
    ) -> Result<(), Error> {
        if let Some(actions) = TesterRequestProcessor::tester_actions(ctx) {
            if actions.clear_response_body.unwrap_or(false) {
                ctx.clear_body()?;
            } else if let Some(replace) = &actions.replace_response_body {
                if !replace.is_empty() {
                    ctx.replace_body(replace.as_bytes())?;
                }
            }
        }
        ctx.continue_request()
    }

    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn error_handler(&self, _ctx: &mut RequestContext, _phase: RequestPhase, _err: Error) {}

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}

/// Report how the SDK parsed the inbound headers, back into the phase response,
/// mirroring the Go SDK's `reportHeaderParsing`.
fn report_header_parsing(ctx: &mut RequestContext, headers: &AllHeaders) {
    let str = |s: String| HeaderValue {
        value: String::new(),
        raw_value: Some(s.into_bytes()),
    };
    let _ = ctx.update_header(
        "x-parsed-str-count",
        &str(headers.headers.len().to_string()),
        HeaderAppendAction::AddIfAbsent,
    );
    let _ = ctx.update_header(
        "x-parsed-raw-count",
        &str(headers.raw_headers.len().to_string()),
        HeaderAppendAction::AddIfAbsent,
    );
    if let Some(v) = headers.get("x-probe-list") {
        let _ = ctx.update_header(
            "x-parsed-list-value",
            &str(v.clone()),
            HeaderAppendAction::AddIfAbsent,
        );
        let _ = ctx.update_header(
            "x-parsed-list-count",
            &str(headers.values("x-probe-list").len().to_string()),
            HeaderAppendAction::AddIfAbsent,
        );
        let _ = ctx.update_header(
            "x-parsed-list-split",
            &str(headers.split_list("x-probe-list").join("|")),
            HeaderAppendAction::AddIfAbsent,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::RequestContext;
    use crate::headers::AllHeaders;

    fn all_headers(pairs: &[(&str, &str)]) -> AllHeaders {
        let mut headers = AllHeaders::default();
        for (k, v) in pairs {
            headers
                .headers
                .insert(k.to_lowercase(), vec![v.to_string()]);
            headers
                .raw_headers
                .insert(k.to_lowercase(), vec![v.as_bytes().to_vec()]);
        }
        headers
    }

    #[tokio::test]
    async fn test_process_request_headers_clears_route_cache_for_upstream_override() {
        // An upstream override (`x-route-to-upstream`) rewrites the `:authority`
        // and clears the route cache, so envoy recomputes the route. The clear
        // decision must surface in the envoy-facing response.
        let proc = TesterRequestProcessor::new(ProcessingOptions::new());
        let mut ctx = RequestContext::new();
        ctx.reset_phase().unwrap();
        let headers = all_headers(&[("x-route-to-upstream", "upstream-copy")]);
        proc.process_request_headers(&mut ctx, &headers)
            .await
            .unwrap();
        assert!(ctx.response.clear_route_cache);
        let mutation = ctx.response.header_mutation.as_ref().unwrap();
        let authority = mutation
            .set_headers
            .iter()
            .find(|o| {
                o.header
                    .as_ref()
                    .map(|h| h.key == ":authority")
                    .unwrap_or(false)
            })
            .expect("expected an :authority overwrite in the header mutation");
        assert_eq!(
            authority.header.as_ref().unwrap().raw_value,
            b"upstream-copy".to_vec()
        );
    }

    #[tokio::test]
    async fn test_process_request_headers_does_not_clear_route_cache_without_upstream_override() {
        let proc = TesterRequestProcessor::new(ProcessingOptions::new());
        let mut ctx = RequestContext::new();
        ctx.reset_phase().unwrap();
        let headers = all_headers(&[]);
        proc.process_request_headers(&mut ctx, &headers)
            .await
            .unwrap();
        assert!(!ctx.response.clear_route_cache);
    }
}
