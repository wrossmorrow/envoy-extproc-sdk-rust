//! Request context, phase bookkeeping, and phase responses.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use http::Extensions;

pub use envoy_types::pb::envoy::config::core::v3::header_value_option::HeaderAppendAction;
use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;
use envoy_types::pb::envoy::config::core::v3::HeaderValueOption;
use envoy_types::pb::envoy::service::ext_proc::v3::body_mutation::Mutation as BodyMutationMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::processing_response::Response as ProcessingResponseResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::BodyMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::CommonResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::HeaderMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::ImmediateResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingResponse;

use crate::headers::AllHeaders;
use crate::Error;

/// Phase in which a request is being processed, as numbered by the
/// `envoy.service.ext_proc.v3` protocol. Values are the protocol's phase
/// numbers, as used by `envoy`'s ext_proc filter.
///
/// This is a typed enum rather than the raw protocol numbers used by the Go
/// SDK, so that phase handling is checked at compile time instead of relying
/// on integer constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum RequestPhase {
    /// The phase is unknown, e.g. the request headers phase was skipped via
    /// `processing_mode.request_header_mode: SKIP`.
    #[default]
    Undetermined = 0,
    /// The request headers phase.
    RequestHeaders = 1,
    /// The request body phase.
    RequestBody = 2,
    /// The request trailers phase.
    RequestTrailers = 3,
    /// The response headers phase.
    ResponseHeaders = 4,
    /// The response body phase.
    ResponseBody = 5,
    /// The response trailers phase.
    ResponseTrailers = 6,
}

impl RequestPhase {
    /// The protocol phase number for this phase.
    pub const fn as_u32(self) -> u32 {
        self as u32
    }
}

const K_CONTENT_LENGTH: &str = "content-length";

/// Map a protocol phase number onto the typed phase, defaulting to
/// [`RequestPhase::Undetermined`] when the number is not a known phase.
pub fn default_phase_if_invalid(phase: u32) -> RequestPhase {
    match phase {
        0 => RequestPhase::Undetermined,
        1 => RequestPhase::RequestHeaders,
        2 => RequestPhase::RequestBody,
        3 => RequestPhase::RequestTrailers,
        4 => RequestPhase::ResponseHeaders,
        5 => RequestPhase::ResponseBody,
        6 => RequestPhase::ResponseTrailers,
        _ => RequestPhase::Undetermined,
    }
}

/// The name of a phase, e.g. `REQUEST_HEADERS`.
pub fn request_phase_to_string(phase: RequestPhase) -> String {
    match phase {
        RequestPhase::Undetermined => "UNDETERMINED".to_string(),
        RequestPhase::RequestHeaders => "REQUEST_HEADERS".to_string(),
        RequestPhase::RequestBody => "REQUEST_BODY".to_string(),
        RequestPhase::RequestTrailers => "REQUEST_TRAILERS".to_string(),
        RequestPhase::ResponseHeaders => "RESPONSE_HEADERS".to_string(),
        RequestPhase::ResponseBody => "RESPONSE_BODY".to_string(),
        RequestPhase::ResponseTrailers => "RESPONSE_TRAILERS".to_string(),
    }
}

/// A single HTTP header value, as accepted by the SDK and converted to the
/// envoy wire format (`raw_value`) when building responses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeaderValue {
    /// Header value as a UTF-8 string, when set.
    pub value: String,
    /// Header value as raw bytes, when set.
    pub raw_value: Option<Vec<u8>>,
}

impl HeaderValue {
    /// A header value may set either `value` or `raw_value`, but not both.
    pub fn is_valid(&self) -> bool {
        !(!self.value.is_empty() && self.raw_value.is_some())
    }

    /// Convert to the envoy [`HeaderValue`] wire format, using `raw_value`.
    pub fn to_envoy_header_value(&self, name: &str) -> EnvoyHeaderValue {
        let raw = if !self.value.is_empty() && self.raw_value.is_none() {
            self.value.as_bytes().to_vec()
        } else {
            self.raw_value.clone().unwrap_or_default()
        };
        EnvoyHeaderValue {
            key: name.to_string(),
            value: String::new(),
            raw_value: raw,
        }
    }

    /// Validate and convert to a
    /// [`HeaderValueOption`](envoy_types::pb::envoy::config::core::v3::HeaderValueOption).
    ///
    /// `action` is one of the [`HeaderAppendAction`] values the envoy protocol
    /// defines, e.g. [`HeaderAppendAction::AddIfAbsent`],
    /// [`HeaderAppendAction::AppendIfExistsOrAdd`] or
    /// [`HeaderAppendAction::OverwriteIfExistsOrAdd`]. Because the action is a
    /// typed enum, values outside the protocol's set are not representable
    /// here and cannot produce a runtime error; invalid header values are
    /// still rejected.
    #[allow(deprecated)]
    pub fn to_envoy_header_value_option(
        &self,
        name: &str,
        action: HeaderAppendAction,
    ) -> Result<HeaderValueOption, Error> {
        if !self.is_valid() {
            return Err(format!("invalid header value for {}", name).into());
        }
        Ok(HeaderValueOption {
            header: Some(self.to_envoy_header_value(name)),
            append: None,
            append_action: action as i32,
            keep_empty_value: false,
        })
    }
}

/// Build a map of [`HeaderValue`] from a map of header names to string values,
/// storing each value as a raw value.
pub fn build_header_values_from_map(
    headers: &HashMap<String, String>,
) -> HashMap<String, HeaderValue> {
    let mut out = HashMap::new();
    for (n, v) in headers {
        out.insert(
            n.clone(),
            HeaderValue {
                value: String::new(),
                raw_value: Some(v.as_bytes().to_vec()),
            },
        );
    }
    out
}

/// The mutable, phase-scoped response being assembled by a processor.
#[derive(Clone, Default)]
pub struct PhaseResponse {
    /// Header mutations accumulated for the current phase response.
    pub header_mutation: Option<HeaderMutation>,
    /// Body mutation for the current phase response, if any.
    pub body_mutation: Option<BodyMutation>,
    /// A "continue" response to the data plane, if set.
    pub continue_request: Option<CommonResponse>,
    /// An immediate response to the data plane, if set.
    pub immediate_response: Option<ImmediateResponse>,
    /// Whether to clear the route cache after the current phase.
    pub clear_route_cache: bool,
}

impl PhaseResponse {
    /// Clear all phase response state, starting from an empty continue
    /// response.
    pub fn reset(&mut self) {
        self.header_mutation = Some(HeaderMutation {
            set_headers: Vec::new(),
            remove_headers: Vec::new(),
        });
        self.body_mutation = None;
        self.continue_request = None;
        self.immediate_response = None;
        self.clear_route_cache = false;
    }
}

/// The request context, shared across phases of a single request/response
/// exchange and handed to the processor for each phase.
pub struct RequestContext {
    /// Parsed from the `:scheme` header, if any.
    pub scheme: String,
    /// Parsed from the `:authority` header, if any.
    pub authority: String,
    /// Parsed from the `:method` header, if any.
    pub method: String,
    /// Parsed from the `:path` header, sans query string.
    pub path: String,
    /// Parsed from the `:path` header, including query string.
    pub full_path: String,
    /// Parsed from the `x-request-id` header, if any.
    pub request_id: String,

    /// All headers (or trailers) seen in the current phase.
    pub all_headers: AllHeaders,

    /// When the current request started processing.
    pub started: Instant,
    /// Time spent in external processing, accumulated across phases.
    pub duration: Duration,
    /// Whether the current phase message was the last of its kind.
    pub end_of_stream: bool,

    /// Arbitrary custom values shared across phases of a request, stored in a
    /// type-keyed map ([`http::Extensions`]) so values are strongly typed
    /// rather than string-keyed trait objects.
    pub data: Extensions,

    /// Current processing phase.
    pub phase: RequestPhase,
    /// Response being assembled for the current phase.
    pub response: PhaseResponse,
}

impl RequestContext {
    /// Create a fresh request context, as when `request_header_mode: SKIP`.
    pub fn new() -> Self {
        Self {
            scheme: String::new(),
            authority: String::new(),
            method: String::new(),
            path: String::new(),
            full_path: String::new(),
            request_id: String::new(),
            all_headers: AllHeaders::default(),
            started: Instant::now(),
            duration: Duration::ZERO,
            end_of_stream: false,
            data: Extensions::new(),
            phase: RequestPhase::Undetermined,
            response: PhaseResponse::default(),
        }
    }

    /// Reset the phase and response state. Called at the start of each
    /// processing phase.
    pub fn reset_phase(&mut self) -> Result<(), Error> {
        self.phase = RequestPhase::Undetermined;
        self.end_of_stream = false;
        self.response.reset();
        Ok(())
    }

    /// Set the current phase.
    pub fn set_phase(&mut self, phase: RequestPhase) {
        self.phase = phase;
    }

    /// The current phase.
    pub fn get_phase(&self) -> RequestPhase {
        self.phase
    }

    /// The current phase name, e.g. `REQUEST_HEADERS`.
    pub fn get_phase_name(&self) -> String {
        request_phase_to_string(self.phase)
    }

    /// Store a typed custom value for later phases of this request.
    pub fn set_value<T: Clone + Send + Sync + 'static>(&mut self, val: T) -> Result<(), Error> {
        self.data.insert(val);
        Ok(())
    }

    /// Retrieve a typed custom value stored for this request, if present.
    pub fn get_value<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.data
            .get::<T>()
            .ok_or_else(|| format!("{} does not exist", std::any::type_name::<T>()).into())
    }

    /// Whether to clear the route cache after the current phase.
    pub fn clear_route_cache(&mut self) {
        self.response.clear_route_cache = true;
    }

    /// Whether the route cache was cleared; see
    /// [`clear_route_cache`](RequestContext::clear_route_cache).
    pub fn un_clear_route_cache(&mut self) {
        self.response.clear_route_cache = false;
    }

    /// Build the [`ProcessingResponse`] for the given phase, honoring an
    /// immediate response set by the processor when the phase allows it, and
    /// otherwise returning a "continue" response.
    ///
    /// Immediate responses are honored for the
    /// [`RequestPhase::RequestHeaders`], [`RequestPhase::RequestBody`],
    /// [`RequestPhase::ResponseHeaders`] and [`RequestPhase::ResponseBody`]
    /// phases; the trailer phases ignore them and always continue.
    pub fn get_response(
        &mut self,
        phase: RequestPhase,
    ) -> Result<Option<ProcessingResponse>, Error> {
        if let Some(immediate) = &self.response.immediate_response {
            if phase == RequestPhase::RequestHeaders
                || phase == RequestPhase::RequestBody
                || phase == RequestPhase::ResponseHeaders
                || phase == RequestPhase::ResponseBody
            {
                return Ok(Some(ProcessingResponse {
                    response: Some(ProcessingResponseResponse::ImmediateResponse(
                        immediate.clone(),
                    )),
                    ..Default::default()
                }));
            }
        }

        self.response.continue_request = Some(CommonResponse {
            status: 0, // CommonResponse::ResponseStatus::Continue
            header_mutation: self.response.header_mutation.clone(),
            body_mutation: self.response.body_mutation.clone(),
            trailers: None,
            clear_route_cache: self.response.clear_route_cache,
        });

        let response: Option<ProcessingResponseResponse> = match phase {
            RequestPhase::RequestHeaders => Some(ProcessingResponseResponse::RequestHeaders(
                envoy_types::pb::envoy::service::ext_proc::v3::HeadersResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            RequestPhase::ResponseHeaders => Some(ProcessingResponseResponse::ResponseHeaders(
                envoy_types::pb::envoy::service::ext_proc::v3::HeadersResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            RequestPhase::RequestBody => Some(ProcessingResponseResponse::RequestBody(
                envoy_types::pb::envoy::service::ext_proc::v3::BodyResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            RequestPhase::ResponseBody => Some(ProcessingResponseResponse::ResponseBody(
                envoy_types::pb::envoy::service::ext_proc::v3::BodyResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            RequestPhase::RequestTrailers => Some(ProcessingResponseResponse::RequestTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: self.response.header_mutation.clone(),
                },
            )),
            RequestPhase::ResponseTrailers => Some(ProcessingResponseResponse::ResponseTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: self.response.header_mutation.clone(),
                },
            )),
            RequestPhase::Undetermined => {
                return Err(format!("unknown request phase {}", phase.as_u32()).into());
            }
        };

        Ok(Some(ProcessingResponse {
            response,
            ..Default::default()
        }))
    }

    /// An empty "continue" response, as used when a phase produced no
    /// response at all.
    pub fn empty_continue_response(
        &self,
        phase: RequestPhase,
    ) -> Result<Option<ProcessingResponse>, Error> {
        let response: Option<ProcessingResponseResponse> = match phase {
            RequestPhase::RequestHeaders => Some(ProcessingResponseResponse::RequestHeaders(
                envoy_types::pb::envoy::service::ext_proc::v3::HeadersResponse {
                    response: Some(CommonResponse {
                        status: 0,
                        header_mutation: Some(HeaderMutation {
                            set_headers: Vec::new(),
                            remove_headers: Vec::new(),
                        }),
                        body_mutation: None,
                        trailers: None,
                        clear_route_cache: false,
                    }),
                },
            )),
            RequestPhase::RequestBody => Some(ProcessingResponseResponse::RequestBody(
                envoy_types::pb::envoy::service::ext_proc::v3::BodyResponse {
                    response: Some(CommonResponse {
                        status: 0,
                        header_mutation: Some(HeaderMutation {
                            set_headers: Vec::new(),
                            remove_headers: Vec::new(),
                        }),
                        body_mutation: None,
                        trailers: None,
                        clear_route_cache: false,
                    }),
                },
            )),
            RequestPhase::RequestTrailers => Some(ProcessingResponseResponse::RequestTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: Some(HeaderMutation {
                        set_headers: Vec::new(),
                        remove_headers: Vec::new(),
                    }),
                },
            )),
            RequestPhase::ResponseHeaders => Some(ProcessingResponseResponse::ResponseHeaders(
                envoy_types::pb::envoy::service::ext_proc::v3::HeadersResponse {
                    response: Some(CommonResponse {
                        status: 0,
                        header_mutation: Some(HeaderMutation {
                            set_headers: Vec::new(),
                            remove_headers: Vec::new(),
                        }),
                        body_mutation: None,
                        trailers: None,
                        clear_route_cache: false,
                    }),
                },
            )),
            RequestPhase::ResponseBody => Some(ProcessingResponseResponse::ResponseBody(
                envoy_types::pb::envoy::service::ext_proc::v3::BodyResponse {
                    response: Some(CommonResponse {
                        status: 0,
                        header_mutation: Some(HeaderMutation {
                            set_headers: Vec::new(),
                            remove_headers: Vec::new(),
                        }),
                        body_mutation: None,
                        trailers: None,
                        clear_route_cache: false,
                    }),
                },
            )),
            RequestPhase::ResponseTrailers => Some(ProcessingResponseResponse::ResponseTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: Some(HeaderMutation {
                        set_headers: Vec::new(),
                        remove_headers: Vec::new(),
                    }),
                },
            )),
            RequestPhase::Undetermined => {
                return Err(format!("unknown request phase {}", phase.as_u32()).into());
            }
        };

        Ok(Some(ProcessingResponse {
            response,
            ..Default::default()
        }))
    }

    /// Continue the request: respond with the (possibly mutated) continue
    /// response and wait for further phase messages.
    pub fn continue_request(&mut self) -> Result<(), Error> {
        self.response.immediate_response = None;
        self.response.continue_request = Some(CommonResponse {
            status: 0, // CommonResponse::ResponseStatus::Continue
            header_mutation: self.response.header_mutation.clone(),
            body_mutation: self.response.body_mutation.clone(),
            trailers: None,
            clear_route_cache: self.response.clear_route_cache,
        });
        Ok(())
    }

    /// Respond immediately with a locally generated response (e.g. an error
    /// status), and stop processing further phases of this request/response.
    pub fn cancel_request(
        &mut self,
        status: i32,
        headers: &HashMap<String, HeaderValue>,
        body: &[u8],
    ) -> Result<(), Error> {
        for (n, hv) in headers {
            self.append_header(n, hv)?;
        }
        self.response.continue_request = None;
        self.response.immediate_response = Some(ImmediateResponse {
            status: Some(envoy_types::pb::envoy::r#type::v3::HttpStatus { code: status }),
            headers: self.response.header_mutation.clone(),
            body: body.to_vec(),
            grpc_status: None,
            details: String::new(),
        });
        Ok(())
    }

    /// Add a header mutation for the current phase, applying `action`.
    #[allow(deprecated)]
    pub fn update_header(
        &mut self,
        name: &str,
        hv: &HeaderValue,
        action: HeaderAppendAction,
    ) -> Result<(), Error> {
        let envoy_hv = hv.to_envoy_header_value(name);
        let option = HeaderValueOption {
            header: Some(envoy_hv),
            append: None,
            append_action: action as i32,
            keep_empty_value: false,
        };
        let mutation = self
            .response
            .header_mutation
            .as_mut()
            .ok_or_else(|| "no header mutation in progress".to_string())?;
        mutation.set_headers.push(option);
        Ok(())
    }

    /// Append a header value if it exists, or add it if absent. See
    /// [`update_header`](RequestContext::update_header).
    pub fn append_header(&mut self, name: &str, hv: &HeaderValue) -> Result<(), Error> {
        self.update_header(name, hv, HeaderAppendAction::AppendIfExistsOrAdd)
    }

    /// Add a header value only if the header is absent. See
    /// [`update_header`](RequestContext::update_header).
    pub fn add_header(&mut self, name: &str, hv: &HeaderValue) -> Result<(), Error> {
        self.update_header(name, hv, HeaderAppendAction::AddIfAbsent)
    }

    /// Overwrite a header value. See
    /// [`update_header`](RequestContext::update_header).
    pub fn overwrite_header(&mut self, name: &str, hv: &HeaderValue) -> Result<(), Error> {
        self.update_header(name, hv, HeaderAppendAction::OverwriteIfExistsOrAdd)
    }

    /// Update multiple headers with the given action. See
    /// [`update_header`](RequestContext::update_header).
    #[allow(deprecated)]
    pub fn update_headers(
        &mut self,
        headers: &HashMap<String, HeaderValue>,
        action: HeaderAppendAction,
    ) -> Result<(), Error> {
        let mut options = Vec::new();
        for (n, hv) in headers {
            options.push(HeaderValueOption {
                header: Some(hv.to_envoy_header_value(n)),
                append: None,
                append_action: action as i32,
                keep_empty_value: false,
            });
        }
        let mutation = self
            .response
            .header_mutation
            .as_mut()
            .ok_or_else(|| "no header mutation in progress".to_string())?;
        mutation.set_headers.extend(options);
        Ok(())
    }

    /// Append multiple headers. See [`update_headers`](RequestContext::update_headers).
    pub fn append_headers(&mut self, headers: &HashMap<String, HeaderValue>) -> Result<(), Error> {
        self.update_headers(headers, HeaderAppendAction::AppendIfExistsOrAdd)
    }

    /// Add multiple headers if absent. See [`update_headers`](RequestContext::update_headers).
    pub fn add_headers(&mut self, headers: &HashMap<String, HeaderValue>) -> Result<(), Error> {
        self.update_headers(headers, HeaderAppendAction::AddIfAbsent)
    }

    /// Overwrite multiple headers. See [`update_headers`](RequestContext::update_headers).
    pub fn overwrite_headers(
        &mut self,
        headers: &HashMap<String, HeaderValue>,
    ) -> Result<(), Error> {
        self.update_headers(headers, HeaderAppendAction::OverwriteIfExistsOrAdd)
    }

    /// Queue a header for removal. Removing a header that was never set is not
    /// an error; the name is simply queued for removal.
    pub fn remove_header(&mut self, name: &str) -> Result<(), Error> {
        let mutation = self
            .response
            .header_mutation
            .as_mut()
            .ok_or_else(|| "no header mutation in progress".to_string())?;
        if !mutation.remove_headers.iter().any(|h| h == name) {
            mutation.remove_headers.push(name.to_string());
        }
        Ok(())
    }

    /// Queue multiple headers for removal, without duplicates. See
    /// [`remove_header`](RequestContext::remove_header).
    pub fn remove_headers(&mut self, headers: &[String]) -> Result<(), Error> {
        for h in headers {
            self.remove_header(h)?;
        }
        Ok(())
    }

    /// Replace the body bytes of the current message.
    ///
    /// It deliberately leaves Content-Length alone: under a STREAMED body mode
    /// the chunk is only part of the body, so its length is not the message
    /// length. Use [`ReplaceBody`](RequestContext::replace_body) when the
    /// filter is configured BUFFERED and this call carries the whole body.
    ///
    /// An empty body is a no-op; use
    /// [`ClearBodyChunk`](RequestContext::clear_body_chunk) or
    /// [`ClearBody`](RequestContext::clear_body) to remove a body.
    pub fn replace_body_chunk(&mut self, body: &[u8]) -> Result<(), Error> {
        if body.is_empty() {
            return Ok(());
        }
        self.response.body_mutation = Some(BodyMutation {
            mutation: Some(BodyMutationMutation::Body(body.to_vec())),
        });
        Ok(())
    }

    /// Replace the whole body and update Content-Length to match.
    ///
    /// Only correct when the filter is configured BUFFERED, so that a single
    /// body message carries the entire body. Under STREAMED use
    /// [`ReplaceBodyChunk`](RequestContext::replace_body_chunk). Note too
    /// that a chunked HTTP/1.1 message carries no Content-Length at all, so
    /// neither whole-body method produces a meaningful header in that case.
    pub fn replace_body(&mut self, body: &[u8]) -> Result<(), Error> {
        self.replace_body_chunk(body)?;
        if body.is_empty() {
            return Ok(());
        }
        self.overwrite_header(
            K_CONTENT_LENGTH,
            &HeaderValue {
                value: String::new(),
                raw_value: Some(body.len().to_string().into_bytes()),
            },
        )?;
        Ok(())
    }

    /// Clear the body bytes of the current message, leaving Content-Length
    /// alone. See [`replace_body_chunk`](RequestContext::replace_body_chunk) on
    /// why.
    pub fn clear_body_chunk(&mut self) -> Result<(), Error> {
        self.response.body_mutation = Some(BodyMutation {
            mutation: Some(BodyMutationMutation::ClearBody(true)),
        });
        Ok(())
    }

    /// Clear the body and set Content-Length to 0. See
    /// [`replace_body`](RequestContext::replace_body) on when this is the right
    /// call.
    pub fn clear_body(&mut self) -> Result<(), Error> {
        self.clear_body_chunk()?;
        self.overwrite_header(
            K_CONTENT_LENGTH,
            &HeaderValue {
                value: String::new(),
                raw_value: Some("0".into()),
            },
        )?;
        Ok(())
    }
}

impl Default for RequestContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_phase_if_invalid() {
        assert_eq!(
            default_phase_if_invalid(RequestPhase::Undetermined.as_u32()),
            RequestPhase::Undetermined
        );
        assert_eq!(
            default_phase_if_invalid(RequestPhase::RequestHeaders.as_u32()),
            RequestPhase::RequestHeaders
        );
        assert_eq!(
            default_phase_if_invalid(RequestPhase::RequestBody.as_u32()),
            RequestPhase::RequestBody
        );
        assert_eq!(
            default_phase_if_invalid(RequestPhase::RequestTrailers.as_u32()),
            RequestPhase::RequestTrailers
        );
        assert_eq!(
            default_phase_if_invalid(RequestPhase::ResponseHeaders.as_u32()),
            RequestPhase::ResponseHeaders
        );
        assert_eq!(
            default_phase_if_invalid(RequestPhase::ResponseBody.as_u32()),
            RequestPhase::ResponseBody
        );
        assert_eq!(
            default_phase_if_invalid(RequestPhase::ResponseTrailers.as_u32()),
            RequestPhase::ResponseTrailers
        );
        assert_eq!(default_phase_if_invalid(99), RequestPhase::Undetermined);
    }

    #[test]
    fn test_request_phase_to_string() {
        assert_eq!(
            request_phase_to_string(RequestPhase::Undetermined),
            "UNDETERMINED"
        );
        assert_eq!(
            request_phase_to_string(RequestPhase::RequestHeaders),
            "REQUEST_HEADERS"
        );
        assert_eq!(
            request_phase_to_string(RequestPhase::RequestBody),
            "REQUEST_BODY"
        );
        assert_eq!(
            request_phase_to_string(RequestPhase::RequestTrailers),
            "REQUEST_TRAILERS"
        );
        assert_eq!(
            request_phase_to_string(RequestPhase::ResponseHeaders),
            "RESPONSE_HEADERS"
        );
        assert_eq!(
            request_phase_to_string(RequestPhase::ResponseBody),
            "RESPONSE_BODY"
        );
        assert_eq!(
            request_phase_to_string(RequestPhase::ResponseTrailers),
            "RESPONSE_TRAILERS"
        );
    }

    #[test]
    fn test_header_value_helpers() {
        let hv = HeaderValue {
            value: "hello".to_string(),
            raw_value: None,
        };
        assert!(hv.is_valid());
        assert_eq!(
            hv.to_envoy_header_value("x-foo").raw_value,
            b"hello".to_vec()
        );
        assert_eq!(hv.to_envoy_header_value("x-foo").key, "x-foo");

        let hv = HeaderValue {
            value: String::new(),
            raw_value: Some(b"raw".to_vec()),
        };
        assert!(hv.is_valid());
        assert_eq!(hv.to_envoy_header_value("x-foo").raw_value, b"raw".to_vec());

        let hv = HeaderValue {
            value: "both".to_string(),
            raw_value: Some(b"raw".to_vec()),
        };
        assert!(!hv.is_valid());

        let valid = HeaderValue {
            value: "hello".to_string(),
            raw_value: None,
        };
        let opt = valid
            .to_envoy_header_value_option("x-foo", HeaderAppendAction::AddIfAbsent)
            .unwrap();
        assert_eq!(opt.append_action, 1);

        assert!(hv
            .to_envoy_header_value_option("x-foo", HeaderAppendAction::AddIfAbsent)
            .is_err());
        let bad = HeaderValue {
            value: "both".to_string(),
            raw_value: Some(b"raw".to_vec()),
        };
        assert!(bad
            .to_envoy_header_value_option("x-foo", HeaderAppendAction::AddIfAbsent)
            .is_err());
    }

    #[test]
    fn test_build_header_values_from_map() {
        let mut m = HashMap::new();
        m.insert("x-foo".to_string(), "bar".to_string());
        let values = build_header_values_from_map(&m);
        assert_eq!(
            values
                .get("x-foo")
                .unwrap()
                .to_envoy_header_value("x-foo")
                .raw_value,
            b"bar".to_vec()
        );
    }

    #[test]
    fn test_phase_response_reset() {
        let mut pr = PhaseResponse {
            header_mutation: Some(HeaderMutation {
                set_headers: vec![],
                remove_headers: vec![],
            }),
            body_mutation: None,
            immediate_response: None,
            continue_request: None,
            clear_route_cache: true,
        };
        pr.reset();
        assert!(pr.header_mutation.is_some());
        assert!(pr.continue_request.is_none());
        assert!(pr.immediate_response.is_none());
        assert!(!pr.clear_route_cache);
    }

    #[test]
    fn test_request_context_phase_lifecycle() {
        let mut rc = RequestContext::new();
        assert_eq!(rc.get_phase(), RequestPhase::Undetermined);
        assert_eq!(rc.get_phase_name(), "UNDETERMINED");
        rc.set_phase(RequestPhase::ResponseBody);
        assert_eq!(rc.get_phase(), RequestPhase::ResponseBody);
        assert_eq!(rc.get_phase_name(), "RESPONSE_BODY");
        rc.set_phase(default_phase_if_invalid(42));
        assert_eq!(rc.get_phase(), RequestPhase::Undetermined);

        rc.set_value(42u32).unwrap();
        let v = rc.get_value::<u32>().unwrap();
        assert_eq!(*v, 42);
        assert!(rc.get_value::<u64>().is_err());
    }

    #[test]
    fn test_get_response_typed_per_phase() {
        let mut rc = RequestContext::new();
        rc.response.header_mutation = Some(HeaderMutation {
            set_headers: vec![],
            remove_headers: vec![],
        });
        rc.response.continue_request = Some(CommonResponse {
            status: 0,
            header_mutation: None,
            body_mutation: None,
            trailers: None,
            clear_route_cache: false,
        });
        let resp = rc
            .get_response(RequestPhase::RequestHeaders)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::RequestHeaders(_))
        ));

        let resp = rc
            .get_response(RequestPhase::ResponseTrailers)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::ResponseTrailers(_))
        ));

        assert!(rc.get_response(RequestPhase::Undetermined).is_err());
    }

    #[test]
    fn test_get_response_immediate() {
        let mut rc = RequestContext::new();
        rc.response.immediate_response = Some(ImmediateResponse {
            status: Some(envoy_types::pb::envoy::r#type::v3::HttpStatus { code: 503 }),
            headers: None,
            body: vec![],
            grpc_status: None,
            details: String::new(),
        });
        let resp = rc.get_response(RequestPhase::RequestBody).unwrap().unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::ImmediateResponse(_))
        ));
        // trailers phases ignore immediate responses
        let resp = rc
            .get_response(RequestPhase::ResponseTrailers)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::ResponseTrailers(_))
        ));
    }

    fn response_common(resp: &ProcessingResponse) -> Option<&CommonResponse> {
        match &resp.response {
            Some(ProcessingResponseResponse::RequestHeaders(h)) => h.response.as_ref(),
            Some(ProcessingResponseResponse::ResponseHeaders(h)) => h.response.as_ref(),
            Some(ProcessingResponseResponse::RequestBody(h)) => h.response.as_ref(),
            Some(ProcessingResponseResponse::ResponseBody(h)) => h.response.as_ref(),
            _ => None,
        }
    }

    #[test]
    fn test_get_response_propagates_clear_route_cache() {
        // An external processor that clears the route cache (i.e. is changing the
        // upstream, typically by rewriting a routed header such as `:authority`)
        // must surface that decision in the envoy-facing response, or envoy will
        // keep serving from its cached route.
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();

        rc.clear_route_cache();
        let resp = rc
            .get_response(RequestPhase::RequestHeaders)
            .unwrap()
            .unwrap();
        let common = response_common(&resp)
            .expect("expected a CommonResponse in the request headers response");
        assert!(common.clear_route_cache);

        rc.un_clear_route_cache();
        let resp = rc
            .get_response(RequestPhase::RequestHeaders)
            .unwrap()
            .unwrap();
        let common = response_common(&resp)
            .expect("expected a CommonResponse in the request headers response");
        assert!(!common.clear_route_cache);
    }

    #[test]
    fn test_get_response_propagates_clear_route_cache_request_body() {
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();

        rc.clear_route_cache();
        let resp = rc.get_response(RequestPhase::RequestBody).unwrap().unwrap();
        let common =
            response_common(&resp).expect("expected a CommonResponse in the request body response");
        assert!(common.clear_route_cache);
    }

    #[test]
    fn test_get_response_clear_route_cache_defaults_to_false() {
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();

        let resp = rc
            .get_response(RequestPhase::RequestHeaders)
            .unwrap()
            .unwrap();
        let common = response_common(&resp)
            .expect("expected a CommonResponse in the request headers response");
        assert!(!common.clear_route_cache);
    }

    #[test]
    fn test_update_headers() {
        let mut rc = RequestContext::new();
        rc.response.header_mutation = Some(HeaderMutation {
            set_headers: vec![],
            remove_headers: vec![],
        });
        rc.update_header(
            "x-foo",
            &HeaderValue {
                value: "bar".to_string(),
                raw_value: Some(b"bar".to_vec()),
            },
            HeaderAppendAction::OverwriteIfExistsOrAdd,
        )
        .unwrap();
        assert_eq!(
            rc.response
                .header_mutation
                .as_ref()
                .unwrap()
                .set_headers
                .len(),
            1
        );
        rc.remove_header("x-foo").unwrap();
        assert_eq!(
            rc.response
                .header_mutation
                .as_ref()
                .unwrap()
                .remove_headers
                .len(),
            1
        );
        rc.remove_header("x-foo").unwrap(); // no duplicates
        assert_eq!(
            rc.response
                .header_mutation
                .as_ref()
                .unwrap()
                .remove_headers
                .len(),
            1
        );
        // Without a header mutation in progress, update_header errors.
        rc.response.header_mutation = None;
        assert!(rc
            .update_header(
                "x-foo",
                &HeaderValue {
                    value: "bar".to_string(),
                    raw_value: None,
                },
                HeaderAppendAction::AddIfAbsent
            )
            .is_err());
    }

    #[test]
    fn test_body_mutations() {
        let mut rc = RequestContext::new();
        rc.response.header_mutation = Some(HeaderMutation {
            set_headers: Vec::new(),
            remove_headers: Vec::new(),
        });
        rc.replace_body_chunk(b"abc").unwrap();
        assert!(rc.response.body_mutation.is_some());
        rc.replace_body(b"abcdef").unwrap();
        assert!(rc.response.header_mutation.is_some()); // content-length overwritten via header mutation
        rc.clear_body().unwrap();
        assert!(rc.response.body_mutation.is_some());
        rc.clear_body_chunk().unwrap();
        assert!(rc.response.body_mutation.is_some());
    }
}
