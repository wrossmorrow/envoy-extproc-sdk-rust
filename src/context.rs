//! Request context, phase bookkeeping, and phase responses.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A `'static` trait object shared across phases of a request.
pub type DynAny = dyn std::any::Any + Send + Sync;

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
pub const REQUEST_PHASE_UNDETERMINED: u32 = 0;
pub const REQUEST_PHASE_REQUEST_HEADERS: u32 = 1;
pub const REQUEST_PHASE_REQUEST_BODY: u32 = 2;
pub const REQUEST_PHASE_REQUEST_TRAILERS: u32 = 3;
pub const REQUEST_PHASE_RESPONSE_HEADERS: u32 = 4;
pub const REQUEST_PHASE_RESPONSE_BODY: u32 = 5;
pub const REQUEST_PHASE_RESPONSE_TRAILERS: u32 = 6;

const K_CONTENT_LENGTH: &str = "content-length";

pub fn default_phase_if_invalid(phase: u32) -> u32 {
    match phase {
        REQUEST_PHASE_UNDETERMINED
        | REQUEST_PHASE_REQUEST_HEADERS
        | REQUEST_PHASE_REQUEST_BODY
        | REQUEST_PHASE_REQUEST_TRAILERS
        | REQUEST_PHASE_RESPONSE_HEADERS
        | REQUEST_PHASE_RESPONSE_BODY
        | REQUEST_PHASE_RESPONSE_TRAILERS => phase,
        _ => REQUEST_PHASE_UNDETERMINED,
    }
}

pub fn request_phase_to_string(phase: u32) -> String {
    match phase {
        REQUEST_PHASE_UNDETERMINED => "UNDETERMINED".to_string(),
        REQUEST_PHASE_REQUEST_HEADERS => "REQUEST_HEADERS".to_string(),
        REQUEST_PHASE_REQUEST_BODY => "REQUEST_BODY".to_string(),
        REQUEST_PHASE_REQUEST_TRAILERS => "REQUEST_TRAILERS".to_string(),
        REQUEST_PHASE_RESPONSE_HEADERS => "RESPONSE_HEADERS".to_string(),
        REQUEST_PHASE_RESPONSE_BODY => "RESPONSE_BODY".to_string(),
        REQUEST_PHASE_RESPONSE_TRAILERS => "RESPONSE_TRAILERS".to_string(),
        _ => format!("UNKNOWN({})", phase),
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
        !(self.value.is_empty() == false && self.raw_value.is_some())
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
    /// `action` must be one of the actions the envoy protocol defines, e.g.
    /// `ADD_IF_ABSENT`, `APPEND_IF_EXISTS_OR_ADD`, `OVERWRITE_IF_EXISTS`,
    /// `OVERWRITE_IF_EXISTS_OR_ADD`.
    #[allow(deprecated)]
    pub fn to_envoy_header_value_option(
        &self,
        name: &str,
        action: &str,
    ) -> Result<HeaderValueOption, Error> {
        if !self.is_valid() {
            return Err(format!("invalid header value for {}", name).into());
        }
        let action_enum = HeaderAppendAction::from_str_name(action)
            .ok_or_else(|| format!("unknown header append action {:?}", action))?;
        Ok(HeaderValueOption {
            header: Some(self.to_envoy_header_value(name)),
            append: None,
            append_action: action_enum as i32,
            keep_empty_value: false,
        })
    }
}

/// Build a map of [`HeaderValue`] from a map of header names to string values.
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

    /// Arbitrary custom values shared across phases of a request.
    pub data: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,

    /// Current processing phase.
    pub phase: u32,
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
            data: HashMap::new(),
            phase: REQUEST_PHASE_UNDETERMINED,
            response: PhaseResponse::default(),
        }
    }

    /// Reset the phase and response state. Called at the start of each
    /// processing phase.
    pub fn reset_phase(&mut self) -> Result<(), Error> {
        self.phase = REQUEST_PHASE_UNDETERMINED;
        self.end_of_stream = false;
        self.response.reset();
        Ok(())
    }

    /// Set the phase, mapping invalid values to
    /// [`REQUEST_PHASE_UNDETERMINED`](crate::REQUEST_PHASE_UNDETERMINED).
    pub fn set_phase(&mut self, phase: u32) {
        self.phase = default_phase_if_invalid(phase);
    }

    /// The current phase, as a number.
    pub fn get_phase(&self) -> u32 {
        self.phase
    }

    /// The current phase name, e.g. `REQUEST_HEADERS`.
    pub fn get_phase_name(&self) -> String {
        request_phase_to_string(self.phase)
    }

    /// Store a custom value for later phases of this request.
    pub fn set_value(
        &mut self,
        name: &str,
        val: Box<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), Error> {
        self.data.insert(name.to_string(), val);
        Ok(())
    }

    /// Retrieve a custom value stored for this request, if present.
    pub fn get_value(&self, name: &str) -> Result<&DynAny, Error> {
        let value = match self.data.get(name) {
            Some(v) => &**v,
            None => return Err(format!("{} does not exist", name).into()),
        };
        Ok(value)
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
    pub fn get_response(&mut self, phase: u32) -> Result<Option<ProcessingResponse>, Error> {
        if let Some(immediate) = &self.response.immediate_response {
            if phase == REQUEST_PHASE_REQUEST_HEADERS
                || phase == REQUEST_PHASE_REQUEST_BODY
                || phase == REQUEST_PHASE_RESPONSE_HEADERS
                || phase == REQUEST_PHASE_RESPONSE_BODY
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
            REQUEST_PHASE_REQUEST_HEADERS => Some(ProcessingResponseResponse::RequestHeaders(
                envoy_types::pb::envoy::service::ext_proc::v3::HeadersResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            REQUEST_PHASE_RESPONSE_HEADERS => Some(ProcessingResponseResponse::ResponseHeaders(
                envoy_types::pb::envoy::service::ext_proc::v3::HeadersResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            REQUEST_PHASE_REQUEST_BODY => Some(ProcessingResponseResponse::RequestBody(
                envoy_types::pb::envoy::service::ext_proc::v3::BodyResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            REQUEST_PHASE_RESPONSE_BODY => Some(ProcessingResponseResponse::ResponseBody(
                envoy_types::pb::envoy::service::ext_proc::v3::BodyResponse {
                    response: self.response.continue_request.clone(),
                },
            )),
            REQUEST_PHASE_REQUEST_TRAILERS => Some(ProcessingResponseResponse::RequestTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: self.response.header_mutation.clone(),
                },
            )),
            REQUEST_PHASE_RESPONSE_TRAILERS => Some(ProcessingResponseResponse::ResponseTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: self.response.header_mutation.clone(),
                },
            )),
            _ => return Err(format!("unknown request phase {}", phase).into()),
        };

        Ok(Some(ProcessingResponse {
            response,
            ..Default::default()
        }))
    }

    /// An empty "continue" response, as used when a phase produced no
    /// response at all.
    pub fn empty_continue_response(&self, phase: u32) -> Result<Option<ProcessingResponse>, Error> {
        let response: Option<ProcessingResponseResponse> = match phase {
            REQUEST_PHASE_REQUEST_HEADERS => Some(ProcessingResponseResponse::RequestHeaders(
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
            REQUEST_PHASE_REQUEST_BODY => Some(ProcessingResponseResponse::RequestBody(
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
            REQUEST_PHASE_REQUEST_TRAILERS => Some(ProcessingResponseResponse::RequestTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: Some(HeaderMutation {
                        set_headers: Vec::new(),
                        remove_headers: Vec::new(),
                    }),
                },
            )),
            REQUEST_PHASE_RESPONSE_HEADERS => Some(ProcessingResponseResponse::ResponseHeaders(
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
            REQUEST_PHASE_RESPONSE_BODY => Some(ProcessingResponseResponse::ResponseBody(
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
            REQUEST_PHASE_RESPONSE_TRAILERS => Some(ProcessingResponseResponse::ResponseTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: Some(HeaderMutation {
                        set_headers: Vec::new(),
                        remove_headers: Vec::new(),
                    }),
                },
            )),
            _ => return Err(format!("unknown request phase {}", phase).into()),
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
        action: &str,
    ) -> Result<(), Error> {
        let action_enum = HeaderAppendAction::from_str_name(action)
            .ok_or_else(|| format!("unknown header append action {:?}", action))?;
        let envoy_hv = hv.to_envoy_header_value(name);
        let option = HeaderValueOption {
            header: Some(envoy_hv),
            append: None,
            append_action: action_enum as i32,
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
        self.update_header(name, hv, "APPEND_IF_EXISTS_OR_ADD")
    }

    /// Add a header value only if the header is absent. See
    /// [`update_header`](RequestContext::update_header).
    pub fn add_header(&mut self, name: &str, hv: &HeaderValue) -> Result<(), Error> {
        self.update_header(name, hv, "ADD_IF_ABSENT")
    }

    /// Overwrite a header value. See
    /// [`update_header`](RequestContext::update_header).
    pub fn overwrite_header(&mut self, name: &str, hv: &HeaderValue) -> Result<(), Error> {
        self.update_header(name, hv, "OVERWRITE_IF_EXISTS_OR_ADD")
    }

    /// Update multiple headers with the given action. See
    /// [`update_header`](RequestContext::update_header).
    #[allow(deprecated)]
    pub fn update_headers(
        &mut self,
        headers: &HashMap<String, HeaderValue>,
        action: &str,
    ) -> Result<(), Error> {
        let action_enum = HeaderAppendAction::from_str_name(action)
            .ok_or_else(|| format!("unknown header append action {:?}", action))?;
        let mut options = Vec::new();
        for (n, hv) in headers {
            options.push(HeaderValueOption {
                header: Some(hv.to_envoy_header_value(n)),
                append: None,
                append_action: action_enum as i32,
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
        self.update_headers(headers, "APPEND_IF_EXISTS_OR_ADD")
    }

    /// Add multiple headers if absent. See [`update_headers`](RequestContext::update_headers).
    pub fn add_headers(&mut self, headers: &HashMap<String, HeaderValue>) -> Result<(), Error> {
        self.update_headers(headers, "ADD_IF_ABSENT")
    }

    /// Overwrite multiple headers. See [`update_headers`](RequestContext::update_headers).
    pub fn overwrite_headers(
        &mut self,
        headers: &HashMap<String, HeaderValue>,
    ) -> Result<(), Error> {
        self.update_headers(headers, "OVERWRITE_IF_EXISTS_OR_ADD")
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

    /// Queue multiple headers for removal, without duplicates. See
    /// [`remove_header`](RequestContext::remove_header).
    pub fn remove_headers_variadic(&mut self, headers: &[String]) -> Result<(), Error> {
        self.remove_headers(headers)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_phase_if_invalid() {
        assert_eq!(
            default_phase_if_invalid(REQUEST_PHASE_UNDETERMINED),
            REQUEST_PHASE_UNDETERMINED
        );
        assert_eq!(
            default_phase_if_invalid(REQUEST_PHASE_REQUEST_HEADERS),
            REQUEST_PHASE_REQUEST_HEADERS
        );
        assert_eq!(
            default_phase_if_invalid(REQUEST_PHASE_REQUEST_BODY),
            REQUEST_PHASE_REQUEST_BODY
        );
        assert_eq!(
            default_phase_if_invalid(REQUEST_PHASE_REQUEST_TRAILERS),
            REQUEST_PHASE_REQUEST_TRAILERS
        );
        assert_eq!(
            default_phase_if_invalid(REQUEST_PHASE_RESPONSE_HEADERS),
            REQUEST_PHASE_RESPONSE_HEADERS
        );
        assert_eq!(
            default_phase_if_invalid(REQUEST_PHASE_RESPONSE_BODY),
            REQUEST_PHASE_RESPONSE_BODY
        );
        assert_eq!(
            default_phase_if_invalid(REQUEST_PHASE_RESPONSE_TRAILERS),
            REQUEST_PHASE_RESPONSE_TRAILERS
        );
        assert_eq!(default_phase_if_invalid(99), REQUEST_PHASE_UNDETERMINED);
    }

    #[test]
    fn test_request_phase_to_string() {
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_UNDETERMINED),
            "UNDETERMINED"
        );
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_REQUEST_HEADERS),
            "REQUEST_HEADERS"
        );
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_REQUEST_BODY),
            "REQUEST_BODY"
        );
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_REQUEST_TRAILERS),
            "REQUEST_TRAILERS"
        );
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_RESPONSE_HEADERS),
            "RESPONSE_HEADERS"
        );
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_RESPONSE_BODY),
            "RESPONSE_BODY"
        );
        assert_eq!(
            request_phase_to_string(REQUEST_PHASE_RESPONSE_TRAILERS),
            "RESPONSE_TRAILERS"
        );
        assert_eq!(request_phase_to_string(99), "UNKNOWN(99)");
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

        let opt = hv
            .to_envoy_header_value_option("x-foo", "ADD_IF_ABSENT")
            .unwrap();
        assert_eq!(opt.append_action, 1);

        assert!(hv
            .to_envoy_header_value_option("x-foo", "NOT_A_REAL_ACTION")
            .is_err());
        let bad = HeaderValue {
            value: String::new(),
            raw_value: None,
        };
        assert!(bad
            .to_envoy_header_value_option("x-foo", "ADD_IF_ABSENT")
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
        let mut pr = PhaseResponse::default();
        pr.header_mutation = Some(HeaderMutation {
            set_headers: vec![],
            remove_headers: vec![],
        });
        pr.immediate_response = None;
        pr.continue_request = None;
        pr.clear_route_cache = true;
        pr.reset();
        assert!(pr.header_mutation.is_some());
        assert!(pr.continue_request.is_none());
        assert!(pr.immediate_response.is_none());
        assert!(!pr.clear_route_cache);
    }

    #[test]
    fn test_request_context_phase_lifecycle() {
        let mut rc = RequestContext::new();
        assert_eq!(rc.get_phase(), REQUEST_PHASE_UNDETERMINED);
        assert_eq!(rc.get_phase_name(), "UNDETERMINED");
        rc.set_phase(REQUEST_PHASE_RESPONSE_BODY);
        assert_eq!(rc.get_phase(), REQUEST_PHASE_RESPONSE_BODY);
        assert_eq!(rc.get_phase_name(), "RESPONSE_BODY");
        rc.set_phase(42);
        assert_eq!(rc.get_phase(), REQUEST_PHASE_UNDETERMINED);

        rc.set_value(
            "foo",
            Box::new(42u32) as Box<dyn std::any::Any + Send + Sync>,
        )
        .unwrap();
        let v = rc.get_value("foo").unwrap();
        assert_eq!(*v.downcast_ref::<u32>().unwrap(), 42);
        assert!(rc.get_value("nope").is_err());
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
            .get_response(REQUEST_PHASE_REQUEST_HEADERS)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::RequestHeaders(_))
        ));

        let resp = rc
            .get_response(REQUEST_PHASE_RESPONSE_TRAILERS)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::ResponseTrailers(_))
        ));

        assert!(rc.get_response(99).is_err());
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
        let resp = rc
            .get_response(REQUEST_PHASE_REQUEST_BODY)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::ImmediateResponse(_))
        ));
        // trailers phases ignore immediate responses
        let resp = rc
            .get_response(REQUEST_PHASE_RESPONSE_TRAILERS)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resp.response,
            Some(ProcessingResponseResponse::ResponseTrailers(_))
        ));
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
            "OVERWRITE_IF_EXISTS_OR_ADD",
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
        assert!(rc
            .update_header(
                "x-foo",
                &HeaderValue {
                    value: "bar".to_string(),
                    raw_value: Some(b"bar".to_vec())
                },
                "NOT_A_REAL_ACTION"
            )
            .is_err());
    }

    #[test]
    fn test_body_mutations() {
        let mut rc = RequestContext::new();
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
