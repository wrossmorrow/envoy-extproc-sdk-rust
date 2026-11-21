//! Request context, phase bookkeeping, and phase responses.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use http::Extensions;

pub use envoy_types::pb::envoy::config::core::v3::header_value_option::HeaderAppendAction;
use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;
use envoy_types::pb::envoy::config::core::v3::HeaderValueOption;
use envoy_types::pb::envoy::config::core::v3::Metadata;
pub use envoy_types::pb::envoy::extensions::filters::http::ext_proc::v3::processing_mode::{
    BodySendMode, HeaderSendMode,
};
use envoy_types::pb::envoy::extensions::filters::http::ext_proc::v3::ProcessingMode;
use envoy_types::pb::envoy::service::ext_proc::v3::body_mutation::Mutation as BodyMutationMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::processing_response::Response as ProcessingResponseResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::BodyMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::CommonResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::HeaderMutation;
use envoy_types::pb::envoy::service::ext_proc::v3::ImmediateResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::ProtocolConfiguration;
use envoy_types::pb::google::protobuf::value::Kind as ValueKind;
use envoy_types::pb::google::protobuf::{Any, Struct, Value};

use crate::headers::AllHeaders;
use crate::Error;

/// Sends an interim phase response to envoy, e.g. an
/// [`override_message_timeout`](RequestContext::override_message_timeout)
/// interim response. Implemented as a boxed closure by the streaming phase task
/// that owns the context; `None` when interim sends are not possible for the
/// current exchange (e.g. a context driven outside a phase task, or during envoy
/// observability mode).
pub(crate) type InterimSend = Box<dyn Fn(&ProcessingResponse) -> Result<(), Error> + Send + Sync>;

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
        let mut option = HeaderValueOption {
            header: Some(self.to_envoy_header_value(name)),
            append: None,
            append_action: action as i32,
            keep_empty_value: false,
        };
        // Envoy's ext_proc MutationUtils::applyHeaderMutations evaluates the
        // deprecated HeaderValueOption.append (defaulting to false) rather than
        // append_action (envoyproxy/envoy#48886). Setting Append=true for
        // APPEND_IF_EXISTS_OR_ADD preserves append behavior without colliding
        // with a non-zero append_action.
        option.append = append_flag_for_action(action);
        Ok(option)
    }
}

fn append_flag_for_action(
    action: HeaderAppendAction,
) -> Option<envoy_types::pb::google::protobuf::BoolValue> {
    if action == HeaderAppendAction::AppendIfExistsOrAdd {
        Some(envoy_types::pb::google::protobuf::BoolValue { value: true })
    } else {
        None
    }
}

/// The set of lower-cased header names observed in a phase, as sent by
/// envoy, used to enforce `ADD_IF_ABSENT` and `OVERWRITE_IF_EXISTS` semantics
/// while envoy's ext_proc filter ignores `HeaderValueOption.append_action`
/// (envoyproxy/envoy#48886).
pub(crate) fn header_key_set(h: &AllHeaders) -> HashSet<String> {
    h.headers.keys().map(|k| k.to_lowercase()).collect()
}

/// Resolve the header mutation for a phase against the set of header names
/// known to exist in that phase. Envoy ignores
/// `HeaderValueOption.append_action`, so `AddHeader`/`AddHeaders`
/// (`ADD_IF_ABSENT`) and `UpdateHeader`/`UpdateHeaders` with
/// `OVERWRITE_IF_EXISTS` are enforced here by filtering the mutation set.
///
/// Returns the resolved mutation and the post-mutation key set. The key set
/// reflects removals and adds applied by the mutation, and is the state the
/// next phase of the same kind should be evaluated against. `known` is never
/// mutated; the returned set is the post-mutation state.
pub(crate) fn resolve_header_mutation(
    hm: &Option<HeaderMutation>,
    known: &Option<HashSet<String>>,
) -> (Option<HeaderMutation>, Option<HashSet<String>>) {
    let Some(hm) = hm else {
        return (None, None);
    };
    let Some(known) = known else {
        return (Some(hm.clone()), None);
    };

    let mut state: HashSet<String> = known.clone();
    for rm in &hm.remove_headers {
        let k = rm.to_lowercase();
        // Envoy ignores attempts to remove pseudo-headers or the host header.
        if !k.starts_with(':') && k != "host" {
            state.remove(&k);
        }
    }

    let mut filtered: Vec<HeaderValueOption> = Vec::with_capacity(hm.set_headers.len());
    for ho in &hm.set_headers {
        let k = ho
            .header
            .as_ref()
            .map(|h| h.key.to_lowercase())
            .unwrap_or_default();
        let exists = state.contains(&k);
        if ho.append_action == HeaderAppendAction::AddIfAbsent as i32 {
            if exists {
                continue;
            }
            state.insert(k);
            filtered.push(ho.clone());
        } else if ho.append_action == HeaderAppendAction::OverwriteIfExists as i32 {
            if !exists {
                continue;
            }
            filtered.push(ho.clone());
        } else {
            state.insert(k);
            filtered.push(ho.clone());
        }
    }

    (
        Some(HeaderMutation {
            set_headers: filtered,
            remove_headers: hm.remove_headers.clone(),
        }),
        Some(state),
    )
}

/// Resolve the phase's header mutation against the phase header key set, and
/// return the post-mutation key set for the following phase(s) of the same
/// kind.
impl RequestContext {
    pub(crate) fn resolved_header_mutation(
        &self,
    ) -> (Option<HeaderMutation>, Option<HashSet<String>>) {
        resolve_header_mutation(&self.response.header_mutation, &self.phase_header_keys)
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

/// A dynamic processing mode override, as requested by an external processor
/// via [`override_mode`](RequestContext::override_mode), and surfaced to envoy
/// as [`ProcessingResponse::mode_override`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingResponse::mode_override).
///
/// Envoy only honors overrides while the `ext_proc` filter is configured with
/// `allow_mode_override: true`, and only during the phases where an override
/// may be expressed (see [`override_mode`](RequestContext::override_mode)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct ModeOverride {
    /// How envoy should send response headers in subsequent phases. Use
    /// [`HeaderSendMode::Default`] to keep the filter's configured mode.
    pub response_header_mode: HeaderSendMode,
    /// How envoy should send request bodies in subsequent phases. Use
    /// [`BodySendMode::None`] to not send the body at all.
    pub request_body_mode: BodySendMode,
    /// How envoy should send response bodies in subsequent phases. Use
    /// [`BodySendMode::None`] to not send the body at all.
    pub response_body_mode: BodySendMode,
    /// How envoy should send request trailers in subsequent phases. Use
    /// [`HeaderSendMode::Default`] to keep the filter's configured mode, or
    /// [`HeaderSendMode::Skip`] to not send them.
    pub request_trailer_mode: HeaderSendMode,
    /// How envoy should send response trailers in subsequent phases. Use
    /// [`HeaderSendMode::Default`] to keep the filter's configured mode, or
    /// [`HeaderSendMode::Skip`] to not send them.
    pub response_trailer_mode: HeaderSendMode,
}

impl ModeOverride {
    /// Convert the override into envoy's [`ProcessingMode`], validating that
    /// every mode value is one envoy accepts. Envoy has no "keep default" body
    /// mode (`BodySendMode::None` means "do not send"), so callers overriding
    /// during the request headers phase should set both body modes explicitly.
    pub fn to_envoy_processing_mode(&self) -> Result<ProcessingMode, Error> {
        let response_header_mode = HeaderSendMode::try_from(self.response_header_mode as i32)
            .map_err(|_| "invalid mode override: unknown response header mode".to_string())?;
        let request_trailer_mode = HeaderSendMode::try_from(self.request_trailer_mode as i32)
            .map_err(|_| "invalid mode override: unknown request trailer mode".to_string())?;
        let response_trailer_mode = HeaderSendMode::try_from(self.response_trailer_mode as i32)
            .map_err(|_| "invalid mode override: unknown response trailer mode".to_string())?;
        let request_body_mode = BodySendMode::try_from(self.request_body_mode as i32)
            .map_err(|_| "invalid mode override: unknown request body mode".to_string())?;
        let response_body_mode = BodySendMode::try_from(self.response_body_mode as i32)
            .map_err(|_| "invalid mode override: unknown response body mode".to_string())?;
        Ok(ProcessingMode {
            request_header_mode: i32::from(HeaderSendMode::Default),
            response_header_mode: i32::from(response_header_mode),
            request_body_mode: i32::from(request_body_mode),
            response_body_mode: i32::from(response_body_mode),
            request_trailer_mode: i32::from(request_trailer_mode),
            response_trailer_mode: i32::from(response_trailer_mode),
        })
    }
}

impl RequestContext {
    /// Returns a [`ModeOverride`] pre-populated with envoy's configured body
    /// modes from the [`ProtocolConfig`](RequestContext::protocol_config) (when
    /// present) and [`HeaderSendMode::Default`] for header and trailer modes.
    ///
    /// Because envoy's body send mode has no "keep default" enum (`BodySendMode::None`
    /// means "do not send"), callers overriding a single body mode during the
    /// request headers phase can start from `DefaultModeOverride()` to preserve
    /// the other direction's body mode.
    pub fn default_mode_override(&self) -> ModeOverride {
        let Some(pc) = &self.protocol_config else {
            return ModeOverride::default();
        };
        ModeOverride {
            response_header_mode: HeaderSendMode::Default,
            request_body_mode: pc.request_body_mode,
            response_body_mode: pc.response_body_mode,
            request_trailer_mode: HeaderSendMode::Default,
            response_trailer_mode: HeaderSendMode::Default,
        }
    }
}

/// The envoy ext_proc filter's protocol configuration for the current stream,
/// captured from the first
/// [`ProcessingRequest`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest)
/// of the stream and retained across the phases of that stream.
///
/// Envoy sends the protocol configuration only on the first message of a
/// processing stream; the SDK surfaces it on the [`RequestContext`] so
/// processors can adapt to the filter's configured body modes instead of
/// assuming defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ProtocolConfig {
    /// How envoy should send the request body in subsequent phases, from the
    /// filter's `processing_mode.request_body_mode`.
    pub request_body_mode: BodySendMode,
    /// How envoy should send the response body in subsequent phases, from the
    /// filter's `processing_mode.response_body_mode`.
    pub response_body_mode: BodySendMode,
    /// Whether envoy should send the request body without waiting for the
    /// response headers phase to complete, from the filter's
    /// `send_body_without_waiting_for_header_response`.
    pub send_body_without_waiting_for_header_response: bool,
}

impl ProtocolConfig {
    /// Convert an envoy
    /// [`ProtocolConfiguration`](envoy_types::pb::envoy::service::ext_proc::v3::ProtocolConfiguration)
    /// into the typed SDK [`ProtocolConfig`], rejecting body modes the envoy
    /// protocol does not define.
    pub fn from_proto(pc: &ProtocolConfiguration) -> Result<Self, Error> {
        Ok(Self {
            request_body_mode: BodySendMode::try_from(pc.request_body_mode)
                .map_err(|_| "invalid protocol_config: unknown request body mode".to_string())?,
            response_body_mode: BodySendMode::try_from(pc.response_body_mode)
                .map_err(|_| "invalid protocol_config: unknown response body mode".to_string())?,
            send_body_without_waiting_for_header_response: pc
                .send_body_without_waiting_for_header_response,
        })
    }
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
    /// Outbound dynamic metadata pending for the current phase, attached to
    /// the phase response envoy sends (including immediate responses), and
    /// cleared by [`reset_phase`](RequestContext::reset_phase).
    pub dynamic_metadata: Option<Struct>,
    /// The dynamic mode override pending for the current phase, attached to
    /// the header responses envoy accepts alongside `mode_override`. Cleared
    /// by [`reset_phase`](RequestContext::reset_phase).
    pub mode_override: Option<ProcessingMode>,
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
        self.dynamic_metadata = None;
        self.mode_override = None;
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

    /// Whether the current exchange is running in envoy `observability_mode`,
    /// reflecting `ProcessingRequest.observability_mode`. When `true`,
    /// processor callbacks run for observation and the SDK suppresses
    /// `ProcessingResponse` sends (keeping envoy's `spurious_msgs_received ==
    /// 0`).
    pub observability_mode: bool,

    /// The envoy ext_proc filter's protocol configuration for the current
    /// stream, captured from the first
    /// [`ProcessingRequest`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest)
    /// of the stream and retained across the phases of that stream. `None` when
    /// envoy did not send a `protocol_config`.
    pub protocol_config: Option<ProtocolConfig>,

    /// Inbound untyped dynamic metadata forwarded by envoy for the current
    /// stream, merged across phases, keyed first by namespace and then by
    /// metadata key. Populated from the
    /// [`ProcessingRequest::metadata_context`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest::metadata_context)
    /// envoy sends when the ext_proc filter is configured with
    /// `metadata_options.forwarding_namespaces`.
    pub metadata: HashMap<String, HashMap<String, Value>>,

    /// Inbound typed dynamic metadata forwarded by envoy for the current
    /// stream, keyed by namespace, from the
    /// [`ProcessingRequest::metadata_context`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest::metadata_context)
    /// envoy sends when the ext_proc filter is configured with typed forwarding.
    pub typed_metadata: HashMap<String, Any>,

    /// Evaluated envoy/CEL attributes for the current stream, merged across
    /// phases, flattened by attribute name (e.g. `request.protocol`,
    /// `response.code`). Populated from the
    /// [`ProcessingRequest::attributes`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest::attributes)
    /// envoy sends for the `request_attributes` / `response_attributes`
    /// configured on the ext_proc filter.
    pub attributes: HashMap<String, Value>,

    /// Arbitrary custom values shared across phases of a request, stored in a
    /// type-keyed map ([`http::Extensions`]) so values are strongly typed
    /// rather than string-keyed trait objects.
    pub data: Extensions,

    /// Current processing phase.
    pub phase: RequestPhase,
    /// Response being assembled for the current phase.
    pub response: PhaseResponse,

    /// Header names observed for the current phase, tracked so that
    /// `ADD_IF_ABSENT` and `OVERWRITE_IF_EXISTS` semantics can be enforced
    /// while envoy's ext_proc filter ignores `HeaderValueOption.append_action`
    /// (envoyproxy/envoy#48886). Populated when the phase is initialized and
    /// updated after each phase's mutations are resolved.
    pub(crate) req_header_keys: HashSet<String>,

    /// Header names observed for the current response phase, tracked for the
    /// same reasons as [`RequestContext::req_header_keys`].
    pub(crate) resp_header_keys: HashSet<String>,

    /// The header names known to exist at the current phase, if the phase has
    /// been initialized. `None` until the phase is initialized (e.g. before the
    /// request headers phase runs).
    pub(crate) phase_header_keys: Option<HashSet<String>>,

    /// Sends interim phase responses to envoy, e.g. for an
    /// [`override_message_timeout`](RequestContext::override_message_timeout)
    /// extension, when one has been wired for the current stream. Installed by
    /// the streaming phase task that owns the context; `None` when interim
    /// sends are not possible for the current exchange (e.g. a context driven
    /// outside a phase task, or during envoy observability mode).
    pub(crate) send_timeout_override: Option<InterimSend>,

    /// Error from the most recent interim phase response send for the current
    /// phase, if any, e.g. an `override_message_timeout` interim response that
    /// failed to send. Consumed by
    /// [`process_phase`](crate::server::GenericExtProcServer::process_phase)
    /// once the phase completes, ahead of phase error handling, so that an
    /// interim send failure is handled as a send error rather than a phase
    /// error.
    pub(crate) interim_send_error: Option<Error>,

    /// Whether an `override_message_timeout` was already sent for the current
    /// phase. Reset by [`reset_phase`](RequestContext::reset_phase).
    pub(crate) timeout_extended: bool,
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
            observability_mode: false,
            protocol_config: None,
            metadata: HashMap::new(),
            typed_metadata: HashMap::new(),
            attributes: HashMap::new(),
            data: Extensions::new(),
            phase: RequestPhase::Undetermined,
            response: PhaseResponse::default(),
            req_header_keys: HashSet::new(),
            resp_header_keys: HashSet::new(),
            phase_header_keys: None,
            send_timeout_override: None,
            interim_send_error: None,
            timeout_extended: false,
        }
    }

    /// Reset the phase and response state. Called at the start of each
    /// processing phase.
    pub fn reset_phase(&mut self) -> Result<(), Error> {
        self.phase = RequestPhase::Undetermined;
        self.end_of_stream = false;
        self.observability_mode = false;
        self.phase_header_keys = None;
        self.response.reset();
        self.interim_send_error = None;
        self.timeout_extended = false;
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

    /// Take and clear the pending interim phase response send error, if any.
    ///
    /// Called by
    /// [`process_phase`](crate::server::GenericExtProcServer::process_phase)
    /// once the phase completes, ahead of phase error handling, so that an
    /// interim send failure (e.g. an `override_message_timeout` interim response
    /// that failed to send) is handled as a send error rather than a phase
    /// error.
    pub(crate) fn take_interim_send_error(&mut self) -> Option<Error> {
        self.interim_send_error.take()
    }

    /// Whether a pending interim phase response send error exists, without
    /// consuming it. Used by phase dispatch to tell an interim-send failure
    /// apart from a phase error before deciding whether to count a phase error.
    pub(crate) fn has_interim_send_error(&self) -> bool {
        self.interim_send_error.is_some()
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

    /// Override how envoy sends headers, bodies, and trailers in subsequent
    /// phases of the current request/response, via envoy's
    /// `ProcessingResponse.mode_override`.
    ///
    /// Only the request headers and response headers phases accept an
    /// override (the phases in which envoy lets a processor express one);
    /// errors are returned for every other phase without mutating state. The
    /// override is attached to the next phase response envoy sends for this
    /// exchange, and cleared by [`reset_phase`](RequestContext::reset_phase).
    ///
    /// Requires the envoy `ext_proc` filter to be configured with
    /// `allow_mode_override: true`.
    ///
    /// # Errors
    ///
    /// Returns an error when called during a phase that does not accept
    /// overrides, or when a mode value is not valid for envoy.
    pub fn override_mode(&mut self, mode: ModeOverride) -> Result<(), Error> {
        let allows_override = matches!(
            self.phase,
            RequestPhase::Undetermined
                | RequestPhase::RequestHeaders
                | RequestPhase::ResponseHeaders
        );
        if !allows_override {
            return Err(format!(
                "mode_override is only supported during header phases, not {}",
                self.get_phase_name()
            )
            .into());
        }
        let envoy_mode = mode.to_envoy_processing_mode()?;
        self.response.mode_override = Some(envoy_mode);
        Ok(())
    }

    /// Clear any pending mode override for the current phase, without sending
    /// a response. See [`override_mode`](RequestContext::override_mode).
    pub fn clear_mode_override(&mut self) {
        self.response.mode_override = None;
    }

    /// Skip all remaining phases of the current request/response, via envoy's
    /// `ProcessingResponse.mode_override`, and continue the exchange without
    /// further processing. See [`override_mode`](RequestContext::override_mode).
    ///
    /// # Errors
    ///
    /// Returns an error when the current phase does not accept an override, or
    /// when the resulting mode values are not valid for envoy.
    pub fn skip_remaining_phases(&mut self) -> Result<(), Error> {
        self.override_mode(ModeOverride {
            response_header_mode: HeaderSendMode::Skip,
            request_body_mode: BodySendMode::None,
            response_body_mode: BodySendMode::None,
            request_trailer_mode: HeaderSendMode::Skip,
            response_trailer_mode: HeaderSendMode::Skip,
        })
    }

    /// Request an `override_message_timeout` for the current phase, asking the
    /// data plane to replace the current phase's message timer with `timeout`.
    ///
    /// Envoy honors the override by immediately sending an interim
    /// [`ProcessingResponse`] with `override_message_timeout` set mid-phase,
    /// before the processor returns its phase response, stopping the original
    /// message timer and starting a new one with the requested duration while
    /// keeping the data plane ext_proc filter state machine intact. The override
    /// requires the envoy `ext_proc` filter to be configured with
    /// `max_message_timeout >= timeout >= 1ms`.
    ///
    /// The override may be expressed at most once per phase, and is a no-op
    /// (Ok) when the exchange runs in envoy `observability_mode`; the duplicate
    /// call is still rejected. It is cleared by
    /// [`reset_phase`](RequestContext::reset_phase), so a later phase of the
    /// same exchange may request its own override.
    ///
    /// # Errors
    ///
    /// Returns an error when the timeout is below 1ms, when there is no active
    /// phase, when an override was already sent for the phase, when no interim
    /// send is wired for the current exchange, or when the interim response
    /// could not be sent.
    pub fn override_message_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        if timeout < Duration::from_millis(1) {
            return Err(format!(
                "override_message_timeout must be >= 1ms, got {}",
                timeout.as_millis()
            )
            .into());
        }
        if self.phase == RequestPhase::Undetermined {
            return Err("override_message_timeout requires an active request phase".into());
        }
        if self.timeout_extended {
            return Err("override_message_timeout already sent in current phase".into());
        }
        if self.observability_mode {
            self.timeout_extended = true;
            return Ok(());
        }
        let Some(send) = self.send_timeout_override.as_ref() else {
            return Err("override_message_timeout requires an active gRPC stream".into());
        };
        let resp = self.new_timeout_override_response(self.phase, timeout)?;
        if let Err(send_err) = send(&resp) {
            // Record the interim send error for the phase task to handle, then
            // propagate a failure so phase processing stops.
            self.interim_send_error = Some(send_err);
            return Err("override_message_timeout interim response send failed".into());
        }
        self.timeout_extended = true;
        Ok(())
    }

    /// Build the interim [`ProcessingResponse`] that asks envoy to extend the
    /// current phase's message timeout to `timeout`, as an empty "continue"
    /// response carrying the override.
    ///
    /// The data plane ignores every other field of the interim response; only
    /// `override_message_timeout` is honored.
    fn new_timeout_override_response(
        &self,
        phase: RequestPhase,
        timeout: Duration,
    ) -> Result<ProcessingResponse, Error> {
        if timeout < Duration::from_millis(1) {
            return Err(format!(
                "override_message_timeout must be >= 1ms, got {}",
                timeout.as_millis()
            )
            .into());
        }
        let mut resp = self.empty_continue_response(phase)?.ok_or_else(|| {
            "override_message_timeout requires an active request phase".to_string()
        })?;
        resp.override_message_timeout = Some(envoy_types::pb::google::protobuf::Duration {
            seconds: timeout.as_secs() as i64,
            nanos: timeout.subsec_nanos() as i32,
        });
        Ok(resp)
    }

    /// Merge an inbound envoy [`Metadata`] context into the current context,
    /// accumulating across the phases of the stream. Unmerged namespaces and
    /// keys persist across phases; values for the same key overwrite earlier
    /// ones, mirroring envoy's later-values-override semantics.
    ///
    /// Called by the SDK when a
    /// [`ProcessingRequest`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest)
    /// arrives with a `metadata_context`, i.e. when the ext_proc filter is
    /// configured with `metadata_options.forwarding_namespaces`.
    pub fn merge_metadata_context(&mut self, md: &Metadata) {
        if !md.filter_metadata.is_empty() {
            for (namespace, struct_) in &md.filter_metadata {
                if struct_.fields.is_empty() {
                    continue;
                }
                let namespace_map = self.metadata.entry(namespace.clone()).or_default();
                for (key, value) in &struct_.fields {
                    namespace_map.insert(key.clone(), value.clone());
                }
            }
        }
        if !md.typed_filter_metadata.is_empty() {
            for (namespace, any) in &md.typed_filter_metadata {
                self.typed_metadata.insert(namespace.clone(), any.clone());
            }
        }
    }

    /// Merge an inbound envoy attributes map into the current context,
    /// accumulating across the phases of the stream. Attributes are flattened
    /// by attribute name, and later phases overwrite earlier values for the
    /// same attribute name.
    ///
    /// Called by the SDK when a
    /// [`ProcessingRequest`](envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest)
    /// arrives with `attributes`, i.e. when the ext_proc filter is configured
    /// with `request_attributes` / `response_attributes`.
    pub fn merge_attributes(&mut self, attrs: &HashMap<String, Struct>) {
        for struct_ in attrs.values() {
            if struct_.fields.is_empty() {
                continue;
            }
            for (key, value) in &struct_.fields {
                self.attributes.insert(key.clone(), value.clone());
            }
        }
    }

    /// Set a single dynamic metadata key/value in the current phase's pending
    /// outbound `ProcessingResponse.dynamic_metadata`, attaching it to the next
    /// phase response envoy sends for this exchange. Envoy only accepts
    /// namespaces listed in the ext_proc filter's
    /// `metadata_options.receiving_namespaces.untyped`.
    ///
    /// # Errors
    ///
    /// Returns an error when the namespace or key is empty.
    pub fn set_dynamic_metadata(
        &mut self,
        namespace: &str,
        key: &str,
        value: Value,
    ) -> Result<(), Error> {
        if namespace.is_empty() {
            return Err("dynamic metadata namespace cannot be empty".into());
        }
        if key.is_empty() {
            return Err("dynamic metadata key cannot be empty".into());
        }
        let namespace_map = self.ensure_dynamic_metadata_namespace(namespace)?;
        namespace_map.insert(key.to_string(), value);
        Ok(())
    }

    /// Set multiple dynamic metadata values under `namespace` in the current
    /// phase's pending outbound `ProcessingResponse.dynamic_metadata`.
    ///
    /// Validation is all-or-nothing: when the namespace or any key is empty, no
    /// changes are applied. Values are merged into the namespace map.
    ///
    /// # Errors
    ///
    /// Returns an error when the namespace or any key is empty.
    pub fn set_dynamic_metadata_namespace(
        &mut self,
        namespace: &str,
        values: &HashMap<String, Value>,
    ) -> Result<(), Error> {
        if namespace.is_empty() {
            return Err("dynamic metadata namespace cannot be empty".into());
        }
        let mut converted = HashMap::with_capacity(values.len());
        for (k, v) in values {
            if k.is_empty() {
                return Err("dynamic metadata key cannot be empty".into());
            }
            converted.insert(k.clone(), v.clone());
        }
        let namespace_map = self.ensure_dynamic_metadata_namespace(namespace)?;
        for (k, v) in converted {
            namespace_map.insert(k, v);
        }
        Ok(())
    }

    /// Clear the pending outbound dynamic metadata for the current phase, so
    /// the next phase response envoy sends carries no dynamic metadata. See
    /// [`clear_dynamic_metadata`](RequestContext::clear_dynamic_metadata).
    pub fn clear_dynamic_metadata(&mut self) {
        self.response.dynamic_metadata = None;
    }

    /// Ensure the outbound dynamic metadata namespace entry exists for
    /// `namespace`, creating an empty namespace struct when the current phase
    /// has no pending dynamic metadata yet, and return it.
    fn ensure_dynamic_metadata_namespace(
        &mut self,
        namespace: &str,
    ) -> Result<&mut HashMap<String, Value>, Error> {
        if namespace.is_empty() {
            return Err("dynamic metadata namespace cannot be empty".into());
        }
        if self.response.dynamic_metadata.is_none() {
            self.response.dynamic_metadata = Some(Struct {
                fields: HashMap::new(),
            });
        }
        let envoy_struct = self
            .response
            .dynamic_metadata
            .as_mut()
            .expect("dynamic metadata just ensured");
        let is_struct = envoy_struct
            .fields
            .get(namespace)
            .is_some_and(|v| matches!(v.kind.as_ref(), Some(ValueKind::StructValue(_))));
        if !is_struct {
            envoy_struct.fields.insert(
                namespace.to_string(),
                Value {
                    kind: Some(ValueKind::StructValue(Struct {
                        fields: HashMap::new(),
                    })),
                },
            );
        }
        let value = envoy_struct
            .fields
            .get_mut(namespace)
            .expect("dynamic metadata namespace just ensured");
        if let Some(ValueKind::StructValue(namespace_struct)) = value.kind.as_mut() {
            return Ok(&mut namespace_struct.fields);
        }
        Err("dynamic metadata namespace entry is not a struct".into())
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
                    dynamic_metadata: self.response.dynamic_metadata.clone(),
                    ..Default::default()
                }));
            }
        }

        let (hm, _) = self.resolved_header_mutation();
        self.response.continue_request = Some(CommonResponse {
            status: 0, // CommonResponse::ResponseStatus::Continue
            header_mutation: hm.clone(),
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
                    header_mutation: hm.clone(),
                },
            )),
            RequestPhase::ResponseTrailers => Some(ProcessingResponseResponse::ResponseTrailers(
                envoy_types::pb::envoy::service::ext_proc::v3::TrailersResponse {
                    header_mutation: hm.clone(),
                },
            )),
            RequestPhase::Undetermined => {
                return Err(format!("unknown request phase {}", phase.as_u32()).into());
            }
        };

        Ok(Some(ProcessingResponse {
            response,
            dynamic_metadata: self.response.dynamic_metadata.clone(),
            mode_override: if matches!(
                phase,
                RequestPhase::RequestHeaders | RequestPhase::ResponseHeaders
            ) {
                self.response.mode_override
            } else {
                None
            },
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
        let (hm, _) = self.resolved_header_mutation();
        self.response.continue_request = Some(CommonResponse {
            status: 0, // CommonResponse::ResponseStatus::Continue
            header_mutation: hm.clone(),
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
        let option = HeaderValueOption {
            header: Some(hv.to_envoy_header_value(name)),
            append: append_flag_for_action(action),
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
        let append = append_flag_for_action(action);
        let mut options = Vec::with_capacity(headers.len());
        for (n, hv) in headers {
            options.push(HeaderValueOption {
                header: Some(hv.to_envoy_header_value(n)),
                append,
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
    use std::sync::{Arc, Mutex};

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
            dynamic_metadata: None,
            continue_request: None,
            mode_override: None,
            clear_route_cache: true,
        };
        pr.reset();
        assert!(pr.header_mutation.is_some());
        assert!(pr.continue_request.is_none());
        assert!(pr.immediate_response.is_none());
        assert!(pr.mode_override.is_none());
        assert!(!pr.clear_route_cache);
    }

    // -----------------------------------------------------------------------
    // Dynamic processing mode overrides (envoy `ProcessingResponse.mode_override`)
    // -----------------------------------------------------------------------

    fn envoy_processing_mode(
        mode: &ModeOverride,
    ) -> envoy_types::pb::envoy::extensions::filters::http::ext_proc::v3::ProcessingMode {
        mode.to_envoy_processing_mode()
            .expect("expected valid mode override")
    }

    #[test]
    fn test_mode_override_to_envoy_processing_mode() {
        // A dynamic override expresses how envoy should send subsequent phases;
        // the request header mode is always DEFAULT (envoy ignores overrides
        // for the phase that produced them).
        let mode = ModeOverride {
            response_header_mode: HeaderSendMode::Default,
            request_body_mode: BodySendMode::Buffered,
            response_body_mode: BodySendMode::Streamed,
            request_trailer_mode: HeaderSendMode::Send,
            response_trailer_mode: HeaderSendMode::Skip,
        };
        let pm = envoy_processing_mode(&mode);
        assert_eq!(pm.request_header_mode, i32::from(HeaderSendMode::Default));
        assert_eq!(pm.response_header_mode, i32::from(HeaderSendMode::Default));
        assert_eq!(pm.request_body_mode, i32::from(BodySendMode::Buffered));
        assert_eq!(pm.response_body_mode, i32::from(BodySendMode::Streamed));
        assert_eq!(pm.request_trailer_mode, i32::from(HeaderSendMode::Send));
        assert_eq!(pm.response_trailer_mode, i32::from(HeaderSendMode::Skip));

        // Skipping every remaining phase (the SkipRemainingPhases shortcut)
        // maps onto the envoy wire values.
        let skip = ModeOverride {
            response_header_mode: HeaderSendMode::Skip,
            request_body_mode: BodySendMode::None,
            response_body_mode: BodySendMode::None,
            request_trailer_mode: HeaderSendMode::Skip,
            response_trailer_mode: HeaderSendMode::Skip,
        };
        let pm = envoy_processing_mode(&skip);
        assert_eq!(pm.request_header_mode, i32::from(HeaderSendMode::Default));
        assert_eq!(pm.response_header_mode, i32::from(HeaderSendMode::Skip));
        assert_eq!(pm.request_body_mode, i32::from(BodySendMode::None));
        assert_eq!(pm.response_body_mode, i32::from(BodySendMode::None));
        assert_eq!(pm.request_trailer_mode, i32::from(HeaderSendMode::Skip));
        assert_eq!(pm.response_trailer_mode, i32::from(HeaderSendMode::Skip));
    }

    #[test]
    fn test_override_mode_allowed_phases_and_rejected_phases() {
        // Overrides may be expressed during the request headers and response
        // headers phases (and while undetermined), and must be rejected during
        // body and trailer phases without mutating state.
        for phase in [
            RequestPhase::Undetermined,
            RequestPhase::RequestHeaders,
            RequestPhase::ResponseHeaders,
        ] {
            let mut rc = RequestContext::new();
            rc.reset_phase().unwrap();
            rc.set_phase(phase);
            assert!(rc.override_mode(ModeOverride::default()).is_ok());
            assert!(rc.skip_remaining_phases().is_ok());
        }

        for phase in [
            RequestPhase::RequestBody,
            RequestPhase::ResponseBody,
            RequestPhase::RequestTrailers,
            RequestPhase::ResponseTrailers,
        ] {
            let mut rc = RequestContext::new();
            rc.reset_phase().unwrap();
            rc.set_phase(phase);
            assert!(rc
                .override_mode(ModeOverride::default())
                .err()
                .is_some_and(|e| e
                    .to_string()
                    .contains("mode_override is only supported during header phases")));
            assert!(rc.skip_remaining_phases().is_err());
            assert!(rc.response.mode_override.is_none());
        }
    }

    #[test]
    fn test_override_mode_stores_accepted_override_and_replaces_it() {
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.set_phase(RequestPhase::RequestHeaders);
        assert!(rc
            .override_mode(ModeOverride {
                response_header_mode: HeaderSendMode::Default,
                request_body_mode: BodySendMode::Buffered,
                response_body_mode: BodySendMode::Streamed,
                request_trailer_mode: HeaderSendMode::Send,
                response_trailer_mode: HeaderSendMode::Skip,
            })
            .is_ok());
        assert!(rc
            .response
            .mode_override
            .as_ref()
            .is_some_and(|m| m.request_body_mode == i32::from(BodySendMode::Buffered)));

        // A later accepted override replaces the pending one for the phase.
        assert!(rc
            .override_mode(ModeOverride {
                response_header_mode: HeaderSendMode::Skip,
                request_body_mode: BodySendMode::None,
                response_body_mode: BodySendMode::Buffered,
                request_trailer_mode: HeaderSendMode::Skip,
                response_trailer_mode: HeaderSendMode::Skip,
            })
            .is_ok());
        assert!(rc
            .response
            .mode_override
            .as_ref()
            .is_some_and(|m| m.response_header_mode == i32::from(HeaderSendMode::Skip)));
        assert!(rc
            .response
            .mode_override
            .as_ref()
            .is_some_and(|m| m.request_body_mode == i32::from(BodySendMode::None)));
    }

    #[test]
    fn test_get_response_propagates_mode_override_and_clears_it() {
        for phase in [RequestPhase::RequestHeaders, RequestPhase::ResponseHeaders] {
            let mut rc = RequestContext::new();
            rc.reset_phase().unwrap();
            rc.set_phase(phase);
            assert!(rc
                .override_mode(ModeOverride {
                    response_header_mode: HeaderSendMode::Skip,
                    request_body_mode: BodySendMode::Buffered,
                    response_body_mode: BodySendMode::Streamed,
                    request_trailer_mode: HeaderSendMode::Send,
                    response_trailer_mode: HeaderSendMode::Skip,
                })
                .is_ok());

            let resp = rc.get_response(phase).unwrap().unwrap();
            let mo = resp.mode_override.as_ref().expect("expected mode override");
            assert_eq!(mo.response_header_mode, i32::from(HeaderSendMode::Skip));
            assert_eq!(mo.request_body_mode, i32::from(BodySendMode::Buffered));
            assert_eq!(mo.response_body_mode, i32::from(BodySendMode::Streamed));
            assert_eq!(mo.request_trailer_mode, i32::from(HeaderSendMode::Send));
            assert_eq!(mo.response_trailer_mode, i32::from(HeaderSendMode::Skip));

            rc.clear_mode_override();
            let resp = rc.get_response(phase).unwrap().unwrap();
            assert!(resp.mode_override.is_none());

            assert!(rc.skip_remaining_phases().is_ok());
            assert!(rc.reset_phase().is_ok());
            let resp = rc.get_response(phase).unwrap().unwrap();
            assert!(resp.mode_override.is_none());
        }
    }

    #[test]
    fn test_get_response_omits_mode_override_for_immediate_response() {
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.set_phase(RequestPhase::RequestHeaders);
        assert!(rc
            .override_mode(ModeOverride {
                response_header_mode: HeaderSendMode::Default,
                request_body_mode: BodySendMode::Buffered,
                response_body_mode: BodySendMode::None,
                request_trailer_mode: HeaderSendMode::Default,
                response_trailer_mode: HeaderSendMode::Default,
            })
            .is_ok());
        rc.cancel_request(429, &HashMap::new(), &[]).unwrap();
        let resp = rc
            .get_response(RequestPhase::RequestHeaders)
            .unwrap()
            .unwrap();
        assert!(resp.mode_override.is_none());
        assert!(resp.response.is_some());
    }

    // -----------------------------------------------------------------------
    // Protocol configuration (envoy `ProcessingRequest.protocol_config`)
    // -----------------------------------------------------------------------

    fn envoy_protocol_configuration(
        request_body_mode: i32,
        response_body_mode: i32,
        send_body_without_waiting_for_header_response: bool,
    ) -> ProtocolConfiguration {
        ProtocolConfiguration {
            request_body_mode,
            response_body_mode,
            send_body_without_waiting_for_header_response,
        }
    }

    #[test]
    fn test_protocol_config_from_proto() {
        let pc = ProtocolConfig::from_proto(&envoy_protocol_configuration(
            BodySendMode::Buffered as i32,
            BodySendMode::Streamed as i32,
            true,
        ))
        .unwrap();
        assert_eq!(pc.request_body_mode, BodySendMode::Buffered);
        assert_eq!(pc.response_body_mode, BodySendMode::Streamed);
        assert!(pc.send_body_without_waiting_for_header_response);

        // Unknown body modes are rejected.
        assert!(ProtocolConfig::from_proto(&envoy_protocol_configuration(-1, -1, false)).is_err());
    }

    #[test]
    fn test_default_mode_override_handles_nil_and_populated() {
        // Without a protocol config, the default override is the zero override.
        let rc = RequestContext::new();
        assert_eq!(rc.default_mode_override(), ModeOverride::default());

        let mut rc = RequestContext::new();
        rc.protocol_config = Some(ProtocolConfig {
            request_body_mode: BodySendMode::None,
            response_body_mode: BodySendMode::Buffered,
            send_body_without_waiting_for_header_response: true,
        });
        let mode = rc.default_mode_override();
        assert_eq!(mode.response_header_mode, HeaderSendMode::Default);
        assert_eq!(mode.request_body_mode, BodySendMode::None);
        assert_eq!(mode.response_body_mode, BodySendMode::Buffered);
        assert_eq!(mode.request_trailer_mode, HeaderSendMode::Default);
        assert_eq!(mode.response_trailer_mode, HeaderSendMode::Default);

        // The default override preserves the envoy-configured response body
        // mode until the processor overrides it.
        let envoy_mode = mode
            .to_envoy_processing_mode()
            .expect("expected valid default mode override");
        assert_eq!(
            envoy_mode.response_body_mode,
            i32::from(BodySendMode::Buffered)
        );
    }

    #[test]
    fn test_reset_and_init_preserve_protocol_config() {
        let mut rc = RequestContext::new();
        let expected = ProtocolConfig {
            request_body_mode: BodySendMode::Buffered,
            response_body_mode: BodySendMode::Streamed,
            send_body_without_waiting_for_header_response: true,
        };
        rc.protocol_config = Some(expected);

        rc.reset_phase().unwrap();
        assert_eq!(rc.protocol_config, Some(expected));

        rc.reset_phase().unwrap();
        assert_eq!(rc.protocol_config, Some(expected));
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
    #[allow(deprecated)]
    fn test_to_envoy_header_value_option_append_flag() {
        // Envoy's ext_proc filter evaluates the deprecated HeaderValueOption
        // `append` (defaulting to false) rather than `append_action`
        // (envoyproxy/envoy#48886), so the SDK must set `append = true` for
        // APPEND_IF_EXISTS_OR_ADD to preserve append behavior.
        let hv = HeaderValue {
            value: String::new(),
            raw_value: Some(b"v".to_vec()),
        };

        let ho = hv
            .to_envoy_header_value_option("x-foo", HeaderAppendAction::AppendIfExistsOrAdd)
            .unwrap();
        assert_eq!(
            ho.append_action,
            HeaderAppendAction::AppendIfExistsOrAdd as i32
        );
        assert!(ho.append.as_ref().map(|b| b.value).unwrap_or(false));

        let ho = hv
            .to_envoy_header_value_option("x-foo", HeaderAppendAction::AddIfAbsent)
            .unwrap();
        assert_eq!(ho.append_action, HeaderAppendAction::AddIfAbsent as i32);
        assert!(ho.append.is_none());

        let ho = hv
            .to_envoy_header_value_option("x-foo", HeaderAppendAction::OverwriteIfExistsOrAdd)
            .unwrap();
        assert_eq!(
            ho.append_action,
            HeaderAppendAction::OverwriteIfExistsOrAdd as i32
        );
        assert!(ho.append.is_none());

        let ho = hv
            .to_envoy_header_value_option("x-foo", HeaderAppendAction::OverwriteIfExists)
            .unwrap();
        assert_eq!(
            ho.append_action,
            HeaderAppendAction::OverwriteIfExists as i32
        );
        assert!(ho.append.is_none());
    }

    #[test]
    fn test_resolve_header_mutation_enforces_add_if_absent_and_overwrite_if_exists() {
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.set_phase(RequestPhase::RequestHeaders);
        rc.phase_header_keys = Some(
            [":authority", "host", "x-existing", "x-remove-then-add"]
                .into_iter()
                .map(String::from)
                .collect(),
        );
        rc.response.header_mutation = Some(HeaderMutation {
            set_headers: Vec::new(),
            remove_headers: Vec::new(),
        });

        rc.add_header(
            "x-existing",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"ignored".to_vec()),
            },
        )
        .unwrap(); // ADD_IF_ABSENT on an existing key: skipped
        rc.add_header(
            "x-absent",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"first".to_vec()),
            },
        )
        .unwrap();
        rc.add_header(
            "x-absent",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"second".to_vec()),
            },
        )
        .unwrap(); // ADD_IF_ABSENT on an existing key: skipped
        rc.update_header(
            "x-missing",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"nope".to_vec()),
            },
            HeaderAppendAction::OverwriteIfExists,
        )
        .unwrap(); // OVERWRITE_IF_EXISTS on a missing key: skipped
        rc.update_header(
            "x-existing",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"replaced".to_vec()),
            },
            HeaderAppendAction::OverwriteIfExists,
        )
        .unwrap(); // OVERWRITE_IF_EXISTS on an existing key: applied
        rc.remove_header("x-remove-then-add").unwrap();
        rc.add_header(
            "x-remove-then-add",
            &HeaderValue {
                value: String::new(),
                raw_value: Some(b"fresh".to_vec()),
            },
        )
        .unwrap(); // re-added after removal: applied
        rc.remove_header(":authority").unwrap(); // pseudo-headers cannot be removed

        let (hm, next) = rc.resolved_header_mutation();
        let set = hm
            .clone()
            .expect("expected a resolved mutation")
            .set_headers;
        assert_eq!(set.len(), 3);
        assert_eq!(set[0].append_action, HeaderAppendAction::AddIfAbsent as i32);
        assert_eq!(set[0].header.as_ref().unwrap().key, "x-absent");
        assert_eq!(set[0].header.as_ref().unwrap().raw_value, b"first".to_vec());
        assert_eq!(
            set[1].append_action,
            HeaderAppendAction::OverwriteIfExists as i32
        );
        assert_eq!(set[1].header.as_ref().unwrap().key, "x-existing");
        assert_eq!(
            set[1].header.as_ref().unwrap().raw_value,
            b"replaced".to_vec()
        );
        assert_eq!(set[2].append_action, HeaderAppendAction::AddIfAbsent as i32);
        assert_eq!(set[2].header.as_ref().unwrap().key, "x-remove-then-add");
        assert_eq!(set[2].header.as_ref().unwrap().raw_value, b"fresh".to_vec());

        let next = next.expect("expected a post-mutation key set");
        assert!(next.contains("x-absent"));
        assert!(next.contains("x-existing"));
        assert!(next.contains("x-remove-then-add"));
        assert!(next.contains(":authority")); // pseudo-headers are not removable
        assert!(next.contains("host"));
        assert!(!next.contains("x-missing"));

        // The resolution is deterministic across repeated phases.
        let (hm2, next2) = rc.resolved_header_mutation();
        assert_eq!(hm, hm2);
        assert_eq!(next, next2.expect("expected a post-mutation key set"));
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

    // -----------------------------------------------------------------------
    // Dynamic metadata & CEL attributes (envoy `ProcessingRequest.metadata_context`,
    // `ProcessingRequest.attributes`, `ProcessingResponse.dynamic_metadata`)
    // -----------------------------------------------------------------------

    fn string_value(s: &str) -> Value {
        Value {
            kind: Some(ValueKind::StringValue(s.to_string())),
        }
    }

    fn bool_value(b: bool) -> Value {
        Value {
            kind: Some(ValueKind::BoolValue(b)),
        }
    }

    fn number_value(n: f64) -> Value {
        Value {
            kind: Some(ValueKind::NumberValue(n)),
        }
    }

    fn struct_value(fields: HashMap<String, Value>) -> Value {
        Value {
            kind: Some(ValueKind::StructValue(Struct { fields })),
        }
    }

    fn envoy_metadata(
        filter_metadata: HashMap<String, Struct>,
        typed_filter_metadata: HashMap<String, Any>,
    ) -> Metadata {
        Metadata {
            filter_metadata,
            typed_filter_metadata,
        }
    }

    fn namespace_dynamic_metadata(rc: &RequestContext, namespace: &str) -> HashMap<String, Value> {
        let envoy_struct = rc
            .response
            .dynamic_metadata
            .as_ref()
            .expect("expected pending dynamic metadata");
        let value = envoy_struct
            .fields
            .get(namespace)
            .expect("expected namespace entry");
        match value.kind.as_ref() {
            Some(ValueKind::StructValue(struct_)) => struct_.fields.clone(),
            _ => panic!("expected struct value for namespace {}", namespace),
        }
    }

    #[test]
    fn test_dynamic_metadata_validation_and_merging() {
        let mut rc = RequestContext::new();
        // Empty namespaces and keys are rejected, without mutating state.
        assert!(rc.set_dynamic_metadata("", "k", string_value("v")).is_err());
        assert!(rc
            .set_dynamic_metadata("com.example.ns", "", string_value("v"))
            .is_err());
        assert!(rc.response.dynamic_metadata.is_none());

        // A single key can be set under a namespace.
        assert!(rc
            .set_dynamic_metadata("com.example.ns", "initial", string_value("ok"))
            .is_ok());
        assert_eq!(
            namespace_dynamic_metadata(&rc, "com.example.ns").get("initial"),
            Some(&string_value("ok"))
        );

        // A failed namespace update must not apply partial changes.
        let mut bad = HashMap::new();
        bad.insert("".to_string(), string_value("bad"));
        assert!(rc
            .set_dynamic_metadata_namespace("com.example.ns", &bad)
            .is_err());
        assert_eq!(
            namespace_dynamic_metadata(&rc, "com.example.ns").get("initial"),
            Some(&string_value("ok"))
        );

        // A namespace update merges values into the namespace map.
        let mut values = HashMap::new();
        values.insert("count".to_string(), number_value(42.0));
        values.insert("tags".to_string(), struct_value(HashMap::new()));
        values.insert(
            "nested".to_string(),
            struct_value(HashMap::from([("enabled".to_string(), bool_value(true))])),
        );
        assert!(rc
            .set_dynamic_metadata_namespace("com.example.ns", &values)
            .is_ok());
        let merged = namespace_dynamic_metadata(&rc, "com.example.ns");
        assert_eq!(merged.get("initial"), Some(&string_value("ok")));
        assert_eq!(merged.get("count"), Some(&number_value(42.0)));
        assert_eq!(
            merged.get("nested").and_then(|v| v.kind.as_ref()),
            Some(&ValueKind::StructValue(Struct {
                fields: HashMap::from([("enabled".to_string(), bool_value(true),)]),
            }))
        );

        // A second namespace can be set independently.
        assert!(rc
            .set_dynamic_metadata("com.example.other", "flag", bool_value(true))
            .is_ok());
        assert_eq!(
            namespace_dynamic_metadata(&rc, "com.example.other").get("flag"),
            Some(&bool_value(true))
        );
    }

    #[test]
    fn test_dynamic_metadata_attached_to_all_phases_and_immediate_response_and_reset_clears_outbound(
    ) {
        let phases = [
            RequestPhase::RequestHeaders,
            RequestPhase::RequestBody,
            RequestPhase::RequestTrailers,
            RequestPhase::ResponseHeaders,
            RequestPhase::ResponseBody,
            RequestPhase::ResponseTrailers,
        ];
        for phase in phases {
            let mut rc = RequestContext::new();
            rc.reset_phase().unwrap();
            rc.set_phase(phase);
            assert!(rc
                .set_dynamic_metadata("com.example.phase", "name", string_value("x"))
                .is_ok());
            let resp = rc.get_response(phase).unwrap().unwrap();
            assert!(resp.dynamic_metadata.is_some());
            assert_eq!(
                namespace_dynamic_metadata(&rc, "com.example.phase").get("name"),
                Some(&string_value("x"))
            );

            rc.clear_dynamic_metadata();
            let resp = rc.get_response(phase).unwrap().unwrap();
            assert!(resp.dynamic_metadata.is_none());
        }

        // Immediate responses carry the pending dynamic metadata.
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.set_phase(RequestPhase::RequestHeaders);
        assert!(rc
            .set_dynamic_metadata("com.example.auth", "denied", bool_value(true))
            .is_ok());
        rc.cancel_request(403, &HashMap::new(), &[]).unwrap();
        let resp = rc
            .get_response(RequestPhase::RequestHeaders)
            .unwrap()
            .unwrap();
        assert!(resp.dynamic_metadata.is_some());
        assert_eq!(
            namespace_dynamic_metadata(&rc, "com.example.auth").get("denied"),
            Some(&bool_value(true))
        );

        // Phase resets clear the pending outbound dynamic metadata, while the
        // inbound metadata and attributes persist across phases.
        rc.reset_phase().unwrap();
        assert!(rc.response.dynamic_metadata.is_none());
        assert!(rc.metadata.is_empty());
        assert!(rc.attributes.is_empty());
    }

    #[test]
    fn test_dynamic_metadata_inbound_merge_and_persistence_across_phases() {
        let mut rc = RequestContext::new();
        let mut fields = HashMap::new();
        fields.insert("sub".to_string(), string_value("alice"));
        fields.insert("role".to_string(), string_value("reader"));
        let mut filter_metadata = HashMap::new();
        filter_metadata.insert(
            "com.example.auth".to_string(),
            Struct {
                fields: fields.clone(),
            },
        );
        let mut typed_filter_metadata = HashMap::new();
        typed_filter_metadata.insert(
            "com.example.typed".to_string(),
            Any {
                type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
                value: b"typed-v1".to_vec(),
            },
        );
        rc.merge_metadata_context(&envoy_metadata(filter_metadata, typed_filter_metadata));

        let mut attr_fields = HashMap::new();
        attr_fields.insert("request.protocol".to_string(), string_value("HTTP/2"));
        attr_fields.insert("xds.route_name".to_string(), string_value("r1"));
        let mut attrs = HashMap::new();
        attrs.insert(
            "envoy.filters.http.ext_proc".to_string(),
            Struct {
                fields: attr_fields,
            },
        );
        rc.merge_attributes(&attrs);

        assert_eq!(
            rc.metadata
                .get("com.example.auth")
                .and_then(|m| m.get("sub")),
            Some(&string_value("alice"))
        );
        assert_eq!(
            rc.typed_metadata
                .get("com.example.typed")
                .map(|a| a.type_url.clone()),
            Some("type.googleapis.com/google.protobuf.StringValue".to_string())
        );
        assert_eq!(
            rc.attributes.get("request.protocol"),
            Some(&string_value("HTTP/2"))
        );

        // Phase resets clear only the outbound pending metadata; the inbound
        // metadata and attributes persist across phases of the stream.
        rc.reset_phase().unwrap();
        assert!(rc.response.dynamic_metadata.is_none());
        assert_eq!(
            rc.metadata
                .get("com.example.auth")
                .and_then(|m| m.get("sub")),
            Some(&string_value("alice"))
        );
        assert_eq!(
            rc.attributes.get("request.protocol"),
            Some(&string_value("HTTP/2"))
        );
    }

    #[test]
    fn test_override_message_timeout_validation_guards() {
        // Timeouts below 1ms are rejected, with and without a wired send.
        for bad in [Duration::from_nanos(0), Duration::from_nanos(999)] {
            let mut rc = RequestContext::new();
            rc.reset_phase().unwrap();
            rc.set_phase(RequestPhase::RequestHeaders);
            rc.send_timeout_override = Some(Box::new(|_resp: &ProcessingResponse| Ok(())));
            assert!(rc.override_message_timeout(bad).is_err());
        }

        // An override requires an active phase, a wired send, and is at most
        // once per phase.
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.send_timeout_override = Some(Box::new(|_resp: &ProcessingResponse| Ok(())));
        assert!(rc
            .override_message_timeout(Duration::from_millis(50))
            .is_err());

        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.set_phase(RequestPhase::RequestHeaders);
        assert!(rc
            .override_message_timeout(Duration::from_millis(50))
            .is_err());

        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.set_phase(RequestPhase::RequestHeaders);
        rc.send_timeout_override = Some(Box::new(|_resp: &ProcessingResponse| Ok(())));
        rc.override_message_timeout(Duration::from_millis(50))
            .unwrap();
        assert!(rc
            .override_message_timeout(Duration::from_millis(25))
            .is_err());
    }

    #[test]
    fn test_override_message_timeout_sends_interim_across_phases_and_resets() {
        // An interim `override_message_timeout` response is sent through the
        // wired send, carrying the requested duration, at most once per phase,
        // and the once-per-phase state resets for the next phase.
        for phase in [
            RequestPhase::RequestHeaders,
            RequestPhase::RequestBody,
            RequestPhase::RequestTrailers,
            RequestPhase::ResponseHeaders,
            RequestPhase::ResponseBody,
            RequestPhase::ResponseTrailers,
        ] {
            let mut rc = RequestContext::new();
            rc.reset_phase().unwrap();
            rc.set_phase(phase);
            let sent = Arc::new(Mutex::new(Vec::new()));
            rc.send_timeout_override = Some(Box::new({
                let sent = sent.clone();
                move |resp: &ProcessingResponse| {
                    sent.lock().unwrap().push(resp.clone());
                    Ok(())
                }
            }));
            rc.override_message_timeout(Duration::from_millis(500))
                .unwrap();
            assert_eq!(sent.lock().unwrap().len(), 1);
            let resp = sent.lock().unwrap().last().unwrap().clone();
            assert_eq!(
                resp.override_message_timeout,
                Some(envoy_types::pb::google::protobuf::Duration {
                    seconds: 0,
                    nanos: 500_000_000,
                })
            );
            assert!(rc.take_interim_send_error().is_none());

            assert!(rc
                .override_message_timeout(Duration::from_millis(250))
                .is_err());

            rc.reset_phase().unwrap();
            rc.set_phase(phase);
            rc.override_message_timeout(Duration::from_millis(200))
                .unwrap();
        }
    }

    #[test]
    fn test_override_message_timeout_observability_mode_is_noop() {
        let mut rc = RequestContext::new();
        rc.reset_phase().unwrap();
        rc.set_phase(RequestPhase::RequestHeaders);
        rc.observability_mode = true;
        rc.send_timeout_override = Some(Box::new(|_resp: &ProcessingResponse| Ok(())));
        assert!(rc
            .override_message_timeout(Duration::from_millis(500))
            .is_ok());
        assert!(rc.take_interim_send_error().is_none());
        // The once-per-phase guard still applies in observability mode.
        assert!(rc
            .override_message_timeout(Duration::from_millis(250))
            .is_err());
    }
}
