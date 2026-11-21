//! Envoy bootstrap generation for conformance scenarios.

use envoy_extproc_sdk::Error;
use serde_json::{json, Value};

const EXTPROC_FILTER_NAME: &str = "envoy.filters.http.ext_proc";
const ROUTER_FILTER_NAME: &str = "envoy.filters.http.router";
const HTTP_CONNECTION_MANAGER_FILTER_NAME: &str = "envoy.filters.network.http_connection_manager";

const DEFAULT_EXT_PROC_CLUSTER: &str = "extproc_service";
const ABORT_EXT_PROC_CLUSTER: &str = "extproc_abort_service";
const SECONDARY_EXT_PROC_CLUSTER: &str = "extproc_secondary_service";

const UPSTREAM_PRIMARY_CLUSTER: &str = "upstream_primary";
const UPSTREAM_SECONDARY_CLUSTER: &str = "upstream_secondary";

const OBSERVABILITY_FILTER_NAME: &str = "envoy.filters.http.ext_proc.observability";
const EXTPROC_MODE_OVERRIDE_FILTER_NAME: &str = "extproc_mode_override";
const EXTPROC_MODE_OVERRIDE_ALLOWLIST_FILTER_NAME: &str = "extproc_mode_override_allowlist";
const EXTPROC_SEND_BODY_NO_WAIT_FILTER_NAME: &str = "extproc_send_body_no_wait";
const EXTPROC_META_FIRST_FILTER_NAME: &str = "extproc_meta_first";
const EXTPROC_META_SECOND_FILTER_NAME: &str = "extproc_meta_second";
const EXTPROC_ATTRIBUTES_FILTER_NAME: &str = "extproc_attributes";
const EXTPROC_TIMEOUT_OVERRIDE_FILTER_NAME: &str = "extproc_timeout_override";

const OBSERVED_HEADER_SEND: &str = "SEND";
const OBSERVED_HEADER_SKIP: &str = "SKIP";
const OBSERVED_BODY_BUFFERED: &str = "BUFFERED";
const OBSERVED_BODY_STREAMED: &str = "STREAMED";
const OBSERVED_BODY_BUFFERED_PARTIAL: &str = "BUFFERED_PARTIAL";
const OBSERVED_BODY_NONE: &str = "NONE";

/// Parameters that shape the Envoy bootstrap used by conformance scenarios.
pub struct BootstrapParams {
    pub version: String,
    pub upstream_host: String,
    pub primary_upstream_port: u16,
    pub secondary_upstream_port: u16,
    pub ext_proc_port: u16,
    pub abort_ext_proc_port: u16,
    pub secondary_ext_proc_port: u16,
    pub listener_port: u16,
    pub admin_port: u16,
}

impl BootstrapParams {
    /// Build a default bootstrap parameter set for the given scenario ports.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: &str,
        upstream_host: &str,
        primary_upstream_port: u16,
        secondary_upstream_port: u16,
        ext_proc_port: u16,
        abort_ext_proc_port: u16,
        secondary_ext_proc_port: u16,
        listener_port: u16,
        admin_port: u16,
    ) -> Self {
        Self {
            version: version.to_string(),
            upstream_host: upstream_host.to_string(),
            primary_upstream_port,
            secondary_upstream_port,
            ext_proc_port,
            abort_ext_proc_port,
            secondary_ext_proc_port,
            listener_port,
            admin_port,
        }
    }
}

/// Build the Envoy bootstrap configuration as a JSON string.
pub fn build_bootstrap_json(params: &BootstrapParams) -> Result<String, Error> {
    let bootstrap = build_bootstrap(params)?;
    Ok(serde_json::to_string(&bootstrap).map_err(|e| format!("serialize envoy bootstrap: {e}"))?)
}

/// Build the Envoy bootstrap configuration as a JSON value.
pub fn build_bootstrap(params: &BootstrapParams) -> Result<Value, Error> {
    if params.version.is_empty() {
        return Err("envoy bootstrap version must not be empty".into());
    }
    if params.upstream_host.is_empty() {
        return Err("envoy bootstrap upstream host must not be empty".into());
    }

    let default_mode = processing_mode_json(
        OBSERVED_HEADER_SEND,
        OBSERVED_HEADER_SEND,
        OBSERVED_BODY_BUFFERED,
        OBSERVED_BODY_BUFFERED,
        OBSERVED_HEADER_SEND,
        OBSERVED_HEADER_SEND,
    );

    let default_filter = ext_proc_filter_json(
        EXTPROC_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        false,
        5000,
        false,
        false,
        &default_mode,
        false,
        None,
        false,
    );

    let default_cluster = cluster_json(
        UPSTREAM_PRIMARY_CLUSTER,
        &params.upstream_host,
        params.primary_upstream_port,
        false,
    );
    let secondary_cluster = cluster_json(
        UPSTREAM_SECONDARY_CLUSTER,
        &params.upstream_host,
        params.secondary_upstream_port,
        false,
    );
    let ext_proc_cluster = cluster_json(
        DEFAULT_EXT_PROC_CLUSTER,
        &params.upstream_host,
        params.ext_proc_port,
        true,
    );
    let abort_ext_proc_cluster = cluster_json(
        ABORT_EXT_PROC_CLUSTER,
        &params.upstream_host,
        params.abort_ext_proc_port,
        true,
    );
    let secondary_ext_proc_cluster = cluster_json(
        SECONDARY_EXT_PROC_CLUSTER,
        &params.upstream_host,
        params.secondary_ext_proc_port,
        true,
    );

    let mut http_filters = vec![default_filter.clone()];
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.short_timeout_fail_closed",
        DEFAULT_EXT_PROC_CLUSTER,
        false,
        150,
        false,
        true,
        &default_mode,
        false,
        None,
        false,
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.short_timeout_fail_open",
        DEFAULT_EXT_PROC_CLUSTER,
        true,
        150,
        false,
        true,
        &default_mode,
        false,
        None,
        false,
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.abort",
        ABORT_EXT_PROC_CLUSTER,
        false,
        5000,
        false,
        true,
        &default_mode,
        false,
        None,
        false,
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.chain_first",
        DEFAULT_EXT_PROC_CLUSTER,
        false,
        5000,
        false,
        true,
        &default_mode,
        false,
        None,
        false,
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.chain_second",
        SECONDARY_EXT_PROC_CLUSTER,
        false,
        5000,
        false,
        true,
        &default_mode,
        false,
        None,
        false,
    ));
    http_filters.push(ext_proc_filter_json(
        OBSERVABILITY_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        true,
        5000,
        true,
        true,
        &processing_mode_json(
            OBSERVED_HEADER_SEND,
            OBSERVED_HEADER_SEND,
            OBSERVED_BODY_STREAMED,
            OBSERVED_BODY_STREAMED,
            OBSERVED_HEADER_SEND,
            OBSERVED_HEADER_SEND,
        ),
        false,
        None,
        false,
    ));
    http_filters.push(ext_proc_filter_json(
        EXTPROC_MODE_OVERRIDE_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        false,
        5000,
        false,
        true,
        &none_processing_mode_json(),
        true,
        None,
        false,
    ));
    let mode_override_allowed_modes = vec![json!({
        "response_header_mode": OBSERVED_HEADER_SEND,
        "request_body_mode": OBSERVED_BODY_STREAMED,
        "response_body_mode": OBSERVED_BODY_NONE,
        "request_trailer_mode": OBSERVED_HEADER_SKIP,
        "response_trailer_mode": OBSERVED_HEADER_SKIP,
    })];
    http_filters.push(ext_proc_filter_json(
        EXTPROC_MODE_OVERRIDE_ALLOWLIST_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        false,
        5000,
        false,
        true,
        &none_processing_mode_json(),
        true,
        Some(&mode_override_allowed_modes),
        false,
    ));
    http_filters.push(ext_proc_filter_json(
        EXTPROC_SEND_BODY_NO_WAIT_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        false,
        5000,
        false,
        true,
        &processing_mode_json(
            OBSERVED_HEADER_SEND,
            OBSERVED_HEADER_SEND,
            OBSERVED_BODY_STREAMED,
            OBSERVED_BODY_STREAMED,
            OBSERVED_HEADER_SEND,
            OBSERVED_HEADER_SEND,
        ),
        false,
        None,
        true,
    ));
    http_filters.push(ext_proc_filter_json_with_dynamic_metadata(
        EXTPROC_META_FIRST_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        5_000,
        &default_mode,
        Some(&json!({
            "receiving_namespaces": {
                "untyped": ["com.conformance.extproc"],
            },
        })),
        &[],
        &[],
        true,
    ));
    http_filters.push(ext_proc_filter_json_with_dynamic_metadata(
        EXTPROC_META_SECOND_FILTER_NAME,
        SECONDARY_EXT_PROC_CLUSTER,
        5_000,
        &default_mode,
        Some(&json!({
            "forwarding_namespaces": {
                "untyped": ["com.conformance.extproc", "com.conformance.unallowed"],
            },
        })),
        &[],
        &[],
        true,
    ));
    http_filters.push(ext_proc_filter_json_with_dynamic_metadata(
        EXTPROC_ATTRIBUTES_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        5_000,
        &default_mode,
        None,
        &[
            "request.path".to_string(),
            "request.url_path".to_string(),
            "request.method".to_string(),
            "request.protocol".to_string(),
            "xds.route_name".to_string(),
        ],
        &["response.code".to_string()],
        true,
    ));
    http_filters.push(ext_proc_filter_json_with_max_timeout(
        EXTPROC_TIMEOUT_OVERRIDE_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        150,
        1500,
        &default_mode,
        true,
    ));
    http_filters.push(json!({
        "name": ROUTER_FILTER_NAME,
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
        },
    }));

    let hcm = http_connection_manager_json(&default_mode, &http_filters);

    let listener = json!({
        "name": "conformance_listener",
        "address": {
            "socket_address": {
                "address": "0.0.0.0",
                "port_value": params.listener_port,
            },
        },
        "filter_chains": [
            {
                "filters": [
                    {
                        "name": HTTP_CONNECTION_MANAGER_FILTER_NAME,
                        "typed_config": hcm,
                    },
                ],
            },
        ],
    });

    Ok(json!({
        "static_resources": {
            "listeners": [listener],
            "clusters": [
                default_cluster,
                secondary_cluster,
                ext_proc_cluster,
                abort_ext_proc_cluster,
                secondary_ext_proc_cluster,
            ],
        },
        "admin": {
            "address": {
                "socket_address": {
                    "address": "0.0.0.0",
                    "port_value": params.admin_port,
                },
            },
        },
    }))
}

fn processing_mode_json(
    request_header_mode: &str,
    response_header_mode: &str,
    request_body_mode: &str,
    response_body_mode: &str,
    request_trailer_mode: &str,
    response_trailer_mode: &str,
) -> Value {
    json!({
        "request_header_mode": request_header_mode,
        "response_header_mode": response_header_mode,
        "request_body_mode": request_body_mode,
        "response_body_mode": response_body_mode,
        "request_trailer_mode": request_trailer_mode,
        "response_trailer_mode": response_trailer_mode,
    })
}

fn none_processing_mode_json() -> Value {
    json!({
        "request_header_mode": OBSERVED_HEADER_SEND,
        "response_header_mode": OBSERVED_HEADER_SEND,
        "request_body_mode": OBSERVED_BODY_NONE,
        "response_body_mode": OBSERVED_BODY_NONE,
        "request_trailer_mode": OBSERVED_HEADER_SKIP,
        "response_trailer_mode": OBSERVED_HEADER_SKIP,
    })
}

#[allow(clippy::too_many_arguments)]
fn ext_proc_filter_json(
    name: &str,
    cluster_name: &str,
    failure_mode_allow: bool,
    message_timeout_ms: u64,
    observability_mode: bool,
    disabled: bool,
    mode: &Value,
    allow_mode_override: bool,
    allowed_override_modes: Option<&Vec<Value>>,
    send_body_without_waiting_for_header_response: bool,
) -> Value {
    let seconds = message_timeout_ms / 1000;
    let nanos = (message_timeout_ms % 1000) * 1_000_000;
    let mut filter = json!({
        "name": name,
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor",
            "grpc_service": {
                "envoy_grpc": {
                    "cluster_name": cluster_name,
                },
            },
            "failure_mode_allow": failure_mode_allow,
            "observability_mode": observability_mode,
            "message_timeout": {
                "seconds": seconds,
                "nanos": nanos,
            },
            "mutation_rules": {
                "allow_all_routing": true,
            },
            "processing_mode": mode,
        },
    });
    if disabled {
        filter["disabled"] = json!(true);
    }
    filter["typed_config"]["send_body_without_waiting_for_header_response"] =
        json!(send_body_without_waiting_for_header_response);
    filter["typed_config"]["allow_mode_override"] = json!(allow_mode_override);
    if let Some(modes) = allowed_override_modes {
        filter["typed_config"]["allowed_override_modes"] = Value::Array(modes.clone());
    }
    filter
}

/// Build an ext_proc filter JSON with dynamic metadata options
/// (`metadataOptions`) and CEL request/response attributes (`requestAttributes` /
/// `responseAttributes`) attached, for the `dynamic_metadata_and_attributes`
/// conformance scenarios. The filters built here are disabled by default and
/// enabled per-route by the dynamic metadata & CEL attributes scenarios.
#[allow(clippy::too_many_arguments)]
fn ext_proc_filter_json_with_dynamic_metadata(
    name: &str,
    cluster_name: &str,
    message_timeout_ms: u64,
    mode: &Value,
    metadata_options: Option<&Value>,
    request_attributes: &[String],
    response_attributes: &[String],
    disabled: bool,
) -> Value {
    let mut filter = ext_proc_filter_json(
        name,
        cluster_name,
        false,
        message_timeout_ms,
        false,
        disabled,
        mode,
        false,
        None,
        false,
    );
    if let Some(metadata_options) = metadata_options {
        filter["typed_config"]["metadata_options"] = metadata_options.clone();
    }
    if !request_attributes.is_empty() {
        let values: Vec<Value> = request_attributes.iter().map(|s| json!(s)).collect();
        filter["typed_config"]["request_attributes"] = Value::Array(values);
    }
    if !response_attributes.is_empty() {
        let values: Vec<Value> = response_attributes.iter().map(|s| json!(s)).collect();
        filter["typed_config"]["response_attributes"] = Value::Array(values);
    }
    filter
}

/// Build an ext_proc filter JSON with a `max_message_timeout`, for the
/// `override_message_timeout` conformance scenario. The filter is disabled by
/// default and enabled per-route by the `/profile/timeout-override` profile
/// route. `message_timeout` is kept short (150ms) so the scenario can observe
/// the interim override extend the deadline past Envoy's base timer.
fn ext_proc_filter_json_with_max_timeout(
    name: &str,
    cluster_name: &str,
    message_timeout_ms: u64,
    max_message_timeout_ms: u64,
    mode: &Value,
    disabled: bool,
) -> Value {
    let mut filter = ext_proc_filter_json(
        name,
        cluster_name,
        false, // failure_mode_allow: fail closed on phase errors (e.g. message timeout)
        message_timeout_ms,
        false, // observability_mode
        disabled,
        mode,
        false, // allow_mode_override
        None,  // allowed_override_modes
        false, // send_body_without_waiting_for_header_response
    );
    filter["typed_config"]["max_message_timeout"] = json!({
        "seconds": max_message_timeout_ms / 1000,
        "nanos": (max_message_timeout_ms % 1000) * 1_000_000,
    });
    filter
}

fn ext_proc_overrides_json(mode: &Value, failure_mode_allow: Option<bool>) -> Value {
    let mut override_value = json!({
        "processing_mode": mode,
    });
    if let Some(failure_mode_allow) = failure_mode_allow {
        override_value["failure_mode_allow"] = json!(failure_mode_allow);
    }
    override_value
}

fn route_json(
    prefix: &str,
    mode: &Value,
    failure_mode_allow: Option<bool>,
    disabled: bool,
    cluster: &str,
) -> Value {
    if disabled {
        return json!({
            "match": {
                "prefix": prefix,
            },
            "route": {
                "cluster": cluster,
            },
            "typed_per_filter_config": {
                EXTPROC_FILTER_NAME: {
                    "@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
                    "disabled": true,
                },
            },
        });
    }
    json!({
        "match": {
            "prefix": prefix,
        },
        "route": {
            "cluster": cluster,
        },
        "typed_per_filter_config": {
            EXTPROC_FILTER_NAME: {
                "@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
                "overrides": ext_proc_overrides_json(mode, failure_mode_allow),
            },
        },
    })
}

fn profile_route_json(
    prefix: &str,
    enable_filters: &[(&str, &Value, Option<bool>)],
    cluster: &str,
) -> Value {
    let mut per_filter = serde_json::Map::new();
    per_filter.insert(
        EXTPROC_FILTER_NAME.to_string(),
        json!({
            "@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
            "disabled": true,
        }),
    );
    for (filter_name, mode, failure_mode_allow) in enable_filters {
        per_filter.insert(
            (*filter_name).to_string(),
            json!({
                "@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
                "overrides": ext_proc_overrides_json(mode, *failure_mode_allow),
            }),
        );
    }
    json!({
        "match": {
            "prefix": prefix,
        },
        "route": {
            "cluster": cluster,
        },
        "typed_per_filter_config": Value::Object(per_filter),
    })
}

fn buffered_partial_route_json(
    prefix: &str,
    mode: &Value,
    buffer_limit_bytes: u32,
    cluster: &str,
) -> Value {
    json!({
        "match": {
            "prefix": prefix,
        },
        "route": {
            "cluster": cluster,
        },
        "per_request_buffer_limit_bytes": buffer_limit_bytes,
        "typed_per_filter_config": {
            EXTPROC_FILTER_NAME: {
                "@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
                "overrides": ext_proc_overrides_json(mode, Some(false)),
            },
        },
    })
}

fn http_connection_manager_json(mode: &Value, http_filters: &[Value]) -> Value {
    let primary_routes = vec![
        route_json(
            "/mode/disabled",
            mode,
            Some(false),
            true,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/buffered-partial",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_BUFFERED_PARTIAL,
                OBSERVED_BODY_BUFFERED_PARTIAL,
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        buffered_partial_route_json(
            "/mode/buffered-partial-small",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_BUFFERED_PARTIAL,
                OBSERVED_BODY_BUFFERED_PARTIAL,
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
            ),
            1024,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/buffered",
            mode,
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/streamed",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_STREAMED,
                OBSERVED_BODY_STREAMED,
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/trailers",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_STREAMED,
                OBSERVED_BODY_STREAMED,
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/none",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_NONE,
                OBSERVED_BODY_NONE,
                OBSERVED_HEADER_SKIP,
                OBSERVED_HEADER_SKIP,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/skip-req-headers",
            &processing_mode_json(
                OBSERVED_HEADER_SKIP,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_NONE,
                OBSERVED_BODY_BUFFERED,
                OBSERVED_HEADER_SKIP,
                OBSERVED_HEADER_SKIP,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/timeout-fail-closed",
            &[(
                "envoy.filters.http.ext_proc.short_timeout_fail_closed",
                mode,
                Some(false),
            )],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/timeout-fail-open",
            &[(
                "envoy.filters.http.ext_proc.short_timeout_fail_open",
                mode,
                Some(true),
            )],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/abort-fail-closed",
            &[("envoy.filters.http.ext_proc.abort", mode, Some(false))],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/abort-fail-open",
            &[("envoy.filters.http.ext_proc.abort", mode, Some(true))],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/chained",
            &[
                ("envoy.filters.http.ext_proc.chain_first", mode, Some(false)),
                (
                    "envoy.filters.http.ext_proc.chain_second",
                    mode,
                    Some(false),
                ),
            ],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/observability",
            &[(
                OBSERVABILITY_FILTER_NAME,
                &processing_mode_json(
                    OBSERVED_HEADER_SEND,
                    OBSERVED_HEADER_SEND,
                    OBSERVED_BODY_STREAMED,
                    OBSERVED_BODY_STREAMED,
                    OBSERVED_HEADER_SEND,
                    OBSERVED_HEADER_SEND,
                ),
                Some(true),
            )],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/mode-override-allowlist",
            &[(
                EXTPROC_MODE_OVERRIDE_ALLOWLIST_FILTER_NAME,
                &none_processing_mode_json(),
                None,
            )],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/mode-override",
            &[(
                EXTPROC_MODE_OVERRIDE_FILTER_NAME,
                &none_processing_mode_json(),
                None,
            )],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/mode-override-resp-buffered",
            &[(
                EXTPROC_MODE_OVERRIDE_FILTER_NAME,
                &processing_mode_json(
                    OBSERVED_HEADER_SEND,
                    OBSERVED_HEADER_SEND,
                    OBSERVED_BODY_NONE,
                    OBSERVED_BODY_BUFFERED,
                    OBSERVED_HEADER_SKIP,
                    OBSERVED_HEADER_SKIP,
                ),
                None,
            )],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/send-body-without-waiting",
            &[(
                EXTPROC_SEND_BODY_NO_WAIT_FILTER_NAME,
                &processing_mode_json(
                    OBSERVED_HEADER_SEND,
                    OBSERVED_HEADER_SEND,
                    OBSERVED_BODY_STREAMED,
                    OBSERVED_BODY_STREAMED,
                    OBSERVED_HEADER_SEND,
                    OBSERVED_HEADER_SEND,
                ),
                None,
            )],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        profile_route_json(
            "/profile/metadata-chain",
            &[
                (EXTPROC_META_FIRST_FILTER_NAME, mode, None),
                (EXTPROC_META_SECOND_FILTER_NAME, mode, None),
            ],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        {
            let mut attributes_route = profile_route_json(
                "/profile/attributes",
                &[(EXTPROC_ATTRIBUTES_FILTER_NAME, mode, None)],
                UPSTREAM_PRIMARY_CLUSTER,
            );
            attributes_route["name"] = json!("conformance_attributes_route");
            attributes_route
        },
        profile_route_json(
            "/profile/timeout-override",
            &[(EXTPROC_TIMEOUT_OVERRIDE_FILTER_NAME, mode, Some(false))],
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/none/protocol-config",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_NONE,
                OBSERVED_BODY_NONE,
                OBSERVED_HEADER_SKIP,
                OBSERVED_HEADER_SKIP,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/buffered/protocol-config",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_BUFFERED,
                OBSERVED_BODY_BUFFERED,
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/streamed/protocol-config",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_STREAMED,
                OBSERVED_BODY_STREAMED,
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/buffered-partial/protocol-config",
            &processing_mode_json(
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_BUFFERED_PARTIAL,
                OBSERVED_BODY_BUFFERED_PARTIAL,
                OBSERVED_HEADER_SEND,
                OBSERVED_HEADER_SEND,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        route_json(
            "/mode/skip-req-headers/protocol-config",
            &processing_mode_json(
                OBSERVED_HEADER_SKIP,
                OBSERVED_HEADER_SEND,
                OBSERVED_BODY_NONE,
                OBSERVED_BODY_BUFFERED,
                OBSERVED_HEADER_SKIP,
                OBSERVED_HEADER_SKIP,
            ),
            Some(false),
            false,
            UPSTREAM_PRIMARY_CLUSTER,
        ),
        json!({
            "match": {
                "prefix": "/",
            },
            "route": {
                "cluster": UPSTREAM_PRIMARY_CLUSTER,
            },
        }),
    ];

    json!({
        "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
        "stat_prefix": "conformance_http",
        "codec_type": "AUTO",
        "http_protocol_options": {
            "enable_trailers": true,
        },
        "http2_protocol_options": {
            "max_concurrent_streams": 1024,
        },
        "route_config": {
            "name": "conformance_routes",
            "virtual_hosts": [
                {
                    "name": "secondary_vhost",
                    "domains": ["secondary.local", "secondary.local:*"],
                    "routes": [
                        {
                            "match": {
                                "prefix": "/",
                            },
                            "route": {
                                "cluster": UPSTREAM_SECONDARY_CLUSTER,
                            },
                        },
                    ],
                },
                {
                    "name": "conformance_vhost",
                    "domains": ["*"],
                    "routes": primary_routes,
                },
            ],
        },
        "http_filters": http_filters,
    })
}

fn cluster_json(name: &str, host: &str, port: u16, is_grpc: bool) -> Value {
    let protocol_options = if is_grpc {
        json!({
            "envoy.extensions.upstreams.http.v3.HttpProtocolOptions": {
                "@type": "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
                "explicit_http_config": {
                    "http2_protocol_options": {},
                },
            },
        })
    } else {
        json!({
            "envoy.extensions.upstreams.http.v3.HttpProtocolOptions": {
                "@type": "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
                "use_downstream_protocol_config": {
                    "http_protocol_options": {
                        "enable_trailers": true,
                    },
                    "http2_protocol_options": {},
                },
            },
        })
    };
    json!({
        "name": name,
        "type": "LOGICAL_DNS",
        "dns_lookup_family": "V4_ONLY",
        "connect_timeout": {
            "seconds": 2,
        },
        "lb_policy": "ROUND_ROBIN",
        "typed_extension_protocol_options": protocol_options,
        "load_assignment": {
            "cluster_name": name,
            "endpoints": [
                {
                    "lb_endpoints": [
                        {
                            "endpoint": {
                                "address": {
                                    "socket_address": {
                                        "address": host,
                                        "port_value": port,
                                    },
                                },
                            },
                        },
                    ],
                },
            ],
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> BootstrapParams {
        BootstrapParams::new(
            "v1.40-latest",
            "127.0.0.1",
            19001,
            19002,
            19000,
            19003,
            19004,
            19000,
            19005,
        )
    }

    #[test]
    fn bootstrap_json_is_valid_json_with_expected_top_level_shape() {
        let json_str = build_bootstrap_json(&params()).expect("bootstrap should build");
        let value: serde_json::Value =
            serde_json::from_str(&json_str).expect("bootstrap should parse");
        assert!(value.get("static_resources").is_some());
        assert!(value.get("admin").is_some());
    }

    #[test]
    fn bootstrap_json_sets_admin_address() {
        let json_str = build_bootstrap_json(&params()).expect("bootstrap should build");
        let value: serde_json::Value =
            serde_json::from_str(&json_str).expect("bootstrap should parse");
        let admin = value.get("admin").expect("admin should exist").clone();
        assert_eq!(
            admin
                .get("address")
                .expect("admin address")
                .get("socket_address")
                .unwrap()["port_value"],
            19005
        );
    }

    #[test]
    fn bootstrap_json_includes_expected_clusters() {
        let json_str = build_bootstrap_json(&params()).expect("bootstrap should build");
        let value: serde_json::Value =
            serde_json::from_str(&json_str).expect("bootstrap should parse");
        let clusters = value
            .get("static_resources")
            .expect("static_resources")
            .get("clusters")
            .expect("clusters")
            .as_array()
            .expect("clusters array");
        let names: Vec<&str> = clusters
            .iter()
            .map(|c| c.get("name").and_then(|n| n.as_str()).unwrap_or_default())
            .collect();
        assert!(names.contains(&UPSTREAM_PRIMARY_CLUSTER));
        assert!(names.contains(&UPSTREAM_SECONDARY_CLUSTER));
        assert!(names.contains(&DEFAULT_EXT_PROC_CLUSTER));
        assert!(names.contains(&ABORT_EXT_PROC_CLUSTER));
        assert!(names.contains(&SECONDARY_EXT_PROC_CLUSTER));
    }

    #[test]
    fn bootstrap_json_default_ext_proc_filter_uses_buffered_mode() {
        let bootstrap = build_bootstrap(&params()).expect("bootstrap should build");
        let hcm = bootstrap
            .get("static_resources")
            .expect("static_resources")
            .get("listeners")
            .and_then(|l| l.as_array())
            .and_then(|l| l.first())
            .and_then(|l| l.get("filter_chains"))
            .and_then(|fc| fc.as_array())
            .and_then(|fc| fc.first())
            .and_then(|fc| fc.get("filters"))
            .and_then(|f| f.as_array())
            .and_then(|f| f.first())
            .expect("http connection manager filter");
        let typed = hcm.get("typed_config").expect("typed_config");
        assert_eq!(
            typed
                .get("@type")
                .and_then(|t| t.as_str())
                .expect("typed_config @type"),
            "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
        );
        let filters = typed
            .get("http_filters")
            .and_then(|f| f.as_array())
            .expect("http_filters");
        assert_eq!(filters.len(), 15);
        assert!(filters
            .iter()
            .find(|f| f.get("name").and_then(|n| n.as_str())
                == Some(EXTPROC_MODE_OVERRIDE_FILTER_NAME))
            .expect("mode override filter")
            .get("typed_config")
            .and_then(|t| t.get("allow_mode_override"))
            .and_then(|v| v.as_bool())
            .expect("mode override allow_mode_override"));
        assert_eq!(
            filters
                .iter()
                .find(|f| f.get("name").and_then(|n| n.as_str())
                    == Some(EXTPROC_MODE_OVERRIDE_ALLOWLIST_FILTER_NAME))
                .expect("mode override allowlist filter")
                .get("typed_config")
                .and_then(|t| t.get("allowed_override_modes"))
                .and_then(|v| v.as_array())
                .expect("mode override allowed_override_modes")
                .len(),
            1
        );
        assert_eq!(
            filters[0]
                .get("name")
                .and_then(|n| n.as_str())
                .expect("filter name"),
            EXTPROC_FILTER_NAME
        );
        assert_eq!(
            filters[0]
                .get("typed_config")
                .and_then(|t| t.get("processing_mode"))
                .and_then(|m| m.get("request_body_mode"))
                .and_then(|m| m.as_str())
                .expect("request_body_mode"),
            OBSERVED_BODY_BUFFERED
        );
    }

    #[test]
    fn bootstrap_json_mode_routes_use_expected_prefixes() {
        let bootstrap = build_bootstrap(&params()).expect("bootstrap should build");
        let hcm = bootstrap
            .get("static_resources")
            .expect("static_resources")
            .get("listeners")
            .and_then(|l| l.as_array())
            .and_then(|l| l.first())
            .and_then(|l| l.get("filter_chains"))
            .and_then(|fc| fc.as_array())
            .and_then(|fc| fc.first())
            .and_then(|fc| fc.get("filters"))
            .and_then(|f| f.as_array())
            .and_then(|f| f.first())
            .expect("http connection manager filter");
        let virtual_hosts = hcm
            .get("typed_config")
            .and_then(|t| t.get("route_config"))
            .and_then(|rc| rc.get("virtual_hosts"))
            .and_then(|v| v.as_array())
            .expect("virtual hosts");
        assert_eq!(virtual_hosts.len(), 2);
        let vhost = &virtual_hosts[1];
        let routes = vhost
            .get("routes")
            .and_then(|r| r.as_array())
            .expect("routes");
        let prefixes: Vec<&str> = routes
            .iter()
            .map(|r| {
                r.get("match")
                    .and_then(|m| m.get("prefix"))
                    .and_then(|p| p.as_str())
                    .unwrap_or_default()
            })
            .collect();
        assert!(prefixes.contains(&"/mode/buffered"));
        assert!(prefixes.contains(&"/mode/streamed"));
        assert!(prefixes.contains(&"/mode/none"));
        assert!(prefixes.contains(&"/mode/disabled"));
    }

    #[test]
    fn bootstrap_json_rejects_empty_version() {
        let bad = BootstrapParams::new("", "127.0.0.1", 1, 2, 3, 4, 5, 6, 7);
        assert!(build_bootstrap_json(&bad).is_err());
    }

    #[test]
    fn bootstrap_json_includes_dynamic_metadata_filters_and_routes() {
        let bootstrap = build_bootstrap(&params()).expect("bootstrap should build");
        let hcm = bootstrap
            .get("static_resources")
            .expect("static_resources")
            .get("listeners")
            .and_then(|l| l.as_array())
            .and_then(|l| l.first())
            .and_then(|l| l.get("filter_chains"))
            .and_then(|fc| fc.as_array())
            .and_then(|fc| fc.first())
            .and_then(|fc| fc.get("filters"))
            .and_then(|f| f.as_array())
            .and_then(|f| f.first())
            .expect("http connection manager filter");
        let typed = hcm.get("typed_config").expect("typed_config");
        let filters = typed
            .get("http_filters")
            .and_then(|f| f.as_array())
            .expect("http_filters");
        let find_filter = |name: &str| {
            filters
                .iter()
                .find(|f| f.get("name").and_then(|n| n.as_str()) == Some(name))
                .unwrap_or_else(|| panic!("filter {name} should be present"))
        };
        let meta_first = find_filter(EXTPROC_META_FIRST_FILTER_NAME);
        let receiving_namespaces = meta_first
            .get("typed_config")
            .and_then(|t| t.get("metadata_options"))
            .and_then(|m| m.get("receiving_namespaces"))
            .and_then(|m| m.get("untyped"))
            .and_then(|v| v.as_array())
            .expect("meta first receiving_namespaces");
        assert_eq!(receiving_namespaces.len(), 1);
        assert_eq!(
            receiving_namespaces[0].as_str(),
            Some("com.conformance.extproc")
        );
        let meta_second = find_filter(EXTPROC_META_SECOND_FILTER_NAME);
        let forwarding_namespaces = meta_second
            .get("typed_config")
            .and_then(|t| t.get("metadata_options"))
            .and_then(|m| m.get("forwarding_namespaces"))
            .and_then(|m| m.get("untyped"))
            .and_then(|v| v.as_array())
            .expect("meta second forwarding_namespaces");
        assert_eq!(forwarding_namespaces.len(), 2);
        assert_eq!(
            forwarding_namespaces[0].as_str(),
            Some("com.conformance.extproc")
        );
        assert_eq!(
            forwarding_namespaces[1].as_str(),
            Some("com.conformance.unallowed")
        );
        let attributes = find_filter(EXTPROC_ATTRIBUTES_FILTER_NAME);
        let request_attributes = attributes
            .get("typed_config")
            .and_then(|t| t.get("request_attributes"))
            .and_then(|v| v.as_array())
            .expect("attributes request_attributes");
        assert_eq!(request_attributes.len(), 5);
        assert_eq!(request_attributes[0].as_str(), Some("request.path"));
        assert_eq!(request_attributes[4].as_str(), Some("xds.route_name"));
        let response_attributes = attributes
            .get("typed_config")
            .and_then(|t| t.get("response_attributes"))
            .and_then(|v| v.as_array())
            .expect("attributes response_attributes");
        assert_eq!(response_attributes.len(), 1);
        assert_eq!(response_attributes[0].as_str(), Some("response.code"));

        let virtual_hosts = typed
            .get("route_config")
            .and_then(|rc| rc.get("virtual_hosts"))
            .and_then(|v| v.as_array())
            .expect("virtual hosts");
        let routes = virtual_hosts[1]
            .get("routes")
            .and_then(|r| r.as_array())
            .expect("routes");
        let prefixes: Vec<&str> = routes
            .iter()
            .map(|r| {
                r.get("match")
                    .and_then(|m| m.get("prefix"))
                    .and_then(|p| p.as_str())
                    .unwrap_or_default()
            })
            .collect();
        assert!(prefixes.contains(&"/profile/metadata-chain"));
        assert!(prefixes.contains(&"/profile/attributes"));

        // Envoy requires the router filter to be the terminal HTTP filter;
        // any ext_proc filter placed after it makes the bootstrap invalid and
        // the whole envoy process fail to start (conformance e2e stall/failure).
        let last_filter = filters
            .last()
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            .expect("last http filter should be the router");
        assert_eq!(last_filter, ROUTER_FILTER_NAME);
    }
}
