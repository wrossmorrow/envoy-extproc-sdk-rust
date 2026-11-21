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
        5,
        false,
        false,
        &default_mode,
    );

    let default_cluster = cluster_json(
        UPSTREAM_PRIMARY_CLUSTER,
        &params.upstream_host,
        params.primary_upstream_port,
    );
    let secondary_cluster = cluster_json(
        UPSTREAM_SECONDARY_CLUSTER,
        &params.upstream_host,
        params.secondary_upstream_port,
    );
    let ext_proc_cluster = cluster_json(
        DEFAULT_EXT_PROC_CLUSTER,
        &params.upstream_host,
        params.ext_proc_port,
    );
    let abort_ext_proc_cluster = cluster_json(
        ABORT_EXT_PROC_CLUSTER,
        &params.upstream_host,
        params.abort_ext_proc_port,
    );
    let secondary_ext_proc_cluster = cluster_json(
        SECONDARY_EXT_PROC_CLUSTER,
        &params.upstream_host,
        params.secondary_ext_proc_port,
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
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.short_timeout_fail_open",
        DEFAULT_EXT_PROC_CLUSTER,
        true,
        150,
        false,
        true,
        &default_mode,
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.abort",
        ABORT_EXT_PROC_CLUSTER,
        false,
        5,
        false,
        true,
        &default_mode,
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.chain_first",
        DEFAULT_EXT_PROC_CLUSTER,
        false,
        5,
        false,
        true,
        &default_mode,
    ));
    http_filters.push(ext_proc_filter_json(
        "envoy.filters.http.ext_proc.chain_second",
        SECONDARY_EXT_PROC_CLUSTER,
        false,
        5,
        false,
        true,
        &default_mode,
    ));
    http_filters.push(ext_proc_filter_json(
        OBSERVABILITY_FILTER_NAME,
        DEFAULT_EXT_PROC_CLUSTER,
        true,
        5,
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
                    "address": "127.0.0.1",
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

fn ext_proc_filter_json(
    name: &str,
    cluster_name: &str,
    failure_mode_allow: bool,
    message_timeout_seconds: u64,
    observability_mode: bool,
    disabled: bool,
    mode: &Value,
) -> Value {
    let _ = disabled;
    json!({
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
                "seconds": message_timeout_seconds,
            },
            "mutation_rules": {
                "allow_all_routing": true,
            },
            "processing_mode": mode,
        },
    })
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

fn http_connection_manager_json(mode: &Value, http_filters: &[Value]) -> Value {
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
                    "name": "conformance_vhost",
                    "domains": ["*"],
                    "routes": [
                        route_json("/mode/disabled", mode, Some(false), true, UPSTREAM_PRIMARY_CLUSTER),
                        route_json("/mode/buffered", mode, Some(false), false, UPSTREAM_PRIMARY_CLUSTER),
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
                        route_json(
                            "/profile/timeout-fail-closed",
                            mode,
                            Some(false),
                            true,
                            UPSTREAM_PRIMARY_CLUSTER,
                        ),
                        route_json(
                            "/profile/timeout-fail-open",
                            mode,
                            Some(true),
                            true,
                            UPSTREAM_PRIMARY_CLUSTER,
                        ),
                        route_json(
                            "/profile/abort-fail-closed",
                            mode,
                            Some(false),
                            true,
                            UPSTREAM_PRIMARY_CLUSTER,
                        ),
                        route_json(
                            "/profile/abort-fail-open",
                            mode,
                            Some(true),
                            true,
                            UPSTREAM_PRIMARY_CLUSTER,
                        ),
                        route_json(
                            "/profile/chained",
                            mode,
                            Some(false),
                            true,
                            UPSTREAM_PRIMARY_CLUSTER,
                        ),
                        route_json(
                            "/profile/observability",
                            &processing_mode_json(
                                OBSERVED_HEADER_SEND,
                                OBSERVED_HEADER_SEND,
                                OBSERVED_BODY_STREAMED,
                                OBSERVED_BODY_STREAMED,
                                OBSERVED_HEADER_SEND,
                                OBSERVED_HEADER_SEND,
                            ),
                            Some(true),
                            true,
                            UPSTREAM_PRIMARY_CLUSTER,
                        ),
                        {
                            "match": {
                                "prefix": "/",
                            },
                            "route": {
                                "cluster": UPSTREAM_PRIMARY_CLUSTER,
                            },
                        },
                    ],
                },
            ],
        },
        "http_filters": http_filters,
    })
}

fn cluster_json(name: &str, host: &str, port: u16) -> Value {
    json!({
        "name": name,
        "connect_timeout": {
            "seconds": 2,
        },
        "lb_policy": "ROUND_ROBIN",
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
        assert_eq!(filters.len(), 8);
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
        assert_eq!(virtual_hosts.len(), 1);
        let vhost = &virtual_hosts[0];
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
}
