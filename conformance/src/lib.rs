//! Envoy conformance harness and reporting tooling for the
//! [`envoy_extproc_sdk`] crate.
//!
//! This crate hosts the purpose-built conformance suite pieces that are
//! independent of running a live Envoy proxy: the supported-version manifest,
//! the feature coverage manifest, and the results aggregation / reporting
//! tools. Live Envoy scenario tests drive the SDK through an in-process
//! [`envoy_extproc_sdk::GenericExtProcServer`] against a running Envoy
//! instance.

pub mod bootstrap;
pub mod envoy_runner;
pub mod envoy_versions;
pub mod features;
pub mod processor;
pub mod report;
pub mod scenarios;
pub mod upstream;

pub use bootstrap::{build_bootstrap, build_bootstrap_json, BootstrapParams};
pub use envoy_runner::{
    check_docker_available, start_envoy_runner, upstream_host_for_runner, EnvoyConfig, EnvoyRunner,
};
pub use envoy_versions::{
    load_envoy_versions, resolve_envoy_version, sync_compose_yaml, sync_conformance_doc,
    sync_envoy_versions_from_eol, sync_justfile, EnvoyVersionManifest,
};
pub use features::{load_features_manifest, FeatureSpec, FeatureStatus, FeaturesManifest};
pub use processor::{
    start_conformance_extproc_server, CaseHandler, ConformanceExtProcServer, ConformanceProcessor,
};
pub use report::{
    build_summary_report, load_results_dir, render_markdown_table, render_step_summary,
    update_conformance_doc, validate_strict, CellStatus, CellSummary, FeatureSummary,
    SummaryReport, TestOutcome, VersionResult, VersionTestRun,
};
pub use scenarios::{
    buffered_body_replace_grow_and_shrink_handler, cel_attributes_persist_handler,
    dynamic_metadata_emitter_handler, dynamic_metadata_immediate_deny_handler,
    dynamic_metadata_receiver_handler, get_nobody_mode_none_handler,
    immediate_response_request_headers_handler, mode_override_allowlist_denied_handler,
    mode_override_allowlist_ok_handler, mode_override_default_from_protocol_config_handler,
    mode_override_disabled_handler, mode_override_full_resp_handler, mode_override_handler,
    mode_override_resp_stream_handler, mode_override_skip_handler,
    post_with_body_mode_none_handler, protocol_config_handler, register_scenario_handlers,
    scenario_handler, scenario_handlers, timeout_override_exceed_handler,
    timeout_override_extend_handler, timeout_override_multi_handler,
    timeout_override_no_max_handler,
};
pub use upstream::{UpstreamEcho, UpstreamServer};
