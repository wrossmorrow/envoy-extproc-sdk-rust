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
pub use envoy_runner::{check_docker_available, start_envoy_runner, EnvoyConfig, EnvoyRunner};
pub use envoy_versions::{
    load_envoy_versions, resolve_envoy_version, sync_envoy_versions_from_eol, EnvoyVersionManifest,
};
pub use features::{load_features_manifest, FeatureSpec, FeatureStatus, FeaturesManifest};
pub use processor::{CaseHandler, ConformanceProcessor};
pub use report::{
    build_summary_report, load_results_dir, render_markdown_table, render_step_summary,
    update_conformance_doc, validate_strict, CellStatus, CellSummary, FeatureSummary,
    SummaryReport, TestOutcome, VersionResult, VersionTestRun,
};
pub use scenarios::{register_scenario_handlers, scenario_handler, scenario_handlers};
pub use upstream::{UpstreamEcho, UpstreamServer};
