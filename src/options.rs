//! Server and processing options, including JSON and YAML file loading.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Options controlling the ext_proc server itself (ports, timeouts, ...).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ServerOptions {
    /// Port the ext_proc gRPC server listens on.
    #[serde(rename = "extproc_port")]
    pub ext_proc_port: u16,
    /// Port the Prometheus metrics HTTP server listens on.
    #[serde(rename = "metrics_http_port")]
    pub metrics_http_port: u16,
    /// Maximum number of concurrent gRPC streams.
    #[serde(rename = "max_concurrent_streams")]
    pub max_concurrent_streams: u32,
    /// Seconds to wait for unready propagation after shutdown begins.
    #[serde(rename = "unready_propagation_delay_seconds")]
    pub unready_propagation_delay_seconds: u32,
    /// Seconds to wait for graceful shutdown before forcing stop.
    #[serde(rename = "termination_grace_period_seconds")]
    pub termination_grace_period_seconds: i32,
}

impl ServerOptions {
    /// Default server options: ext_proc on 50051, metrics on 9090,
    /// 1000 max concurrent streams, 5s unready propagation, 10s grace period.
    pub fn new() -> Self {
        Self {
            ext_proc_port: 50051,
            metrics_http_port: 9090,
            max_concurrent_streams: 1000,
            unready_propagation_delay_seconds: 5,
            termination_grace_period_seconds: 10,
        }
    }

    /// Load server options from a JSON file.
    pub fn from_json<P: AsRef<Path>>(file_path: P) -> Result<Self, crate::Error> {
        let mut file = fs::File::open(file_path)?;
        let opts = serde_json::from_reader(&mut file)?;
        Ok(opts)
    }

    /// Load server options from a YAML file.
    pub fn from_yaml<P: AsRef<Path>>(file_path: P) -> Result<Self, crate::Error> {
        let mut file = fs::File::open(file_path)?;
        let opts = serde_yaml::from_reader(&mut file)?;
        Ok(opts)
    }
}

/// Options controlling a single processor's behavior.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProcessingOptions {
    /// Whether to append an `x-extproc-names` header to responses.
    #[serde(rename = "UpdateExtProcHeader")]
    pub update_ext_proc_header: bool,
    /// Whether to append an `x-extproc-duration-ns` header to responses.
    #[serde(rename = "UpdateDurationHeader")]
    pub update_duration_header: bool,
    /// Whether to abort the stream when the processor fails.
    #[serde(rename = "AbortOnProcessorFailure")]
    pub abort_on_processor_failure: bool,
}

impl ProcessingOptions {
    /// Default processing options: no updates, no aborts.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load processing options from a JSON file.
    pub fn from_json<P: AsRef<Path>>(file_path: P) -> Result<Self, crate::Error> {
        let mut file = fs::File::open(file_path)?;
        let opts = serde_json::from_reader(&mut file)?;
        Ok(opts)
    }

    /// Load processing options from a YAML file.
    pub fn from_yaml<P: AsRef<Path>>(file_path: P) -> Result<Self, crate::Error> {
        let mut file = fs::File::open(file_path)?;
        let opts = serde_yaml::from_reader(&mut file)?;
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_server_options() {
        let opts = ServerOptions::new();
        assert_eq!(opts.ext_proc_port, 50051);
        assert_eq!(opts.metrics_http_port, 9090);
        assert_eq!(opts.max_concurrent_streams, 1000);
        assert_eq!(opts.unready_propagation_delay_seconds, 5);
        assert_eq!(opts.termination_grace_period_seconds, 10);
    }

    #[test]
    fn test_default_processing_options() {
        let opts = ProcessingOptions::new();
        assert!(!opts.update_ext_proc_header);
        assert!(!opts.update_duration_header);
        assert!(!opts.abort_on_processor_failure);
    }

    #[test]
    fn test_server_options_from_json_and_yaml() {
        let dir = std::env::temp_dir();
        let json_path = dir.join("extproc_test_options.json");
        std::fs::write(
            &json_path,
            r#"{"extproc_port": 51001, "metrics_http_port": 9101, "max_concurrent_streams": 11, "unready_propagation_delay_seconds": 3, "termination_grace_period_seconds": 7}"#,
        )
        .unwrap();
        let opts = ServerOptions::from_json(&json_path).unwrap();
        assert_eq!(opts.ext_proc_port, 51001);
        assert_eq!(opts.termination_grace_period_seconds, 7);
        std::fs::remove_file(&json_path).unwrap();

        let yaml_path = dir.join("extproc_test_options.yaml");
        std::fs::write(&yaml_path, "extproc_port: 52002\nmetrics_http_port: 9202\n").unwrap();
        let opts = ServerOptions::from_yaml(&yaml_path).unwrap();
        assert_eq!(opts.ext_proc_port, 52002);
        std::fs::remove_file(&yaml_path).unwrap();

        assert!(ServerOptions::from_json("/tmp/definitely_missing_extproc_options.json").is_err());
        assert!(ServerOptions::from_yaml("/tmp/definitely_missing_extproc_options.yaml").is_err());
    }
}
