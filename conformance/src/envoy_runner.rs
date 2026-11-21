//! Live Envoy runner harness: launches Envoy (Docker or native binary) for
//! conformance scenarios and queries its admin interface.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::bootstrap::{build_bootstrap_json, BootstrapParams};
use envoy_extproc_sdk::Error;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes as HyperBytes;
use hyper::http::Uri;
use hyper::StatusCode;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpListener;
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::sleep;

const DOCKER_LISTENER_PORT: u16 = 18080;
const DOCKER_ADMIN_PORT: u16 = 19901;

const READY_TIMEOUT: Duration = Duration::from_secs(45);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(150);

/// Envoy configuration for the conformance harness runner.
pub struct EnvoyConfig {
    pub version: String,
    pub upstream_host: String,
    pub primary_upstream_port: u16,
    pub secondary_upstream_port: u16,
    pub ext_proc_port: u16,
    pub abort_ext_proc_port: u16,
    pub secondary_ext_proc_port: u16,
    pub listener_port: u16,
    pub admin_port: u16,
    /// Envoy bootstrap configuration (JSON) used when launching Envoy.
    pub bootstrap_json: String,
}

/// Manages a live Envoy proxy instance (containerized or via `ENVOY_BIN`) for
/// conformance testing.
pub struct EnvoyRunner {
    version: String,
    container_name: Option<String>,
    child: Option<Child>,
    log_buf: Arc<Mutex<String>>,
    proxy_addr: String,
    admin_addr: String,
}

impl EnvoyRunner {
    /// Envoy version this runner launched.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Host:port address of Envoy's HTTP listener.
    pub fn proxy_addr(&self) -> &str {
        &self.proxy_addr
    }

    /// Base `http://host:port` URL of Envoy's HTTP listener.
    pub fn proxy_url(&self) -> String {
        format!("http://{}", self.proxy_addr)
    }

    /// Base `http://host:port` URL of Envoy's admin interface.
    pub fn admin_url(&self) -> String {
        format!("http://{}", self.admin_addr)
    }

    /// Query Envoy's admin `/stats` endpoint for ext_proc counters and gauges.
    pub async fn ext_proc_stats(&self) -> Result<HashMap<String, u64>, Error> {
        let client = http_client();
        let url = format!("{}/stats?filter=ext_proc&format=json", self.admin_url());
        let payload = get_json(&client, &url).await?;
        let mut out = HashMap::new();
        if let Some(stats) = payload.get("stats").and_then(|v| v.as_array()) {
            for entry in stats {
                let name = match entry.get("name").and_then(|v| v.as_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                if let Some(value) = entry.get("value").and_then(|v| v.as_u64()) {
                    out.insert(name, value);
                }
            }
        }
        Ok(out)
    }

    /// Assert that Envoy's ext_proc filter recorded zero spurious messages.
    pub async fn assert_no_protocol_errors(&self) -> Result<(), Error> {
        let stats = self.ext_proc_stats().await?;
        assert_no_spurious_messages(&stats)
    }

    /// Stop the Envoy container or child process.
    pub async fn close(&mut self) -> Result<(), Error> {
        if let Some(name) = &self.container_name {
            let output = Command::new("docker")
                .args(["rm", "-f", name])
                .output()
                .await
                .map_err(|e| format!("docker rm -f {name} failed: {e}"))?;
            if !output.status.success() {
                return Err(format!(
                    "docker rm -f {name} failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                )
                .into());
            }
            return Ok(());
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        Ok(())
    }
}

fn assert_no_spurious_messages(stats: &HashMap<String, u64>) -> Result<(), Error> {
    let mut checked = 0;
    for (name, value) in stats.iter() {
        if name.ends_with(".spurious_msgs_received") {
            checked += 1;
            if *value != 0 {
                return Err(format!("Envoy stat {name} = {value}, want 0").into());
            }
        }
    }
    if checked == 0 {
        tracing::debug!(
            "no *.spurious_msgs_received counter emitted in Envoy stats ({} stats collected)",
            stats.len()
        );
    }
    Ok(())
}

async fn pick_free_port() -> Result<u16, Error> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("pick free port: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("pick free port: {e}"))?
        .port();
    drop(listener);
    Ok(port)
}

pub async fn check_docker_available() -> Result<(), Error> {
    let output = Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .output()
        .await
        .map_err(|e| format!("docker unavailable: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "docker unavailable: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

fn bootstrap_json_from_cfg(cfg: &EnvoyConfig) -> Result<String, Error> {
    if !cfg.bootstrap_json.is_empty() {
        return Ok(cfg.bootstrap_json.clone());
    }
    build_bootstrap_json(&BootstrapParams {
        version: cfg.version.clone(),
        upstream_host: cfg.upstream_host.clone(),
        primary_upstream_port: cfg.primary_upstream_port,
        secondary_upstream_port: cfg.secondary_upstream_port,
        ext_proc_port: cfg.ext_proc_port,
        abort_ext_proc_port: cfg.abort_ext_proc_port,
        secondary_ext_proc_port: cfg.secondary_ext_proc_port,
        listener_port: cfg.listener_port,
        admin_port: cfg.admin_port,
    })
}

fn http_client() -> Client<HttpConnector, Full<HyperBytes>> {
    Client::builder(TokioExecutor::new()).build_http()
}

async fn get_status(
    client: &Client<HttpConnector, Full<HyperBytes>>,
    url: &str,
) -> Result<StatusCode, Error> {
    let uri: Uri = url.parse().map_err(|e| format!("invalid uri {url}: {e}"))?;
    let resp = client
        .get(uri)
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    Ok(resp.status())
}

async fn get_json(
    client: &Client<HttpConnector, Full<HyperBytes>>,
    url: &str,
) -> Result<serde_json::Value, Error> {
    let uri: Uri = url.parse().map_err(|e| format!("invalid uri {url}: {e}"))?;
    let resp = client
        .get(uri)
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    if resp.status() != StatusCode::OK {
        return Err(format!("GET {url} status {}", resp.status()).into());
    }
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("read body {url}: {e}"))?
        .to_bytes();
    serde_json::from_slice(&body)
        .map_err(|e| format!("decode json {url}: {e}"))
        .map_err(Error::from)
}

async fn wait_ready(runner: &mut EnvoyRunner, timeout: Duration) -> Result<(), Error> {
    let client = http_client();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(child) = runner.child.as_mut() {
            if let Ok(Some(_exit_status)) = child.try_wait() {
                return Err("envoy process exited early".into());
            }
        }
        let url = format!("{}/ready", runner.admin_url());
        let status = get_status(&client, &url).await?;
        if status == StatusCode::OK {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            return Err(format!("timed out waiting for envoy readiness at {url}").into());
        }
        sleep(READY_POLL_INTERVAL).await;
    }
}

async fn resolve_docker_port(container_name: &str, container_port: u16) -> Result<u16, Error> {
    let output = Command::new("docker")
        .args(["port", container_name, &format!("{container_port}/tcp")])
        .output()
        .await
        .map_err(|e| format!("docker port failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "docker port {} for {}: {}",
            container_port,
            container_name,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let first_line = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string();
    let (_host, port_str) = first_line
        .rsplit_once(':')
        .ok_or(format!("could not parse docker port output {first_line}"))?;
    let port: u16 = port_str
        .parse()
        .map_err(|e| format!("parse docker port output {first_line}: {e}"))?;
    Ok(port)
}

async fn start_binary_envoy(
    envoy_bin: &str,
    cfg: &EnvoyConfig,
    version: &str,
) -> Result<EnvoyRunner, Error> {
    let listener_port = if cfg.listener_port == 0 {
        pick_free_port().await?
    } else {
        cfg.listener_port
    };
    let admin_port = if cfg.admin_port == 0 {
        pick_free_port().await?
    } else {
        cfg.admin_port
    };

    let bootstrap_json = bootstrap_json_from_cfg(cfg)?;
    let mut cmd = Command::new(envoy_bin);
    cmd.args(["--config-yaml", &bootstrap_json])
        .args(["--log-level", "warn"])
        .args(["--concurrency", "2"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("start {envoy_bin}: {e}"))?;
    let log_buf = Arc::new(Mutex::new(String::new()));
    let log_buf_clone = Arc::clone(&log_buf);
    let stdout_half = child.stdout.take();
    let stderr_half = child.stderr.take();
    tokio::task::spawn(async move {
        let mut out = String::new();
        let mut err = String::new();
        if let Some(mut half) = stdout_half {
            let mut lines = tokio::io::BufReader::new(&mut half).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                out.push_str(&line);
                out.push('\n');
            }
        }
        if let Some(mut half) = stderr_half {
            let mut lines = tokio::io::BufReader::new(&mut half).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                err.push_str(&line);
                err.push('\n');
            }
        }
        let mut buf = log_buf_clone.lock().unwrap();
        *buf = format!("{out}\n{err}");
    });

    let mut runner = EnvoyRunner {
        version: version.to_string(),
        container_name: None,
        child: Some(child),
        log_buf,
        proxy_addr: format!("127.0.0.1:{listener_port}"),
        admin_addr: format!("127.0.0.1:{admin_port}"),
    };

    if let Err(e) = wait_ready(&mut runner, READY_TIMEOUT).await {
        let logs = runner.log_buf.lock().unwrap().to_string();
        let _ = runner.close().await;
        return Err(format!("envoy binary failed readiness: {e}\nlogs:\n{logs}").into());
    }
    Ok(runner)
}

async fn start_docker_envoy(version: &str, cfg: &EnvoyConfig) -> Result<EnvoyRunner, Error> {
    check_docker_available().await?;
    let bootstrap_json = bootstrap_json_from_cfg(cfg)?;
    let container_name = format!("extproc-conformance-{}", std::process::id());
    let run_cmd = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &container_name,
            "--add-host",
            "host.docker.internal:host-gateway",
        ])
        .args(["-p", &format!("127.0.0.1::{}", DOCKER_LISTENER_PORT)])
        .args(["-p", &format!("127.0.0.1::{}", DOCKER_ADMIN_PORT)])
        .arg("envoyproxy/envoy")
        .args(["envoy", "--config-yaml", &bootstrap_json])
        .args(["--log-level", "warn"])
        .args(["--concurrency", "2"])
        .output()
        .await
        .map_err(|e| format!("docker run failed: {e}"))?;
    if !run_cmd.status.success() {
        return Err(format!(
            "docker run envoyproxy/envoy:{version} failed: {}",
            String::from_utf8_lossy(&run_cmd.stderr)
        )
        .into());
    }

    let mut runner = EnvoyRunner {
        version: version.to_string(),
        container_name: Some(container_name),
        child: None,
        log_buf: Arc::new(Mutex::new(String::new())),
        proxy_addr: String::new(),
        admin_addr: String::new(),
    };
    let container_name = runner.container_name.as_deref().unwrap().to_string();
    let listener_port = resolve_docker_port(&container_name, DOCKER_LISTENER_PORT).await?;
    let admin_port = resolve_docker_port(&container_name, DOCKER_ADMIN_PORT).await?;
    runner.proxy_addr = format!("127.0.0.1:{listener_port}");
    runner.admin_addr = format!("127.0.0.1:{admin_port}");

    if let Err(e) = wait_ready(&mut runner, READY_TIMEOUT).await {
        let logs_out = Command::new("docker")
            .args(["logs", &container_name])
            .output()
            .await
            .map_err(|e| format!("docker logs failed: {e}"))?;
        let _ = runner.close().await;
        return Err(format!(
            "envoy container {container_name} failed readiness: {e}\nlogs:\n{}",
            String::from_utf8_lossy(&logs_out.stdout)
        )
        .into());
    }
    Ok(runner)
}

/// Start an Envoy runner. Uses the binary named by `ENVOY_BIN` when set,
/// otherwise launches the `envoyproxy/envoy:<version>` Docker image.
pub async fn start_envoy_runner(cfg: &EnvoyConfig) -> Result<EnvoyRunner, Error> {
    let version = if cfg.version.is_empty() {
        envoy_version_from_env().await?
    } else {
        cfg.version.clone()
    };

    if let Ok(envoy_bin) = std::env::var("ENVOY_BIN") {
        if !envoy_bin.is_empty() {
            return start_binary_envoy(&envoy_bin, cfg, &version).await;
        }
    }
    start_docker_envoy(&version, cfg).await
}

async fn envoy_version_from_env() -> Result<String, Error> {
    if let Ok(v) = std::env::var("ENVOY_VERSION") {
        if !v.is_empty() {
            return Ok(v);
        }
    }
    Ok("v1.40-latest".to_string())
}
