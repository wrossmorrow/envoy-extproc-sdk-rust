//! `conformance sync-versions` command: sync the supported Envoy version
//! manifest from the endoflife.date API.

use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DEFAULT_EOL_URL: &str = "https://endoflife.date/api/envoy.json";

async fn fetch_eol_json(url: &str) -> Result<String, String> {
    // Minimal HTTP/1.1 GET over a TCP stream; no extra HTTP client dependency.
    let authority = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("endoflife.date")
        .to_string();
    let path = if url.contains('/') {
        url.split("://").nth(1).unwrap_or(url).to_string()
    } else {
        "/api/envoy.json".to_string()
    };
    let port = url
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split(':').next())
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(443);
    let addr = format!("{}:{}", authority, port);
    let host_res = std::net::ToSocketAddrs::to_socket_addrs(&addr)
        .map_err(|e| format!("resolve {}: {}", addr, e))?
        .next()
        .ok_or_else(|| format!("could not resolve {}", addr))?;

    let mut stream = tokio::net::TcpStream::connect((host_res.ip().to_string(), port))
        .await
        .map_err(|e| format!("connect {}: {}", addr, e))?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: envoy-conformance-sync/0.1\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        path, authority
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write: {}", e))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|e| format!("read: {}", e))?;
    let text = String::from_utf8_lossy(&response).to_string();
    let status_line = text
        .lines()
        .next()
        .ok_or_else(|| "empty response".to_string())?;
    if !status_line.contains(" 200 ") {
        return Err(format!("GET {} returned {}", path, status_line));
    }
    let body_start = text
        .find("\r\n\r\n")
        .ok_or_else(|| "malformed response".to_string())?;
    Ok(text[body_start + 4..].trim().to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut url = DEFAULT_EOL_URL.to_string();
    let mut from_file = String::new();
    let mut write_path = String::from("envoy-versions.json");

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let (key, value) = if let Some(rest) = arg.strip_prefix("--") {
            if let Some(eq) = rest.find('=') {
                (rest[..eq].to_string(), Some(rest[eq + 1..].to_string()))
            } else {
                (rest.to_string(), iter.next().map(|s| s.to_string()))
            }
        } else {
            (arg.clone(), None)
        };
        match key.as_str() {
            "url" => url = value.unwrap_or_else(|| DEFAULT_EOL_URL.to_string()),
            "from-file" => from_file = value.unwrap_or_else(String::new),
            "write" => write_path = value.unwrap_or_else(|| "envoy-versions.json".to_string()),
            other => {
                eprintln!("sync-versions: unknown flag {}", other);
                std::process::exit(2);
            }
        }
    }

    let eol_json = if from_file.is_empty() {
        let rt = tokio::runtime::Runtime::new().map_err(|e| format!("build runtime: {}", e))?;
        rt.block_on(async { fetch_eol_json(&url).await })
            .map_err(|e| format!("sync-versions: {}", e))?
    } else {
        std::fs::read_to_string(&from_file).map_err(|e| format!("read {}: {}", from_file, e))?
    };

    let now = SystemTime::now();
    let canary: Vec<String> = if from_file.is_empty() {
        envoy_conformance::envoy_versions::EnvoyVersionManifest::load()
            .map(|m| m.canary)
            .map_err(|e| format!("sync-versions: {}", e))?
    } else {
        Vec::new()
    };
    let manifest =
        envoy_conformance::envoy_versions::sync_envoy_versions_from_eol(&eol_json, now, &canary)
            .map_err(|e| format!("sync-versions: {}", e))?;
    let out = serde_json::to_string_pretty(&manifest).map_err(|e| format!("marshal: {}", e))?;
    std::fs::write(&write_path, format!("{}\n", out))
        .map_err(|e| format!("write {}: {}", write_path, e))?;
    println!(
        "synced {}: default={} supported={:?} canary={:?}",
        write_path, manifest.default, manifest.supported, manifest.canary
    );
    Ok(())
}
