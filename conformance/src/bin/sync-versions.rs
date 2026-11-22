//! `conformance sync-versions` command: sync the supported Envoy version
//! manifest from the endoflife.date API.

use std::process::Command;
use std::time::SystemTime;

const DEFAULT_EOL_URL: &str = "https://endoflife.date/api/envoy.json";

fn fetch_eol_json(url: &str) -> Result<String, String> {
    let output = Command::new("curl")
        .args(["-fsSL", url])
        .output()
        .map_err(|e| format!("run curl {url}: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "GET {url} failed ({}): {}",
            output.status,
            stderr.trim()
        ));
    }
    let body =
        String::from_utf8(output.stdout).map_err(|e| format!("decode response from {url}: {e}"))?;
    if body.trim().is_empty() {
        return Err(format!("empty response from {url}"));
    }
    Ok(body)
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
        fetch_eol_json(&url).map_err(|e| format!("sync-versions: {}", e))?
    } else {
        std::fs::read_to_string(&from_file).map_err(|e| format!("read {}: {}", from_file, e))?
    };

    let now = SystemTime::now();
    let canary: Vec<String> = envoy_conformance::envoy_versions::EnvoyVersionManifest::load()
        .map(|m| m.canary)
        .map_err(|e| format!("sync-versions: {}", e))?;
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
