//! Envoy version manifest and end-of-life based version sync.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvoyVersionManifest {
    pub default: String,
    pub supported: Vec<String>,
    pub canary: Vec<String>,
}

impl EnvoyVersionManifest {
    /// Load the embedded `envoy-versions.json` manifest.
    pub fn load() -> Result<Self, String> {
        let raw = include_str!("../envoy-versions.json");
        let manifest: EnvoyVersionManifest =
            serde_json::from_str(raw).map_err(|e| format!("parse envoy-versions.json: {}", e))?;
        if manifest.default.trim().is_empty() {
            return Err("envoy-versions.json: default version is empty".to_string());
        }
        Ok(manifest)
    }

    /// Resolve the Envoy version to run: `ENVOY_VERSION` when set, else the
    /// manifest default.
    pub fn resolve(envoy_version: Option<&str>) -> Result<String, String> {
        if let Some(v) = envoy_version {
            if !v.trim().is_empty() {
                return Ok(v.trim().to_string());
            }
        }
        Ok(EnvoyVersionManifest::load()?.default)
    }
}

fn is_cycle_active(raw_eol: &str, today_utc: &str) -> Result<bool, String> {
    let trimmed = raw_eol.trim();
    if trimmed.is_empty() || trimmed == "false" || trimmed == "null" {
        return Ok(true);
    }
    if trimmed == "true" {
        return Ok(false);
    }
    if trimmed.is_empty() {
        return Ok(true);
    }
    Ok(trimmed > today_utc)
}

fn parse_cycle_major_minor(cycle: &str) -> Option<(i64, i64, bool)> {
    let parts: Vec<&str> = cycle.trim().trim_start_matches('v').split('.').collect();
    if parts.len() != 2 {
        return None;
    }
    let major = parts[0].parse::<i64>().ok()?;
    let minor = parts[1].parse::<i64>().ok()?;
    Some((major, minor, true))
}

/// Load the embedded `envoy-versions.json` manifest.
pub fn load_envoy_versions() -> Result<EnvoyVersionManifest, String> {
    EnvoyVersionManifest::load()
}

/// Resolve the Envoy version to run: `ENVOY_VERSION` when set, else the
/// manifest default.
pub fn resolve_envoy_version(envoy_version: Option<&str>) -> Result<String, String> {
    EnvoyVersionManifest::resolve(envoy_version)
}

fn utc_today(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let days = (secs / 86_400) as i64;
    // Howard Hinnant's civil_from_days algorithm, valid for the proleptic
    // Gregorian calendar.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as i64; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", year, m, d)
}

/// Sync the Envoy version manifest from an `endoflife.date` style JSON payload,
/// retaining cycles whose EOL has not yet passed as of `now`.
pub fn sync_envoy_versions_from_eol(
    eol_json: &str,
    now: SystemTime,
    canary: &[String],
) -> Result<EnvoyVersionManifest, String> {
    let entries: serde_json::Value = serde_json::from_str(eol_json)
        .map_err(|e| format!("parse endoflife.date envoy JSON: {}", e))?;
    let entries = entries
        .as_array()
        .ok_or_else(|| "endoflife.date envoy JSON: expected an array of cycles".to_string())?;

    let today_utc = utc_today(now);
    let mut active: Vec<(i64, i64, String)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for entry in entries {
        let cycle = entry.get("cycle").and_then(|v| v.as_str()).ok_or_else(|| {
            "endoflife.date envoy JSON: cycle entry missing string `cycle`".to_string()
        })?;
        let eol_value = entry.get("eol");
        let eol = match eol_value {
            Some(serde_json::Value::Bool(b)) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Null) => "null".to_string(),
            Some(_) => {
                return Err(format!(
                    "endoflife.date envoy JSON: cycle {} has invalid `eol`",
                    cycle
                ))
            }
            None => {
                return Err(format!(
                    "endoflife.date envoy JSON: cycle {} missing `eol`",
                    cycle
                ))
            }
        };
        let is_active = is_cycle_active(&eol, &today_utc)?;
        if !is_active {
            continue;
        }
        let (major, minor, ok) = parse_cycle_major_minor(cycle)
            .ok_or_else(|| format!("endoflife.date envoy JSON: cannot parse cycle {:?}", cycle))?;
        if !ok {
            continue;
        }
        let tag = format!("v{}.{}-latest", major, minor);
        if !seen.insert(tag.clone()) {
            continue;
        }
        active.push((major, minor, tag));
    }

    if active.is_empty() {
        return Err(format!(
            "no active Envoy release cycles found as of {}",
            today_utc
        ));
    }

    active.sort_by(|a, b| {
        if a.0 != b.0 {
            a.0.cmp(&b.0)
        } else {
            a.1.cmp(&b.1)
        }
    });

    let supported: Vec<String> = active.iter().map(|c| c.2.clone()).collect();
    let mut canary_copy = canary.to_vec();
    if canary_copy.is_empty() {
        canary_copy = vec!["dev".to_string()];
    }

    Ok(EnvoyVersionManifest {
        default: supported[supported.len() - 1].clone(),
        supported,
        canary: canary_copy,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_envoy_version_manifest() {
        let manifest = EnvoyVersionManifest::load().expect("load envoy-versions.json");
        assert!(manifest.supported.contains(&manifest.default));
        for want in [
            "v1.37-latest",
            "v1.38-latest",
            "v1.39-latest",
            "v1.40-latest",
        ] {
            assert!(
                manifest.supported.contains(&want.to_string()),
                "supported versions {:?} missing {}",
                manifest.supported,
                want
            );
        }
        assert!(manifest.canary.contains(&"dev".to_string()));

        assert_eq!(
            resolve_envoy_version(Some("v1.39-latest")).unwrap(),
            "v1.39-latest"
        );
        assert_eq!(resolve_envoy_version(Some("")).unwrap(), manifest.default);
        assert_eq!(resolve_envoy_version(None).unwrap(), manifest.default);
    }

    #[test]
    fn test_sync_envoy_versions_from_eol() {
        let payload = r#"[
            {"cycle":"1.40","eol":false},
            {"cycle":"1.39","eol":"2027-07-14"},
            {"cycle":"1.38","eol":"2027-04-23"},
            {"cycle":"1.37","eol":"2027-01-13"},
            {"cycle":"1.36","eol":"2026-10-14"},
            {"cycle":"1.9","eol":"2020-04-01"}
        ]"#;
        let now = UNIX_EPOCH + std::time::Duration::from_secs(1_794_969_600);
        let got = sync_envoy_versions_from_eol(payload, now, &["dev".to_string()])
            .expect("sync envoy versions");
        let want_supported = vec![
            "v1.37-latest".to_string(),
            "v1.38-latest".to_string(),
            "v1.39-latest".to_string(),
            "v1.40-latest".to_string(),
        ];
        assert_eq!(got.supported, want_supported);
        assert_eq!(got.default, "v1.40-latest");
        assert_eq!(got.canary, vec!["dev".to_string()]);
    }
}

#[cfg(test)]
mod tmp_tests {
    use super::*;
    #[test]
    fn tmp_utc_today_check() {
        let now = UNIX_EPOCH + std::time::Duration::from_secs(1_794_969_600);
        println!("today = {}", utc_today(now));
        println!(
            "active(2026-10-14, today) = {}",
            is_cycle_active("2026-10-14", &utc_today(now)).unwrap()
        );
        println!(
            "active(2027-07-14, today) = {}",
            is_cycle_active("2027-07-14", &utc_today(now)).unwrap()
        );
    }
}
