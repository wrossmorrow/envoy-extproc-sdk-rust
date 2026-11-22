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

fn strip_latest_suffix(tag: &str) -> &str {
    tag.strip_suffix("-latest").unwrap_or(tag)
}

fn replace_between(text: &str, prefix: &str, suffix: &str, replacement_value: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(prefix) {
        let after_prefix = &rest[start + prefix.len()..];
        let line_end = after_prefix
            .find(['\n', '\r'])
            .unwrap_or(after_prefix.len());
        if let Some(rel_end) = after_prefix[..line_end].find(suffix) {
            out.push_str(&rest[..start + prefix.len()]);
            out.push_str(replacement_value);
            out.push_str(suffix);
            rest = &after_prefix[rel_end + suffix.len()..];
        } else {
            out.push_str(&rest[..start + prefix.len()]);
            rest = after_prefix;
        }
    }
    out.push_str(rest);
    out
}

fn replace_until_eol(text: &str, prefix: &str, replacement_value: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(prefix) {
        let after_prefix = &rest[start + prefix.len()..];
        let line_end = after_prefix
            .find(['\n', '\r'])
            .unwrap_or(after_prefix.len());
        out.push_str(&rest[..start + prefix.len()]);
        out.push_str(replacement_value);
        rest = &after_prefix[line_end..];
    }
    out.push_str(rest);
    out
}

fn is_minor_cycle(s: &str) -> bool {
    let Some((maj, min)) = s.strip_prefix('v').and_then(|v| v.split_once('.')) else {
        return false;
    };
    !maj.is_empty()
        && !min.is_empty()
        && maj.bytes().all(|b| b.is_ascii_digit())
        && min.bytes().all(|b| b.is_ascii_digit())
}

fn sync_matrix_range(text: &str, first_cycle: &str, last_cycle: &str) -> String {
    const START_MARKER: &str = "| `";
    const MID_MARKER: &str = "` \u{2013} `";
    const END_MARKER: &str = "` + `dev` |";

    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(START_MARKER) {
        let after_start = &rest[start + START_MARKER.len()..];
        let line_end = after_start.find(['\n', '\r']).unwrap_or(after_start.len());
        let line_slice = &after_start[..line_end];
        if let Some(end_rel) = line_slice.find(END_MARKER) {
            let between = &line_slice[..end_rel];
            if let Some((first, second)) = between.split_once(MID_MARKER) {
                if is_minor_cycle(first) && is_minor_cycle(second) {
                    out.push_str(&rest[..start]);
                    out.push_str(&format!(
                        "| `{first_cycle}` \u{2013} `{last_cycle}` + `dev` |"
                    ));
                    rest = &after_start[end_rel + END_MARKER.len()..];
                    continue;
                }
            }
        }
        out.push_str(&rest[..start + START_MARKER.len()]);
        rest = after_start;
    }
    out.push_str(rest);
    out
}

/// Update `compose.yaml` default `ENVOY_VERSION` image tags to match `manifest.default`.
pub fn sync_compose_yaml(content: &str, manifest: &EnvoyVersionManifest) -> String {
    let default = manifest.default.trim();
    if default.is_empty() {
        return content.to_string();
    }
    replace_between(content, "envoyproxy/envoy:${ENVOY_VERSION:-", "}", default)
}

/// Update `CONFORMANCE.md` default Envoy tag, integration/conformance examples,
/// and supported minor-release range to match `manifest`.
pub fn sync_conformance_doc(content: &str, manifest: &EnvoyVersionManifest) -> String {
    let mut updated = content.to_string();
    let default = manifest.default.trim();
    if !default.is_empty() {
        updated = replace_between(&updated, "envoyproxy/envoy:${ENVOY_VERSION:-", "}", default);
        updated = replace_between(&updated, "defaults to `ENVOY_VERSION=", "`", default);
        updated = replace_between(
            &updated,
            "ENVOY_VERSION=",
            " just integration-test",
            default,
        );
        updated = replace_until_eol(&updated, "just conformance-version ", default);
    }
    if let (Some(first), Some(last)) = (manifest.supported.first(), manifest.supported.last()) {
        updated = sync_matrix_range(
            &updated,
            strip_latest_suffix(first),
            strip_latest_suffix(last),
        );
    }
    updated
}

/// Update `Justfile` `conformance-version` default argument and example comment
/// to match `manifest.default`.
pub fn sync_justfile(content: &str, manifest: &EnvoyVersionManifest) -> String {
    let default = manifest.default.trim();
    if default.is_empty() {
        return content.to_string();
    }
    let updated = replace_between(
        content,
        "conformance-version envoy_version=\"",
        "\"",
        default,
    );
    replace_between(&updated, "specific Envoy image tag (e.g. ", ")", default)
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

    #[test]
    fn test_compose_docs_and_justfile_match_default_envoy_version() {
        let manifest = EnvoyVersionManifest::load().expect("load envoy-versions.json");
        let default_version = manifest.default.as_str();
        assert!(!default_version.is_empty());

        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("conformance parent should be repo root");

        let expected_image = format!("envoyproxy/envoy:${{ENVOY_VERSION:-{default_version}}}");
        for rel in ["examples/compose.yaml", "tests/compose.yaml"] {
            let compose = std::fs::read_to_string(repo_root.join(rel))
                .unwrap_or_else(|e| panic!("read {rel}: {e}"));
            assert!(
                compose.contains(&expected_image),
                "{rel} missing {expected_image}"
            );
        }

        let conformance_doc =
            std::fs::read_to_string(repo_root.join("CONFORMANCE.md")).expect("read CONFORMANCE.md");
        assert!(
            conformance_doc.contains(&expected_image),
            "CONFORMANCE.md missing {expected_image}"
        );
        assert!(
            conformance_doc.contains(&format!("defaults to `ENVOY_VERSION={default_version}`")),
            "CONFORMANCE.md default ENVOY_VERSION prose out of sync"
        );
        assert!(
            conformance_doc.contains(&format!(
                "ENVOY_VERSION={default_version} just integration-test"
            )),
            "CONFORMANCE.md ENVOY_VERSION example out of sync"
        );
        assert!(
            conformance_doc.contains(&format!("just conformance-version {default_version}")),
            "CONFORMANCE.md conformance-version example out of sync"
        );
        let first_cycle = strip_latest_suffix(
            manifest
                .supported
                .first()
                .expect("supported should not be empty"),
        );
        let last_cycle = strip_latest_suffix(
            manifest
                .supported
                .last()
                .expect("supported should not be empty"),
        );
        let expected_range = format!("| `{first_cycle}` \u{2013} `{last_cycle}` + `dev` |");
        assert!(
            conformance_doc.contains(&expected_range),
            "CONFORMANCE.md missing supported range {expected_range}"
        );

        let justfile = std::fs::read_to_string(repo_root.join("Justfile")).expect("read Justfile");
        assert!(
            justfile.contains(&format!(
                "conformance-version envoy_version=\"{default_version}\""
            )),
            "Justfile conformance-version default out of sync"
        );
    }

    #[test]
    fn test_sync_helpers_rewrite_compose_docs_and_justfile() {
        let manifest = EnvoyVersionManifest {
            default: "v1.40-latest".to_string(),
            supported: vec![
                "v1.37-latest".to_string(),
                "v1.38-latest".to_string(),
                "v1.39-latest".to_string(),
                "v1.40-latest".to_string(),
            ],
            canary: vec!["dev".to_string()],
        };

        let old_compose = "  envoy:\n    image: envoyproxy/envoy:${ENVOY_VERSION:-v1.26-latest}\n";
        let updated_compose = sync_compose_yaml(old_compose, &manifest);
        assert_eq!(
            updated_compose,
            "  envoy:\n    image: envoyproxy/envoy:${ENVOY_VERSION:-v1.40-latest}\n"
        );
        assert_eq!(
            sync_compose_yaml(&updated_compose, &manifest),
            updated_compose
        );

        let old_doc = concat!(
            "| Routine CI & Examples Envoy | `envoyproxy/envoy:${ENVOY_VERSION:-v1.26-latest}` | note |\n",
            "| Multi-Version Matrix | `v1.35` \u{2013} `v1.38` + `dev` | notes |\n",
            "Run (defaults to `ENVOY_VERSION=v1.26-latest`):\n",
            "ENVOY_VERSION=v1.26-latest just integration-test\n",
            "just conformance-version v1.26-latest\n",
        );
        let updated_doc = sync_conformance_doc(old_doc, &manifest);
        assert_eq!(
            updated_doc,
            concat!(
                "| Routine CI & Examples Envoy | `envoyproxy/envoy:${ENVOY_VERSION:-v1.40-latest}` | note |\n",
                "| Multi-Version Matrix | `v1.37` \u{2013} `v1.40` + `dev` | notes |\n",
                "Run (defaults to `ENVOY_VERSION=v1.40-latest`):\n",
                "ENVOY_VERSION=v1.40-latest just integration-test\n",
                "just conformance-version v1.40-latest\n",
            )
        );
        assert_eq!(sync_conformance_doc(&updated_doc, &manifest), updated_doc);

        let old_just = concat!(
            "# Run the Envoy conformance suite against a specific Envoy image tag (e.g. v1.26-latest).\n",
            "conformance-version envoy_version=\"v1.26-latest\" *flags=\"-v\":\n",
        );
        let updated_just = sync_justfile(old_just, &manifest);
        assert_eq!(
            updated_just,
            concat!(
                "# Run the Envoy conformance suite against a specific Envoy image tag (e.g. v1.40-latest).\n",
                "conformance-version envoy_version=\"v1.40-latest\" *flags=\"-v\":\n",
            )
        );
        assert_eq!(sync_justfile(&updated_just, &manifest), updated_just);
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
