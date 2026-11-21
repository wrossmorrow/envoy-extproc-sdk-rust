//! Conformance results aggregation and reporting.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub const CONFORMANCE_MATRIX_START_MARKER: &str = "<!-- BEGIN GENERATED CONFORMANCE MATRIX -->";
pub const CONFORMANCE_MATRIX_END_MARKER: &str = "<!-- END GENERATED CONFORMANCE MATRIX -->";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TestOutcome {
    Pass,
    Fail,
    Skip,
}

impl TestOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            TestOutcome::Pass => "pass",
            TestOutcome::Fail => "fail",
            TestOutcome::Skip => "skip",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VersionTestRun {
    #[serde(default)]
    pub outcomes: HashMap<String, TestOutcome>,
    #[serde(default)]
    pub package_failed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionResult {
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run: Option<VersionTestRun>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CellStatus {
    Pass,
    Fail,
    Partial,
    KnownGap,
    NotRun,
    Unsupported,
}

impl CellStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CellStatus::Pass => "pass",
            CellStatus::Fail => "fail",
            CellStatus::Partial => "partial",
            CellStatus::KnownGap => "known_gap",
            CellStatus::NotRun => "not_run",
            CellStatus::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellSummary {
    pub status: CellStatus,
    pub display: String,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub missing: usize,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSummary {
    pub id: String,
    pub category: String,
    pub title: String,
    pub sdk_status: String,
    pub sdk_surface: String,
    pub notes: String,
    pub results: HashMap<String, CellSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryReport {
    pub versions: Vec<String>,
    pub features: Vec<FeatureSummary>,
}

/// Parse a JSON-lines stream of `go test -json`-style events into a
/// per-version test run outcome map.
pub fn parse_version_results(json_lines: &str) -> Result<VersionTestRun, String> {
    let mut run = VersionTestRun::default();
    for line in json_lines.lines() {
        let line = line.trim();
        if line.is_empty() || !line.starts_with('{') {
            continue;
        }
        let event: serde_json::Value =
            serde_json::from_str(line).map_err(|e| format!("parse test event {}: {}", line, e))?;
        let action = event
            .get("Action")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let test = event
            .get("Test")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if test.is_empty() {
            if action == "fail" {
                run.package_failed = true;
            }
            continue;
        }
        let outcome = match action {
            "pass" => Some(TestOutcome::Pass),
            "fail" => Some(TestOutcome::Fail),
            "skip" => Some(TestOutcome::Skip),
            _ => None,
        };
        if let Some(outcome) = outcome {
            run.outcomes.insert(test.to_string(), outcome);
        }
    }
    Ok(run)
}

/// Load every `results-<version>.json` file in `dir`, ordered by version order
/// followed by any extras sorted lexicographically.
pub fn load_results_dir(
    dir: &Path,
    version_order: &[String],
) -> Result<Vec<VersionResult>, String> {
    let entries =
        fs::read_dir(dir).map_err(|e| format!("read results dir {}: {}", dir.display(), e))?;
    let mut by_version: HashMap<String, VersionResult> = HashMap::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("read results dir entry: {}", e))?;
        if entry.file_type().map(|t| !t.is_file()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("results-") || !name.ends_with(".json") {
            continue;
        }
        let version = name
            .trim_start_matches("results-")
            .trim_end_matches(".json")
            .to_string();
        if version.is_empty() {
            continue;
        }
        let path = entry.path();
        let raw =
            fs::read_to_string(&path).map_err(|e| format!("open {}: {}", path.display(), e))?;
        let run = parse_version_results(&raw)?;
        by_version.insert(
            version.clone(),
            VersionResult {
                version,
                run: Some(run),
            },
        );
    }

    let mut ordered = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for version in version_order {
        if let Some(result) = by_version.get(version) {
            ordered.push(result.clone());
            seen.insert(version.clone());
        }
    }
    let mut extras: Vec<String> = by_version
        .keys()
        .filter(|v| !seen.contains(*v))
        .cloned()
        .collect();
    extras.sort();
    for version in extras {
        if let Some(result) = by_version.get(&version) {
            ordered.push(result.clone());
        }
    }
    Ok(ordered)
}

fn evaluate_cell(spec: &crate::features::FeatureSpec, run: &Option<VersionTestRun>) -> CellSummary {
    if spec.sdk_status == crate::features::FeatureStatus::Unsupported && spec.tests.is_empty() {
        return CellSummary {
            status: CellStatus::Unsupported,
            display: "—".to_string(),
            passed: 0,
            failed: 0,
            skipped: 0,
            missing: 0,
            total: 0,
        };
    }

    let total = spec.tests.len();
    let run = match run {
        Some(run) => run,
        None => {
            return CellSummary {
                status: CellStatus::NotRun,
                display: "N/R".to_string(),
                passed: 0,
                failed: 0,
                skipped: 0,
                missing: total,
                total,
            }
        }
    };

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;
    let mut missing = 0usize;
    for test_name in &spec.tests {
        match run.outcomes.get(test_name) {
            Some(TestOutcome::Pass) => passed += 1,
            Some(TestOutcome::Fail) => failed += 1,
            Some(TestOutcome::Skip) => skipped += 1,
            None => missing += 1,
        }
    }

    if run.package_failed && passed == 0 && skipped == 0 {
        return CellSummary {
            status: CellStatus::Fail,
            display: format!("FAIL (0/{})", total),
            passed,
            failed: total,
            skipped,
            missing,
            total,
        };
    }

    if failed > 0 {
        return CellSummary {
            status: CellStatus::Fail,
            display: format!("FAIL ({}/{})", passed, total),
            passed,
            failed,
            skipped,
            missing,
            total,
        };
    }

    if total > 0 && passed == total {
        return CellSummary {
            status: CellStatus::Pass,
            display: format!("PASS ({}/{})", passed, total),
            passed,
            failed,
            skipped,
            missing,
            total,
        };
    }

    if total > 0 && passed > 0 && skipped > 0 && missing == 0 {
        return CellSummary {
            status: CellStatus::Partial,
            display: format!("PARTIAL ({}/{})", passed, total),
            passed,
            failed,
            skipped,
            missing,
            total,
        };
    }

    if total > 0 && skipped == total {
        return CellSummary {
            status: CellStatus::KnownGap,
            display: "KNOWN GAP".to_string(),
            passed,
            failed,
            skipped,
            missing,
            total,
        };
    }

    CellSummary {
        status: CellStatus::NotRun,
        display: "N/R".to_string(),
        passed,
        failed,
        skipped,
        missing,
        total,
    }
}

/// Aggregate parsed test runs against the features manifest.
pub fn build_summary_report(
    manifest: &crate::features::FeaturesManifest,
    runs: &[VersionResult],
) -> Result<SummaryReport, String> {
    if manifest.features.is_empty() {
        return Err("manifest has no features".to_string());
    }
    let versions: Vec<String> = runs.iter().map(|r| r.version.clone()).collect();
    let features = manifest
        .features
        .iter()
        .map(|spec| {
            let mut results = HashMap::new();
            for run in runs {
                results.insert(run.version.clone(), evaluate_cell(spec, &run.run));
            }
            FeatureSummary {
                id: spec.id.clone(),
                category: spec.category.clone(),
                title: spec.title.clone(),
                sdk_status: spec.sdk_status.as_str().to_string(),
                sdk_surface: spec.sdk_surface.clone(),
                notes: spec.notes.clone(),
                results,
            }
        })
        .collect();
    Ok(SummaryReport { versions, features })
}

/// Validate a summary report for strict mode: at least one version run loaded,
/// no failed cells, and every supported feature passing on every version.
pub fn validate_strict(report: &SummaryReport) -> Result<(), String> {
    if report.versions.is_empty() {
        return Err("no conformance result files found".to_string());
    }
    for feature in &report.features {
        for version in &report.versions {
            let cell = &feature.results[version];
            if cell.status == CellStatus::Fail {
                return Err(format!(
                    "feature {} failed on Envoy {} ({})",
                    feature.id, version, cell.display
                ));
            }
            if feature.sdk_status == "supported" && cell.status != CellStatus::Pass {
                return Err(format!(
                    "supported feature {} did not pass on Envoy {} (got {})",
                    feature.id, version, cell.display
                ));
            }
        }
    }
    Ok(())
}

fn format_sdk_status(status: &str) -> String {
    match status {
        "supported" => "Supported".to_string(),
        "partial" => "Partial".to_string(),
        "protocol_gap" => "Protocol Gap".to_string(),
        "unsupported" => "Not Yet Supported".to_string(),
        other => other.to_string(),
    }
}

fn escape_pipe(s: &str) -> String {
    s.replace('|', "\\|")
}

/// Render the Capability × Envoy Version matrix as GitHub Markdown.
pub fn render_markdown_table(report: &SummaryReport) -> String {
    let mut out = String::new();
    out.push_str("| Category | Capability | SDK Status | SDK Surface |");
    for version in &report.versions {
        out.push_str(&format!(" {} |", escape_pipe(version)));
    }
    out.push_str(" Notes |\n");
    out.push_str("| :--- | :--- | :--- | :--- |");
    for _ in &report.versions {
        out.push_str(" :---: |");
    }
    out.push_str(" :--- |\n");
    for feature in &report.features {
        out.push_str("| ");
        out.push_str(&escape_pipe(&feature.category));
        out.push_str(" | ");
        out.push_str(&escape_pipe(&feature.title));
        out.push_str(" | ");
        out.push_str(&format_sdk_status(&feature.sdk_status));
        out.push_str(" | ");
        if !feature.sdk_surface.is_empty() {
            out.push_str(&format!("`{}`", escape_pipe(&feature.sdk_surface)));
        }
        out.push_str(" |");
        for version in &report.versions {
            let cell = &feature.results[version];
            out.push_str(&format!(" {} |", escape_pipe(&cell.display)));
        }
        out.push(' ');
        out.push_str(&escape_pipe(&feature.notes));
        out.push_str(" |\n");
    }
    out
}

/// Render a full GitHub Actions step summary document.
pub fn render_step_summary(report: &SummaryReport) -> String {
    let mut out = String::new();
    out.push_str("## Envoy Conformance Matrix\n\n");
    if report.versions.is_empty() {
        out.push_str("_No conformance test results were collected._\n");
        return out;
    }
    out.push_str(&render_markdown_table(report));
    out
}

/// Replace the section bounded by the generated-matrix markers in `doc_path`
/// with the generated matrix table.
pub fn update_conformance_doc(doc_path: &Path, report: &SummaryReport) -> Result<(), String> {
    let raw =
        fs::read_to_string(doc_path).map_err(|e| format!("read {}: {}", doc_path.display(), e))?;
    let content = raw.as_str();
    let start_idx = content
        .find(CONFORMANCE_MATRIX_START_MARKER)
        .ok_or_else(|| {
            format!(
                "{} missing marker {:?}",
                doc_path.display(),
                CONFORMANCE_MATRIX_START_MARKER
            )
        })?;
    let end_idx = content.find(CONFORMANCE_MATRIX_END_MARKER).ok_or_else(|| {
        format!(
            "{} missing marker {:?}",
            doc_path.display(),
            CONFORMANCE_MATRIX_END_MARKER
        )
    })?;
    if end_idx < start_idx {
        return Err(format!("{} markers out of order", doc_path.display()));
    }

    let replacement = format!(
        "{}\n\n{}\n{}",
        CONFORMANCE_MATRIX_START_MARKER,
        render_markdown_table(report),
        CONFORMANCE_MATRIX_END_MARKER
    );
    let updated = format!(
        "{}{}{}",
        &content[..start_idx],
        replacement,
        &content[end_idx + CONFORMANCE_MATRIX_END_MARKER.len()..]
    );
    fs::write(doc_path, updated).map_err(|e| format!("write {}: {}", doc_path.display(), e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_features_manifest() {
        let manifest = crate::features::load_features_manifest().expect("load features.yaml");
        assert!(manifest.features.len() >= 15);
    }

    #[test]
    fn test_parse_version_results_and_build_report() {
        let v140_json = [
            r#"{"Action":"start","Package":"envoy-conformance"}"#,
            r#"{"Action":"pass","Package":"envoy-conformance","Test":"envoy_conformance::headers_mutations_http1"}"#,
            r#"{"Action":"pass","Package":"envoy-conformance","Test":"envoy_conformance::headers_mutations_h2c"}"#,
            r#"{"Action":"pass","Package":"envoy-conformance","Test":"envoy_conformance::headers_multivalue_and_context_state"}"#,
            r#"{"Action":"pass","Package":"envoy-conformance","Test":"envoy_conformance::mode_override"}"#,
            r#"{"Action":"skip","Package":"envoy-conformance","Test":"envoy_conformance::header_append_action_envoy_gap"}"#,
            r#"{"Action":"skip","Package":"envoy-conformance","Test":"envoy_conformance::observability_mode_envoy_gap"}"#,
        ]
        .join("\n");

        let dev_json = [
            r#"{"Action":"pass","Package":"envoy-conformance","Test":"envoy_conformance::headers_mutations_http1"}"#,
            r#"{"Action":"fail","Package":"envoy-conformance","Test":"envoy_conformance::headers_mutations_h2c"}"#,
        ]
        .join("\n");

        let dir = std::env::temp_dir().join(format!(
            "envoy-conformance-report-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        std::fs::write(dir.join("results-v1.40-latest.json"), v140_json).expect("write results");
        std::fs::write(dir.join("results-dev.json"), dev_json).expect("write results");

        let runs = load_results_dir(&dir, &["v1.40-latest".to_string(), "dev".to_string()])
            .expect("load results dir");
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].version, "v1.40-latest");
        assert_eq!(runs[1].version, "dev");

        let manifest = crate::features::load_features_manifest().expect("load features");
        let report = build_summary_report(&manifest, &runs).expect("build report");

        let first = report.features.first().expect("first feature");
        let v140 = first
            .results
            .get("v1.40-latest")
            .expect("v1.40-latest cell");
        assert_eq!(v140.status, CellStatus::Pass);
        assert_eq!(v140.display, "PASS (3/3)");
        let dev = first.results.get("dev").expect("dev cell");
        assert_eq!(dev.status, CellStatus::Fail);
        assert_eq!(dev.display, "FAIL (1/3)");
        let second = report
            .features
            .iter()
            .find(|f| f.id == "headers_append_action")
            .expect("headers_append_action feature");
        let append = second.results.get("v1.40-latest").expect("append cell");
        assert_eq!(append.status, CellStatus::KnownGap);
        assert_eq!(append.display, "KNOWN GAP");
        let mode_override = report
            .features
            .iter()
            .find(|f| f.id == "mode_override")
            .expect("mode_override feature");
        let mode_cell = mode_override
            .results
            .get("v1.40-latest")
            .expect("mode cell");
        assert_eq!(mode_cell.status, CellStatus::Pass);
        assert_eq!(mode_cell.display, "PASS (1/1)");
    }
}
