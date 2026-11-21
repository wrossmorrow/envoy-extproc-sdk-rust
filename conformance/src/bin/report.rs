//! `conformance report` command: aggregate conformance results and update
//! the generated matrix in CONFORMANCE.md.

use std::path::PathBuf;

use envoy_conformance::report::{
    build_summary_report, load_results_dir, render_step_summary, update_conformance_doc,
    validate_strict,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut results_dir = std::path::PathBuf::from("conformance");
    let mut markdown_out: Option<PathBuf> = None;
    let mut json_out: Option<PathBuf> = None;
    let mut update_doc: Option<PathBuf> = None;
    let mut strict = false;

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
            "results-dir" => {
                let value = value.unwrap_or_else(|| ".".to_string());
                results_dir = std::path::PathBuf::from(value);
            }
            "markdown-out" => markdown_out = value.map(PathBuf::from),
            "json-out" => json_out = value.map(PathBuf::from),
            "update-conformance-md" => update_doc = value.map(PathBuf::from),
            "strict" => strict = value.is_some(),
            other => {
                eprintln!("conformance-report: unknown flag {}", other);
                std::process::exit(2);
            }
        }
    }

    let manifest = envoy_conformance::features::load_features_manifest()
        .map_err(|e| format!("conformance-report: {}", e))?;
    let version_order = envoy_conformance::envoy_versions::EnvoyVersionManifest::load()
        .map(|m| m.supported)
        .map_err(|e| format!("conformance-report: {}", e))?;
    let order: Vec<String> = version_order.iter().map(|s| s.to_string()).collect();
    let runs =
        load_results_dir(&results_dir, &order).map_err(|e| format!("conformance-report: {}", e))?;
    let report =
        build_summary_report(&manifest, &runs).map_err(|e| format!("conformance-report: {}", e))?;

    let summary = render_step_summary(&report);
    if let Some(out) = &markdown_out {
        std::fs::write(out, &summary).map_err(|e| {
            format!(
                "conformance-report: write markdown {}: {}",
                out.display(),
                e
            )
        })?;
    } else {
        print!("{}", summary);
    }

    if let Some(out) = &json_out {
        let raw = serde_json::to_string_pretty(&report)
            .map_err(|e| format!("conformance-report: marshal JSON: {}", e))?;
        std::fs::write(out, format!("{}\n", raw))
            .map_err(|e| format!("conformance-report: write JSON {}: {}", out.display(), e))?;
    }

    if let Some(doc) = &update_doc {
        update_conformance_doc(doc, &report).map_err(|e| format!("conformance-report: {}", e))?;
    }

    if strict {
        validate_strict(&report)
            .map_err(|e| format!("conformance-report: strict validation failed: {}", e))?;
    }
    Ok(())
}
