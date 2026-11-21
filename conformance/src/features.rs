//! Feature coverage manifest loaded from `conformance/features.yaml`.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const FEATURES_YAML: &str = include_str!("../features.yaml");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureStatus {
    Supported,
    Partial,
    ProtocolGap,
    Unsupported,
}

impl FeatureStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeatureStatus::Supported => "Supported",
            FeatureStatus::Partial => "Partial",
            FeatureStatus::ProtocolGap => "Protocol Gap",
            FeatureStatus::Unsupported => "Not Yet Supported",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSpec {
    pub id: String,
    pub category: String,
    pub title: String,
    #[serde(rename = "sdk_status")]
    pub sdk_status: FeatureStatus,
    #[serde(rename = "sdk_surface")]
    pub sdk_surface: String,
    pub tests: Vec<String>,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeaturesManifest {
    pub features: Vec<FeatureSpec>,
}

/// Load and validate the embedded feature coverage manifest.
pub fn load_features_manifest() -> Result<FeaturesManifest, String> {
    let manifest: FeaturesManifest =
        serde_yaml::from_str(FEATURES_YAML).map_err(|e| format!("parse features.yaml: {}", e))?;
    if manifest.features.is_empty() {
        return Err("features.yaml: no features defined".to_string());
    }

    let mut seen: HashSet<String> = HashSet::new();
    for feature in &manifest.features {
        if feature.id.trim().is_empty() {
            return Err("features.yaml: feature entry with empty id".to_string());
        }
        if !seen.insert(feature.id.clone()) {
            return Err(format!(
                "features.yaml: duplicate feature id {}",
                feature.id
            ));
        }

        if feature.sdk_status != FeatureStatus::Unsupported && feature.tests.is_empty() {
            return Err(format!(
                "features.yaml: feature {} ({}) must declare at least one conformance test",
                feature.id,
                feature.sdk_status.as_str()
            ));
        }
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_features_manifest() {
        let manifest = load_features_manifest().expect("load features.yaml");
        assert!(manifest.features.len() >= 15);
        for feature in &manifest.features {
            if feature.sdk_status == FeatureStatus::Unsupported {
                assert!(feature.tests.is_empty());
            } else {
                assert!(!feature.tests.is_empty());
            }
        }
    }
}
