use std::collections::BTreeSet;

use remotemedia_bundle::{
    PreflightReport, RuntimeCapabilities, VariantCandidate, VariantRejection, BUNDLE_SCHEMA_VERSION,
};

pub fn preflight(
    capabilities: &RuntimeCapabilities,
    variants: &[VariantCandidate],
    cached_digests: &BTreeSet<String>,
) -> PreflightReport {
    let mut accepted = Vec::new();
    let mut rejections = Vec::new();
    for variant in variants {
        let constraints = rejected_constraints(capabilities, variant);
        if constraints.is_empty() {
            accepted.push(variant);
        } else {
            rejections.push(VariantRejection {
                descriptor_digest: variant.descriptor.digest.clone(),
                constraints,
            });
        }
    }

    if accepted.len() != 1 {
        if accepted.len() > 1 {
            for variant in accepted {
                rejections.push(VariantRejection {
                    descriptor_digest: variant.descriptor.digest.clone(),
                    constraints: vec!["variant selection is ambiguous".to_owned()],
                });
            }
        }
        return PreflightReport {
            schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
            selected: None,
            cached_bytes: 0,
            missing_bytes: 0,
            additional_required_bytes: 0,
            missing_blobs: Vec::new(),
            rejections,
        };
    }

    let selected = accepted[0];
    let mut cached_bytes = 0_u64;
    let mut missing_bytes = 0_u64;
    let mut missing_blobs = Vec::new();
    for blob in &selected.required_blobs {
        if cached_digests.contains(&blob.digest) {
            cached_bytes = cached_bytes.saturating_add(blob.size);
        } else {
            missing_bytes = missing_bytes.saturating_add(blob.size);
            missing_blobs.push(blob.clone());
        }
    }
    if missing_bytes > capabilities.available_cache_bytes {
        rejections.push(VariantRejection {
            descriptor_digest: selected.descriptor.digest.clone(),
            constraints: vec![format!(
                "requires {missing_bytes} additional cache bytes but {} are available",
                capabilities.available_cache_bytes
            )],
        });
        return PreflightReport {
            schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
            selected: None,
            cached_bytes,
            missing_bytes,
            additional_required_bytes: missing_bytes,
            missing_blobs,
            rejections,
        };
    }
    PreflightReport {
        schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
        selected: Some(selected.descriptor.clone()),
        cached_bytes,
        missing_bytes,
        additional_required_bytes: missing_bytes,
        missing_blobs,
        rejections,
    }
}

fn rejected_constraints(
    capabilities: &RuntimeCapabilities,
    variant: &VariantCandidate,
) -> Vec<String> {
    let required = &variant.requirements;
    let mut reasons = Vec::new();
    if required.os != capabilities.os {
        reasons.push(format!("OS {} is required", required.os));
    }
    if required.architecture != capabilities.architecture {
        reasons.push(format!(
            "architecture {} is required",
            required.architecture
        ));
    }
    if required.native_abi.is_some() && required.native_abi != capabilities.native_abi {
        reasons.push(format!("native ABI {:?} is required", required.native_abi));
    }
    if !required
        .manifest_schemas
        .iter()
        .any(|schema| capabilities.manifest_schemas.contains(schema))
    {
        reasons.push("no supported manifest schema overlaps".to_owned());
    }
    if !compatible_minimum(
        &required.plugin_abi.minimum,
        &capabilities.plugin_abi.minimum,
    ) {
        reasons.push(format!(
            "plugin ABI {} or newer is required",
            required.plugin_abi.minimum
        ));
    }
    if let Some(python) = &required.python {
        if !capabilities.python.contains(python) {
            reasons.push(format!("Python ABI {} is required", python.abi));
        }
    }
    if !capabilities.accelerators.contains(&required.accelerator) {
        reasons.push(format!(
            "accelerator {:?} is required",
            required.accelerator
        ));
    }
    if required.minimum_memory_bytes > capabilities.memory_bytes {
        reasons.push(format!(
            "{} memory bytes are required",
            required.minimum_memory_bytes
        ));
    }
    for device in &required.media_devices {
        if !capabilities.media_devices.contains(device) {
            reasons.push(format!("media device {device} is required"));
        }
    }
    for feature in &required.runtime_features {
        if !capabilities.runtime_features.contains(feature) {
            reasons.push(format!("runtime feature {feature} is required"));
        }
    }
    reasons
}

fn compatible_minimum(required: &str, available: &str) -> bool {
    version_tuple(available) >= version_tuple(required)
}

fn version_tuple(value: &str) -> Vec<u64> {
    value
        .split('.')
        .map(|part| part.parse().unwrap_or(0))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use remotemedia_bundle::{
        AcceleratorBackend, CompatibilityRange, DescriptorIdentity, PythonTarget,
        TargetRequirements,
    };

    use super::*;

    fn capabilities() -> RuntimeCapabilities {
        RuntimeCapabilities {
            schema_version: "1".to_owned(),
            os: "linux".to_owned(),
            architecture: "amd64".to_owned(),
            native_abi: Some("gnu.2.35".to_owned()),
            manifest_schemas: vec!["v1".to_owned()],
            plugin_abi: CompatibilityRange {
                minimum: "0.4.0".to_owned(),
                maximum_exclusive: None,
            },
            python: vec![PythonTarget {
                implementation: "cpython".to_owned(),
                version: "3.11".to_owned(),
                abi: "cp311".to_owned(),
            }],
            accelerators: vec![AcceleratorBackend::Cpu],
            memory_bytes: 8_000,
            available_cache_bytes: 1_000,
            media_devices: vec!["audio_input".to_owned()],
            runtime_features: vec!["streaming".to_owned()],
        }
    }

    fn candidate(accelerator: AcceleratorBackend) -> VariantCandidate {
        VariantCandidate {
            descriptor: DescriptorIdentity {
                digest: format!("sha256:{}", "a".repeat(64)),
                size: 10,
            },
            requirements: TargetRequirements {
                schema_version: "1".to_owned(),
                target_id: "linux-amd64".to_owned(),
                os: "linux".to_owned(),
                architecture: "amd64".to_owned(),
                native_abi: Some("gnu.2.35".to_owned()),
                manifest_schemas: vec!["v1".to_owned()],
                plugin_abi: CompatibilityRange {
                    minimum: "0.3.0".to_owned(),
                    maximum_exclusive: None,
                },
                python: None,
                accelerator,
                minimum_memory_bytes: 1_000,
                minimum_disk_bytes: 0,
                media_devices: vec!["audio_input".to_owned()],
                runtime_features: vec!["streaming".to_owned()],
            },
            required_blobs: vec![DescriptorIdentity {
                digest: format!("sha256:{}", "b".repeat(64)),
                size: 400,
            }],
        }
    }

    #[test]
    fn selects_exactly_one_variant_and_accounts_for_cache() {
        let candidate = candidate(AcceleratorBackend::Cpu);
        let cached = BTreeSet::from([candidate.required_blobs[0].digest.clone()]);
        let report = preflight(&capabilities(), std::slice::from_ref(&candidate), &cached);
        assert_eq!(report.selected, Some(candidate.descriptor));
        assert_eq!(report.cached_bytes, 400);
        assert_eq!(report.missing_bytes, 0);
    }

    #[test]
    fn does_not_silently_fallback_accelerator() {
        let report = preflight(
            &capabilities(),
            &[candidate(AcceleratorBackend::Cuda {
                version: "12.4".to_owned(),
            })],
            &BTreeSet::new(),
        );
        assert!(report.selected.is_none());
        assert!(report.rejections[0].constraints[0].contains("accelerator"));
    }
}
