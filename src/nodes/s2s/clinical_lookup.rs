//! Built-in `clinical_lookup` tool — resolves patient queries against
//! the `NHSEDataScience/synthetic_clinical_notes` schema.
//!
//! For v1 the records come from a local JSON fixture path
//! (defaults to `tests/fixtures/clinical_notes.json` under the
//! workspace root, overridable via `REMOTEMEDIA_CLINICAL_NOTES_FILE`).
//! Adding an HF-Hub fetch is a downstream concern — the lookup logic
//! is decoupled from the source so it's a one-method extension.
//!
//! Match strategy: exact normalized name → trigram cosine ≥ 0.85
//! over name variants. Soundex / metaphone fallback is *not*
//! implemented in v1 (deferred).

use super::tool::{ContextTool, ContextToolError};
use crate::nodes::tool_spec::{ToolKind, ToolSpec};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::OnceLock;

const TRIGRAM_MIN_COSINE: f64 = 0.85;
const OUTPUT_CHAR_CAP: usize = 800;

/// One record in the `synthetic_clinical_notes` schema. The full
/// schema is wider; we extract only the fields the demo formats use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClinicalRecord {
    #[serde(default)]
    pub nhs_number: Option<String>,
    pub full_name: String,
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub surname: Option<String>,
    #[serde(default)]
    pub ward: Option<String>,
    #[serde(default)]
    pub bed_location: Option<String>,
    #[serde(default)]
    pub admission_timestamp: Option<String>,
    #[serde(default)]
    pub admission_title: Option<String>,
    #[serde(default)]
    pub trust: Option<String>,
}

/// Configuration for [`ClinicalLookupTool`].
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, Default)]
#[serde(default)]
pub struct ClinicalLookupConfig {
    /// Path to a local JSON fixture. If `None`, the tool tries the
    /// `REMOTEMEDIA_CLINICAL_NOTES_FILE` env var, then falls back to
    /// `tests/fixtures/clinical_notes.json` relative to the
    /// workspace root.
    pub fixture_path: Option<PathBuf>,
}

/// `ContextTool` impl for `clinical_lookup`.
///
/// The fixture is loaded lazily on the first `execute` so library
/// users who never call the tool don't pay the I/O cost.
pub struct ClinicalLookupTool {
    cfg: ClinicalLookupConfig,
    index: OnceLock<Index>,
}

impl Default for ClinicalLookupTool {
    fn default() -> Self {
        Self::new(ClinicalLookupConfig::default())
    }
}

impl ClinicalLookupTool {
    pub fn new(cfg: ClinicalLookupConfig) -> Self {
        Self {
            cfg,
            index: OnceLock::new(),
        }
    }

    /// Construct an in-memory tool directly from a slice of records.
    /// Used by tests and library callers that don't want a file
    /// fixture.
    pub fn from_records(records: Vec<ClinicalRecord>) -> Self {
        let index = Index::build(records);
        let cell = OnceLock::new();
        let _ = cell.set(index);
        Self {
            cfg: ClinicalLookupConfig::default(),
            index: cell,
        }
    }

    fn resolve_fixture_path(&self) -> PathBuf {
        if let Some(p) = &self.cfg.fixture_path {
            return p.clone();
        }
        if let Ok(env_p) = std::env::var("REMOTEMEDIA_CLINICAL_NOTES_FILE") {
            return PathBuf::from(env_p);
        }
        // Default: workspace-relative. Best-effort — the tool emits
        // a clean `None` if the file isn't present.
        PathBuf::from("tests/fixtures/clinical_notes.json")
    }

    fn ensure_index(&self) -> &Index {
        self.index.get_or_init(|| {
            let path = self.resolve_fixture_path();
            match Index::load_from_file(&path) {
                Ok(idx) => {
                    tracing::info!(
                        target: "s2s::clinical_lookup",
                        path = %path.display(),
                        records = idx.records.len(),
                        "loaded clinical notes fixture"
                    );
                    idx
                }
                Err(e) => {
                    tracing::warn!(
                        target: "s2s::clinical_lookup",
                        path = %path.display(),
                        error = %e,
                        "clinical notes fixture missing or unreadable; tool will always miss"
                    );
                    Index::empty()
                }
            }
        })
    }

    fn format_record(rec: &ClinicalRecord, field: Option<&str>) -> String {
        let body = match field {
            Some("bed_location") => format!(
                "Patient {} is at {}.",
                rec.full_name,
                rec.bed_location.as_deref().unwrap_or("an unrecorded bed")
            ),
            Some("ward") => format!(
                "Patient {} is in {}.",
                rec.full_name,
                rec.ward.as_deref().unwrap_or("an unrecorded ward")
            ),
            Some("admission_timestamp") => format!(
                "Patient {} was admitted at {}.",
                rec.full_name,
                rec.admission_timestamp
                    .as_deref()
                    .unwrap_or("an unrecorded time")
            ),
            Some("nhs_number") => format!(
                "Patient {} has NHS number {}.",
                rec.full_name,
                rec.nhs_number.as_deref().unwrap_or("(not recorded)")
            ),
            Some("admission_title") => format!(
                "Patient {}'s admission reason is recorded as {}.",
                rec.full_name,
                rec.admission_title.as_deref().unwrap_or("(not recorded)")
            ),
            _ => {
                // Default formatter: bed + ward + admission time + title in one sentence.
                let mut parts = vec![format!("Patient {}", rec.full_name)];
                if let Some(b) = &rec.bed_location {
                    parts.push(format!("is at {b}"));
                }
                if let Some(w) = &rec.ward {
                    parts.push(format!("on {w}"));
                }
                if let Some(t) = &rec.admission_timestamp {
                    parts.push(format!("admitted {t}"));
                }
                if let Some(title) = &rec.admission_title {
                    parts.push(format!("for {title}"));
                }
                if let Some(trust) = &rec.trust {
                    parts.push(format!("at {trust}"));
                }
                format!("{}.", parts.join(", "))
            }
        };
        cap_chars(&body, OUTPUT_CHAR_CAP)
    }
}

#[async_trait]
impl ContextTool for ClinicalLookupTool {
    fn name(&self) -> &str {
        "clinical_lookup"
    }

    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "clinical_lookup".into(),
            description: "Look up a patient by name and return a one-sentence \
clinical summary suitable as a system-prompt addendum. \
Use this when the user asks WHERE a patient is, WHEN they \
were admitted, WHICH ward / bed they're on, or for any \
identifier (NHS number, admission reason)."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description":
                            "The patient's full name as the user said it. \
            The tool does fuzzy matching, so transcription errors are okay.",
                        "minLength": 1
                    },
                    "field": {
                        "type": ["string", "null"],
                        "enum": ["bed_location", "ward", "admission_timestamp",
                                 "nhs_number", "admission_title", null],
                        "description":
                            "Which field to retrieve. Omit / null for a default \
            one-line summary covering bed, ward, admission time, and reason."
                    }
                },
                "required": ["name"]
            }),
            kind: ToolKind::SideEffect,
            cancelable: true,
        }
    }

    async fn execute(&self, args: &Value) -> Result<Option<String>, ContextToolError> {
        let name = args
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ContextToolError::InvalidArgs("missing required `name`".into()))?
            .trim();
        if name.is_empty() {
            return Err(ContextToolError::InvalidArgs(
                "`name` must be non-empty".into(),
            ));
        }
        let field = args.get("field").and_then(Value::as_str);

        let index = self.ensure_index();
        let Some(rec) = index.find(name) else {
            return Ok(Some(format!("No patient record found for '{}'.", name)));
        };

        Ok(Some(Self::format_record(rec, field)))
    }
}

// ---------------------------------------------------------------------------
// Index — exact + trigram fuzzy lookup
// ---------------------------------------------------------------------------

/// In-memory index over `Vec<ClinicalRecord>`.
struct Index {
    records: Vec<ClinicalRecord>,
    /// `normalized_full_name → record idx` for the O(1) exact path.
    by_normalized: HashMap<String, usize>,
    /// Pre-computed trigram sets per record for fuzzy lookup.
    trigrams: Vec<HashSet<String>>,
}

impl Index {
    fn empty() -> Self {
        Self {
            records: Vec::new(),
            by_normalized: HashMap::new(),
            trigrams: Vec::new(),
        }
    }

    fn load_from_file(path: &std::path::Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        let recs: Vec<ClinicalRecord> = serde_json::from_slice(&bytes).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("parse JSON: {e}"))
        })?;
        Ok(Self::build(recs))
    }

    fn build(records: Vec<ClinicalRecord>) -> Self {
        let mut by_normalized: HashMap<String, usize> = HashMap::new();
        let mut trigrams: Vec<HashSet<String>> = Vec::with_capacity(records.len());

        for (idx, rec) in records.iter().enumerate() {
            let variants = name_variants_for(rec);
            for v in &variants {
                by_normalized.entry(v.clone()).or_insert(idx);
            }
            // Trigrams over the union of variants — boosts recall
            // when the user said "Wells Judith" instead of "Judith Wells".
            let mut tgs = HashSet::new();
            for v in &variants {
                tgs.extend(trigrams_of(v));
            }
            trigrams.push(tgs);
        }

        Self {
            records,
            by_normalized,
            trigrams,
        }
    }

    fn find(&self, query: &str) -> Option<&ClinicalRecord> {
        let normalized = normalize(query);
        if let Some(idx) = self.by_normalized.get(&normalized) {
            return self.records.get(*idx);
        }
        // Trigram cosine fallback.
        let q_grams = trigrams_of(&normalized);
        if q_grams.is_empty() {
            return None;
        }
        let mut best: Option<(usize, f64)> = None;
        for (idx, rec_grams) in self.trigrams.iter().enumerate() {
            if rec_grams.is_empty() {
                continue;
            }
            let cos = trigram_cosine(&q_grams, rec_grams);
            if cos >= TRIGRAM_MIN_COSINE {
                if best.map(|(_, b)| cos > b).unwrap_or(true) {
                    best = Some((idx, cos));
                }
            }
        }
        best.and_then(|(idx, _)| self.records.get(idx))
    }
}

fn name_variants_for(rec: &ClinicalRecord) -> Vec<String> {
    let mut variants: Vec<String> = Vec::new();
    variants.push(normalize(&rec.full_name));
    if let (Some(first), Some(last)) = (&rec.first_name, &rec.surname) {
        variants.push(normalize(&format!("{first} {last}")));
        variants.push(normalize(&format!("{last} {first}")));
    }
    // De-duplicate while preserving order.
    let mut seen = HashSet::new();
    variants.retain(|v| !v.is_empty() && seen.insert(v.clone()));
    variants
}

fn normalize(s: &str) -> String {
    // Lower-case + collapse whitespace + strip punctuation that
    // models commonly emit ("." in initials, "," between names).
    let mut out = String::with_capacity(s.len());
    let mut last_was_space = true;
    for c in s.chars() {
        if c.is_alphanumeric() {
            for lower in c.to_lowercase() {
                out.push(lower);
            }
            last_was_space = false;
        } else if c.is_whitespace() {
            if !last_was_space {
                out.push(' ');
            }
            last_was_space = true;
        }
        // Other characters (punctuation, symbols) are dropped.
    }
    out.trim().to_string()
}

fn trigrams_of(s: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    if s.len() < 3 {
        return out;
    }
    // Pad with a single space at each end so the first and last
    // character contribute trigrams too.
    let padded = format!(" {s} ");
    let chars: Vec<char> = padded.chars().collect();
    for w in chars.windows(3) {
        out.insert(w.iter().collect::<String>());
    }
    out
}

fn trigram_cosine(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let inter = a.intersection(b).count() as f64;
    let denom = ((a.len() as f64).sqrt()) * ((b.len() as f64).sqrt());
    if denom == 0.0 {
        0.0
    } else {
        inter / denom
    }
}

fn cap_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    // Truncate on a sentence boundary if possible, else hard-cut.
    let truncated: String = s.chars().take(max).collect();
    if let Some(idx) = truncated.rfind(['.', '!', '?', ';']) {
        truncated[..=idx].to_string()
    } else {
        truncated
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Vec<ClinicalRecord> {
        vec![
            ClinicalRecord {
                nhs_number: Some("999 000 1234".into()),
                full_name: "Judith Ada Wells".into(),
                first_name: Some("Judith".into()),
                surname: Some("Wells".into()),
                ward: Some("Neurology Ward".into()),
                bed_location: Some("Bay 00, Bed B00".into()),
                admission_timestamp: Some("07/01/2026 14:05".into()),
                admission_title: Some("transient ischaemic attack".into()),
                trust: Some("Apple Tree Trust".into()),
            },
            ClinicalRecord {
                nhs_number: Some("999 000 9876".into()),
                full_name: "Tom Bradley".into(),
                first_name: Some("Tom".into()),
                surname: Some("Bradley".into()),
                ward: Some("Cardiology Ward".into()),
                bed_location: Some("Bay 02, Bed C03".into()),
                admission_timestamp: Some("04/02/2026 08:11".into()),
                admission_title: Some("acute myocardial infarction".into()),
                trust: Some("Apple Tree Trust".into()),
            },
        ]
    }

    #[tokio::test]
    async fn exact_match_returns_default_summary() {
        let tool = ClinicalLookupTool::from_records(fixture());
        let out = tool
            .execute(&json!({"name": "Judith Ada Wells"}))
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("Judith Ada Wells"));
        assert!(out.contains("Bed B00"));
        assert!(out.contains("Neurology Ward"));
    }

    #[tokio::test]
    async fn exact_match_with_specific_field() {
        let tool = ClinicalLookupTool::from_records(fixture());
        let out = tool
            .execute(&json!({"name": "Judith Ada Wells", "field": "bed_location"}))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out, "Patient Judith Ada Wells is at Bay 00, Bed B00.");
    }

    #[tokio::test]
    async fn fuzzy_match_above_threshold_succeeds() {
        let tool = ClinicalLookupTool::from_records(fixture());
        // Whisper might mis-hear or drop the middle name.
        let out = tool
            .execute(&json!({"name": "Judith Wells"}))
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("Judith Ada Wells"), "got: {out}");
    }

    #[tokio::test]
    async fn last_first_order_matched_via_variants() {
        let tool = ClinicalLookupTool::from_records(fixture());
        let out = tool
            .execute(&json!({"name": "Wells Judith"}))
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("Judith Ada Wells"));
    }

    #[tokio::test]
    async fn miss_returns_no_record_message() {
        let tool = ClinicalLookupTool::from_records(fixture());
        let out = tool
            .execute(&json!({"name": "Nobody McNobody"}))
            .await
            .unwrap()
            .unwrap();
        assert!(out.contains("No patient record found"));
    }

    #[tokio::test]
    async fn missing_name_is_invalid_args() {
        let tool = ClinicalLookupTool::from_records(fixture());
        let err = tool.execute(&json!({})).await.unwrap_err();
        assert!(matches!(err, ContextToolError::InvalidArgs(_)));
    }

    #[tokio::test]
    async fn empty_name_is_invalid_args() {
        let tool = ClinicalLookupTool::from_records(fixture());
        let err = tool.execute(&json!({"name": "   "})).await.unwrap_err();
        assert!(matches!(err, ContextToolError::InvalidArgs(_)));
    }

    #[test]
    fn normalize_lowercases_and_strips_punctuation() {
        assert_eq!(normalize("  Judith   ADA  Wells.  "), "judith ada wells");
        assert_eq!(normalize("Smith, J."), "smith j");
    }

    #[test]
    fn trigram_cosine_perfect_self_match() {
        let a = trigrams_of("judith");
        assert!((trigram_cosine(&a, &a) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn cap_chars_prefers_sentence_boundary() {
        let s = "One. Two. Three is long enough to push beyond the cap so we truncate.";
        let out = cap_chars(s, 25);
        assert!(out.ends_with('.'));
        assert!(out.chars().count() <= 25);
    }
}
