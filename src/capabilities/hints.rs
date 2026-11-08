//! Hints for agents (and people) building pipelines, shared by every
//! surface that reports on manifests or node types: `remotemedia
//! validate`, the `rm_validate` / `rm_node_catalog` agent tools, and the
//! FFI `validate_manifest` / `node_catalog` bindings
//! (openspec `add-agent-harness-integration` section 7):
//!
//! - the closest registered names for an unknown node type,
//! - node params checked against each node's published JSON Schema,
//!   naming unknown keys of closed schemas,
//! - the pipeline's ingress: what a client must send to each entry node,
//! - per-type catalog facts: role, requirements, media capabilities in the
//!   caps DSL, and whether params are checked.

use super::report::{codes, IngressEntry, ManifestReport, ReportIssue};
use super::MediaCapabilities;
use crate::manifest::Manifest;
use crate::nodes::schema::{NodeSchema, NodeSchemaRegistry};
use crate::nodes::streaming_node::StreamingNodeRegistry;
use crate::validation::SchemaValidator;
use serde_json::{json, Map, Value};
use std::collections::HashSet;

/// Lower-case words of a CamelCase / snake_case type name, without the
/// generic `node` suffix word (`FastResampleNode` -> `fast`, `resample`).
pub fn words(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_alphanumeric() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        let boundary = c.is_uppercase()
            && !cur.is_empty()
            && (chars[i - 1].is_lowercase()
                || chars[i - 1].is_ascii_digit()
                || chars.get(i + 1).map(|n| n.is_lowercase()).unwrap_or(false));
        if boundary {
            out.push(std::mem::take(&mut cur));
        }
        cur.push(c.to_ascii_lowercase());
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out.retain(|w| w != "node");
    out
}

fn norm(name: &str) -> String {
    words(name).concat()
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != *cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

/// Whether word `w` of a misspelt/guessed name matches word `k` of a
/// registered one: equal, or one is a prefix of the other (>= 4 chars).
fn word_match(w: &str, k: &str) -> bool {
    w == k || (w.len().min(k.len()) >= 4 && (k.starts_with(w) || w.starts_with(k)))
}

/// Up to `max` registered types closest to `unknown`, best first.
///
/// Scores rare shared words highest (`resample` beats `audio`), plus
/// containment (`Whisper` in `WhisperSTTNode`) and small edit distance
/// (`PassThroug`). Returns nothing when no candidate is plausibly meant.
pub fn similar_types(unknown: &str, known: &[String], max: usize) -> Vec<String> {
    let uw = words(unknown);
    let un = norm(unknown);
    if un.is_empty() {
        return Vec::new();
    }
    let kw: Vec<Vec<String>> = known.iter().map(|k| words(k)).collect();
    let df = |w: &str| {
        kw.iter()
            .filter(|ws| ws.iter().any(|k| word_match(w, k)))
            .count()
            .max(1) as f64
    };
    let mut scored: Vec<(f64, usize, &String)> = known
        .iter()
        .zip(&kw)
        .filter_map(|(k, ws)| {
            let kn: String = ws.concat();
            if kn == un {
                return Some((10.0, 0, k));
            }
            let matched: Vec<&String> = uw
                .iter()
                .filter(|w| ws.iter().any(|x| word_match(w, x)))
                .collect();
            let mut score: f64 = matched.iter().map(|w| 1.0 / df(w)).sum();
            // Sharing only a generic word (`Text` of `TextNormalize`) is
            // weak evidence: the guess's longest word is its specific part.
            let longest = uw.iter().max_by_key(|w| w.len());
            if matched.len() < uw.len() && longest.map(|l| !matched.contains(&l)).unwrap_or(true) {
                score *= 0.3;
            }
            if un.len() >= 4 && kn.len() >= 4 && (kn.contains(&un) || un.contains(&kn)) {
                score += 1.0;
            }
            let d = levenshtein(&un, &kn);
            if d <= (un.len() / 4).max(1) {
                score += 1.5 - 0.1 * d as f64;
            }
            (score >= 0.34).then_some((score, d, k))
        })
        .collect();
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(b.2))
    });
    scored
        .into_iter()
        .take(max)
        .map(|(_, _, k)| k.clone())
        .collect()
}

/// The `suggestion` text of an `unknown_node_type` error.
pub fn unknown_type_suggestion(unknown: &str, known: &[String]) -> String {
    let near = similar_types(unknown, known, 4);
    // Filter on the guessed word the candidates share (`whisper`), or the
    // candidate's own name for a typo (`PassThroug` -> `PassThrough`).
    let filter = if near.len() == 1 {
        near[0].clone()
    } else {
        words(unknown)
            .into_iter()
            .filter(|w| w.len() >= 3)
            .map(|w| {
                let hits = near
                    .iter()
                    .filter(|n| words(n).iter().any(|k| k == &w))
                    .count();
                (hits, w.len(), w)
            })
            .filter(|(hits, _, _)| *hits > 0)
            .max()
            .map(|(_, _, w)| w)
            .or_else(|| near.first().cloned())
            .unwrap_or_default()
    };
    match near.as_slice() {
        [] => "no registered type is close; browse the node catalog by category".to_string(),
        [one] => format!("did you mean '{one}'? Check it in the node catalog (filter \"{filter}\")"),
        many => format!(
            "did you mean one of: {}? They differ; compare them in the node catalog (filter \"{filter}\")",
            many.iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// One constraint value as DSL text: `16000`, `8000..48000`, `[f32|i16]`.
fn constraint_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(constraint_text).collect::<Vec<_>>().join("|")
        ),
        Value::Object(o) if o.contains_key("min") && o.contains_key("max") => {
            format!(
                "{}..{}",
                constraint_text(&o["min"]),
                constraint_text(&o["max"])
            )
        }
        other => other.to_string(),
    }
}

/// A port's constraints in the `#[node]` caps DSL, e.g.
/// `audio(sample_rate=16000, channels=1, format=f32)`.
pub fn constraints_text(c: &Value) -> String {
    let Some(o) = c.as_object() else {
        return c.to_string();
    };
    let ty = o.get("type").and_then(|t| t.as_str()).unwrap_or("any");
    let mut fields: Vec<String> = o
        .iter()
        .filter(|(k, v)| k.as_str() != "type" && !v.is_null())
        .map(|(k, v)| format!("{k}={}", constraint_text(v)))
        .collect();
    fields.sort();
    if fields.is_empty() {
        ty.to_string()
    } else {
        format!("{ty}({})", fields.join(", "))
    }
}

/// `{port: dsl}` for one side of a node's capabilities; `None` when empty.
fn side_text(side: &std::collections::HashMap<String, impl serde::Serialize>) -> Option<Value> {
    if side.is_empty() {
        return None;
    }
    let mut ports: Vec<(&String, String)> = side
        .iter()
        .map(|(p, c)| {
            (
                p,
                constraints_text(&serde_json::to_value(c).unwrap_or(Value::Null)),
            )
        })
        .collect();
    ports.sort();
    if ports.len() == 1 {
        return Some(Value::String(ports.remove(0).1));
    }
    Some(Value::Object(
        ports
            .into_iter()
            .map(|(p, t)| (p.clone(), Value::String(t)))
            .collect(),
    ))
}

/// Capabilities a node of `node_type` declares for `params` (broad
/// "potential" ones for runtime-discovered nodes such as a default mic).
pub fn caps_for(
    reg: &StreamingNodeRegistry,
    node_type: &str,
    params: &Value,
) -> Option<MediaCapabilities> {
    reg.get_media_capabilities(node_type, params)
        .or_else(|| reg.get_potential_capabilities(node_type, params))
}

/// `(input_caps, output_caps)` as DSL text.
pub fn caps_text(caps: Option<&MediaCapabilities>) -> (Option<Value>, Option<Value>) {
    match caps {
        Some(c) => (side_text(&c.inputs), side_text(&c.outputs)),
        None => (None, None),
    }
}

/// `source` (makes its own media; nothing to inject), `sink` (end of the
/// line: playback, files, subtitles) or `transform`.
pub fn role(
    node_type: &str,
    schema: Option<&NodeSchema>,
    caps: Option<&MediaCapabilities>,
) -> &'static str {
    let declares_no_input = schema.map(|s| s.accepts.is_empty()).unwrap_or(true);
    if let Some(c) = caps {
        if c.inputs.is_empty() && !c.outputs.is_empty() && declares_no_input {
            return "source";
        }
    }
    let lower = node_type.to_ascii_lowercase();
    if ["output", "sink", "writer"]
        .iter()
        .any(|s| lower.ends_with(s) || lower.ends_with(&format!("{s}node")))
    {
        return "sink";
    }
    "transform"
}

/// What running a node of this type needs beyond the binary: a Python
/// environment, model downloads, a plugin.
pub fn requires(
    reg: &StreamingNodeRegistry,
    node_type: &str,
    schema: Option<&NodeSchema>,
) -> Option<Value> {
    let mut out = Map::new();
    let imp = reg.implementation(node_type);
    if reg.is_python_node(node_type) || schema.map(|s| s.is_python).unwrap_or(false) {
        out.insert("python".into(), json!(true));
    }
    if let Some(name) = imp.plugin_name.as_ref() {
        out.insert("plugin".into(), json!(name));
    }
    if let Some(models) = schema.and_then(|s| s.model_sources.as_ref()) {
        let files: Vec<&str> = models
            .files
            .iter()
            .filter(|f| f.required)
            .map(|f| f.filename.as_str())
            .collect();
        if !files.is_empty() {
            out.insert("models".into(), json!(files));
        }
    }
    (!out.is_empty()).then_some(Value::Object(out))
}

/// First sentence of a description, at most `max` chars.
pub fn short_description(d: &str, max: usize) -> String {
    let d = d.split_whitespace().collect::<Vec<_>>().join(" ");
    let first = match d.find(". ") {
        Some(i) => &d[..=i],
        None => d.as_str(),
    };
    if first.chars().count() <= max {
        first.to_string()
    } else {
        let cut: String = first.chars().take(max.saturating_sub(1)).collect();
        format!("{}…", cut.trim_end())
    }
}

/// Shorten the `description` of catalog entries so the whole listing
/// serializes within `budget` bytes: one sentence each, as long as the
/// budget allows (at most 110 chars). When even 40 chars each do not fit,
/// descriptions are dropped and a note says how to get them.
pub fn fit_descriptions(entries: &mut [Value], budget: usize) -> Option<String> {
    let size = |e: &[Value]| {
        serde_json::to_string(e)
            .map(|s| s.len())
            .unwrap_or(usize::MAX)
    };
    let full: Vec<String> = entries
        .iter_mut()
        .map(|e| match e.get_mut("description") {
            Some(d) => std::mem::take(d).as_str().unwrap_or("").to_string(),
            None => String::new(),
        })
        .collect();
    for e in entries.iter_mut() {
        e["description"] = json!("");
    }
    let base = size(entries);
    let n = entries.len().max(1);
    // Bytes per description, less a margin for escapes and the ellipsis.
    let mut per = (budget.saturating_sub(base) / n).min(113).saturating_sub(3);
    while per >= 40 {
        for (e, d) in entries.iter_mut().zip(&full) {
            e["description"] = json!(short_description(d, per));
        }
        if size(entries) <= budget {
            return None;
        }
        per -= 10;
    }
    for e in entries.iter_mut() {
        if let Some(o) = e.as_object_mut() {
            o.remove("description");
        }
    }
    Some("descriptions omitted to fit one reply; pass `filter` or `category` to get them".into())
}

// ─── Manifest report hints ──────────────────────────────────────────────

/// A validator over the JSON Schemas the registry's factories publish.
pub fn schema_validator(registry: &StreamingNodeRegistry) -> SchemaValidator {
    let mut schemas = NodeSchemaRegistry::new();
    for s in registry.collect_schemas() {
        schemas.register(s);
    }
    SchemaValidator::from_registry(&schemas).unwrap_or_else(|_| SchemaValidator::empty())
}

/// Add hints to a manifest report: near-miss suggestions on
/// `unknown_node_type` errors, `invalid_params` errors and
/// `params_unchecked` warnings, and the pipeline's `ingress`.
pub fn apply_hints(
    report: &mut ManifestReport,
    manifest: &Manifest,
    registry: &StreamingNodeRegistry,
) {
    let known = registry.list_types();
    for e in report.errors.iter_mut() {
        if e.code != codes::UNKNOWN_NODE_TYPE || e.suggestion.is_some() {
            continue;
        }
        let ty = e
            .node
            .as_deref()
            .and_then(|id| manifest.nodes.iter().find(|n| n.id == id))
            .map(|n| n.node_type.clone());
        if let Some(ty) = ty {
            e.suggestion = Some(unknown_type_suggestion(&ty, &known));
        }
    }

    let validator = schema_validator(registry);
    for node in &manifest.nodes {
        if !registry.has_node_type(&node.node_type) {
            continue;
        }
        let keys: Vec<&String> = node
            .params
            .as_object()
            .map(|o| o.keys().collect())
            .unwrap_or_default();
        if !validator.has_schema(&node.node_type) {
            if !keys.is_empty() {
                let mut keys: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
                keys.sort();
                report.warnings.push(ReportIssue {
                    node: Some(node.id.clone()),
                    ..ReportIssue::new(
                        codes::PARAMS_UNCHECKED,
                        format!(
                            "params of node '{}' ({}) are not checked: the node publishes no parameter schema, so a misspelt or unsupported key ({}) is silently ignored",
                            node.id,
                            node.node_type,
                            keys.join(", ")
                        ),
                    )
                });
            }
            continue;
        }
        let params = if node.params.is_null() {
            json!({})
        } else {
            node.params.clone()
        };
        for issue in param_issues(
            &node.id,
            &node.node_type,
            &params,
            validator.get_schema(&node.node_type).as_ref(),
            &validator,
        ) {
            report.push_error(issue);
        }
    }

    // Ingress: nodes nothing feeds, minus sources.
    let fed: HashSet<&str> = manifest.connections.iter().map(|c| c.to.as_str()).collect();
    for node in &manifest.nodes {
        if fed.contains(node.id.as_str()) || !registry.has_node_type(&node.node_type) {
            continue;
        }
        let schema = registry
            .get_factory(&node.node_type)
            .and_then(|f| f.schema());
        let caps = caps_for(registry, &node.node_type, &node.params);
        if role(&node.node_type, schema.as_ref(), caps.as_ref()) == "source" {
            continue;
        }
        let (accepts, _) = caps_text(caps.as_ref());
        report.ingress.push(IngressEntry {
            node: node.id.clone(),
            node_type: node.node_type.clone(),
            accepts,
        });
    }
}

/// `invalid_params` issues of one node. Keys a closed schema
/// (`additionalProperties: false`) does not list are named with the
/// closest listed key; other violations come from the JSON Schema check.
pub fn param_issues(
    node_id: &str,
    node_type: &str,
    params: &Value,
    schema: Option<&Value>,
    validator: &SchemaValidator,
) -> Vec<ReportIssue> {
    let pointer = format!("see params_schema in the node catalog (filter \"{node_type}\")");
    let issue = |message: String, suggestion: String| ReportIssue {
        node: Some(node_id.to_string()),
        suggestion: Some(suggestion),
        ..ReportIssue::new(codes::INVALID_PARAMS, message)
    };
    let mut out = Vec::new();
    let props: Vec<String> = schema
        .and_then(|s| s.get("properties"))
        .and_then(|p| p.as_object())
        .map(|p| p.keys().cloned().collect())
        .unwrap_or_default();
    let closed = schema
        .and_then(|s| s.get("additionalProperties"))
        .and_then(|a| a.as_bool())
        == Some(false);
    if closed {
        for key in params.as_object().into_iter().flat_map(|o| o.keys()) {
            if props.contains(key) {
                continue;
            }
            let near = similar_types(key, &props, 1);
            out.push(issue(
                format!("node '{node_id}' ({node_type}): unknown param '{key}'"),
                match near.first() {
                    Some(k) => format!(
                        "did you mean '{k}'? Known params: {}; {pointer}",
                        props.join(", ")
                    ),
                    None => format!("known params: {}; {pointer}", props.join(", ")),
                },
            ));
        }
    }
    if let Err(errs) = validator.validate_node(node_id, node_type, params) {
        for v in errs {
            if closed && v.message.contains("unexpected property") {
                continue; // named above
            }
            let path = v.path.trim_start_matches('/').replace('/', ".");
            let at = if path.is_empty() {
                "params".to_string()
            } else {
                format!("params.{path}")
            };
            let message = v
                .message
                .strip_prefix(&format!("Node '{node_id}' ({node_type}): "))
                .unwrap_or(&v.message)
                .to_string();
            let expected = v.expected.trim();
            out.push(issue(
                format!("node '{node_id}' ({node_type}): {message}"),
                if expected.is_empty() || expected == "constraint" {
                    format!("check {at}; {pointer}")
                } else {
                    format!("expected {expected} at {at}; {pointer}")
                },
            ));
        }
    }
    out
}

// ─── Node catalog ───────────────────────────────────────────────────────

/// What to list in [`node_catalog`].
#[derive(Debug, Clone, Default)]
pub struct CatalogQuery {
    /// Case-insensitive substring of the type name or description.
    pub filter: Option<String>,
    /// Case-insensitive substring of the category.
    pub category: Option<String>,
    pub include_params_schema: bool,
    /// Byte budget of the unfiltered listing (descriptions shrink to fit);
    /// 0 = unbounded.
    pub budget: usize,
}

/// Media types of a caps DSL value (`"audio(...)"` -> `audio`).
fn caps_kinds(c: &Option<Value>) -> Vec<String> {
    let kind = |d: &str| d.split('(').next().unwrap_or("").to_string();
    let mut kinds: Vec<String> = match c {
        Some(Value::String(dsl)) => vec![kind(dsl)],
        Some(Value::Object(ports)) => ports
            .values()
            .filter_map(|d| d.as_str())
            .map(kind)
            .collect(),
        _ => Vec::new(),
    };
    kinds.sort();
    kinds.dedup();
    kinds
}

/// One catalog entry: `{type, category, description, role?, inputs,
/// outputs, params, input_caps?, output_caps?, requires?, params_schema?}`.
/// `role` is left out when `omit_transform_role` and the node is a
/// transform (the unfiltered listing's default).
pub fn catalog_entry(
    registry: &StreamingNodeRegistry,
    node_type: &str,
    include_params_schema: bool,
    omit_transform_role: bool,
) -> Value {
    let s = registry.get_factory(node_type).and_then(|f| f.schema());
    let s = s.as_ref();
    let caps = caps_for(registry, node_type, &json!({}));
    let (input_caps, output_caps) = caps_text(caps.as_ref());
    let io = |v: Option<&Vec<crate::nodes::schema::RuntimeDataType>>,
              c: &Option<Value>|
     -> Vec<String> {
        let declared: Vec<String> = v
            .map(|v| v.iter().map(|d| format!("{d:?}").to_lowercase()).collect())
            .unwrap_or_default();
        if declared.is_empty() {
            caps_kinds(c)
        } else {
            declared
        }
    };
    let mut e = json!({
        "type": node_type,
        "category": s.and_then(|s| s.category.clone()).unwrap_or_else(|| "uncategorized".into()),
        "description": s.and_then(|s| s.description.clone()),
        "inputs": io(s.map(|s| &s.accepts), &input_caps),
        "outputs": io(s.map(|s| &s.produces), &output_caps),
        "params": if s.and_then(|s| s.config_schema.as_ref()).is_some() { "schema" } else { "unchecked" },
    });
    let r = role(node_type, s, caps.as_ref());
    if !omit_transform_role || r != "transform" {
        e["role"] = json!(r);
    }
    if let Some(c) = input_caps {
        e["input_caps"] = c;
    }
    if let Some(c) = output_caps {
        e["output_caps"] = c;
    }
    if let Some(r) = requires(registry, node_type, s) {
        e["requires"] = r;
    }
    if include_params_schema {
        e["params_schema"] = s
            .and_then(|s| s.config_schema.clone())
            .unwrap_or(Value::Null);
    }
    e
}

/// The node catalog: `{node_types: [...], note?}`, sorted by type. The
/// unfiltered listing is compact (no `transform` role, one-sentence
/// descriptions sized to `budget`).
pub fn node_catalog(registry: &StreamingNodeRegistry, q: &CatalogQuery) -> Value {
    let filter = q.filter.as_ref().map(|f| f.to_lowercase());
    let category = q.category.as_ref().map(|c| c.to_lowercase());
    let compact = filter.is_none() && category.is_none() && !q.include_params_schema;
    let mut out = Vec::new();
    for t in registry.list_types() {
        let e = catalog_entry(registry, &t, q.include_params_schema, compact);
        let desc = e["description"].as_str().unwrap_or("").to_lowercase();
        if let Some(f) = filter.as_ref() {
            if !t.to_lowercase().contains(f) && !desc.contains(f) {
                continue;
            }
        }
        if let Some(c) = category.as_ref() {
            if !e["category"]
                .as_str()
                .unwrap_or("")
                .to_lowercase()
                .contains(c)
            {
                continue;
            }
        }
        out.push(e);
    }
    let mut v = json!({});
    if compact && q.budget > 0 {
        if let Some(note) = fit_descriptions(&mut out, q.budget) {
            v["note"] = json!(note);
        }
    }
    v["node_types"] = Value::Array(out);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known() -> Vec<String> {
        [
            "FastResampleNode",
            "AudioChunkerNode",
            "AudioLevelNode",
            "AudioBufferAccumulatorNode",
            "WhisperSTTNode",
            "HFWhisperNode",
            "WhisperXNode",
            "PassThrough",
            "TextCollectorNode",
            "KokoroTTSNode",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn words_split_camel_case_and_drop_node() {
        assert_eq!(words("FastResampleNode"), vec!["fast", "resample"]);
        assert_eq!(words("WhisperSTTNode"), vec!["whisper", "stt"]);
        assert_eq!(words("HFWhisperNode"), vec!["hf", "whisper"]);
        assert_eq!(words("text_collector"), vec!["text", "collector"]);
    }

    #[test]
    fn close_names_are_suggested() {
        let k = known();
        assert_eq!(similar_types("PassThroug", &k, 3), vec!["PassThrough"]);
        assert_eq!(similar_types("AudioResample", &k, 3)[0], "FastResampleNode");
        let w = similar_types("WhisperNode", &k, 4);
        assert_eq!(w.len(), 3, "{w:?}");
        assert!(w.iter().all(|t| t.contains("Whisper")), "{w:?}");
        assert!(similar_types("TextNormalize", &k, 3).is_empty());
        assert!(similar_types("Zzzz", &k, 3).is_empty());
    }

    #[test]
    fn suggestion_text_names_the_catalog() {
        let k = known();
        let s = unknown_type_suggestion("PassThroug", &k);
        assert!(
            s.contains("'PassThrough'") && s.contains("node catalog"),
            "{s}"
        );
        assert!(s.contains("filter \"PassThrough\""), "{s}");
        let s = unknown_type_suggestion("WhisperNode", &k);
        assert!(s.contains("filter \"whisper\""), "{s}");
        let s = unknown_type_suggestion("AudioResample", &k);
        assert!(s.contains("filter \"FastResampleNode\""), "{s}");
        let s = unknown_type_suggestion("TextNormalize", &k);
        assert!(s.starts_with("no registered type is close"), "{s}");
    }

    #[test]
    fn caps_render_as_dsl() {
        let c = json!({"type": "audio", "sample_rate": 16000, "channels": {"min": 1, "max": 2}, "format": "f32"});
        assert_eq!(
            constraints_text(&c),
            "audio(channels=1..2, format=f32, sample_rate=16000)"
        );
        assert_eq!(constraints_text(&json!({"type": "text"})), "text");
    }

    #[test]
    fn descriptions_fit_the_budget() {
        let long = "x".repeat(300);
        let mut many: Vec<Value> = (0..40)
            .map(|i| json!({"type": format!("T{i}"), "description": long}))
            .collect();
        assert!(fit_descriptions(&mut many, 4000).is_none());
        assert!(serde_json::to_string(&many).unwrap().len() <= 4000);
        assert!(many[0]["description"].as_str().unwrap().len() >= 40);
        let note = fit_descriptions(&mut many, 1500);
        assert!(note.is_some() && many[0].get("description").is_none());
    }

    #[test]
    fn short_description_keeps_the_first_sentence() {
        assert_eq!(short_description("Does a. Then b.", 80), "Does a.");
        assert_eq!(short_description("abcdefghij", 5), "abcd…");
    }

    fn manifest(v: Value) -> Manifest {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn core_report_carries_hints() {
        let registry = crate::nodes::streaming_registry::create_default_streaming_registry();
        let m = manifest(json!({"version": "v1", "metadata": {"name": "t"},
            "nodes": [
                {"id": "a", "node_type": "PassThroug"},
                {"id": "rs", "node_type": "FastResampleNode", "params": {"quality": "Ultra"}},
                {"id": "p", "node_type": "PassThrough"}],
            "connections": [{"from": "rs", "to": "p"}]}));
        let r = crate::capabilities::validate_manifest_report(&m, &registry, &[]);
        let unknown = r
            .errors
            .iter()
            .find(|e| e.code == codes::UNKNOWN_NODE_TYPE)
            .unwrap();
        assert!(
            unknown
                .suggestion
                .as_deref()
                .unwrap()
                .contains("'PassThrough'"),
            "{unknown:?}"
        );
        assert!(r
            .errors
            .iter()
            .any(|e| e.code == codes::INVALID_PARAMS && e.node.as_deref() == Some("rs")));
        let ingress: Vec<&str> = r.ingress.iter().map(|i| i.node.as_str()).collect();
        assert_eq!(ingress, vec!["rs"], "{:?}", r.ingress);
        assert!(r.ingress[0]
            .accepts
            .as_ref()
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("audio("));
        // Ingress serializes; an empty one is left out.
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["ingress"][0]["node"], "rs");
    }

    #[test]
    fn catalog_is_compact_unfiltered_and_full_when_filtered() {
        let registry = crate::nodes::streaming_registry::create_default_streaming_registry();
        let all = node_catalog(
            &registry,
            &CatalogQuery {
                budget: 15 * 1024,
                ..Default::default()
            },
        );
        let types = all["node_types"].as_array().unwrap();
        assert!(!types.is_empty());
        assert!(serde_json::to_string(&all).unwrap().len() <= 15 * 1024 + 64);
        assert!(types
            .iter()
            .all(|e| e.get("role") != Some(&json!("transform"))));
        let one = node_catalog(
            &registry,
            &CatalogQuery {
                filter: Some("FastResample".into()),
                ..Default::default()
            },
        );
        let e = &one["node_types"][0];
        assert_eq!(e["type"], "FastResampleNode");
        assert_eq!(e["role"], "transform");
        assert_eq!(e["params"], "schema");
        assert!(
            e["input_caps"].as_str().unwrap().starts_with("audio("),
            "{e}"
        );
    }
}
