//! Runtime capability handshake (follow-up to spec 025).
//!
//! After every node of a session is initialized and before any data flows,
//! nodes that discovered their real formats (a mic opened on its device, a
//! speaker that knows what its device plays) have reported them through
//! `actual_capabilities()`. The handshake settles one audio format per
//! connection from those reports:
//!
//! 1. **Offers** (forward): a node's output format, from its own declared
//!    output (`actual_capabilities().outputs`) or, for an `Adaptive` node
//!    such as `FastResampleNode`, from what it chose in step 2. A node that
//!    declares no output has no offer; the handshake never guesses one
//!    through it.
//! 2. **Demands** (backward): a node's accepted inputs
//!    (`actual_capabilities().inputs`) travel up through `Passthrough` nodes
//!    to the nearest `Adaptive` node, which receives the intersection of all
//!    its demands in `configure_for_downstream` and picks its output.
//! 3. **Answers**: every `Adaptive` / `Passthrough` node with a known input
//!    offer gets it in `configure_from_upstream` (a speaker opens its device
//!    in that format).
//! 4. **Check**: every connection whose offer and accepted inputs are both
//!    known must agree.
//!
//! The handshake rule, per dimension (sample rate, channels): keep the
//! offered value when the other side accepts it, otherwise take the nearest
//! accepted value. Mono stays mono unless a node needs otherwise; nothing
//! is converted that does not have to be.
//!
//! A mismatch is an error when either end's format was discovered at
//! runtime (its `actual_capabilities()` differ from its declared
//! `media_capabilities()`, as for devices): that audio would play at the
//! wrong speed or with garbled channels. Between declared-only formats it
//! is a warning, since static validation already reports those and some
//! nodes tolerate them.

use crate::capabilities::{
    AudioConstraints, CapabilityBehavior, ConstraintValue, MediaCapabilities, MediaConstraints,
};
use crate::manifest::Manifest;
use crate::nodes::StreamingNode;
use std::collections::{HashMap, HashSet, VecDeque};

/// The audio format on one connection; `None` = not known.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AudioFormat {
    pub sample_rate: Option<u32>,
    pub channels: Option<u32>,
}

fn exact(c: &Option<ConstraintValue<u32>>) -> Option<u32> {
    match c {
        Some(ConstraintValue::Exact(v)) => Some(*v),
        _ => None,
    }
}

impl AudioFormat {
    /// The exact audio output declared in `caps`, if any.
    pub fn of_output(caps: &MediaCapabilities) -> Option<Self> {
        match caps.default_output()? {
            MediaConstraints::Audio(a) => {
                let f = Self {
                    sample_rate: exact(&a.sample_rate),
                    channels: exact(&a.channels),
                };
                (f.sample_rate.is_some() || f.channels.is_some()).then_some(f)
            }
            _ => None,
        }
    }

    /// As an output capability (what `configure_from_upstream` reads).
    pub fn to_caps(self) -> MediaCapabilities {
        MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: self.sample_rate.map(ConstraintValue::Exact),
            channels: self.channels.map(ConstraintValue::Exact),
            format: None,
        }))
    }

    fn describe(self) -> String {
        let mut parts = Vec::new();
        if let Some(r) = self.sample_rate {
            parts.push(format!("{r} Hz"));
        }
        if let Some(c) = self.channels {
            parts.push(format!("{c} channel(s)"));
        }
        parts.join(", ")
    }
}

/// The audio a node accepts on its default input, if it says.
pub fn accepted_audio(caps: &MediaCapabilities) -> Option<AudioConstraints> {
    match caps.default_input()? {
        MediaConstraints::Audio(a) => Some(a.clone()),
        _ => None,
    }
}

/// Values both constraints accept; `None` when there are none.
pub fn intersect(
    a: &ConstraintValue<u32>,
    b: &ConstraintValue<u32>,
) -> Option<ConstraintValue<u32>> {
    use ConstraintValue::*;
    match (a, b) {
        (Exact(x), o) | (o, Exact(x)) => o.satisfies(x).then_some(Exact(*x)),
        (Set(s), o) | (o, Set(s)) => {
            let v: Vec<u32> = s.iter().copied().filter(|x| o.satisfies(x)).collect();
            match v.len() {
                0 => None,
                1 => Some(Exact(v[0])),
                _ => Some(Set(v)),
            }
        }
        (Range { min: a1, max: a2 }, Range { min: b1, max: b2 }) => {
            let (lo, hi) = ((*a1).max(*b1), (*a2).min(*b2));
            match lo.cmp(&hi) {
                std::cmp::Ordering::Greater => None,
                std::cmp::Ordering::Equal => Some(Exact(lo)),
                std::cmp::Ordering::Less => Some(Range { min: lo, max: hi }),
            }
        }
    }
}

/// The handshake rule: keep `offer` when `accepted` allows it, else the
/// accepted value nearest to it (the higher one on a tie). `None` when
/// nothing is offered and `accepted` leaves a choice.
pub fn choose(offer: Option<u32>, accepted: &ConstraintValue<u32>) -> Option<u32> {
    if let Some(v) = offer {
        if accepted.satisfies(&v) {
            return Some(v);
        }
    }
    match accepted {
        ConstraintValue::Exact(x) => Some(*x),
        ConstraintValue::Range { min, max } => offer.map(|v| v.clamp(*min, *max)),
        ConstraintValue::Set(s) => offer.and_then(|v| {
            s.iter()
                .copied()
                .min_by_key(|x| (x.abs_diff(v), std::cmp::Reverse(*x)))
        }),
    }
}

/// `"48000 Hz"`, `"8000..48000 Hz"`, `"1..8 channel(s)"`.
pub fn describe_constraint(c: &ConstraintValue<u32>, unit: &str) -> String {
    match c {
        ConstraintValue::Exact(v) => format!("{v} {unit}"),
        ConstraintValue::Range { min, max } => format!("{min}..{max} {unit}"),
        ConstraintValue::Set(s) => format!(
            "{} {unit}",
            s.iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(" or ")
        ),
    }
}

/// What the handshake settled and found.
#[derive(Debug, Default)]
pub struct Outcome {
    /// Problems that would make the audio wrong: startup fails on these.
    pub errors: Vec<String>,
    /// Mismatches between declared formats only.
    pub warnings: Vec<String>,
    /// Each node's output format, where known.
    pub formats: HashMap<String, AudioFormat>,
}

/// Run the handshake over `manifest`'s graph of initialized `nodes`.
pub fn negotiate(
    session_id: &str,
    manifest: &Manifest,
    nodes: &HashMap<String, Box<dyn StreamingNode>>,
) -> Outcome {
    let mut out = Outcome::default();
    let ids: Vec<&str> = manifest
        .nodes
        .iter()
        .map(|n| n.id.as_str())
        .filter(|id| nodes.contains_key(*id))
        .collect();
    let mut upstream: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut downstream: HashMap<&str, Vec<&str>> = HashMap::new();
    for c in &manifest.connections {
        let (f, t) = (c.from.as_str(), c.to.as_str());
        if nodes.contains_key(f) && nodes.contains_key(t) {
            upstream.entry(t).or_default().push(f);
            downstream.entry(f).or_default().push(t);
        }
    }

    // Topological order (Kahn); a cycle leaves the handshake out.
    let mut indegree: HashMap<&str, usize> = ids
        .iter()
        .map(|id| (*id, upstream.get(id).map_or(0, Vec::len)))
        .collect();
    let mut queue: VecDeque<&str> = ids.iter().copied().filter(|id| indegree[id] == 0).collect();
    let mut order = Vec::with_capacity(ids.len());
    while let Some(id) = queue.pop_front() {
        order.push(id);
        for d in downstream.get(id).into_iter().flatten() {
            let e = indegree.get_mut(d).expect("known node");
            *e -= 1;
            if *e == 0 {
                queue.push_back(d);
            }
        }
    }
    if order.len() != ids.len() {
        return out;
    }

    let behavior: HashMap<&str, CapabilityBehavior> = ids
        .iter()
        .map(|id| (*id, nodes[*id].capability_behavior()))
        .collect();
    let actual: HashMap<&str, Option<MediaCapabilities>> = ids
        .iter()
        .map(|id| (*id, nodes[*id].actual_capabilities()))
        .collect();
    // Discovered at runtime: authoritative.
    let discovered: HashSet<&str> = ids
        .iter()
        .copied()
        .filter(|id| actual[id].is_some() && actual[id] != nodes[*id].media_capabilities())
        .collect();
    let is_adaptive = |id: &str| behavior[id] == CapabilityBehavior::Adaptive;
    let accepts: HashMap<&str, AudioConstraints> = ids
        .iter()
        .copied()
        .filter(|id| !is_adaptive(id))
        .filter_map(|id| Some((id, accepted_audio(actual[id].as_ref()?)?)))
        .collect();

    // Demands: accepted inputs, walked up through Passthrough nodes to the
    // nearest Adaptive node.
    let mut demands: HashMap<&str, Vec<(&str, AudioConstraints)>> = HashMap::new();
    for (&sink, acc) in &accepts {
        let mut stack: Vec<&str> = upstream.get(sink).cloned().unwrap_or_default();
        let mut seen = HashSet::new();
        while let Some(u) = stack.pop() {
            if !seen.insert(u) {
                continue;
            }
            if is_adaptive(u) {
                demands.entry(u).or_default().push((sink, acc.clone()));
            } else if behavior[u] == CapabilityBehavior::Passthrough
                && actual[u]
                    .as_ref()
                    .and_then(AudioFormat::of_output)
                    .is_none()
            {
                stack.extend(upstream.get(u).into_iter().flatten().copied());
            }
        }
    }

    let input_of = |id: &str, formats: &HashMap<String, AudioFormat>| -> Option<AudioFormat> {
        let ups = upstream.get(id)?;
        let mut it = ups.iter().map(|u| formats.get(*u).copied());
        let first = it.next()??;
        it.all(|f| f == Some(first)).then_some(first)
    };

    for &id in &order {
        let node = &nodes[id];
        // The node's configure calls log as the node (`rm_node_logs`).
        let _span = crate::telemetry::node_logs::node_span(session_id, id).entered();
        let input = input_of(id, &out.formats);
        let declared = actual[id].as_ref().and_then(AudioFormat::of_output);
        let format = match behavior[id] {
            CapabilityBehavior::Adaptive => {
                if let Some(i) = input {
                    if let Err(e) = node.configure_from_upstream(&i.to_caps()) {
                        out.errors.push(format!("Node '{id}': {e}"));
                    }
                }
                match demands.get(id) {
                    None => declared.or(input),
                    Some(list) => match merge_demands(id, list) {
                        Err(e) => {
                            out.errors.push(e);
                            None
                        }
                        Ok(merged) => {
                            let accepted =
                                MediaCapabilities::with_input(MediaConstraints::Audio(merged));
                            match node.configure_for_downstream(&accepted) {
                                Ok(Some(c)) => AudioFormat::of_output(&c),
                                Ok(None) => declared.or(input),
                                Err(e) => {
                                    let sinks: Vec<String> =
                                        list.iter().map(|(s, _)| format!("'{s}'")).collect();
                                    out.errors.push(format!(
                                        "Node '{id}' cannot produce what {} accept(s): {e}",
                                        sinks.join(" and ")
                                    ));
                                    None
                                }
                            }
                        }
                    },
                }
            }
            CapabilityBehavior::Passthrough => {
                if let Some(i) = input {
                    if let Err(e) = node.configure_from_upstream(&i.to_caps()) {
                        let msg = format!("Node '{id}': {e}");
                        if discovered.contains(id) {
                            out.errors.push(msg);
                        } else {
                            out.warnings.push(msg);
                        }
                    }
                }
                declared
            }
            _ => declared,
        };
        if let Some(f) = format {
            out.formats.insert(id.to_string(), f);
        }
    }

    // Check every connection with a known offer and known accepted inputs.
    for c in &manifest.connections {
        let (u, v) = (c.from.as_str(), c.to.as_str());
        let (Some(offer), Some(acc)) = (out.formats.get(u).copied(), accepts.get(v)) else {
            continue;
        };
        let mut wrong = Vec::new();
        let mut fix_rate = None;
        if let (Some(r), Some(c)) = (offer.sample_rate, &acc.sample_rate) {
            if !c.satisfies(&r) {
                wrong.push(format!("'{v}' accepts {}", describe_constraint(c, "Hz")));
                fix_rate = choose(Some(r), c);
            }
        }
        if let (Some(ch), Some(c)) = (offer.channels, &acc.channels) {
            if !c.satisfies(&ch) {
                wrong.push(format!(
                    "'{v}' accepts {}",
                    describe_constraint(c, "channel(s)")
                ));
            }
        }
        if wrong.is_empty() {
            continue;
        }
        let fix = match (fix_rate, offer.sample_rate) {
            (Some(t), Some(s)) => format!(
                " Insert a FastResampleNode between them ({{\"source_rate\": {s}, \"target_rate\": {t}}}), or leave its target_rate unset to follow '{v}'."
            ),
            _ => String::new(),
        };
        let msg = format!(
            "Connection '{u}' -> '{v}': '{u}' produces {}, but {}.{fix}",
            offer.describe(),
            wrong.join(" and ")
        );
        if discovered.contains(u) || discovered.contains(v) {
            out.errors.push(msg);
        } else {
            out.warnings.push(msg);
        }
    }
    out
}

/// Intersect every demand on `node`, dimension by dimension.
fn merge_demands(
    node: &str,
    list: &[(&str, AudioConstraints)],
) -> Result<AudioConstraints, String> {
    let mut merged = AudioConstraints::default();
    let mut rate_from: Vec<&str> = Vec::new();
    let mut ch_from: Vec<&str> = Vec::new();
    for (sink, c) in list {
        if let Some(r) = &c.sample_rate {
            rate_from.push(sink);
            merged.sample_rate = match &merged.sample_rate {
                None => Some(r.clone()),
                Some(m) => Some(intersect(m, r).ok_or_else(|| {
                    format!(
                        "Node '{node}' feeds {} which accept no common sample rate",
                        rate_from
                            .iter()
                            .map(|s| format!("'{s}'"))
                            .collect::<Vec<_>>()
                            .join(" and ")
                    )
                })?),
            };
        }
        if let Some(ch) = &c.channels {
            ch_from.push(sink);
            merged.channels = match &merged.channels {
                None => Some(ch.clone()),
                Some(m) => Some(intersect(m, ch).ok_or_else(|| {
                    format!(
                        "Node '{node}' feeds {} which accept no common channel count",
                        ch_from
                            .iter()
                            .map(|s| format!("'{s}'"))
                            .collect::<Vec<_>>()
                            .join(" and ")
                    )
                })?),
            };
        }
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::AudioSampleFormat;
    use crate::data::RuntimeData;
    use crate::manifest::{Connection, ManifestMetadata, NodeManifest};
    use crate::nodes::{NodeRuntimeContextRead, StreamingNode};
    use crate::Error;
    use std::sync::{Arc, Mutex};

    fn audio(
        rate: Option<ConstraintValue<u32>>,
        ch: Option<ConstraintValue<u32>>,
    ) -> MediaConstraints {
        MediaConstraints::Audio(AudioConstraints {
            sample_rate: rate,
            channels: ch,
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        })
    }

    /// A device-like node: `actual` differs from its (absent) declared caps.
    struct Device {
        actual: MediaCapabilities,
        behavior: CapabilityBehavior,
        got: Arc<Mutex<Option<MediaCapabilities>>>,
    }

    /// A resampler that adapts the rate only, like FastResampleNode.
    struct Resampler {
        source: Mutex<Option<AudioFormat>>,
        explicit_target: Option<u32>,
        chose: Arc<Mutex<Option<u32>>>,
    }

    #[async_trait::async_trait]
    impl StreamingNode for Device {
        fn node_type(&self) -> &str {
            "Device"
        }
        async fn process_async(
            &self,
            d: RuntimeData,
            _: &dyn NodeRuntimeContextRead,
        ) -> crate::Result<RuntimeData> {
            Ok(d)
        }
        async fn process_multi_async(
            &self,
            _: HashMap<String, RuntimeData>,
            _: &dyn NodeRuntimeContextRead,
        ) -> crate::Result<RuntimeData> {
            unreachable!()
        }
        fn is_multi_input(&self) -> bool {
            false
        }
        fn capability_behavior(&self) -> CapabilityBehavior {
            self.behavior
        }
        fn actual_capabilities(&self) -> Option<MediaCapabilities> {
            Some(self.actual.clone())
        }
        fn configure_from_upstream(&self, caps: &MediaCapabilities) -> crate::Result<()> {
            *self.got.lock().unwrap() = Some(caps.clone());
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl StreamingNode for Resampler {
        fn node_type(&self) -> &str {
            "Resampler"
        }
        async fn process_async(
            &self,
            d: RuntimeData,
            _: &dyn NodeRuntimeContextRead,
        ) -> crate::Result<RuntimeData> {
            Ok(d)
        }
        async fn process_multi_async(
            &self,
            _: HashMap<String, RuntimeData>,
            _: &dyn NodeRuntimeContextRead,
        ) -> crate::Result<RuntimeData> {
            unreachable!()
        }
        fn is_multi_input(&self) -> bool {
            false
        }
        fn capability_behavior(&self) -> CapabilityBehavior {
            CapabilityBehavior::Adaptive
        }
        fn configure_from_upstream(&self, caps: &MediaCapabilities) -> crate::Result<()> {
            *self.source.lock().unwrap() = AudioFormat::of_output(caps);
            Ok(())
        }
        fn configure_for_downstream(
            &self,
            accepted: &MediaCapabilities,
        ) -> crate::Result<Option<MediaCapabilities>> {
            let src = self.source.lock().unwrap().unwrap_or_default();
            let acc = accepted_audio(accepted).unwrap_or_default();
            let rate = match (self.explicit_target, &acc.sample_rate) {
                (Some(t), Some(c)) if !c.satisfies(&t) => {
                    return Err(Error::Execution(format!("target_rate {t} is not accepted")))
                }
                (Some(t), _) => Some(t),
                (None, Some(c)) => choose(src.sample_rate, c),
                (None, None) => src.sample_rate,
            };
            *self.chose.lock().unwrap() = rate;
            Ok(Some(
                AudioFormat {
                    sample_rate: rate,
                    channels: src.channels,
                }
                .to_caps(),
            ))
        }
    }

    fn manifest(nodes: &[&str], edges: &[(&str, &str)]) -> Manifest {
        Manifest {
            version: "v1".into(),
            metadata: ManifestMetadata {
                name: "handshake".into(),
                ..Default::default()
            },
            nodes: nodes
                .iter()
                .map(|id| NodeManifest {
                    id: id.to_string(),
                    node_type: "T".into(),
                    ..Default::default()
                })
                .collect(),
            connections: edges
                .iter()
                .map(|(f, t)| Connection {
                    from: f.to_string(),
                    to: t.to_string(),
                    ..Default::default()
                })
                .collect(),
            python_env: None,
            plugins: Vec::new(),
        }
    }

    fn mic(rate: u32, ch: u32) -> Device {
        Device {
            actual: MediaCapabilities::with_output(audio(
                Some(ConstraintValue::Exact(rate)),
                Some(ConstraintValue::Exact(ch)),
            )),
            behavior: CapabilityBehavior::RuntimeDiscovered,
            got: Default::default(),
        }
    }

    /// A speaker whose device plays `rates`, mapping 1..8 channels.
    fn speaker(rates: Vec<u32>, got: Arc<Mutex<Option<MediaCapabilities>>>) -> Device {
        Device {
            actual: MediaCapabilities::with_input(audio(
                Some(ConstraintValue::Set(rates)),
                Some(ConstraintValue::Range { min: 1, max: 8 }),
            )),
            behavior: CapabilityBehavior::Passthrough,
            got,
        }
    }

    fn resampler(explicit_target: Option<u32>, chose: Arc<Mutex<Option<u32>>>) -> Resampler {
        Resampler {
            source: Mutex::new(None),
            explicit_target,
            chose,
        }
    }

    #[test]
    fn mono_stays_mono_and_the_resampler_targets_the_speakers_rate() {
        let (chose, heard) = (Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)));
        let mut nodes: HashMap<String, Box<dyn StreamingNode>> = HashMap::new();
        nodes.insert("tts".into(), Box::new(mic(24000, 1)));
        nodes.insert("rs".into(), Box::new(resampler(None, chose.clone())));
        nodes.insert("spk".into(), Box::new(speaker(vec![48000], heard.clone())));
        let out = negotiate(
            "s",
            &manifest(&["tts", "rs", "spk"], &[("tts", "rs"), ("rs", "spk")]),
            &nodes,
        );
        assert!(out.errors.is_empty() && out.warnings.is_empty(), "{out:?}");
        assert_eq!(*chose.lock().unwrap(), Some(48000));
        // The speaker is answered with the negotiated format: 48 kHz, still mono.
        let got = heard.lock().unwrap().clone().expect("speaker configured");
        assert_eq!(
            AudioFormat::of_output(&got),
            Some(AudioFormat {
                sample_rate: Some(48000),
                channels: Some(1)
            })
        );
    }

    #[test]
    fn an_accepted_format_is_kept_as_is() {
        let (chose, heard) = (Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)));
        let mut nodes: HashMap<String, Box<dyn StreamingNode>> = HashMap::new();
        nodes.insert("mic".into(), Box::new(mic(44100, 2)));
        nodes.insert("rs".into(), Box::new(resampler(None, chose.clone())));
        nodes.insert("spk".into(), Box::new(speaker(vec![44100, 48000], heard)));
        let out = negotiate(
            "s",
            &manifest(&["mic", "rs", "spk"], &[("mic", "rs"), ("rs", "spk")]),
            &nodes,
        );
        assert!(out.errors.is_empty(), "{out:?}");
        assert_eq!(
            *chose.lock().unwrap(),
            Some(44100),
            "no needless resampling"
        );
    }

    #[test]
    fn a_device_mismatch_with_nothing_to_adapt_fails_with_the_fix() {
        let mut nodes: HashMap<String, Box<dyn StreamingNode>> = HashMap::new();
        nodes.insert("mic".into(), Box::new(mic(44100, 1)));
        nodes.insert(
            "spk".into(),
            Box::new(speaker(vec![48000], Default::default())),
        );
        let out = negotiate("s", &manifest(&["mic", "spk"], &[("mic", "spk")]), &nodes);
        assert_eq!(out.errors.len(), 1, "{out:?}");
        let e = &out.errors[0];
        assert!(
            e.contains("'mic' produces 44100 Hz, 1 channel(s), but 'spk' accepts 48000 Hz"),
            "{e}"
        );
        assert!(
            e.contains("{\"source_rate\": 44100, \"target_rate\": 48000}"),
            "{e}"
        );
    }

    #[test]
    fn an_explicit_target_the_device_rejects_is_an_error() {
        let mut nodes: HashMap<String, Box<dyn StreamingNode>> = HashMap::new();
        nodes.insert("tts".into(), Box::new(mic(24000, 1)));
        nodes.insert(
            "rs".into(),
            Box::new(resampler(Some(16000), Default::default())),
        );
        nodes.insert(
            "spk".into(),
            Box::new(speaker(vec![48000], Default::default())),
        );
        let out = negotiate(
            "s",
            &manifest(&["tts", "rs", "spk"], &[("tts", "rs"), ("rs", "spk")]),
            &nodes,
        );
        assert!(
            out.errors
                .iter()
                .any(|e| e.contains("Node 'rs' cannot produce what 'spk' accept(s)")),
            "{out:?}"
        );
    }

    #[test]
    fn two_sinks_with_no_common_rate_are_named_together() {
        let mut nodes: HashMap<String, Box<dyn StreamingNode>> = HashMap::new();
        nodes.insert("mic".into(), Box::new(mic(48000, 1)));
        nodes.insert("rs".into(), Box::new(resampler(None, Default::default())));
        nodes.insert(
            "a".into(),
            Box::new(speaker(vec![44100], Default::default())),
        );
        nodes.insert(
            "b".into(),
            Box::new(speaker(vec![48000], Default::default())),
        );
        let out = negotiate(
            "s",
            &manifest(
                &["mic", "rs", "a", "b"],
                &[("mic", "rs"), ("rs", "a"), ("rs", "b")],
            ),
            &nodes,
        );
        assert!(
            out.errors
                .iter()
                .any(|e| e.contains("accept no common sample rate")
                    && e.contains("'a'")
                    && e.contains("'b'")),
            "{out:?}"
        );
    }

    #[test]
    fn the_handshake_rule_keeps_or_picks_the_nearest() {
        use ConstraintValue::*;
        assert_eq!(choose(Some(1), &Range { min: 1, max: 8 }), Some(1));
        assert_eq!(choose(Some(24000), &Set(vec![44100, 48000])), Some(44100));
        assert_eq!(choose(Some(46050), &Set(vec![44100, 48000])), Some(48000));
        assert_eq!(
            choose(
                Some(96000),
                &Range {
                    min: 8000,
                    max: 48000
                }
            ),
            Some(48000)
        );
        assert_eq!(choose(None, &Exact(16000)), Some(16000));
        assert_eq!(choose(None, &Set(vec![44100, 48000])), None);
        assert_eq!(
            intersect(
                &Set(vec![44100, 48000]),
                &Range {
                    min: 45000,
                    max: 96000
                }
            ),
            Some(Exact(48000))
        );
        assert_eq!(intersect(&Exact(16000), &Set(vec![48000])), None);
    }
}
