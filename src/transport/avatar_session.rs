//! Session-owned office feedback, deliberately outside the acyclic media graph.
use super::session_control::{ControlAddress, SessionControl};
use crate::{data::RuntimeData, manifest::Manifest, Error, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
    time::Instant,
};

/// Declared on the renderer's `params.avatar_session`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BridgeConfig {
    motion_node: String,
    llm_node: Option<String>,
    #[serde(default = "timeout_secs")]
    request_timeout_secs: u64,
}
fn timeout_secs() -> u64 {
    30
}

/// Transport watchdog only. The renderer owns request scheduling, revision
/// coalescing and acknowledgement gating; the bridge never edits or queues goals.
#[derive(Default)]
struct MotionWatchdog {
    active: Option<(Value, Instant)>,
}
impl MotionWatchdog {
    fn observe(&mut self, value: Value) {
        self.active = Some((value, Instant::now()));
    }
    fn acknowledge(&mut self, value: &Value) {
        if self.active.as_ref().is_some_and(|(request, _)| {
            request["session_id"] == value["session_id"]
                && request["generation_revision"] == value["generation_revision"]
                && request["pts_ms"] == value["pts_ms"]
        }) {
            self.active = None;
        }
    }
}

/// Drop aborts the bridge even when session initialization or routing fails.
pub struct AvatarSessionBridge(JoinHandle<()>);
impl Drop for AvatarSessionBridge {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl AvatarSessionBridge {
    pub fn attach(manifest: &Manifest, control: Arc<SessionControl>) -> Result<Option<Self>> {
        let owners: Vec<_> = manifest
            .nodes
            .iter()
            .filter(|n| n.params.get("avatar_session").is_some())
            .collect();
        if owners.is_empty() {
            return Ok(None);
        }
        if owners.len() != 1 {
            return Err(Error::Execution(
                "one avatar_session renderer is supported".into(),
            ));
        }
        let owner = owners[0];
        let config: BridgeConfig = serde_json::from_value(owner.params["avatar_session"].clone())
            .map_err(|e| Error::Execution(e.to_string()))?;
        if config.request_timeout_secs == 0
            || !manifest.nodes.iter().any(|n| n.id == config.motion_node)
            || config
                .llm_node
                .as_ref()
                .is_some_and(|id| !manifest.nodes.iter().any(|n| &n.id == id))
        {
            return Err(Error::Execution(
                "invalid avatar_session node references or timeout".into(),
            ));
        }
        let renderer = owner.id.clone();
        let scene = control.subscribe(&ControlAddress::node_out(&renderer))?;
        let motion = control.subscribe(&ControlAddress::node_out(&config.motion_node))?;
        Ok(Some(Self(tokio::spawn(async move {
            if let Err(error) = run_bridge(&config, &control, scene, motion).await {
                tracing::error!(%error, "avatar session bridge stopped");
                let _ = control
                    .publish(
                        &ControlAddress::node_in(&renderer),
                        RuntimeData::Json(
                            json!({"kind":"motion_error", "error":error.to_string()}),
                        ),
                    )
                    .await;
            }
        }))))
    }
}

struct SceneFeedback {
    llm_node: Option<String>,
    last_event_sequence: u64,
}

/// LLM backpressure must never delay motion acknowledgements or generation.
/// Ordinary snapshots replace each other; correlated query replies are queued.
struct FeedbackMailbox {
    latest: watch::Sender<Option<Value>>,
    queries: mpsc::Sender<Value>,
    task: JoinHandle<()>,
}
impl Drop for FeedbackMailbox {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl FeedbackMailbox {
    fn start(llm: String, control: Arc<SessionControl>) -> Self {
        let (latest, mut snapshots) = watch::channel::<Option<Value>>(None);
        let (queries, mut replies) = mpsc::channel::<Value>(64);
        let task = tokio::spawn(async move {
            let mut close = control.close_subscriber();
            let mut feedback = SceneFeedback {
                llm_node: Some(llm),
                last_event_sequence: 0,
            };
            loop {
                let value = tokio::select! {
                    biased;
                    _ = close.recv() => return,
                    value = replies.recv() => match value { Some(v) => v, None => return },
                    changed = snapshots.changed() => {
                        if changed.is_err() { return; }
                        let value = snapshots.borrow_and_update().clone();
                        let Some(value) = value else { continue; };
                        value
                    },
                };
                if let Err(error) = feedback.forward(&value, &control).await {
                    tracing::error!(%error, "office LLM feedback stopped; motion continues");
                    return;
                }
            }
        });
        Self {
            latest,
            queries,
            task,
        }
    }

    fn observe(&self, value: Value) {
        if value.get("request_id").is_some() {
            if let Err(error) = self.queries.try_send(value) {
                tracing::error!(%error, "office scene query feedback unavailable");
            }
        } else {
            self.latest.send_replace(Some(value));
        }
    }
}
impl SceneFeedback {
    async fn forward(&mut self, value: &Value, control: &SessionControl) -> Result<()> {
        if value["kind"] == "waypoint_path" {
            if let Some(llm) = &self.llm_node {
                control.publish(&ControlAddress::node_in(llm), RuntimeData::Json(json!({
                    "kind":"office_tool_result", "request_id":value["request_id"],
                    "text":format!("generate_waypoint returned a read-only preview, not action completion: {value}")
                }))).await?;
            }
            return Ok(());
        }
        if let (Some(llm), Some(office)) = (&self.llm_node, value.pointer("/state/office")) {
            if let Some(events) = office["events"].as_array() {
                let fresh: Vec<_> = events
                    .iter()
                    .filter(|e| e["sequence"].as_u64().unwrap_or(0) > self.last_event_sequence)
                    .cloned()
                    .collect();
                for event in fresh {
                    self.last_event_sequence = self
                        .last_event_sequence
                        .max(event["sequence"].as_u64().unwrap_or(0));
                    if matches!(
                        event["status"].as_str(),
                        Some("completed" | "failed" | "cancelled")
                    ) {
                        control.publish(&ControlAddress::node_in(llm), RuntimeData::Json(json!({"kind":"office_action_result", "text":format!("Observed action outcome: {event}")}))).await?;
                    }
                }
            }
            if let Some(id) = value.get("request_id") {
                // run_bridge already refreshed the bounded system observation.
                // Repeating the whole home here can exceed a 16K context even
                // after older history is pruned.
                control.publish(&ControlAddress::node_in(llm), RuntimeData::Json(json!({
                    "kind":"office_tool_result", "request_id":id,
                    "text":"get_scene_state completed. Use the current observed office state in the system context to resolve targets and continue the user's task."
                }))).await?;
            }
        }
        Ok(())
    }
}

/// Keep authoritative interaction coordinates; renderer geometry and historical
/// events are redundant with world keypoints and separately delivered outcomes.
fn interaction_state(office: &Value) -> Value {
    let mut state = office.clone();
    if let Some(object) = state.as_object_mut() {
        object.remove("events");
    }
    if let Some(objects) = state["objects"].as_array_mut() {
        for object in objects.iter_mut().filter_map(Value::as_object_mut) {
            for key in [
                "parts",
                "color",
                "keypoints",
                "approach",
                "grasp",
                "seat",
                "surface",
            ] {
                object.remove(key);
            }
        }
    }
    if let Some(motion) = state["motion"].as_object_mut() {
        motion.remove("timeline");
        motion.remove("joints");
    }
    state
}

async fn run_bridge(
    config: &BridgeConfig,
    control: &Arc<SessionControl>,
    mut scene: broadcast::Receiver<RuntimeData>,
    mut motion: broadcast::Receiver<RuntimeData>,
) -> Result<()> {
    let mut close = control.close_subscriber();
    let mut flight = MotionWatchdog::default();
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    let feedback = config
        .llm_node
        .as_ref()
        .map(|llm| FeedbackMailbox::start(llm.clone(), control.clone()));
    loop {
        let (from_renderer, data) = tokio::select! {
            _ = close.recv() => return Ok(()),
            _ = poll.tick() => {
                if flight.active.as_ref().is_some_and(|(_, at)| at.elapsed() > Duration::from_secs(config.request_timeout_secs)) {
                    return Err(Error::Execution("ARDY generation acknowledgement timed out".into()));
                }
                continue;
            }
            event = scene.recv() => (true, event),
            event = motion.recv() => (false, event),
        };
        let data = data.map_err(|e| Error::Execution(format!("avatar feedback tap lost data: {e}")))?;
        let value = match &data {
            RuntimeData::Json(v) => v.clone(),
            RuntimeData::Text(t) => match serde_json::from_str(crate::data::split_text_str(&t).1) {
                Ok(v) => v,
                Err(_) => continue,
            },
            _ => continue,
        };
        match value["kind"].as_str() {
            Some("motion_goal") if from_renderer => {
                flight.observe(value);
                control.publish(&ControlAddress::node_in(&config.motion_node), data).await?;
            }
            Some("scene_step") if from_renderer => {
                // Forward already-signed bytes only. Output taps can be written
                // by control clients, so this bridge must NEVER sign context.
                let payload = value["payload"].as_str().filter(|p| p.len() <= 1_048_576)
                    .ok_or_else(|| Error::Execution("invalid signed scene envelope".into()))?;
                let job: Value = serde_json::from_str(payload)
                    .map_err(|e| Error::Execution(format!("invalid scene payload: {e}")))?;
                flight.observe(job["request"].clone());
                control.publish(&ControlAddress::node_in(&config.motion_node), data).await?;
            }
            Some("motion_request") if from_renderer => {
                return Err(Error::Execution("legacy motion_request is unsupported; submit motion_goal v2".into()));
            }
            Some("motion_tick") if !from_renderer => flight.acknowledge(&value),
            Some("motion_error") => {
                return Err(Error::Execution(format!(
                    "ARDY worker failed: {}",
                    value["error"]
                )))
            }
            Some("scene_state" | "waypoint_path") => {
                if let Some(office) = value.pointer("/state/office") {
                    control.set_office_observation(interaction_state(office))?;
                }
                if let Some(feedback) = &feedback {
                    feedback.observe(value);
                }
            }
            _ => {}
        }

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn waypoint_reply_reaches_llm_with_correlation_and_failure_details() {
        let control = SessionControl::new("waypoint-feedback");
        let (tx, mut rx) = mpsc::channel(2);
        control.attach_input_sender(tx).await;
        let mailbox = FeedbackMailbox::start("llm".into(), control);
        mailbox.observe(json!({"kind":"waypoint_path","request_id":"route-1","ok":false,
            "error":{"code":"no_path","message":"blocked"}}));
        let packet = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
        assert_eq!(packet.to_node.as_deref(), Some("llm"));
        let RuntimeData::Json(value) = packet.data else { panic!("expected query feedback") };
        assert_eq!(value["request_id"], "route-1");
        assert!(value["text"].as_str().unwrap().contains("no_path"));
        assert!(value["text"].as_str().unwrap().contains("not action completion"));
    }
    #[tokio::test]
    async fn blocked_llm_delivery_keeps_only_latest_scene_snapshot() {
        let control = SessionControl::new("blocked-llm");
        let (tx, mut rx) = mpsc::channel(1);
        control.attach_input_sender(tx).await;
        let mailbox = FeedbackMailbox::start("llm".into(), control);
        let query = |id| json!({"kind":"scene_state", "request_id":id, "state":{"office":{"objects":[], "events":[]}}});
        mailbox.observe(query("first"));
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        mailbox.observe(query("second"));
        mailbox.observe(query("third"));
        tokio::task::yield_now().await;
        for revision in 0..1000 {
            mailbox
                .observe(json!({"kind":"scene_state", "state":{"office":{"revision":revision}}}));
        }
        assert_eq!(
            mailbox.latest.borrow().as_ref().unwrap()["state"]["office"]["revision"],
            999
        );
        assert!(!mailbox.task.is_finished());
    }
    #[test]
    fn conversation_state_keeps_world_anchors_without_mesh_geometry() {
        let scene = json!({"objects":[{"id":"mug", "position":[1,2,3], "parts":[{"mesh":"detail"}], "keypoints":{"handle":"local"}}],
            "world_keypoints":{"mug":{"handle":[1,2,3]}}, "occupied_hands":{"right":"mug"},
            "motion":{"generation_revision":7,"timeline":{"prompts":[]}}, "events":[{"sequence":1}]});
        let context = interaction_state(&scene);
        assert_eq!(context["world_keypoints"], scene["world_keypoints"]);
        assert_eq!(context["occupied_hands"], scene["occupied_hands"]);
        assert_eq!(context["motion"]["generation_revision"], 7);
        assert!(context["objects"][0].get("parts").is_none());
        assert!(context.get("events").is_none());
        assert!(scene["objects"][0].get("parts").is_some());
    }
    fn goal(revision: u64, pts: u64) -> Value {
        json!({"kind":"motion_goal", "version":2, "session_id":"native",
            "generation_revision":revision, "pts_ms":pts, "seed":42,
            "goal":{"description":"A person stands up from a squat."}})
    }

    #[tokio::test]
    async fn native_renderer_feedback_is_forwarded_without_protocol_mutation() {
        let control = SessionControl::new("native");
        let (tx, mut rx) = mpsc::channel(8);
        control.attach_input_sender(tx).await;
        let (scene_tx, scene) = broadcast::channel(8);
        let (motion_tx, motion) = broadcast::channel(8);
        let config = BridgeConfig { motion_node:"ardy".into(), llm_node:None, request_timeout_secs:1 };
        let c = control.clone();
        let task = tokio::spawn(async move { run_bridge(&config, &c, scene, motion).await });
        let first = format!("  {}  ", goal(1, 0));
        scene_tx.send(RuntimeData::Text(first.clone())).unwrap();
        let packet = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.unwrap().unwrap();
        assert_eq!(packet.to_node.as_deref(), Some("ardy"));
        assert!(matches!(packet.data, RuntimeData::Text(t) if t == first));
        motion_tx.send(RuntimeData::Json(json!({"kind":"motion_tick", "session_id":"native",
            "generation_revision":1,"pts_ms":0}))).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        scene_tx.send(RuntimeData::Json(goal(2, 200))).unwrap();
        let packet = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.unwrap().unwrap();
        assert!(matches!(packet.data, RuntimeData::Json(v) if v == goal(2, 200)));
        let mut scene_goal = goal(3, 250);
        scene_goal["version"] = 3.into();
        scene_goal["goal"] = json!({"operation":"navigate","target_id":"desk"});
        let payload = format!("  {}  ", json!({"request":scene_goal}));
        let signed = json!({"kind":"scene_step","payload":payload,"signature":"5c".repeat(32)});
        scene_tx.send(RuntimeData::Json(signed.clone())).unwrap();
        let packet = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.unwrap().unwrap();
        assert!(matches!(packet.data, RuntimeData::Json(v) if v == signed));
        // A stale acknowledgement must not disable the active request's watchdog.
        for (session, revision, pts) in [("wrong",2,200), ("native",1,200), ("native",2,0)] {
            motion_tx.send(RuntimeData::Json(json!({"kind":"motion_tick", "session_id":session,
                "generation_revision":revision,"pts_ms":pts}))).unwrap();
        }
        assert!(tokio::time::timeout(Duration::from_secs(2), task).await.unwrap().unwrap()
            .unwrap_err().to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn bridge_close_and_drop_release_subscriptions() {
        let manifest: Manifest = serde_json::from_value(json!({"version":"v1",
            "metadata":{"name":"native"}, "connections":[], "nodes":[
                {"id":"renderer","node_type":"CcRenderNode","params":{"avatar_session":{"motion_node":"ardy"}}},
                {"id":"ardy","node_type":"ArdyMotionNode","params":{}}
            ]})).unwrap();
        let control = SessionControl::new("native");
        let bridge = AvatarSessionBridge::attach(&manifest, control.clone()).unwrap().unwrap();
        tokio::task::yield_now().await;
        control.signal_close(super::super::session_control::CloseReason::Normal);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(bridge.0.is_finished());
        let bridge = AvatarSessionBridge::attach(&manifest, control.clone()).unwrap().unwrap();
        drop(bridge);
        tokio::task::yield_now().await;
        assert_eq!(control.tap_receiver_count("renderer", None), 0);
        assert_eq!(control.tap_receiver_count("ardy", None), 0);
    }
}
