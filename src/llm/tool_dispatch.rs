//! Streaming tool-call accumulation and side-effect dispatch.
//!
//! The implementation moved to the skinny `remotemedia-modality` crate so
//! loadable-node plugins (which can't link `remotemedia-core`) share
//! one source of truth with the in-tree LLM nodes. This file is a
//! transparent re-export shim — `crate::llm::tool_dispatch::ToolCallAccum`
//! and `dispatch_tool_call` still resolve to the same items they
//! always did.

pub use remotemedia_modality::llm::tool_dispatch::ToolCallAccum;

/// Office tools use the same structured envelopes as the renderer API.
pub fn dispatch_tool_call<F>(
    registry: &[crate::nodes::tool_spec::ToolSpec],
    call: &ToolCallAccum,
    output_channel: &str,
    callback: &mut F,
) -> Result<(), crate::Error>
where
    F: FnMut(crate::data::RuntimeData) -> Result<(), crate::Error>,
{
    if matches!(
        call.name.as_str(),
        "get_scene_state" | "generate_waypoint" | "motion_goal"
    ) && registry
        .iter()
        .any(|s| s.name == call.name && s.kind == crate::nodes::tool_spec::ToolKind::SideEffect)
    {
        let mut args: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&call.arguments).map_err(|e| {
                crate::Error::Execution(format!("invalid office tool arguments: {e}"))
            })?;
        args.insert("kind".into(), call.name.clone().into());
        if matches!(call.name.as_str(), "get_scene_state" | "generate_waypoint") {
            args.insert("request_id".into(), call.id.clone().into());
        }
        return callback(crate::data::RuntimeData::Json(args.into()));
    }
    remotemedia_modality::llm::tool_dispatch::dispatch_tool_call(
        registry,
        call,
        output_channel,
        callback,
    )
}

#[cfg(test)]
mod office_tests {
    use super::*;
    #[test]
    fn native_goal_is_not_wrapped_or_extended_with_tool_correlation() {
        let registry = serde_json::from_value::<Vec<crate::nodes::tool_spec::ToolSpec>>(serde_json::json!([
            {"name":"motion_goal","description":"Semantic goal", "parameters":{"type":"object"},"kind":"side_effect"}
        ])).unwrap();
        let arguments = serde_json::json!({"kind":"motion_goal","version":2,"session_id":"native",
            "seed":42,"generation_revision":2,"pts_ms":8000,"goal":{"description":"A person stands up from a squat."}});
        let call = ToolCallAccum { id:"tool-id".into(), name:"motion_goal".into(), arguments:arguments.to_string() };
        let mut output = Vec::new();
        dispatch_tool_call(&registry, &call, "tts", &mut |data| { output.push(data); Ok(()) }).unwrap();
        assert!(matches!(&output[0], crate::data::RuntimeData::Json(v) if v == &arguments));
    }
    #[test]
    fn waypoint_query_uses_renderer_envelope_and_tool_call_correlation() {
        let registry = serde_json::from_value::<Vec<crate::nodes::tool_spec::ToolSpec>>(serde_json::json!([
            {"name":"generate_waypoint","description":"Preview", "parameters":{"type":"object"},"kind":"side_effect"}
        ])).unwrap();
        let call = ToolCallAccum { id:"route-7".into(), name:"generate_waypoint".into(),
            arguments:r#"{"from":"avatar","to":"plate","request_id":"invented"}"#.into() };
        let mut output = Vec::new();
        dispatch_tool_call(&registry, &call, "tts", &mut |data| { output.push(data); Ok(()) }).unwrap();
        assert!(matches!(&output[0], crate::data::RuntimeData::Json(v)
            if v["kind"] == "generate_waypoint" && v["request_id"] == "route-7" && v["to"] == "plate"));
    }
    #[test]
    fn scene_query_keeps_the_tool_call_id() {
        let registry: Vec<crate::nodes::tool_spec::ToolSpec> = serde_json::from_value(serde_json::json!([{"name":"get_scene_state","description":"Observe", "parameters":{"type":"object"},"kind":"side_effect"}])).unwrap();
        let call = ToolCallAccum {
            id: "call-17".into(),
            name: "get_scene_state".into(),
            arguments: "{}".into(),
        };
        let mut output = Vec::new();
        dispatch_tool_call(&registry, &call, "tts", &mut |data| {
            output.push(data);
            Ok(())
        })
        .unwrap();
        assert!(
            matches!(&output[0], crate::data::RuntimeData::Json(v) if v["kind"] == "get_scene_state" && v["request_id"] == "call-17")
        );
    }
}
