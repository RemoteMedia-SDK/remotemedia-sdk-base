//! Hermes integration primitives for telephony sessions.

use crate::session::{CallId, CallLegId, ParticipantRole};
use serde::{Deserialize, Serialize};

/// Call-control commands exposed to Hermes or other control-plane clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "command")]
pub enum CallControlCommand {
    /// Inspect all active sessions.
    Inspect,
    /// Stop one call.
    Stop { call_id: CallId },
    /// Join a bot leg to an active call.
    Join { call_id: CallId, leg_id: CallLegId },
    /// Inject synthesized audio or a prepared response into a call leg.
    InjectResponse {
        call_id: CallId,
        leg_id: CallLegId,
        response_id: String,
    },
    /// Mute one call leg.
    Mute { call_id: CallId, leg_id: CallLegId },
    /// Unmute one call leg.
    Unmute { call_id: CallId, leg_id: CallLegId },
}

/// Current tool-call state associated with a call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HermesToolState {
    /// No active tool call.
    Idle,
    /// Tool call is running.
    Running { tool_name: String },
    /// Tool call completed.
    Completed { tool_name: String },
    /// Tool call failed.
    Failed { tool_name: String, message: String },
}

/// Event envelope sent from telephony sessions to Hermes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelephonyHermesEvent {
    /// Transport call identifier.
    pub call_id: CallId,
    /// Participant role associated with the event.
    pub participant_role: ParticipantRole,
    /// Optional transcript/context fragment.
    pub transcription_context: Option<String>,
    /// Active tool-call state.
    pub tool_state: HermesToolState,
}

/// Hermes tool-delegation request generated from mid-call speech.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HermesDelegationRequest {
    /// Tool gateway namespace.
    pub gateway: String,
    /// Telephony event context.
    pub event: TelephonyHermesEvent,
    /// User utterance or semantic request text.
    pub utterance: String,
}

/// Dynamic response to inject into a call after Hermes tool execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HermesDelegationResponse {
    /// Tool name that produced the response.
    pub tool_name: String,
    /// Text to synthesize and inject through TTS/RTP.
    pub response_text: String,
    /// True when fallback text was used after tool failure.
    pub fallback: bool,
}

impl TelephonyHermesEvent {
    /// Create a new idle event for a call.
    pub fn idle(call_id: CallId, participant_role: ParticipantRole) -> Self {
        Self {
            call_id,
            participant_role,
            transcription_context: None,
            tool_state: HermesToolState::Idle,
        }
    }
}

/// Build a Hermes tool-delegation payload for a mid-call request.
pub fn build_tool_delegation_request(
    call_id: CallId,
    participant_role: ParticipantRole,
    utterance: impl Into<String>,
) -> HermesDelegationRequest {
    HermesDelegationRequest {
        gateway: "hermes-tool-delegation".to_string(),
        event: TelephonyHermesEvent {
            call_id,
            participant_role,
            transcription_context: None,
            tool_state: HermesToolState::Idle,
        },
        utterance: utterance.into(),
    }
}

/// Build fallback TTS text when a tool call fails.
pub fn fallback_tool_response(
    tool_name: impl Into<String>,
    message: impl Into<String>,
) -> HermesDelegationResponse {
    let tool_name = tool_name.into();
    HermesDelegationResponse {
        tool_name,
        response_text: message.into(),
        fallback: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_call_control_command() {
        let command = CallControlCommand::InjectResponse {
            call_id: "call".into(),
            leg_id: "bot".into(),
            response_id: "resp-1".into(),
        };
        let json = serde_json::to_value(command).unwrap();
        assert_eq!(json["command"], "inject_response");
        assert_eq!(json["call_id"], "call");
    }

    #[test]
    fn serializes_hermes_event_metadata() {
        let event = TelephonyHermesEvent::idle("call".into(), ParticipantRole::User);
        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["call_id"], "call");
        assert_eq!(json["participant_role"], "user");
        assert_eq!(json["tool_state"], "idle");
    }

    #[test]
    fn builds_mid_call_tool_delegation_request() {
        let request =
            build_tool_delegation_request("call".into(), ParticipantRole::User, "lookup account");
        let json = serde_json::to_value(request).unwrap();
        assert_eq!(json["gateway"], "hermes-tool-delegation");
        assert_eq!(json["event"]["call_id"], "call");
        assert_eq!(json["utterance"], "lookup account");
    }

    #[test]
    fn builds_fallback_tool_response() {
        let response = fallback_tool_response("database_lookup", "I could not reach the database");
        assert!(response.fallback);
        assert_eq!(response.tool_name, "database_lookup");
    }
}
