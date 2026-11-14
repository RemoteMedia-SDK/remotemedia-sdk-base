//! What one control attachment may do.
//!
//! A control channel opened by a peer that was admitted with an identity is
//! bound to that peer's own session and checked, frame by frame, against a
//! [`ControlAccess`]. The host that admits peers decides the access; this
//! module only applies it.

/// A control operation that can be granted on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlOp {
    /// Read the node's output.
    Tap,
    /// Send to the node's input.
    Publish,
    /// Call listed methods on the node.
    Rpc,
    /// Intercept the node's output.
    Intercept,
    /// Enable, bypass or disable the node.
    NodeState,
}

impl ControlOp {
    pub fn as_str(self) -> &'static str {
        match self {
            ControlOp::Tap => "tap",
            ControlOp::Publish => "publish",
            ControlOp::Rpc => "rpc",
            ControlOp::Intercept => "intercept",
            ControlOp::NodeState => "node_state",
        }
    }

    pub fn parse(op: &str) -> Option<Self> {
        Some(match op {
            "tap" => ControlOp::Tap,
            "publish" => ControlOp::Publish,
            "rpc" => ControlOp::Rpc,
            "intercept" => ControlOp::Intercept,
            "node_state" => ControlOp::NodeState,
            _ => return None,
        })
    }
}

/// One grant: an operation on a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlGrant {
    pub op: ControlOp,
    pub node: String,
    /// A tap limited to frames produced from the member's own input.
    pub own: bool,
    /// The methods an `Rpc` grant may call.
    pub methods: Vec<String>,
}

impl ControlGrant {
    pub fn new(op: ControlOp, node: impl Into<String>) -> Self {
        Self {
            op,
            node: node.into(),
            own: false,
            methods: Vec::new(),
        }
    }

    pub fn own(mut self) -> Self {
        self.own = true;
        self
    }

    pub fn methods<I, S>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.methods = methods.into_iter().map(Into::into).collect();
        self
    }
}

/// Which frames a granted tap delivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapScope {
    /// Every frame of the node's output.
    All,
    /// Only frames produced from the member's own input.
    Own,
}

/// What a control attachment may do on its session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlAccess {
    /// Every operation on every node of the session.
    Full,
    /// Only what the grants cover.
    Granted(Vec<ControlGrant>),
}

impl ControlAccess {
    /// `true` when there is anything a control channel could be used for.
    pub fn allows_anything(&self) -> bool {
        match self {
            ControlAccess::Full => true,
            ControlAccess::Granted(grants) => !grants.is_empty(),
        }
    }

    fn grants(&self, op: ControlOp, node: &str) -> impl Iterator<Item = &ControlGrant> {
        let grants: &[ControlGrant] = match self {
            ControlAccess::Full => &[],
            ControlAccess::Granted(grants) => grants,
        };
        let node = node.to_string();
        grants
            .iter()
            .filter(move |grant| grant.op == op && grant.node == node)
    }

    fn refused(op: &str, node: &str) -> String {
        format!("not granted: {op} on '{node}'")
    }

    /// Subscribe to `node`'s output on `port`. The reply port of a node the
    /// member may call is covered by its `rpc` grant.
    pub fn tap(&self, node: &str, port: Option<&str>) -> Result<TapScope, String> {
        if matches!(self, ControlAccess::Full) {
            return Ok(TapScope::All);
        }
        if port == Some("__reply__") && self.grants(ControlOp::Rpc, node).next().is_some() {
            return Ok(TapScope::All);
        }
        let mut scope = None;
        for grant in self.grants(ControlOp::Tap, node) {
            if !grant.own {
                return Ok(TapScope::All);
            }
            scope = Some(TapScope::Own);
        }
        scope.ok_or_else(|| Self::refused("tap", node))
    }

    /// Publish to `node`'s input on `port`. An RPC-shaped publish (a JSON
    /// payload with a `corr_id` on a named port) is a call of the method the
    /// port names and needs an `rpc` grant that lists it; anything else needs
    /// a `publish` grant.
    pub fn publish(&self, node: &str, port: Option<&str>, rpc_shaped: bool) -> Result<(), String> {
        if matches!(self, ControlAccess::Full) {
            return Ok(());
        }
        match port {
            Some(method) if rpc_shaped => {
                let listed = self
                    .grants(ControlOp::Rpc, node)
                    .any(|grant| grant.methods.iter().any(|m| m == method));
                if listed {
                    Ok(())
                } else {
                    Err(format!("not granted: rpc '{method}' on '{node}'"))
                }
            }
            _ => self
                .grants(ControlOp::Publish, node)
                .next()
                .map(|_| ())
                .ok_or_else(|| Self::refused("publish", node)),
        }
    }

    pub fn intercept(&self, node: &str) -> Result<(), String> {
        if matches!(self, ControlAccess::Full) || self.grants(ControlOp::Intercept, node).next().is_some() {
            Ok(())
        } else {
            Err(Self::refused("intercept", node))
        }
    }

    pub fn node_state(&self, node: &str) -> Result<(), String> {
        if matches!(self, ControlAccess::Full) || self.grants(ControlOp::NodeState, node).next().is_some() {
            Ok(())
        } else {
            Err(Self::refused("node_state", node))
        }
    }

    /// Answer an intercept request: any `intercept` grant.
    pub fn intercept_reply(&self) -> Result<(), String> {
        match self {
            ControlAccess::Full => Ok(()),
            ControlAccess::Granted(grants) if grants.iter().any(|g| g.op == ControlOp::Intercept) => Ok(()),
            ControlAccess::Granted(_) => Err("not granted: intercept".to_string()),
        }
    }

    /// Operations that have no grant: session-wide settings.
    pub fn session_wide(&self, what: &str) -> Result<(), String> {
        if matches!(self, ControlAccess::Full) {
            Ok(())
        } else {
            Err(format!("not granted: {what} has no grant"))
        }
    }
}

/// A control channel's binding: whose it is and what it may do. The session
/// is the one the peer was admitted to, never one the peer names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlBinding {
    /// The member's identity (`participant.id`): what it publishes is
    /// attributed to it, and an `own` tap delivers only its frames.
    pub member: String,
    pub access: ControlAccess,
}

/// How a server peer treats a `remotemedia-control` data channel.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ControlChannelPolicy {
    /// The peer names a session in its `Hello` and gets the whole bus. For
    /// hosts that do not admit peers (a local single-user server).
    #[default]
    Open,
    /// The channel is closed as soon as it opens.
    Refused,
    /// Bound to the peer's own session and checked against its access.
    Bound(ControlBinding),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access() -> ControlAccess {
        ControlAccess::Granted(vec![
            ControlGrant::new(ControlOp::Tap, "transcriber").own(),
            ControlGrant::new(ControlOp::Tap, "mixer"),
            ControlGrant::new(ControlOp::Publish, "chat_in"),
            ControlGrant::new(ControlOp::Rpc, "avatar").methods(["set_expression"]),
            ControlGrant::new(ControlOp::Intercept, "llm"),
            ControlGrant::new(ControlOp::NodeState, "vad"),
        ])
    }

    #[test]
    fn full_access_allows_everything() {
        let full = ControlAccess::Full;
        assert_eq!(full.tap("any", None), Ok(TapScope::All));
        assert!(full.publish("any", Some("m"), true).is_ok());
        assert!(full.intercept("any").is_ok());
        assert!(full.node_state("any").is_ok());
        assert!(full.session_wide("telemetry").is_ok());
        assert!(full.allows_anything());
    }

    #[test]
    fn taps_follow_their_grants() {
        let access = access();
        assert_eq!(access.tap("transcriber", None), Ok(TapScope::Own));
        assert_eq!(access.tap("mixer", None), Ok(TapScope::All));
        assert_eq!(access.tap("llm", None), Err("not granted: tap on 'llm'".into()));
        // An rpc grant covers the reply port of that node, and nothing else of it.
        assert_eq!(access.tap("avatar", Some("__reply__")), Ok(TapScope::All));
        assert!(access.tap("avatar", None).is_err());
        assert!(access.tap("chat_in", Some("__reply__")).is_err());
    }

    #[test]
    fn a_wider_tap_grant_wins_over_an_own_grant() {
        let access = ControlAccess::Granted(vec![
            ControlGrant::new(ControlOp::Tap, "t").own(),
            ControlGrant::new(ControlOp::Tap, "t"),
        ]);
        assert_eq!(access.tap("t", None), Ok(TapScope::All));
    }

    #[test]
    fn publish_and_rpc_are_separate_grants() {
        let access = access();
        assert!(access.publish("chat_in", None, false).is_ok());
        assert!(access.publish("chat_in", Some("context"), false).is_ok());
        // A publish grant does not let the member call methods.
        assert_eq!(
            access.publish("chat_in", Some("reset"), true),
            Err("not granted: rpc 'reset' on 'chat_in'".into())
        );
        assert!(access.publish("avatar", Some("set_expression"), true).is_ok());
        assert!(access.publish("avatar", Some("shutdown"), true).is_err());
        // An rpc grant does not let the member publish media.
        assert_eq!(
            access.publish("avatar", None, false),
            Err("not granted: publish on 'avatar'".into())
        );
        assert!(access.publish("transcriber", None, false).is_err());
    }

    #[test]
    fn intercept_node_state_and_session_wide() {
        let access = access();
        assert!(access.intercept("llm").is_ok());
        assert!(access.intercept("vad").is_err());
        assert!(access.node_state("vad").is_ok());
        assert!(access.node_state("llm").is_err());
        assert!(access.session_wide("telemetry").is_err());
    }

    #[test]
    fn no_grants_allow_nothing() {
        let none = ControlAccess::Granted(Vec::new());
        assert!(!none.allows_anything());
        assert!(none.tap("a", None).is_err());
        assert!(none.publish("a", None, false).is_err());
    }
}
