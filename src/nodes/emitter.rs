//! Node-initiated output (openspec: add-node-initiated-output).
//!
//! Every node instance has an [`Emitter`]: a handle onto the instance's own
//! output edge that needs no input in progress. What a node returns or yields
//! while processing an input stays that input's output; an emitted frame is
//! the node speaking for itself.
//!
//! ```ignore
//! async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<()> {
//!     let emitter = Emitter::of(ctx);
//!     tokio::spawn(async move {
//!         while emitter.emit(RuntimeData::Text("tick".into())).await.is_ok() {
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!         }
//!     });
//!     Ok(())
//! }
//! ```
//!
//! The handle is created before `initialize` and connected to the instance's
//! fan-out queue when the instance's tasks start, so an emit made in between
//! waits for the connection. It is closed when the instance stops; emitting
//! afterwards is an error, and [`Emitter::closed`] is the stop signal for
//! tasks a node spawned.
//!
//! Emitted frames are unattributed unless the node says otherwise: see
//! [`EmitOrigin`]. A per-participant instance emits as its participant by
//! default.

use crate::data::RuntimeData;
use crate::telemetry::SessionTelemetry;
use crate::transport::origin::{Origin, Roster, Routed};
use crate::{Error, Result};
use remotemedia_traits::runtime_context::InitializeContextRead;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Who an emitted frame is attributed to.
#[derive(Debug, Clone, Default)]
pub enum EmitOrigin {
    /// The instance's default: its participant for a per-participant
    /// instance, nobody for a shared one.
    #[default]
    Default,
    /// Nobody, also on a per-participant instance.
    Unattributed,
    /// An origin the node read from an input (`ctx.origin()`).
    Origin(Arc<Origin>),
    /// A participant currently in the session, by `participant.id`.
    Participant(String),
}

impl From<Arc<Origin>> for EmitOrigin {
    fn from(origin: Arc<Origin>) -> Self {
        Self::Origin(origin)
    }
}

/// What `ctx.origin()` returns: `None` is an unattributed input.
impl From<Option<Arc<Origin>>> for EmitOrigin {
    fn from(origin: Option<Arc<Origin>>) -> Self {
        origin.map_or(Self::Unattributed, Self::Origin)
    }
}

impl From<&str> for EmitOrigin {
    fn from(participant_id: &str) -> Self {
        Self::Participant(participant_id.to_string())
    }
}

impl From<String> for EmitOrigin {
    fn from(participant_id: String) -> Self {
        Self::Participant(participant_id)
    }
}

/// Why a non-waiting emit did not queue its frame. The frame is handed back.
#[derive(Debug)]
pub enum TryEmitError {
    /// The emit queue is full; [`Emitter::emit`] would wait.
    Full(RuntimeData),
    /// The instance has not started its tasks yet.
    NotStarted(RuntimeData),
    /// The frame cannot be emitted: the instance stopped, or the origin
    /// names an unknown participant.
    Rejected(RuntimeData, Error),
}

impl std::fmt::Display for TryEmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_) => f.write_str("emit queue is full"),
            Self::NotStarted(_) => f.write_str("node instance has not started"),
            Self::Rejected(_, error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for TryEmitError {}

/// What an open emitter is connected to.
struct Binding {
    tx: mpsc::Sender<Routed>,
    default_origin: Option<Arc<Origin>>,
    roster: Arc<Roster>,
    /// The node's telemetry slot, when the session has one.
    telemetry: Option<(Arc<SessionTelemetry>, usize)>,
}

enum State {
    /// Created, the instance's tasks have not started.
    Pending,
    Open(Binding),
    Closed,
}

struct Shared {
    node_id: String,
    state: parking_lot::Mutex<State>,
    /// Woken on every state change.
    changed: tokio::sync::Notify,
}

/// A node instance's handle onto its own output edge. Cheap to clone; every
/// clone is the same emitter.
#[derive(Clone)]
pub struct Emitter {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Emitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &*self.shared.state.lock() {
            State::Pending => "pending",
            State::Open(_) => "open",
            State::Closed => "closed",
        };
        f.debug_struct("Emitter")
            .field("node", &self.shared.node_id)
            .field("state", &state)
            .finish()
    }
}

impl Emitter {
    /// An emitter for an instance whose tasks have not started. Emits wait
    /// until the session router connects it.
    pub(crate) fn pending(node_id: impl Into<String>) -> Self {
        Self::with_state(node_id, State::Pending)
    }

    /// An emitter that is connected to nothing: every emit fails. What a
    /// context hands out when its host provides no emitter (unary execution,
    /// unit tests).
    pub fn detached(node_id: impl Into<String>) -> Self {
        Self::with_state(node_id, State::Closed)
    }

    fn with_state(node_id: impl Into<String>, state: State) -> Self {
        Self {
            shared: Arc::new(Shared {
                node_id: node_id.into(),
                state: parking_lot::Mutex::new(state),
                changed: tokio::sync::Notify::new(),
            }),
        }
    }

    /// The emitter of the instance being initialized, or a detached one when
    /// the host provides none.
    pub fn of(ctx: &dyn InitializeContextRead) -> Self {
        ctx.emitter_any()
            .and_then(|emitter| emitter.downcast_ref::<Emitter>())
            .cloned()
            .unwrap_or_else(|| Self::detached(ctx.node_id()))
    }

    /// The emitter of the instance a per-call method runs on, or a detached
    /// one when the call does not come from a streaming session.
    pub fn for_call(ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead) -> Self {
        match crate::nodes::NodeRuntimeContext::of(ctx) {
            Some(ctx) => ctx.emitter(),
            None => Self::detached(ctx.node_id()),
        }
    }

    /// Connect to the instance's fan-out queue. No effect once closed.
    pub(crate) fn bind(
        &self,
        tx: mpsc::Sender<Routed>,
        default_origin: Option<Arc<Origin>>,
        roster: Arc<Roster>,
        telemetry: Option<(Arc<SessionTelemetry>, usize)>,
    ) {
        {
            let mut state = self.shared.state.lock();
            if !matches!(*state, State::Pending) {
                return;
            }
            *state = State::Open(Binding {
                tx,
                default_origin,
                roster,
                telemetry,
            });
        }
        self.shared.changed.notify_waiters();
    }

    /// Close the emitter for every clone. Frames already queued are still
    /// delivered; later emits fail. A node calls this to say it has finished
    /// producing; the runtime calls it when the instance stops.
    pub fn finish(&self) {
        *self.shared.state.lock() = State::Closed;
        self.shared.changed.notify_waiters();
    }

    /// Closes the emitter when dropped, so an instance whose task is aborted
    /// does not keep its output queue open.
    pub(crate) fn close_on_drop(&self) -> CloseOnDrop {
        CloseOnDrop(self.clone())
    }

    /// Whether emitting can no longer succeed.
    pub fn is_closed(&self) -> bool {
        matches!(*self.shared.state.lock(), State::Closed)
    }

    /// Resolves when the instance's tasks have started and emits no longer
    /// wait for them (or the emitter is closed). A node's `initialize` runs
    /// while other nodes are still loading; a task that keeps time waits for
    /// this before it starts its clock.
    pub async fn started(&self) {
        loop {
            let changed = self.shared.changed.notified();
            if !matches!(*self.shared.state.lock(), State::Pending) {
                return;
            }
            changed.await;
        }
    }

    /// Resolves when the emitter is closed: the stop signal for a task that
    /// holds a clone.
    pub async fn closed(&self) {
        loop {
            let changed = self.shared.changed.notified();
            if self.is_closed() {
                return;
            }
            changed.await;
        }
    }

    /// Emit a frame with the instance's default attribution. Waits while the
    /// emit queue is full.
    pub async fn emit(&self, data: RuntimeData) -> Result<()> {
        self.emit_as(data, EmitOrigin::Default).await
    }

    /// Emit a frame attributed to `origin`: one read from an input, or a
    /// current participant's id. Waits while the emit queue is full.
    pub async fn emit_as(&self, data: RuntimeData, origin: impl Into<EmitOrigin>) -> Result<()> {
        let origin = origin.into();
        let (tx, origin, telemetry) = loop {
            let changed = self.shared.changed.notified();
            match self.target(&origin) {
                Target::Ready(target) => break target?,
                Target::Pending => changed.await,
            }
        };
        tx.send(Routed::new(data, origin))
            .await
            .map_err(|_| self.stopped())?;
        count(&telemetry);
        Ok(())
    }

    /// [`Self::emit`] without waiting: a full queue hands the frame back.
    pub fn try_emit(&self, data: RuntimeData) -> std::result::Result<(), TryEmitError> {
        self.try_emit_as(data, EmitOrigin::Default)
    }

    /// [`Self::emit_as`] without waiting: a full queue hands the frame back.
    pub fn try_emit_as(
        &self,
        data: RuntimeData,
        origin: impl Into<EmitOrigin>,
    ) -> std::result::Result<(), TryEmitError> {
        let (tx, origin, telemetry) = match self.target(&origin.into()) {
            Target::Pending => return Err(TryEmitError::NotStarted(data)),
            Target::Ready(Err(error)) => return Err(TryEmitError::Rejected(data, error)),
            Target::Ready(Ok(target)) => target,
        };
        match tx.try_send(Routed::new(data, origin)) {
            Ok(()) => {
                count(&telemetry);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(frame)) => Err(TryEmitError::Full(frame.data)),
            Err(mpsc::error::TrySendError::Closed(frame)) => {
                Err(TryEmitError::Rejected(frame.data, self.stopped()))
            }
        }
    }

    /// The queue and resolved attribution for one emit. The sender clone is
    /// held only for that emit, so a closed emitter never keeps the
    /// instance's output queue open.
    fn target(&self, origin: &EmitOrigin) -> Target {
        let state = self.shared.state.lock();
        let binding = match &*state {
            State::Pending => return Target::Pending,
            State::Closed => return Target::Ready(Err(self.stopped())),
            State::Open(binding) => binding,
        };
        let resolved = match origin {
            EmitOrigin::Default => binding.default_origin.clone(),
            EmitOrigin::Unattributed => None,
            EmitOrigin::Origin(origin) => Some(origin.clone()),
            EmitOrigin::Participant(id) => match binding.roster.get(id) {
                Some(origin) => Some(origin),
                None => {
                    return Target::Ready(Err(Error::InvalidData(format!(
                        "node '{}' emitted as participant '{id}', who is not in the session",
                        self.shared.node_id
                    ))))
                }
            },
        };
        Target::Ready(Ok((binding.tx.clone(), resolved, binding.telemetry.clone())))
    }

    fn stopped(&self) -> Error {
        Error::Execution(format!(
            "node '{}' cannot emit: the node instance is not running",
            self.shared.node_id
        ))
    }
}

type Resolved = (
    mpsc::Sender<Routed>,
    Option<Arc<Origin>>,
    Option<(Arc<SessionTelemetry>, usize)>,
);

enum Target {
    Pending,
    Ready(Result<Resolved>),
}

fn count(telemetry: &Option<(Arc<SessionTelemetry>, usize)>) {
    if let Some((telemetry, index)) = telemetry {
        telemetry.node(*index).emitted.fetch_add(1, Relaxed);
    }
}

/// See [`Emitter::close_on_drop`].
pub(crate) struct CloseOnDrop(Emitter);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::data::participant;
    use std::collections::HashMap;
    use std::time::Duration;

    fn origin(id: &str) -> Arc<Origin> {
        let metadata = HashMap::from([(participant::ID.to_string(), id.to_string())]);
        Arc::new(Origin::from_metadata(&metadata).unwrap())
    }

    fn text(value: &str) -> RuntimeData {
        RuntimeData::Text(value.into())
    }

    fn open(capacity: usize, default: Option<Arc<Origin>>) -> (Emitter, mpsc::Receiver<Routed>, Arc<Roster>) {
        let (tx, rx) = mpsc::channel(capacity);
        let roster = Arc::new(Roster::default());
        let emitter = Emitter::pending("node");
        emitter.bind(tx, default, roster.clone(), None);
        (emitter, rx, roster)
    }

    #[tokio::test]
    async fn an_emit_before_the_instance_starts_waits_for_it() {
        let emitter = Emitter::pending("node");
        let early = tokio::spawn({
            let emitter = emitter.clone();
            async move { emitter.emit(text("early")).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!early.is_finished());
        assert!(matches!(emitter.try_emit(text("x")), Err(TryEmitError::NotStarted(_))));

        let (tx, mut rx) = mpsc::channel(4);
        emitter.bind(tx, None, Arc::new(Roster::default()), None);
        early.await.unwrap().unwrap();
        assert!(matches!(rx.recv().await.unwrap().data, RuntimeData::Text(t) if t == "early"));
    }

    #[tokio::test]
    async fn attribution_is_the_default_unless_the_node_names_one() {
        let (emitter, mut rx, roster) = open(8, Some(origin("alice")));
        roster.note(&origin("bob"));

        emitter.emit(text("default")).await.unwrap();
        emitter.emit_as(text("nobody"), EmitOrigin::Unattributed).await.unwrap();
        emitter.emit_as(text("kept"), origin("carol")).await.unwrap();
        emitter.emit_as(text("by id"), "bob").await.unwrap();
        emitter.emit_as(text("input without one"), None).await.unwrap();

        let mut seen = Vec::new();
        for _ in 0..5 {
            let frame = rx.recv().await.unwrap();
            seen.push(frame.origin.map(|origin| origin.id().to_string()));
        }
        assert_eq!(
            seen,
            vec![Some("alice".into()), None, Some("carol".into()), Some("bob".into()), None]
        );
    }

    #[tokio::test]
    async fn an_unknown_participant_is_an_error_and_nothing_is_queued() {
        let (emitter, mut rx, roster) = open(8, None);
        let error = emitter.emit_as(text("x"), "ghost").await.unwrap_err();
        assert!(error.to_string().contains("ghost"), "{error}");
        assert!(rx.try_recv().is_err());

        // A participant who left is unknown again.
        roster.note(&origin("bob"));
        emitter.emit_as(text("x"), "bob").await.unwrap();
        roster.leave("bob");
        assert!(emitter.emit_as(text("x"), "bob").await.is_err());
    }

    #[tokio::test]
    async fn a_full_queue_makes_emit_wait_and_try_emit_report_it() {
        let (emitter, mut rx, _roster) = open(1, None);
        emitter.emit(text("1")).await.unwrap();
        assert!(matches!(emitter.try_emit(text("2")), Err(TryEmitError::Full(RuntimeData::Text(t))) if t == "2"));

        let waiting = tokio::spawn({
            let emitter = emitter.clone();
            async move { emitter.emit(text("2")).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "emit must wait for room");

        assert!(matches!(rx.recv().await.unwrap().data, RuntimeData::Text(t) if t == "1"));
        waiting.await.unwrap().unwrap();
        assert!(matches!(rx.recv().await.unwrap().data, RuntimeData::Text(t) if t == "2"));
    }

    #[tokio::test]
    async fn closing_fails_later_emits_on_every_clone_and_releases_the_queue() {
        let (emitter, mut rx, _roster) = open(4, None);
        let clone = emitter.clone();
        emitter.emit(text("before")).await.unwrap();

        let stopped = tokio::spawn({
            let clone = clone.clone();
            async move { clone.closed().await }
        });
        drop(emitter.close_on_drop());
        stopped.await.unwrap();

        assert!(clone.is_closed());
        assert!(clone.emit(text("after")).await.is_err());
        assert!(matches!(clone.try_emit(text("after")), Err(TryEmitError::Rejected(..))));
        // The queued frame is still delivered, then the queue ends: the
        // emitter holds no sender once closed.
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_detached_emitter_fails() {
        assert!(Emitter::detached("node").emit(text("x")).await.is_err());
    }
}
