//! Reusable warm-session pool for [`PipelineExecutor`].
//!
//! Heavy pipeline nodes (e.g. LlamaCpp loading a 27B GGUF, Audio2Face
//! compiling ONNX graphs, Python-multiprocess subprocesses, wgpu device
//! init) cold-load in 30–60s the first time their `initialize()` runs.
//! Without prewarming, every first-time peer connection waits for that
//! load before any data flows.
//!
//! [`WarmSessionPool`] solves this by pre-building [`SessionHandle`]s and
//! parking them. When a transport calls `executor.create_session`, the
//! executor (if a pool is attached) consumes a parked entry transparently
//! and refills in the background.
//!
//! # Drop-in for any pipeline
//!
//! ```ignore
//! use std::sync::Arc;
//! use remotemedia_core::transport::{PipelineExecutor, WarmSessionPool};
//!
//! let executor = Arc::new(PipelineExecutor::with_config(cfg)?);
//! let manifest = Arc::new(build_manifest()?);
//!
//! let pool = WarmSessionPool::new(executor.clone());
//! pool.prewarm(manifest.clone()).await?;          // blocks until hot
//! pool.set_target(manifest.clone(), 1).await;     // enable auto-refill
//! executor.set_default_pool(pool.clone());
//!
//! // IMPORTANT: keep `pool` alive for as long as you want delegation
//! // active. The executor stores `Arc::downgrade(&pool)`, so dropping
//! // the caller's last `Arc<WarmSessionPool>` silently disables
//! // delegation — `create_session` falls back to cold-build.
//! let _pool_keepalive = pool;
//!
//! // Transports call `executor.create_session(manifest)` unchanged;
//! // each call now consumes a warm session if one is parked.
//! ```
//!
//! # Phases
//!
//! 1. [`WarmSessionPool::prewarm`]: build + park one warm session.
//! 2. [`WarmSessionPool::set_target`]: declare the desired pool size.
//!    Refills schedule automatically via background tasks.
//! 3. [`WarmSessionPool::acquire`]: pull a warm session out (or
//!    block-and-wait on an in-flight refill, or cold-build via the
//!    underlying executor).
//! 4. A per-pool watchdog prunes parked entries whose router task has
//!    exited unexpectedly, scheduling refills as needed.
//! 5. Refill failures retry with exponential backoff (configurable via
//!    [`WarmPoolConfig`]) and give up after
//!    [`WarmPoolConfig::refill_max_failures`].
//!
//! # Pool lifetime
//!
//! The pool holds `Arc<PipelineExecutor>` (strong) so the executor
//! cannot drop while the pool exists. The executor holds
//! `Weak<WarmSessionPool>` — so the pool drops cleanly when its caller
//! releases the last `Arc`. Watchdog and refill tasks observe
//! `Weak::upgrade() == None` on their next tick and exit.
//!
//! See `openspec/changes/add-warm-session-pool/design.md` for the full
//! design (D3 canonical-JSON hashed key with collision defence; D4
//! prewarm/set_target separation with `Notify`-coalesced refills; D5
//! acquire's block-and-wait fallback).

use crate::manifest::Manifest;
use crate::transport::executor::PipelineExecutor;
use crate::transport::executor::SessionHandle;
use crate::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hasher;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};

/// Configuration knobs for [`WarmSessionPool`]. All fields have sensible
/// defaults; tune only when you need different behaviour.
#[derive(Debug, Clone)]
pub struct WarmPoolConfig {
    /// How often the watchdog (Phase 5) polls for dead entries.
    pub watchdog_interval: Duration,
    /// Initial backoff for failed refills. Doubled on each subsequent
    /// failure up to [`Self::refill_backoff_cap`]. Reset to this value
    /// after a successful refill.
    pub refill_backoff_initial: Duration,
    /// Cap for the exponential backoff curve.
    pub refill_backoff_cap: Duration,
    /// Maximum consecutive refill failures before the pool gives up
    /// for this manifest. Once exceeded, the in-flight marker is
    /// cleared and waiters are signalled so they fall through to
    /// cold-build (or surface the error).
    pub refill_max_failures: u32,
    /// Cap for the per-`SessionControl` `__system__` retained ring buffer
    /// (used by a later phase).
    pub system_retained_cap: usize,
}

impl Default for WarmPoolConfig {
    fn default() -> Self {
        Self {
            watchdog_interval: Duration::from_secs(1),
            refill_backoff_initial: Duration::from_secs(1),
            refill_backoff_cap: Duration::from_secs(60),
            refill_max_failures: 5,
            system_retained_cap: 256,
        }
    }
}

/// Reusable warm-session pool that pre-builds and parks
/// [`SessionHandle`]s so the first peer-connect doesn't pay the
/// 30–60s cold-load cost for heavy nodes.
///
/// See the module-level docs for the full lifetime contract: the
/// executor stores `Arc::downgrade(&pool)`, so the caller MUST keep
/// their strong `Arc<WarmSessionPool>` alive for delegation to remain
/// active.
///
/// # Drop-in for any pipeline
///
/// ```ignore
/// let pool = WarmSessionPool::new(executor.clone());
/// pool.prewarm(manifest.clone()).await?;
/// pool.set_target(manifest.clone(), 1).await;
/// executor.set_default_pool(pool.clone());
/// let _pool_keepalive = pool; // executor only holds Weak — keep Arc alive
/// ```
///
/// Public surface: [`Self::new`], [`Self::with_config`],
/// [`Self::prewarm`], [`Self::acquire`], [`Self::pool_size`],
/// [`Self::set_target`].
pub struct WarmSessionPool {
    executor: Arc<PipelineExecutor>,
    config: WarmPoolConfig,
    state: Mutex<PoolState>,
    /// Used internally to spawn tasks that outlive the calling stack
    /// frame. Set in [`Self::with_config`] via [`Arc::new_cyclic`].
    self_weak: Weak<Self>,
    /// Lazily started on first `set_target` or `prewarm` call. Pool with
    /// no work to do (no targets, no prewarm) spawns no watchdog.
    /// Phase 5 of the warm-session-pool feature.
    watchdog_handle: std::sync::OnceLock<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct PoolState {
    /// Manifest hash → FIFO queue of parked entries.
    entries: HashMap<u64, VecDeque<WarmEntry>>,
    /// Manifest hash → (manifest Arc, target pool size).
    targets: HashMap<u64, (Arc<Manifest>, usize)>,
    /// Manifest hash → `Notify` shared by the in-flight refill task and
    /// any waiters. Presence in this map means a refill is currently
    /// in flight; absence means none.
    refill_in_flight: HashMap<u64, Arc<Notify>>,
    /// Hashes whose refill chain has hit `refill_max_failures` and given up.
    /// Cleared by a successful `prewarm` (manual override) or by any
    /// subsequent `set_target` call (operator retry). Watchdog and
    /// `acquire`-triggered refills SKIP hashes in this set so a
    /// permanently-broken manifest doesn't trigger log-spam storms.
    gave_up: HashSet<u64>,
}

struct WarmEntry {
    handle: SessionHandle,
    /// Canonical-JSON serialization of the manifest used to build this
    /// entry. Compared byte-for-byte against the requesting manifest's
    /// canonical JSON to defend against the astronomically-unlikely
    /// 64-bit hash collision (D3 in design.md).
    manifest_json: String,
    #[allow(dead_code)] // Watchdog (Phase 5) reads this; Phase 4 only writes it.
    parked_at: Instant,
}

impl WarmSessionPool {
    /// Construct a pool with the default [`WarmPoolConfig`].
    pub fn new(executor: Arc<PipelineExecutor>) -> Arc<Self> {
        Self::with_config(executor, WarmPoolConfig::default())
    }

    /// Construct a pool with custom configuration.
    pub fn with_config(executor: Arc<PipelineExecutor>, config: WarmPoolConfig) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            executor,
            config,
            state: Mutex::new(PoolState::default()),
            self_weak: self_weak.clone(),
            watchdog_handle: std::sync::OnceLock::new(),
        })
    }

    /// Get an owned `Arc<Self>` for spawning tasks. Returns `None` only
    /// if the pool is being dropped — in which case the caller should
    /// bail.
    fn arc_self(&self) -> Option<Arc<Self>> {
        self.self_weak.upgrade()
    }

    /// Build and park exactly one warm session for the given manifest.
    /// Awaits `initialize_nodes()` to completion (i.e. all heavy node
    /// loads finish before this call returns).
    ///
    /// Phase 4: idempotent under concurrent calls. If a refill task is
    /// already in flight for `manifest`, this call joins that task's
    /// `Notify` and returns when it completes — it does NOT start a
    /// second parallel build (D4 in design.md).
    pub async fn prewarm(&self, manifest: Arc<Manifest>) -> Result<()> {
        let (hash, json) = compute_manifest_hash(&manifest);
        // Phase 5: a prewarmed entry benefits from watchdog pruning even
        // without a registered target.
        self.ensure_watchdog();

        // If a refill is already in flight for this manifest, join it.
        let join_notify = {
            let state = self.state.lock().await;
            state.refill_in_flight.get(&hash).cloned()
        };
        if let Some(notify) = join_notify {
            notify.notified().await;
            // Verify a session was actually parked before reporting Ok.
            // C2 fix: previously we returned Ok unconditionally, masking
            // refill chains that gave up.
            let state = self.state.lock().await;
            if state
                .entries
                .get(&hash)
                .map(|q| q.iter().any(|e| e.manifest_json == json))
                .unwrap_or(false)
            {
                return Ok(());
            }
            if state.gave_up.contains(&hash) {
                return Err(crate::Error::Execution(format!(
                    "warm-pool refill chain gave up for manifest hash {hash}; \
                     fix the manifest or call set_target() again to retry"
                )));
            }
            // Marker is gone but no entry parked — something else cleaned
            // up. Drop the lock and run one fresh prewarm cycle.
            drop(state);
            return self.prewarm_fresh(manifest, hash, json).await;
        }

        self.prewarm_fresh(manifest, hash, json).await
    }

    /// Internal: install the in-flight marker, cold-build, park the
    /// resulting handle, and signal waiters. No join branch — callers
    /// resolve "join vs. fresh build" before calling this.
    async fn prewarm_fresh(&self, manifest: Arc<Manifest>, hash: u64, json: String) -> Result<()> {
        // Register an in-flight marker so concurrent callers join us.
        let notify = Arc::new(Notify::new());
        {
            let mut state = self.state.lock().await;
            // Race: another task may have inserted between our check
            // and now. If so, drop our notify and join theirs.
            if let Some(existing) = state.refill_in_flight.get(&hash).cloned() {
                drop(state);
                existing.notified().await;
                // Re-check for a parked entry; if none, surface the
                // failure rather than silently looping.
                let state = self.state.lock().await;
                if state
                    .entries
                    .get(&hash)
                    .map(|q| q.iter().any(|e| e.manifest_json == json))
                    .unwrap_or(false)
                {
                    return Ok(());
                }
                if state.gave_up.contains(&hash) {
                    return Err(crate::Error::Execution(format!(
                        "warm-pool refill chain gave up for manifest hash {hash}; \
                         fix the manifest or call set_target() again to retry"
                    )));
                }
                return Err(crate::Error::Execution(format!(
                    "warm-pool concurrent refill for manifest hash {hash} did not produce an entry"
                )));
            }
            state.refill_in_flight.insert(hash, notify.clone());
        }

        let outcome = self.executor.cold_build_session(manifest).await;

        let mut state = self.state.lock().await;
        state.refill_in_flight.remove(&hash);
        match outcome {
            Ok(handle) => {
                // I1 fix: propagate the pool's configured retained cap
                // to every freshly-built session.
                if let Some(control) = self.executor.control_bus().get(&handle.session_id) {
                    control.set_system_retained_cap(self.config.system_retained_cap);
                }
                // C1 fix: a successful prewarm clears any prior give-up
                // marker — operator manual override.
                state.gave_up.remove(&hash);
                state.entries.entry(hash).or_default().push_back(WarmEntry {
                    handle,
                    manifest_json: json,
                    parked_at: Instant::now(),
                });
                drop(state);
                notify.notify_waiters();
                Ok(())
            }
            Err(e) => {
                drop(state);
                notify.notify_waiters();
                Err(e)
            }
        }
    }

    /// Acquire a session for `manifest` — warm if available, otherwise
    /// either block-and-wait on an in-flight refill (when a target>0
    /// policy is set for this manifest) or cold-build directly.
    ///
    /// Phase 4: implements D5's block-and-wait fallback. After
    /// consuming a warm entry, schedules a refill task in the
    /// background if `pool_size < target`.
    pub async fn acquire(&self, manifest: Arc<Manifest>) -> Result<SessionHandle> {
        let (hash, json) = compute_manifest_hash(&manifest);

        loop {
            // Phase 1: snapshot pool state under the lock. Build owned
            // values so we can drop the lock before any await.
            //
            // I2 fix: when consuming a warm entry triggers a refill, we
            // install the refill marker SYNCHRONOUSLY (under this same
            // lock) and pass it into `spawn_refill_task`. This closes the
            // window in which a concurrent `acquire` would observe no
            // marker and cold-build in parallel with the spawned task.
            let (warm, await_refill, scheduled_refill) = {
                let mut state = self.state.lock().await;
                let target = state.targets.get(&hash).map(|(_, n)| *n).unwrap_or(0);

                let mut popped: Option<SessionHandle> = None;
                let mut hash_collision = false;
                if let Some(queue) = state.entries.get_mut(&hash) {
                    if let Some(front) = queue.front() {
                        if front.manifest_json == json {
                            let entry = queue.pop_front().expect("front existed under same lock");
                            popped = Some(entry.handle);
                        } else {
                            // 64-bit hash collision: stored entry has a
                            // different canonical JSON. Leave it parked
                            // and fall through to cold-build for this
                            // request (D3).
                            hash_collision = true;
                        }
                    }
                }

                if let Some(handle) = popped {
                    let remaining = state.entries.get(&hash).map(|q| q.len()).unwrap_or(0);
                    // C1 fix: don't schedule a refill when the chain has
                    // given up. Operator must call prewarm() or
                    // set_target() to clear the give-up flag.
                    let scheduled = if remaining < target
                        && !state.gave_up.contains(&hash)
                        && !state.refill_in_flight.contains_key(&hash)
                    {
                        let n = Arc::new(Notify::new());
                        state.refill_in_flight.insert(hash, n.clone());
                        Some(n)
                    } else {
                        None
                    };
                    (Some(handle), None, scheduled)
                } else if hash_collision {
                    // Skip pool, no waiting — go straight to cold-build.
                    (None, None, None)
                } else {
                    // Pool empty for this hash. If a refill is in flight
                    // and a target policy is set, wait for the refill;
                    // otherwise fall through to cold-build.
                    let waiter = state.refill_in_flight.get(&hash).cloned();
                    if waiter.is_some() && target > 0 {
                        (None, waiter, None)
                    } else {
                        (None, None, None)
                    }
                }
            };

            // Phase 2: act on the snapshot (lock dropped).
            if let Some(handle) = warm {
                if let Some(pre_notify) = scheduled_refill {
                    if let Some(this) = self.arc_self() {
                        Self::spawn_refill_task(this, manifest.clone(), hash, Some(pre_notify));
                    }
                }
                return Ok(handle);
            }

            if let Some(notify) = await_refill {
                notify.notified().await;
                // Loop and retry the pool lookup. If the refill
                // succeeded, we'll find a warm entry. If it failed,
                // the marker is gone and target may have been satisfied
                // by some other path or we'll fall through to
                // cold-build below.
                continue;
            }

            // Pool empty + no in-flight refill to wait on. Cold-build.
            return self.executor.cold_build_session(manifest).await;
        }
    }

    /// Number of parked entries for the given manifest. Useful for
    /// diagnostics and tests.
    pub async fn pool_size(&self, manifest: &Manifest) -> usize {
        let (hash, _json) = compute_manifest_hash(manifest);
        self.state
            .lock()
            .await
            .entries
            .get(&hash)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Set the desired pool size for `manifest`. The pool will maintain
    /// up to `target` warm entries via background refill tasks.
    ///
    /// Calling with `target = 0` removes the policy without draining
    /// existing entries — they remain in the pool until consumed by
    /// `acquire` or pruned by the watchdog (added in Phase 5).
    ///
    /// Returns once the target has been recorded; refill tasks are
    /// scheduled in the background and continue running after this call
    /// returns. Await `prewarm` to block on the first fill, or call
    /// `acquire` and rely on its block-and-wait fallback.
    pub async fn set_target(&self, manifest: Arc<Manifest>, target: usize) {
        let (hash, _json) = compute_manifest_hash(&manifest);

        let kick = {
            let mut state = self.state.lock().await;
            if target == 0 {
                state.targets.remove(&hash);
                return;
            }
            // C1 fix: operator setting a (positive) target clears the
            // gave-up flag — they're explicitly asking us to retry.
            state.gave_up.remove(&hash);
            state.targets.insert(hash, (manifest.clone(), target));
            let parked = state.entries.get(&hash).map(|q| q.len()).unwrap_or(0);
            let in_flight = state.refill_in_flight.contains_key(&hash);
            // We need to start the refill loop only if (a) the pool is
            // below target AND (b) no refill task is currently driving
            // it. The refill task self-chains until it observes
            // pool_size >= target, so a single kick is sufficient.
            parked < target && !in_flight
        };

        // Phase 5: target>0 means we have work to watch over.
        self.ensure_watchdog();

        if kick {
            if let Some(this) = self.arc_self() {
                Self::spawn_refill_task(this, manifest, hash, None);
            }
        }
    }

    /// Spawn the watchdog task on first call. Subsequent calls are no-ops.
    /// The watchdog holds `Weak<Self>` so it does not prevent the pool from
    /// being dropped — it observes the upgrade returning `None` on its next
    /// tick and exits cleanly.
    fn ensure_watchdog(&self) {
        if self.watchdog_handle.get().is_some() {
            return;
        }
        // Need an Arc<Self> to downgrade for the watchdog. arc_self()
        // returns None only while the last Arc is being dropped, in which
        // case there is no work to start.
        let pool_arc = match self.self_weak.upgrade() {
            Some(p) => p,
            None => return,
        };
        let weak = Arc::downgrade(&pool_arc);
        // Release the temporary strong ref so the pool can be dropped freely.
        drop(pool_arc);
        let interval = self.config.watchdog_interval;
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Skip the immediate first tick so the watchdog doesn't fire
            // before any entry has been parked.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let pool = match weak.upgrade() {
                    Some(p) => p,
                    None => {
                        tracing::trace!("warm-pool watchdog exiting: pool dropped");
                        return;
                    }
                };
                let to_refill = pool.prune_dead_entries().await;
                for (hash, manifest) in to_refill {
                    Self::spawn_refill_task(pool.clone(), manifest, hash, None);
                }
            }
        });
        // OnceLock::set returns Err if another thread won the race. Either
        // way exactly one watchdog is now running — abort the loser's
        // task to keep the invariant clean.
        if let Err(losing_handle) = self.watchdog_handle.set(handle) {
            losing_handle.abort();
        }
    }

    /// Walk all parked entries and remove ones whose handle is no longer
    /// active (`SessionHandle::is_active` returns `false`). Returns a
    /// `Vec<(hash, manifest)>` of entries that should be refilled — one
    /// entry per hash whose live count dropped below its registered target
    /// and which does not already have a refill in flight.
    async fn prune_dead_entries(self: &Arc<Self>) -> Vec<(u64, Arc<Manifest>)> {
        let mut to_refill: Vec<(u64, Arc<Manifest>)> = Vec::new();
        let mut state = self.state.lock().await;

        // Step 1: prune dead entries from each queue.
        let mut pruned = 0usize;
        for queue in state.entries.values_mut() {
            let before = queue.len();
            queue.retain(|entry| entry.handle.is_active());
            pruned += before - queue.len();
        }
        if pruned > 0 {
            tracing::debug!(pruned, "warm-pool watchdog pruned dead entries");
        }

        // Step 2: identify hashes whose live count is below their target.
        // Skip hashes that already have a refill in flight — the existing
        // task will produce the entry. Also skip hashes whose chain has
        // given up (C1 fix): re-spawning would just retrigger the same
        // failure and produce log-spam every watchdog tick.
        for (hash, queue) in state.entries.iter() {
            if state.refill_in_flight.contains_key(hash) {
                continue;
            }
            if state.gave_up.contains(hash) {
                continue;
            }
            if let Some((manifest, target)) = state.targets.get(hash) {
                if queue.len() < *target {
                    to_refill.push((*hash, manifest.clone()));
                }
            }
        }

        // Also catch the case where a target is registered but the queue
        // map has no entry at all (e.g. all entries were pruned and the
        // map entry was retained as empty — defensive in case of future
        // changes that drop empty queues).
        for (hash, (manifest, target)) in state.targets.iter() {
            if *target == 0 {
                continue;
            }
            if state.refill_in_flight.contains_key(hash) {
                continue;
            }
            if state.gave_up.contains(hash) {
                continue;
            }
            let queue_len = state.entries.get(hash).map(|q| q.len()).unwrap_or(0);
            if queue_len < *target && !to_refill.iter().any(|(h, _)| h == hash) {
                to_refill.push((*hash, manifest.clone()));
            }
        }

        to_refill
    }

    /// Spawn a refill task that builds one warm session for `manifest`
    /// and self-chains until `pool_size >= target` is observed.
    ///
    /// The task acquires the per-hash `Notify` marker on entry (so
    /// concurrent `prewarm` / `acquire` callers can join us instead of
    /// kicking parallel builds), releases it after each completion,
    /// then re-checks `pool_size < target` and re-spawns to refill the
    /// next slot. This naturally coalesces concurrent kicks: the first
    /// task drives the loop; subsequent kicks see the marker and bail.
    ///
    /// Phase 6: per-iteration cold-build with exponential backoff retry.
    /// On `cold_build_session` failure, the task increments a per-task
    /// `consecutive_failures` counter, sleeps for `current_backoff` (which
    /// doubles on each retry up to `refill_backoff_cap`), and re-attempts
    /// without releasing the in-flight marker. Once the counter reaches
    /// `refill_max_failures`, the task gives up — the marker is cleared
    /// and waiters are signalled so they fall through to cold-build (or
    /// surface the error). On success, both `consecutive_failures` and
    /// `current_backoff` reset, so a recovered chain returns to the fast
    /// path.
    ///
    /// I2 fix: callers that have already installed the in-flight marker
    /// (e.g. `acquire`'s post-consume scheduling) pass `Some(notify)` so
    /// the task uses the pre-existing marker instead of racing to insert
    /// its own. Legacy callers (`set_target`, watchdog) pass `None` and
    /// the task falls back to its original "insert or bail" logic.
    fn spawn_refill_task(
        this: Arc<Self>,
        manifest: Arc<Manifest>,
        hash: u64,
        pre_existing_notify: Option<Arc<Notify>>,
    ) {
        // Pre-compute canonical JSON once.
        let (_h, json) = compute_manifest_hash(&manifest);

        tokio::spawn(async move {
            // Acquire the per-hash marker. If a caller pre-installed
            // one, use it; otherwise insert under lock or bail if
            // someone else got here first.
            let notify = match pre_existing_notify {
                Some(n) => n,
                None => {
                    let mut state = this.state.lock().await;
                    if state.refill_in_flight.contains_key(&hash) {
                        return;
                    }
                    // C1 fix: don't restart a chain that has given up.
                    if state.gave_up.contains(&hash) {
                        return;
                    }
                    // Re-check target/parked here too: we may have raced
                    // with a target=0 reset, or another task may have
                    // already filled the pool past target.
                    let target = state.targets.get(&hash).map(|(_, n)| *n).unwrap_or(0);
                    let parked = state.entries.get(&hash).map(|q| q.len()).unwrap_or(0);
                    if target == 0 || parked >= target {
                        return;
                    }
                    let n = Arc::new(Notify::new());
                    state.refill_in_flight.insert(hash, n.clone());
                    n
                }
            };

            let mut consecutive_failures: u32 = 0;
            let mut current_backoff = this.config.refill_backoff_initial;
            let backoff_cap = this.config.refill_backoff_cap;
            let max_failures = this.config.refill_max_failures;

            'chain: loop {
                // Re-check target/parked at the top of each iteration.
                // A `set_target(_, 0)` while we were retrying should
                // break us out cleanly; same for a raced fill from
                // some other path.
                {
                    let state = this.state.lock().await;
                    let target = state.targets.get(&hash).map(|(_, n)| *n).unwrap_or(0);
                    let parked = state.entries.get(&hash).map(|q| q.len()).unwrap_or(0);
                    if target == 0 || parked >= target {
                        break 'chain;
                    }
                }

                // Cold-build outside the lock.
                let result = this.executor.cold_build_session(manifest.clone()).await;

                match result {
                    Ok(handle) => {
                        // I1 fix: propagate the pool's configured
                        // retained cap to the freshly-built session.
                        if let Some(control) = this.executor.control_bus().get(&handle.session_id) {
                            control.set_system_retained_cap(this.config.system_retained_cap);
                        }
                        let should_continue = {
                            let mut state = this.state.lock().await;
                            state.entries.entry(hash).or_default().push_back(WarmEntry {
                                handle,
                                manifest_json: json.clone(),
                                parked_at: Instant::now(),
                            });
                            tracing::debug!(
                                manifest_hash = hash,
                                "warm-pool refill produced entry"
                            );
                            let target = state.targets.get(&hash).map(|(_, n)| *n).unwrap_or(0);
                            let parked = state.entries.get(&hash).map(|q| q.len()).unwrap_or(0);
                            target > 0 && parked < target
                        };

                        // Reset failure tracking on success.
                        consecutive_failures = 0;
                        current_backoff = this.config.refill_backoff_initial;

                        // Notify waiters that a slot resolved. Successive
                        // waiters can call `notified()` again on the next
                        // iteration's notify (we keep the same Arc<Notify>
                        // for the lifetime of this task).
                        notify.notify_waiters();

                        if !should_continue {
                            break 'chain;
                        }
                        // Loop and refill the next slot.
                    }
                    Err(e) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures >= max_failures {
                            tracing::error!(
                                manifest_hash = hash,
                                consecutive_failures,
                                max_failures,
                                error = %e,
                                "warm-pool refill gave up after max consecutive failures"
                            );
                            break 'chain;
                        }
                        tracing::warn!(
                            manifest_hash = hash,
                            consecutive_failures,
                            backoff_ms = current_backoff.as_millis() as u64,
                            error = %e,
                            "warm-pool refill failed; retrying with backoff"
                        );
                        tokio::time::sleep(current_backoff).await;
                        current_backoff =
                            std::cmp::min(current_backoff.saturating_mul(2), backoff_cap);
                        // Loop and re-attempt; do NOT clear the marker
                        // — we are still actively refilling this slot.
                    }
                }
            }

            // Cleanup: the marker is owned by this task for its entire
            // lifetime. Clear it and signal any remaining waiters so
            // they don't block forever (acquire's block-and-wait
            // fallback retries the pool lookup after notification and
            // falls through to cold-build if the queue is still empty).
            //
            // C1 fix: if we exited via the give-up branch
            // (consecutive_failures >= max_failures), set the sticky
            // gave_up flag so the watchdog and acquire-triggered
            // refills skip this hash until prewarm() or set_target()
            // explicitly clears it.
            {
                let mut state = this.state.lock().await;
                state.refill_in_flight.remove(&hash);
                if consecutive_failures >= max_failures {
                    state.gave_up.insert(hash);
                    tracing::error!(
                        manifest_hash = hash,
                        "warm-pool refill given up; subsequent acquire will cold-build until prewarm() or set_target() is called again"
                    );
                }
            }
            notify.notify_waiters();
        });
    }
}

/// Compute a stable 64-bit hash + canonical-JSON string for a manifest.
/// The canonical form sorts object keys recursively so that two
/// `serde_json::Value`s that are semantically equal hash identically
/// regardless of insertion order.
fn compute_manifest_hash(manifest: &Manifest) -> (u64, String) {
    let value = serde_json::to_value(manifest)
        .expect("Manifest is Serialize; failure indicates a corrupted in-memory value");
    let canonical = canonicalize_json(value);
    let json = canonical.to_string();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(json.as_bytes());
    (hasher.finish(), json)
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let canonical: serde_json::Map<String, Value> = entries
                .into_iter()
                .map(|(k, v)| (k, canonicalize_json(v)))
                .collect();
            Value::Object(canonical)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}

#[cfg(test)]
impl WarmSessionPool {
    /// Test helper: close the front parked entry for `manifest` so the
    /// watchdog will see it as dead on its next tick. Returns the number
    /// of entries actually closed (0 if the queue was empty).
    pub(crate) async fn close_front_entry_for_test(&self, manifest: &Manifest) -> usize {
        let (hash, _) = compute_manifest_hash(manifest);
        let mut state = self.state.lock().await;
        if let Some(queue) = state.entries.get_mut(&hash) {
            if let Some(entry) = queue.front_mut() {
                let _ = entry.handle.close().await;
                return 1;
            }
        }
        0
    }

    /// Test helper: report whether the watchdog has been started.
    pub(crate) fn watchdog_started_for_test(&self) -> bool {
        self.watchdog_handle.get().is_some()
    }

    /// Test helper: report whether a refill task is currently in flight
    /// for the given manifest. Phase 6 uses this to verify the give-up
    /// path clears the marker.
    pub(crate) async fn refill_in_flight_for_test(&self, manifest: &Manifest) -> bool {
        let (hash, _) = compute_manifest_hash(manifest);
        self.state.lock().await.refill_in_flight.contains_key(&hash)
    }

    /// Test helper: report whether the refill chain for `manifest` has
    /// given up (Phase 10 / C1 fix). True after `refill_max_failures`
    /// consecutive failures; cleared by a successful `prewarm` or by
    /// `set_target` (operator retry).
    pub(crate) async fn gave_up_for_test(&self, manifest: &Manifest) -> bool {
        let (hash, _) = compute_manifest_hash(manifest);
        self.state.lock().await.gave_up.contains(&hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ManifestMetadata, NodeManifest};
    use crate::transport::PipelineExecutor;

    /// Build a minimal [`Manifest`] using `CalculatorNode`, which is part of
    /// the default streaming registry installed by `PipelineExecutor::new()`.
    /// `CalculatorNode::new` does not validate params at construction time,
    /// so an empty params object is enough for `cold_build_session` to
    /// succeed without us actually pumping data through the pipeline.
    fn test_manifest() -> Arc<Manifest> {
        Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "warm-pool-test".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "calc".to_string(),
                node_type: "CalculatorNode".to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        })
    }

    /// Build a manifest semantically distinct from [`test_manifest`] so its
    /// canonical-JSON hash differs (different node id + different metadata
    /// name, both of which serialize into the canonical form).
    fn other_test_manifest() -> Arc<Manifest> {
        Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "warm-pool-test-other".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "calc_other".to_string(),
                node_type: "CalculatorNode".to_string(),
                params: serde_json::json!({"flavor": "other"}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        })
    }

    /// Race-tolerant helper: poll `pool_size` until it reaches
    /// `expected` or the timeout fires. Used for assertions that depend
    /// on background refill tasks completing.
    async fn wait_for_pool_size(pool: &WarmSessionPool, manifest: &Manifest, expected: usize) {
        use tokio::time::{timeout, Duration};
        let result = timeout(Duration::from_secs(5), async {
            loop {
                if pool.pool_size(manifest).await == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if result.is_err() {
            let actual = pool.pool_size(manifest).await;
            panic!("timeout waiting for pool size; expected {expected}, got {actual}");
        }
    }

    #[tokio::test]
    async fn prewarm_then_acquire_consumes_warm_entry() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = test_manifest();

        assert_eq!(pool.pool_size(&manifest).await, 0);
        pool.prewarm(manifest.clone()).await.unwrap();
        assert_eq!(pool.pool_size(&manifest).await, 1);

        let _session = pool.acquire(manifest.clone()).await.unwrap();
        assert_eq!(
            pool.pool_size(&manifest).await,
            0,
            "acquire should pop the warm entry"
        );
    }

    #[tokio::test]
    async fn acquire_with_empty_pool_cold_builds() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = test_manifest();

        assert_eq!(pool.pool_size(&manifest).await, 0);
        let _session = pool.acquire(manifest.clone()).await.unwrap();
        assert_eq!(
            pool.pool_size(&manifest).await,
            0,
            "no warm entry was added; pool stays empty"
        );
    }

    #[tokio::test]
    async fn acquire_skips_pool_when_manifest_differs() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest_a = test_manifest();
        let manifest_b = other_test_manifest();

        pool.prewarm(manifest_a.clone()).await.unwrap();
        assert_eq!(pool.pool_size(&manifest_a).await, 1);
        assert_eq!(
            pool.pool_size(&manifest_b).await,
            0,
            "different hash bucket"
        );

        let _session = pool.acquire(manifest_b.clone()).await.unwrap();
        assert_eq!(
            pool.pool_size(&manifest_a).await,
            1,
            "warm entry for manifest_a remains untouched after manifest_b acquire"
        );
    }

    #[tokio::test]
    async fn manifest_hash_is_stable_across_clones() {
        let m1 = test_manifest();
        let m2: Manifest = (*m1).clone();
        let (h1, _) = compute_manifest_hash(&*m1);
        let (h2, _) = compute_manifest_hash(&m2);
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn manifest_hash_differs_for_distinct_manifests() {
        let m1 = test_manifest();
        let m2 = other_test_manifest();
        let (h1, _) = compute_manifest_hash(&*m1);
        let (h2, _) = compute_manifest_hash(&*m2);
        assert_ne!(h1, h2);
    }

    // ---- Phase 4 tests --------------------------------------------------

    #[tokio::test]
    async fn set_target_schedules_initial_refill() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = test_manifest();

        assert_eq!(pool.pool_size(&manifest).await, 0);
        pool.set_target(manifest.clone(), 1).await;
        wait_for_pool_size(&pool, &manifest, 1).await;
    }

    #[tokio::test]
    async fn raising_target_schedules_more_refills() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = test_manifest();

        pool.set_target(manifest.clone(), 1).await;
        wait_for_pool_size(&pool, &manifest, 1).await;

        pool.set_target(manifest.clone(), 3).await;
        wait_for_pool_size(&pool, &manifest, 3).await;
    }

    #[tokio::test]
    async fn target_zero_does_not_drain_existing_entries() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = test_manifest();

        pool.prewarm(manifest.clone()).await.unwrap();
        assert_eq!(pool.pool_size(&manifest).await, 1);

        pool.set_target(manifest.clone(), 0).await;
        // Give a beat for any (non-existent) cancellation to play out.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            pool.pool_size(&manifest).await,
            1,
            "target=0 should not drain"
        );
    }

    #[tokio::test]
    async fn acquire_triggers_auto_refill() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = test_manifest();

        pool.set_target(manifest.clone(), 1).await;
        wait_for_pool_size(&pool, &manifest, 1).await;

        let _session = pool.acquire(manifest.clone()).await.unwrap();
        // Pool drops to 0 immediately, then refills back to 1 in
        // background.
        wait_for_pool_size(&pool, &manifest, 1).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prewarm_joins_in_flight_refill() {
        // Best-effort coalescing test — a tight race may complete the
        // refill before the second `prewarm` registers — but the
        // assertion still holds: pool_size should be 1 (not 2) after
        // both calls return, because they coalesce on a single refill
        // (D4 idempotency).
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = test_manifest();

        let p1 = {
            let pool = pool.clone();
            let m = manifest.clone();
            tokio::spawn(async move { pool.prewarm(m).await })
        };
        let p2 = {
            let pool = pool.clone();
            let m = manifest.clone();
            tokio::spawn(async move { pool.prewarm(m).await })
        };

        p1.await.unwrap().unwrap();
        p2.await.unwrap().unwrap();

        assert_eq!(
            pool.pool_size(&manifest).await,
            1,
            "concurrent prewarm calls should coalesce on one refill"
        );
    }

    // ---- Phase 5 tests --------------------------------------------------

    #[tokio::test]
    async fn watchdog_prunes_dead_entry() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            watchdog_interval: std::time::Duration::from_millis(50),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let manifest = test_manifest();

        pool.prewarm(manifest.clone()).await.unwrap();
        assert_eq!(pool.pool_size(&*manifest).await, 1);

        // Kill the parked entry's underlying session task.
        assert_eq!(pool.close_front_entry_for_test(&*manifest).await, 1);

        // Watchdog ticks every 50ms — wait up to 1s for prune.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while pool.pool_size(&*manifest).await > 0 {
            if std::time::Instant::now() >= deadline {
                panic!("watchdog did not prune dead entry within 1s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn watchdog_triggers_refill_for_target_after_prune() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            watchdog_interval: std::time::Duration::from_millis(50),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let manifest = test_manifest();

        pool.set_target(manifest.clone(), 1).await;
        wait_for_pool_size(&pool, &*manifest, 1).await;

        pool.close_front_entry_for_test(&*manifest).await;

        // Watchdog should prune within ~50ms then refill produces a new
        // entry. The refill is single-attempt (Phase 4) so it should
        // succeed quickly.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let size = pool.pool_size(&*manifest).await;
            if size == 1 {
                // Confirm the entry is live by acquiring it.
                let _h = pool.acquire(manifest.clone()).await.unwrap();
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!("watchdog did not refill after prune within 2s; pool_size={size}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn watchdog_does_not_start_without_set_target_or_prewarm() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        // No set_target, no prewarm. Just observe pool_size.
        assert_eq!(pool.pool_size(&*test_manifest()).await, 0);
        assert!(
            !pool.watchdog_started_for_test(),
            "watchdog should not start unless set_target or prewarm has been called"
        );
    }

    // ---- Phase 6 tests --------------------------------------------------

    /// Build a manifest that fails `cold_build_session` validation. Uses
    /// an unknown node type so manifest validation rejects it before any
    /// graph construction can succeed — produces a permanent Err.
    fn invalid_test_manifest() -> Arc<Manifest> {
        Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "warm-pool-invalid".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "bogus".to_string(),
                node_type: "ThisNodeTypeDoesNotExist".to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        })
    }

    /// Wait until the refill marker for `manifest` is observed at least
    /// once (i.e. the spawned task has reached the marker-acquisition
    /// point) and then cleared (i.e. the task has either succeeded with
    /// no more work, or given up). Returns the wall-clock time between
    /// the marker first appearing and being cleared.
    async fn wait_for_refill_cycle(
        pool: &WarmSessionPool,
        manifest: &Manifest,
        timeout: Duration,
    ) -> Duration {
        let overall_deadline = std::time::Instant::now() + timeout;

        // Phase A: wait for the marker to appear.
        loop {
            if pool.refill_in_flight_for_test(manifest).await {
                break;
            }
            if std::time::Instant::now() >= overall_deadline {
                panic!("refill marker never appeared within {timeout:?}");
            }
            // Tight poll while waiting for the spawned task to run.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let started_at = std::time::Instant::now();

        // Phase B: wait for the marker to clear.
        loop {
            if !pool.refill_in_flight_for_test(manifest).await {
                return started_at.elapsed();
            }
            if std::time::Instant::now() >= overall_deadline {
                panic!("refill marker never cleared within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn refill_gives_up_after_max_failures() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        // Tight backoff so the test completes in milliseconds. Long
        // watchdog interval so the watchdog can't interfere.
        let cfg = WarmPoolConfig {
            refill_backoff_initial: Duration::from_millis(1),
            refill_backoff_cap: Duration::from_millis(1),
            refill_max_failures: 3,
            watchdog_interval: Duration::from_secs(60),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let bad = invalid_test_manifest();

        pool.set_target(bad.clone(), 1).await;

        // After 3 failures (~3 * 1ms = 3ms of sleep + build attempts),
        // the task should clear the in-flight marker.
        let _elapsed = wait_for_refill_cycle(&pool, &*bad, Duration::from_secs(1)).await;
        // Pool should still be empty — the give-up path doesn't
        // produce a warm entry.
        assert_eq!(pool.pool_size(&*bad).await, 0);
    }

    #[tokio::test]
    async fn refill_backoff_doubles_between_attempts() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            refill_backoff_initial: Duration::from_millis(100),
            refill_backoff_cap: Duration::from_secs(60),
            refill_max_failures: 3,
            watchdog_interval: Duration::from_secs(60),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let bad = invalid_test_manifest();

        pool.set_target(bad.clone(), 1).await;

        // 3 failures, 2 backoff sleeps between them: 100ms + 200ms = 300ms minimum.
        let elapsed = wait_for_refill_cycle(&pool, &*bad, Duration::from_secs(3)).await;

        // Generous lower bound to avoid CI flakes (the polling cadence
        // adds up to ~20ms slop on top of the actual backoff).
        assert!(
            elapsed >= Duration::from_millis(280),
            "refill gave up in {elapsed:?} — backoff appears not to be applied"
        );
        // Upper bound to detect runaway retries (should be well under 3s).
        assert!(
            elapsed < Duration::from_secs(3),
            "refill took too long ({elapsed:?}) — possible bug or capped backoff issue"
        );
    }

    #[tokio::test]
    async fn watchdog_exits_on_pool_drop() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            watchdog_interval: std::time::Duration::from_millis(50),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let manifest = test_manifest();
        pool.set_target(manifest.clone(), 1).await;
        wait_for_pool_size(&pool, &*manifest, 1).await;
        let weak = Arc::downgrade(&pool);
        drop(pool);
        // After dropping the strong ref, the watchdog's weak.upgrade()
        // should return None on its next tick and the task should exit.
        // Allow up to 1s for the next tick + task drop propagation.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while weak.upgrade().is_some() {
            if std::time::Instant::now() >= deadline {
                panic!(
                    "pool was not fully dropped within 1s — watchdog may be holding a strong reference"
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    // ---- Phase 10 tests -------------------------------------------------

    /// C1: once the refill chain gives up, the (fast-tick) watchdog
    /// must NOT keep re-spawning new chains. Pre-fix code triggered a
    /// log-spam loop every watchdog tick; post-fix code observes the
    /// sticky `gave_up` flag and stays quiet.
    #[tokio::test]
    async fn refill_give_up_is_sticky_against_watchdog() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            refill_backoff_initial: Duration::from_millis(1),
            refill_backoff_cap: Duration::from_millis(1),
            refill_max_failures: 2,
            // FAST watchdog — exposes the bug pre-fix.
            watchdog_interval: Duration::from_millis(50),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let bad = invalid_test_manifest();

        pool.set_target(bad.clone(), 1).await;

        // Wait for the initial chain to finish (give up).
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if pool.gave_up_for_test(&*bad).await && !pool.refill_in_flight_for_test(&*bad).await {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("initial refill chain did not give up within 2s");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait several watchdog ticks (~6 at 50ms cadence). Pre-fix
        // code would re-spawn the chain every tick; post-fix code stays
        // at gave_up=true and refill_in_flight=false.
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(
            !pool.refill_in_flight_for_test(&*bad).await,
            "watchdog should NOT keep re-spawning failed refill chains"
        );
        assert!(
            pool.gave_up_for_test(&*bad).await,
            "gave_up flag should remain set"
        );
        assert_eq!(pool.pool_size(&*bad).await, 0);

        // Operator retry: set_target clears gave_up. Confirms the
        // clear path works (downstream behavior — whether the retry
        // succeeds — is the manifest's problem, not this test's).
        pool.set_target(bad.clone(), 1).await;
        // Immediately after set_target, gave_up must be cleared.
        assert!(
            !pool.gave_up_for_test(&*bad).await,
            "set_target should clear gave_up so operator retry can proceed"
        );
    }

    /// C2: when a concurrent prewarm joins an in-flight refill chain
    /// that subsequently gives up, the join must propagate Err — not
    /// silently return Ok.
    #[tokio::test]
    async fn prewarm_join_returns_err_when_chain_gave_up() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            refill_backoff_initial: Duration::from_millis(1),
            refill_backoff_cap: Duration::from_millis(1),
            refill_max_failures: 2,
            watchdog_interval: Duration::from_secs(60),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let bad = invalid_test_manifest();

        pool.set_target(bad.clone(), 1).await;
        // Wait for give-up to fire.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if pool.gave_up_for_test(&*bad).await && !pool.refill_in_flight_for_test(&*bad).await {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("refill chain did not give up within 2s");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pool.gave_up_for_test(&*bad).await);

        // Now call prewarm — should propagate the give-up as Err
        // (because the bad manifest's cold-build will itself fail,
        // and the existing gave_up flag is cleared inside
        // prewarm_fresh by a successful build that never happens).
        let result = pool.prewarm(bad.clone()).await;
        assert!(
            result.is_err(),
            "prewarm on a permanently-broken manifest should return Err"
        );
    }

    /// I1: the configured `system_retained_cap` must be propagated to
    /// every pool-built session. Pre-fix code always used the default
    /// 256 because the field was never wired through.
    ///
    /// Strategy: publish 17 events with the pool's cap=16 setting, then
    /// subscribe. The retained ring (capped at 16) should drop the
    /// OLDEST event (index 0), so the first buffered event a subscriber
    /// observes is index 1. Pre-fix code (cap=256) would retain all 17,
    /// so the first event would be index 0.
    #[tokio::test]
    async fn pool_propagates_system_retained_cap_to_built_sessions() {
        use crate::data::RuntimeData;

        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            system_retained_cap: 16, // Non-default
            watchdog_interval: Duration::from_secs(60),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let manifest = test_manifest();

        pool.prewarm(manifest.clone()).await.unwrap();

        let handle = pool.acquire(manifest.clone()).await.unwrap();
        let control = executor
            .control_bus()
            .get(&handle.session_id)
            .expect("control should be registered");

        for i in 0..17u64 {
            control.publish_tap(
                "__system__",
                None,
                RuntimeData::Json(serde_json::json!({"i": i})),
            );
        }
        let mut rx = control.subscribe_system();
        // The first event the subscriber sees comes from the retained
        // buffer (per subscribe_system contract). With cap=16, index 0
        // was evicted, so the first event must be index 1.
        let first = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("recv timed out — no buffered event available")
            .expect("subscriber closed unexpectedly");
        let RuntimeData::Json(v) = first else {
            panic!("expected Json event");
        };
        let first_i = v.get("i").and_then(|x| x.as_u64()).expect("i field");
        assert_eq!(
            first_i, 1,
            "with cap=16 and 17 events published, the oldest (index 0) should have been evicted; \
             pre-fix code (cap=256) would retain all 17 and yield index 0 first"
        );
    }

    /// I2: after consuming a warm entry, the post-consume refill marker
    /// must be installed SYNCHRONOUSLY (under the same lock as the
    /// pop). Pre-fix code only inserted the marker once the spawned
    /// task ran, which left a window during which a concurrent acquire
    /// would observe no marker and cold-build in parallel.
    #[tokio::test]
    async fn second_acquire_after_consume_blocks_on_refill() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let cfg = WarmPoolConfig {
            watchdog_interval: Duration::from_secs(60),
            ..WarmPoolConfig::default()
        };
        let pool = WarmSessionPool::with_config(executor.clone(), cfg);
        let manifest = test_manifest();

        pool.set_target(manifest.clone(), 1).await;
        wait_for_pool_size(&pool, &*manifest, 1).await;

        let _h = pool.acquire(manifest.clone()).await.unwrap();
        // Marker must be present IMMEDIATELY (synchronously installed
        // under the consume lock), before the spawned refill task gets
        // a chance to run. Pre-fix code would observe `false` here.
        assert!(
            pool.refill_in_flight_for_test(&*manifest).await,
            "refill marker should be present immediately after consume, before spawned task runs"
        );
    }
}
