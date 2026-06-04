// AttachedSession — transport-agnostic adapter over the in-proc control bus.
// Mirrors `clients/python/remotemedia/control/client.py`.
//
// The native binding throws ordinary JS Errors with reason prefixes
// (e.g. "SessionNotFoundError: ...") which this module remaps to the
// typed error classes below — so callers can `instanceof`-check just like
// the Python side does with the `pyo3::create_exception!` types.

const { Data } = require('./data');
const { CorrIdGen, _makeNodeProxy } = require('./rpc-proxy');

// ─── Typed errors ────────────────────────────────────────────────────────────

class SessionNotFoundError extends Error {
  constructor(message) {
    super(message);
    this.name = 'SessionNotFoundError';
  }
}

class ControlAddressError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ControlAddressError';
  }
}

const PREFIX_MAP = [
  ['SessionNotFoundError: ', SessionNotFoundError],
  ['ControlAddressError: ', ControlAddressError],
];

function remapNativeError(err) {
  const msg = (err && err.message) || String(err);
  for (const [prefix, Cls] of PREFIX_MAP) {
    if (msg.startsWith(prefix)) {
      const remapped = new Cls(msg.slice(prefix.length));
      remapped.cause = err;
      return remapped;
    }
  }
  return err;
}

async function callAndRemap(fn) {
  try {
    return await fn();
  } catch (err) {
    throw remapNativeError(err);
  }
}

// ─── Node state constants ────────────────────────────────────────────────────

const NodeState = Object.freeze({
  ENABLED: 1,
  BYPASS: 2,
  DISABLED: 3,
});

// ─── Intercept decision constants ────────────────────────────────────────────

const InterceptDecision = Object.freeze({
  PASS: 0,
  REPLACE: 1,
  DROP: 2,
});

// ─── Subscription ────────────────────────────────────────────────────────────

class Subscription {
  /** @internal */
  constructor(nativeSub) {
    this._native = nativeSub;
    this._closed = false;
  }

  /**
   * Await the next frame. Returns a `Data` instance, or `null` on broadcast
   * lag (the stream is still live — keep calling). Throws when the channel
   * closes (session ended).
   */
  async recv() {
    if (this._closed) {
      throw new Error('Subscription is closed');
    }
    let frame;
    try {
      frame = await this._native.recv();
    } catch (err) {
      // Native side throws "tap channel closed" on RecvError::Closed.
      this._closed = true;
      throw remapNativeError(err);
    }
    if (frame == null) {
      // Lag — keep calling.
      return null;
    }
    return Data._fromNative(frame);
  }

  /** Async iterator support: `for await (const data of sub) {...}`. */
  async *[Symbol.asyncIterator]() {
    while (true) {
      try {
        const item = await this.recv();
        if (item != null) {
          yield item;
        }
      } catch {
        return; // channel closed
      }
    }
  }

  close() {
    this._closed = true;
    // Native subscription closes when GC'd; no explicit close fn.
    this._native = null;
  }
}

// ─── InterceptSession ────────────────────────────────────────────────────────

class InterceptSession {
  /** @internal */
  constructor(nativeSess) {
    this._native = nativeSess;
    this._closed = false;
  }

  /**
   * Await the next intercept request. Returns `{correlationId, data}` or
   * `null` when the upstream channel is closed (session ended).
   * `correlationId` is a BigInt — pass it back verbatim to `complete()`.
   */
  async recv() {
    if (this._closed) return null;
    const item = await this._native.recv();
    if (item == null) {
      this._closed = true;
      return null;
    }
    return {
      correlationId: item.correlationId,
      data: Data._fromNative(item.data),
    };
  }

  /**
   * Resolve one intercept request.
   * @param {bigint} correlationId
   * @param {number} decisionKind - 0=pass, 1=replace, 2=drop
   * @param {Data | null} replacement - required if decisionKind===1
   */
  async complete(correlationId, decisionKind, replacement = null) {
    if (this._closed) {
      throw new Error('InterceptSession is closed');
    }
    let native = null;
    if (decisionKind === InterceptDecision.REPLACE) {
      if (!(replacement instanceof Data)) {
        throw new TypeError(
          'decisionKind=REPLACE requires a Data replacement',
        );
      }
      native = replacement._toNative();
    }
    await callAndRemap(() =>
      this._native.complete(correlationId, decisionKind, native),
    );
  }

  close() {
    this._closed = true;
    // Dropping the native handle removes the bus slot via the Rust Drop impl.
    this._native = null;
  }
}

// ─── AttachedSession ─────────────────────────────────────────────────────────

class AttachedSession {
  /** @internal */
  constructor(nativeAttached, opts = {}) {
    this._native = nativeAttached;
    this._closed = false;
    this.attachId = opts.attachId || 'node-inproc';
    this._corrIdGen = new CorrIdGen(this.attachId);
    this._nodeProxies = new Map();
  }

  get sessionId() {
    return this._native ? this._native.sessionId : null;
  }

  /**
   * Explicit node accessor — use this for nodes whose IDs collide with
   * reserved instance member names (`subscribe`, `publish`, `setNodeState`,
   * `clearNodeState`, `intercept`, `close`, `sessionId`, `attachId`, `node`).
   * For non-reserved names, `ctrl.audio === ctrl.node('audio')`.
   */
  node(nodeId) {
    let proxy = this._nodeProxies.get(nodeId);
    if (proxy === undefined) {
      proxy = _makeNodeProxy(this, nodeId);
      this._nodeProxies.set(nodeId, proxy);
    }
    return proxy;
  }

  async subscribe(address) {
    if (this._closed) throw new Error('AttachedSession is closed');
    const nativeSub = await callAndRemap(() => this._native.subscribe(address));
    return new Subscription(nativeSub);
  }

  async publish(address, data) {
    if (this._closed) throw new Error('AttachedSession is closed');
    if (!(data instanceof Data)) {
      throw new TypeError('publish requires a Data instance');
    }
    await callAndRemap(() =>
      this._native.publish(address, data._toNative()),
    );
  }

  async setNodeState(nodeId, state) {
    if (this._closed) throw new Error('AttachedSession is closed');
    await callAndRemap(() => this._native.setNodeState(nodeId, state));
  }

  async clearNodeState(nodeId) {
    if (this._closed) throw new Error('AttachedSession is closed');
    await callAndRemap(() => this._native.clearNodeState(nodeId));
  }

  async intercept(address, deadlineMs) {
    if (this._closed) throw new Error('AttachedSession is closed');
    const nativeSess = await callAndRemap(() =>
      this._native.intercept(address, deadlineMs),
    );
    return new InterceptSession(nativeSess);
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    // Cascade close to each NodeProxy's demux — this rejects in-flight
    // RPC calls and unregisters their corr_ids before we drop the native
    // attach (which would otherwise let them hang forever).
    for (const proxy of this._nodeProxies.values()) {
      try {
        if (proxy && proxy._demux && typeof proxy._demux.close === 'function') {
          proxy._demux.close();
        }
      } catch (_) {
        // best-effort
      }
    }
    this._nodeProxies.clear();
    this._native = null;
  }
}

// ─── Reserved instance keys ──────────────────────────────────────────────────
//
// The Proxy below dispatches `ctrl.<x>` to either the real instance attribute
// or a NodeProxy. Anything in this set wins for the instance.

const _RESERVED_INSTANCE_KEYS = new Set([
  'sessionId',
  'attachId',
  'subscribe',
  'publish',
  'setNodeState',
  'clearNodeState',
  'intercept',
  'close',
  'node',
  'then',
  'constructor',
  'toJSON',
  'valueOf',
  'inspect',
]);

function _wrapAttachedSession(attached) {
  return new Proxy(attached, {
    get(target, prop, receiver) {
      if (typeof prop !== 'string') {
        return Reflect.get(target, prop, receiver);
      }
      if (prop.startsWith('_') || _RESERVED_INSTANCE_KEYS.has(prop)) {
        return Reflect.get(target, prop, receiver);
      }
      return target.node(prop);
    },
  });
}

// ─── attachInproc factory ────────────────────────────────────────────────────

/**
 * Open a control attach against an in-proc streaming session.
 *
 * @param {object} streamingSession - a `NapiStreamingSession` (from
 *   `createStreamingSession(manifestJson)`). NOT `NapiStreamSession` —
 *   the legacy class is not bus-reachable.
 * @param {object} [opts]
 * @param {string} [opts.attachId='node-inproc'] - corr_id prefix for typed
 *   RPC calls; lets server-side logs distinguish callers when more than one
 *   attach talks to the same session.
 * @returns {AttachedSession} A Proxy-wrapped AttachedSession. Instance
 *   members (`subscribe`, `publish`, `setNodeState`, `clearNodeState`,
 *   `intercept`, `close`, `sessionId`, `attachId`, `node`) work as before;
 *   any other property access returns a typed-RPC NodeProxy bound to that
 *   node id.
 */
function attachInproc(streamingSession, opts = {}) {
  if (!streamingSession || typeof streamingSession.control !== 'function') {
    throw new TypeError(
      'attachInproc requires a NapiStreamingSession (with .control()); ' +
        'did you use createStreamingSession() or the legacy createStreamSession()?',
    );
  }
  let native;
  try {
    native = streamingSession.control();
  } catch (err) {
    throw remapNativeError(err);
  }
  const attached = new AttachedSession(native, opts);
  return _wrapAttachedSession(attached);
}

module.exports = {
  AttachedSession,
  Subscription,
  InterceptSession,
  SessionNotFoundError,
  ControlAddressError,
  NodeState,
  InterceptDecision,
  attachInproc,
};
