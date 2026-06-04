// Typed-RPC proxy for Node.js AttachedSession.
//
//   ctrl.<nodeId>.<methodName>(args)
//   await ctrl.audio.setContext('hi')             // ff or reply (mode auto-discovered)
//   for await (const t of ctrl.stt.watchTokens()) // stream
//
// Wire-compatible with the Python proxy in
// `clients/python/remotemedia/control/rpc_proxy.py`. The returned
// `_DeferredCall` is both **thenable** (so `await x` works) and
// **async-iterable** (so `for await of x` works), mirroring Python's duck-typed
// `__await__` / `__aiter__` polymorphism on the same object.

const { Data } = require('./data');
const {
  RpcMode,
  RpcError,
  RpcTimeoutError,
  AUX_PORT_REPLY,
  RPC_META_METHOD,
  CANCEL_SUFFIX,
  DEFAULT_REPLY_TIMEOUT_MS,
  DEFAULT_META_TIMEOUT_MS,
} = require('./rpc');

// ─── CorrIdGen ───────────────────────────────────────────────────────────────

class CorrIdGen {
  constructor(attachId) {
    this._attachId = attachId;
    this._n = 0;
  }
  next() {
    this._n += 1;
    return `${this._attachId}:${this._n}`;
  }
}

// ─── _StreamQueue ────────────────────────────────────────────────────────────

// Unbounded async queue. `put(value)` is non-blocking; `take()` returns a
// Promise that resolves to the next value (or null after close). After
// close, all pending takes resolve to null and subsequent puts are dropped.

class _StreamQueue {
  constructor() {
    this._buffer = [];
    this._waiters = [];
    this._closed = false;
  }

  put(value) {
    if (this._closed) return;
    const waiter = this._waiters.shift();
    if (waiter !== undefined) {
      waiter(value);
    } else {
      this._buffer.push(value);
    }
  }

  take() {
    if (this._buffer.length > 0) {
      return Promise.resolve(this._buffer.shift());
    }
    if (this._closed) {
      return Promise.resolve(null);
    }
    return new Promise((resolve) => {
      this._waiters.push(resolve);
    });
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    while (this._waiters.length > 0) {
      const waiter = this._waiters.shift();
      waiter(null);
    }
  }
}

// ─── ReplyDemux ──────────────────────────────────────────────────────────────

// One per (attach, node_id). Holds a single subscribe loop on
// `<node>.out.__reply__`, dispatches each frame to the unary
// {resolve, reject, timeoutHandle} or _StreamQueue registered under the
// frame's corr_id.

class ReplyDemux {
  constructor(ctrl, nodeId) {
    this._ctrl = ctrl;
    this._nodeId = nodeId;
    this._unary = new Map();
    this._streams = new Map();
    this._subscription = null;
    this._consumerPromise = null;
    this._startPromise = null;
    this._closed = false;
  }

  ensureStarted() {
    if (this._closed) {
      return Promise.reject(new RpcError('attach closed'));
    }
    if (this._startPromise) return this._startPromise;
    this._startPromise = (async () => {
      this._subscription = await this._ctrl.subscribe(
        `${this._nodeId}.out.${AUX_PORT_REPLY}`,
      );
      this._consumerPromise = this._consume();
    })();
    return this._startPromise;
  }

  async _consume() {
    try {
      while (!this._closed) {
        let data;
        try {
          data = await this._subscription.recv();
        } catch (_) {
          // Channel closed by transport. Drain pending callers.
          break;
        }
        if (data == null) {
          // Lag — keep going.
          continue;
        }
        const envelope = this._decodeEnvelope(data);
        if (!envelope) continue;
        const corrId = envelope.corr_id;
        if (corrId == null) continue;
        const kind = envelope.kind;
        const payload = envelope.data;
        this._dispatch(String(corrId), kind, payload);
      }
    } finally {
      // Reject any remaining unary calls so callers don't hang forever.
      for (const [, entry] of this._unary) {
        if (entry.timeoutHandle) clearTimeout(entry.timeoutHandle);
        entry.reject(new RpcError('reply subscription closed'));
      }
      this._unary.clear();
      for (const [, q] of this._streams) q.close();
      this._streams.clear();
    }
  }

  _decodeEnvelope(data) {
    let outer;
    try {
      if (data.kind === 'json') {
        outer = data.jsonValue;
      } else if (data.kind === 'text') {
        // Rust delivers Text frames in the iceoryx2 channel-tagged wire
        // format: `[0x00][channel_len:u8][channel:utf-8][body:utf-8]`.
        // Strip the tag before parsing — the channel is metadata, the
        // body is the actual JSON envelope. Untagged text passes
        // through unchanged.
        let text = data.textValue;
        if (text.length >= 2 && text.charCodeAt(0) === 0x00) {
          const chanLen = text.charCodeAt(1);
          if (2 + chanLen <= text.length) {
            text = text.slice(2 + chanLen);
          }
        }
        outer = JSON.parse(text);
      } else {
        return null;
      }
    } catch (_) {
      return null;
    }
    if (outer && typeof outer === 'object' && '__aux_port__' in outer) {
      return outer.payload && typeof outer.payload === 'object'
        ? outer.payload
        : null;
    }
    return outer && typeof outer === 'object' ? outer : null;
  }

  _dispatch(corrId, kind, payload) {
    const entry = this._unary.get(corrId);
    if (entry !== undefined) {
      this._unary.delete(corrId);
      if (entry.timeoutHandle) clearTimeout(entry.timeoutHandle);
      if (kind === 'value') {
        entry.resolve(payload);
      } else if (kind === 'error') {
        entry.reject(new RpcError(String(payload)));
      } else if (kind === 'end') {
        // Reply-mode end without value — resolve to undefined for compat.
        entry.resolve(undefined);
      }
      return;
    }
    const q = this._streams.get(corrId);
    if (q !== undefined) {
      q.put({ kind, payload });
    }
    // Else: stale frame for an already-unregistered corr_id; silently drop.
  }

  registerUnary(corrId, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timeoutHandle =
        timeoutMs != null && Number.isFinite(timeoutMs) && timeoutMs > 0
          ? setTimeout(() => {
              if (this._unary.delete(corrId)) {
                reject(
                  new RpcTimeoutError(
                    `RPC ${this._nodeId}.<method> timed out after ${timeoutMs}ms`,
                  ),
                );
              }
            }, timeoutMs)
          : null;
      this._unary.set(corrId, { resolve, reject, timeoutHandle });
    });
  }

  registerStream(corrId) {
    const q = new _StreamQueue();
    this._streams.set(corrId, q);
    return q;
  }

  unregister(corrId) {
    const entry = this._unary.get(corrId);
    if (entry !== undefined) {
      if (entry.timeoutHandle) clearTimeout(entry.timeoutHandle);
      this._unary.delete(corrId);
    }
    const q = this._streams.get(corrId);
    if (q !== undefined) {
      q.close();
      this._streams.delete(corrId);
    }
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    // Reject everything in-flight.
    for (const [, entry] of this._unary) {
      if (entry.timeoutHandle) clearTimeout(entry.timeoutHandle);
      entry.reject(new RpcError('attach closed'));
    }
    this._unary.clear();
    for (const [, q] of this._streams) q.close();
    this._streams.clear();
    if (this._subscription) {
      try {
        this._subscription.close();
      } catch (_) {
        // best-effort
      }
      this._subscription = null;
    }
  }
}

// ─── _RpcCallable ────────────────────────────────────────────────────────────

// Wraps `proxy.<methodName>` into a callable that returns a `_DeferredCall`.
// Built as a Proxy with an `apply` trap so it stays `typeof === 'function'`
// for libraries that introspect callables.

function _makeRpcCallable(proxy, methodName, optionsOverride = null) {
  const callable = function () {
    // Placeholder; the apply trap below intercepts every call.
  };
  callable._proxy = proxy;
  callable._methodName = methodName;
  callable._optionsOverride = optionsOverride;

  callable.options = function (opts) {
    const merged = { ...(optionsOverride || {}), ...(opts || {}) };
    return _makeRpcCallable(proxy, methodName, merged);
  };

  callable.invoke = function (spec) {
    const args = (spec && spec.args) || [];
    const kwargs = (spec && spec.kwargs) || {};
    const opts = {
      ...(optionsOverride || {}),
      ...(spec
        ? {
            mode: spec.mode,
            timeout: spec.timeout,
            corrId: spec.corrId,
            signal: spec.signal,
          }
        : {}),
    };
    // Drop undefined keys so they don't shadow defaults later.
    for (const k of Object.keys(opts)) {
      if (opts[k] === undefined) delete opts[k];
    }
    return new _DeferredCall(proxy, methodName, args, kwargs, opts);
  };

  return new Proxy(callable, {
    apply(target, _thisArg, args) {
      return new _DeferredCall(
        target._proxy,
        target._methodName,
        args,
        {},
        target._optionsOverride || {},
      );
    },
  });
}

// ─── _DeferredCall ───────────────────────────────────────────────────────────

class _DeferredCall {
  constructor(proxy, methodName, args, kwargs, options) {
    this._proxy = proxy;
    this._methodName = methodName;
    this._args = args;
    this._kwargs = kwargs;
    this._options = options || {};
    this._consumed = null; // 'then' | 'iter' | null
  }

  _claim(kind) {
    if (this._consumed && this._consumed !== kind) {
      throw new RpcError('DeferredCall already consumed');
    }
    this._consumed = kind;
  }

  then(onFulfilled, onRejected) {
    this._claim('then');
    return this._runUnary().then(onFulfilled, onRejected);
  }

  // Required so `Promise.resolve(deferredCall)` + `.catch` works.
  // We do NOT implement `catch`/`finally` directly — the chained `.then`
  // returns a real Promise, which has them.

  async _runUnary() {
    const mode = await this._resolveMode();
    if (mode === RpcMode.STREAM) {
      throw new RpcError(
        `method '${this._methodName}' is streaming — use 'for await' instead of 'await'`,
      );
    }
    return this._proxy._invokeUnary({
      methodName: this._methodName,
      args: this._args,
      kwargs: this._kwargs,
      mode,
      timeoutMs: this._options.timeout,
      explicitCorrId: this._options.corrId,
      signal: this._options.signal,
    });
  }

  async _resolveMode() {
    if (this._options.mode) return this._options.mode;
    return this._proxy._fetchMethodMode(this._methodName);
  }

  [Symbol.asyncIterator]() {
    this._claim('iter');
    return new _LazyStreamIterator(
      this._proxy,
      this._methodName,
      this._args,
      this._kwargs,
      this._options,
    );
  }
}

// ─── _LazyStreamIterator ─────────────────────────────────────────────────────

// Wraps `_invokeStream`'s deferred setup behind the async iterator protocol.
// The first `next()` triggers `_invokeStream`; further `next()` calls delegate
// to the resulting `_StreamIterator`. `return()` before the first `next()` is a
// no-op (no corr_id allocated yet).

class _LazyStreamIterator {
  constructor(proxy, methodName, args, kwargs, options) {
    this._proxy = proxy;
    this._methodName = methodName;
    this._args = args;
    this._kwargs = kwargs;
    this._options = options || {};
    this._inner = null;
    this._closed = false;
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    if (this._closed) return { value: undefined, done: true };
    if (this._inner === null) {
      this._inner = await this._proxy._invokeStream({
        methodName: this._methodName,
        args: this._args,
        kwargs: this._kwargs,
        explicitCorrId: this._options.corrId,
        signal: this._options.signal,
      });
    }
    return this._inner.next();
  }

  async return(value) {
    this._closed = true;
    if (this._inner !== null) {
      return this._inner.return(value);
    }
    return { value, done: true };
  }

  async throw(err) {
    this._closed = true;
    if (this._inner !== null) {
      return this._inner.throw(err);
    }
    throw err;
  }
}

// ─── _StreamIterator ─────────────────────────────────────────────────────────

class _StreamIterator {
  constructor(ctrl, nodeId, methodName, corrId, queue, demux, signal) {
    this._ctrl = ctrl;
    this._nodeId = nodeId;
    this._methodName = methodName;
    this._corrId = corrId;
    this._queue = queue;
    this._demux = demux;
    this._done = false;
    this._returned = false;
    this._signal = signal || null;
    this._onAbort = null;

    if (this._signal) {
      if (this._signal.aborted) {
        // Schedule cancellation on next tick so callers can still get the
        // iterator handle back synchronously.
        queueMicrotask(() => {
          // Best-effort; if return() rejects (publish failure), swallow.
          this.return().catch(() => {});
        });
      } else {
        this._onAbort = () => {
          this.return().catch(() => {});
        };
        this._signal.addEventListener('abort', this._onAbort, { once: true });
      }
    }
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    if (this._done) return { value: undefined, done: true };
    const item = await this._queue.take();
    if (item === null) {
      this._markDone();
      return { value: undefined, done: true };
    }
    const { kind, payload } = item;
    if (kind === 'value') {
      return { value: payload, done: false };
    }
    if (kind === 'end') {
      this._markDone();
      return { value: undefined, done: true };
    }
    if (kind === 'error') {
      this._markDone();
      throw new RpcError(String(payload));
    }
    // Unknown kind — treat as terminal error.
    this._markDone();
    throw new RpcError(`unknown reply kind: ${kind}`);
  }

  async return(value) {
    if (this._returned) {
      return { value, done: true };
    }
    this._returned = true;
    this._done = true;
    this._detachAbort();
    // Publish cancel frame so the server-side generator unwinds.
    try {
      await this._ctrl.publish(
        `${this._nodeId}.in.${this._methodName}.${CANCEL_SUFFIX}`,
        Data.fromJson({ corr_id: this._corrId }),
      );
    } catch (_) {
      // best-effort
    }
    this._demux.unregister(this._corrId);
    return { value, done: true };
  }

  async throw(err) {
    await this.return();
    throw err;
  }

  _markDone() {
    this._done = true;
    this._detachAbort();
    this._demux.unregister(this._corrId);
  }

  _detachAbort() {
    if (this._signal && this._onAbort) {
      try {
        this._signal.removeEventListener('abort', this._onAbort);
      } catch (_) {
        // ignore
      }
      this._onAbort = null;
    }
  }
}

// ─── NodeProxy ───────────────────────────────────────────────────────────────

// Per-(attach, nodeId) typed-RPC proxy. Returned by `AttachedSession`'s Proxy
// `get` handler. Property access returns a callable `_RpcCallable`. Reserved
// internal names (private/dunder + `then`) pass through so the proxy itself
// is not thenable, doesn't accidentally serialize, etc.

const _RESERVED_PROXY_KEYS = new Set([
  'then',
  'constructor',
  'toJSON',
  'valueOf',
  Symbol.toPrimitive,
  Symbol.iterator,
  Symbol.asyncIterator,
]);

function _makeNodeProxy(ctrl, nodeId) {
  const target = new _NodeProxyCore(ctrl, nodeId);
  return new Proxy(target, {
    get(t, prop, receiver) {
      if (
        typeof prop !== 'string' ||
        prop.startsWith('_') ||
        _RESERVED_PROXY_KEYS.has(prop)
      ) {
        return Reflect.get(t, prop, receiver);
      }
      return _makeRpcCallable(t, prop);
    },
  });
}

class _NodeProxyCore {
  constructor(ctrl, nodeId) {
    this._ctrl = ctrl;
    this._nodeId = nodeId;
    this._demux = new ReplyDemux(ctrl, nodeId);
    this._modeCache = null;
    this._metaPromise = null;
  }

  async _fetchMethodMode(methodName) {
    if (this._modeCache === null) {
      if (this._metaPromise === null) {
        this._metaPromise = this._fetchMeta().catch(() => {
          // Ensure subsequent calls don't retry forever — populate empty.
          this._modeCache = new Map();
        });
      }
      await this._metaPromise;
    }
    return this._modeCache.get(methodName) || RpcMode.REPLY;
  }

  async _fetchMeta() {
    await this._demux.ensureStarted();
    const corrId = this._ctrl._corrIdGen.next();
    const pending = this._demux.registerUnary(corrId, DEFAULT_META_TIMEOUT_MS);
    await this._ctrl.publish(
      `${this._nodeId}.in.${RPC_META_METHOD}`,
      Data.fromJson({ corr_id: corrId, args: [], kwargs: {} }),
    );
    try {
      const meta = await pending;
      const map = new Map();
      if (meta && typeof meta === 'object') {
        for (const k of Object.keys(meta)) {
          map.set(k, String(meta[k]));
        }
      }
      this._modeCache = map;
    } catch (_) {
      // Timeout / error → fall back to empty cache; calls default to reply.
      this._modeCache = new Map();
    }
  }

  async _invokeUnary({
    methodName,
    args,
    kwargs,
    mode,
    timeoutMs,
    explicitCorrId,
    signal,
  }) {
    if (signal && signal.aborted) {
      const err = new RpcError('aborted');
      err.name = 'AbortError';
      throw err;
    }
    if (mode === RpcMode.FF) {
      await this._ctrl.publish(
        `${this._nodeId}.in.${methodName}`,
        Data.fromJson({ corr_id: null, args, kwargs }),
      );
      return undefined;
    }
    // reply
    await this._demux.ensureStarted();
    const corrId = explicitCorrId || this._ctrl._corrIdGen.next();
    const effTimeout =
      timeoutMs != null ? timeoutMs : DEFAULT_REPLY_TIMEOUT_MS;
    const pending = this._demux.registerUnary(corrId, effTimeout);

    let abortHandler = null;
    if (signal) {
      abortHandler = () => {
        this._demux.unregister(corrId);
      };
      signal.addEventListener('abort', abortHandler, { once: true });
    }

    try {
      await this._ctrl.publish(
        `${this._nodeId}.in.${methodName}`,
        Data.fromJson({ corr_id: corrId, args, kwargs }),
      );
      // Race the reply against the signal so we don't hang waiting for a
      // server reply that will never come.
      if (signal) {
        return await new Promise((resolve, reject) => {
          pending.then(resolve, reject);
          signal.addEventListener(
            'abort',
            () => {
              const err = new RpcError('aborted');
              err.name = 'AbortError';
              reject(err);
            },
            { once: true },
          );
        });
      }
      return await pending;
    } catch (err) {
      if (err && err.name === 'AbortError') {
        // Wrap the timeout/abort message to include method context, but only
        // if it isn't already a RpcTimeoutError.
        throw err;
      }
      if (err instanceof RpcTimeoutError) {
        throw new RpcTimeoutError(
          `RPC ${this._nodeId}.${methodName} timed out after ${effTimeout}ms`,
        );
      }
      throw err;
    } finally {
      if (signal && abortHandler) {
        try {
          signal.removeEventListener('abort', abortHandler);
        } catch (_) {
          // ignore
        }
      }
    }
  }

  async _invokeStream({
    methodName,
    args,
    kwargs,
    explicitCorrId,
    signal,
  }) {
    await this._demux.ensureStarted();
    const corrId = explicitCorrId || this._ctrl._corrIdGen.next();
    const q = this._demux.registerStream(corrId);
    await this._ctrl.publish(
      `${this._nodeId}.in.${methodName}`,
      Data.fromJson({ corr_id: corrId, args, kwargs }),
    );
    return new _StreamIterator(
      this._ctrl,
      this._nodeId,
      methodName,
      corrId,
      q,
      this._demux,
      signal,
    );
  }
}

module.exports = {
  CorrIdGen,
  ReplyDemux,
  _StreamQueue,
  _StreamIterator,
  _LazyStreamIterator,
  _DeferredCall,
  _makeRpcCallable,
  _makeNodeProxy,
  _NodeProxyCore,
};
