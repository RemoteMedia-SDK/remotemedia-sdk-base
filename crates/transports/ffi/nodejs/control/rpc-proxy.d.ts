import { Data } from './data';
import { RpcModeValue } from './rpc';

export interface InvokeSpec {
  args?: unknown[];
  kwargs?: Record<string, unknown>;
  mode?: RpcModeValue;
  timeout?: number;
  corrId?: string;
  signal?: AbortSignal;
}

export interface OptionsSpec {
  mode?: RpcModeValue;
  timeout?: number;
  corrId?: string;
  signal?: AbortSignal;
}

/**
 * The object returned by `nodeProxy.<methodName>(...)`. Behaves as a
 * Promise (await it for unary calls) AND as an AsyncIterable (for-await
 * for stream calls). One-shot — each call to `proxy.method(...)` returns
 * a fresh `DeferredCall`. Awaiting after iterating (or vice versa) throws
 * `RpcError('DeferredCall already consumed')`.
 */
export interface DeferredCall<T = unknown> extends PromiseLike<T>, AsyncIterable<T> {}

/**
 * A bound proxy callable. Bare invocation `f(...args)` produces a
 * DeferredCall. `.options({...})` returns a new RpcCallable with merged
 * defaults; `.invoke({...})` produces a DeferredCall with full control
 * over args + kwargs + options.
 */
export interface RpcCallable<TArgs extends unknown[] = unknown[], TResult = unknown> {
  (...args: TArgs): DeferredCall<TResult>;
  options(opts: OptionsSpec): RpcCallable<TArgs, TResult>;
  invoke(spec: InvokeSpec): DeferredCall<TResult>;
}

/**
 * Per-(attach, nodeId) typed-RPC proxy. Returned by `AttachedSession`'s
 * `node(nodeId)` accessor and by the `Proxy`-wrapped attribute access
 * (`ctrl.<nodeId>`). All property access returns a callable `RpcCallable`.
 * Reserved internal names (private, dunder, `then`) are not intercepted.
 */
export interface NodeProxy {
  readonly [methodName: string]: RpcCallable;
}

// ─── Internals exposed for unit tests ────────────────────────────────────────
//
// These exports are public for the rpc-proxy.test.ts suite to assert on demux
// state and to construct fake proxies. Treat as internal API; do not depend on
// them from application code.

/** @internal */
export class CorrIdGen {
  constructor(attachId: string);
  next(): string;
}

/** @internal */
export class ReplyDemux {
  constructor(ctrl: any, nodeId: string);
  ensureStarted(): Promise<void>;
  registerUnary(corrId: string, timeoutMs: number | null): Promise<unknown>;
  registerStream(corrId: string): unknown;
  unregister(corrId: string): void;
  close(): void;
}

/** @internal */
export class _NodeProxyCore {
  constructor(ctrl: any, nodeId: string);
  _ctrl: any;
  _nodeId: string;
  _demux: ReplyDemux;
}

/** @internal */
export function _makeNodeProxy(ctrl: any, nodeId: string): NodeProxy;
