import { Data } from './data';
import { NodeProxy } from './rpc-proxy';

export class SessionNotFoundError extends Error {
  readonly name: 'SessionNotFoundError';
}

export class ControlAddressError extends Error {
  readonly name: 'ControlAddressError';
}

export const NodeState: {
  readonly ENABLED: 1;
  readonly BYPASS: 2;
  readonly DISABLED: 3;
};
export type NodeStateValue = 1 | 2 | 3;

export const InterceptDecision: {
  readonly PASS: 0;
  readonly REPLACE: 1;
  readonly DROP: 2;
};
export type InterceptDecisionValue = 0 | 1 | 2;

export interface InterceptItem {
  correlationId: bigint;
  data: Data;
}

export class Subscription {
  /** @internal */
  private constructor(nativeSub: any);

  /**
   * Await the next frame. Returns `null` on broadcast lag (still live).
   * Throws when the upstream channel closes.
   */
  recv(): Promise<Data | null>;

  [Symbol.asyncIterator](): AsyncIterator<Data>;

  close(): void;
}

export class InterceptSession {
  /** @internal */
  private constructor(nativeSess: any);

  recv(): Promise<InterceptItem | null>;
  complete(
    correlationId: bigint,
    decisionKind: InterceptDecisionValue,
    replacement?: Data | null,
  ): Promise<void>;
  close(): void;
}

export interface AttachInprocOptions {
  /**
   * corr_id prefix for typed-RPC calls. Lets server-side logs distinguish
   * callers when more than one attach talks to the same session.
   * @default 'node-inproc'
   */
  attachId?: string;
}

/**
 * AttachedSession is wrapped in a `Proxy` so that any property access
 * beyond the typed instance members below returns a typed-RPC `NodeProxy`
 * bound to that node id (e.g. `ctrl.audio` returns the proxy for node
 * `'audio'`). The index signature below documents that pattern for
 * TypeScript users.
 *
 * Reserved instance keys win over the index access: `sessionId`,
 * `attachId`, `subscribe`, `publish`, `setNodeState`, `clearNodeState`,
 * `intercept`, `close`, `node`, plus anything starting with `_`.
 * For nodes literally named one of these, use `ctrl.node(nodeId)`.
 */
export class AttachedSession {
  /** @internal */
  private constructor(nativeAttached: any, opts?: AttachInprocOptions);

  readonly sessionId: string | null;
  readonly attachId: string;

  subscribe(address: string): Promise<Subscription>;
  publish(address: string, data: Data): Promise<void>;
  setNodeState(nodeId: string, state: NodeStateValue): Promise<void>;
  clearNodeState(nodeId: string): Promise<void>;
  intercept(address: string, deadlineMs: number): Promise<InterceptSession>;

  /**
   * Explicit accessor for nodes whose IDs collide with reserved instance
   * member names. For non-reserved names, `ctrl.x === ctrl.node('x')`.
   */
  node(nodeId: string): NodeProxy;

  close(): void;

  /**
   * Index signature for typed-RPC node access. `ctrl.<nodeId>` returns a
   * NodeProxy bound to that node id. Authors of typed plugins are encouraged
   * to use module augmentation to refine this to concrete proxy types per
   * node id (a future change will auto-generate these from `@rpc` annotations
   * + node-schemas).
   */
  readonly [nodeId: string]: NodeProxy | unknown;
}

/**
 * Open an in-proc control attach against a `NapiStreamingSession`.
 *
 * Throws `SessionNotFoundError` if the session is not registered on the
 * process-global bus (e.g. it was created via the legacy `createStreamSession`
 * or has already been closed).
 */
export function attachInproc(
  streamingSession: any,
  opts?: AttachInprocOptions,
): AttachedSession;
