export const RpcMode: {
  readonly FF: 'ff';
  readonly REPLY: 'reply';
  readonly STREAM: 'stream';
};
export type RpcModeValue = 'ff' | 'reply' | 'stream';

export class RpcError extends Error {
  readonly name: 'RpcError';
  constructor(message: string);
}

export class RpcTimeoutError extends RpcError {
  readonly name: 'RpcTimeoutError';
  constructor(message: string);
}

export const AUX_PORT_REPLY: '__reply__';
export const RPC_META_METHOD: '__rpc_meta__';
export const CANCEL_SUFFIX: '__cancel__';
export const DEFAULT_REPLY_TIMEOUT_MS: number;
export const DEFAULT_META_TIMEOUT_MS: number;
