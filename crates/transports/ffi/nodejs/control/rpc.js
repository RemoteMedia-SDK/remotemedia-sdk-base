// Typed-RPC primitives for the Node.js control bus.
//
// Wire-compatible with the Python typed-RPC layer
// (`clients/python/remotemedia/rpc.py`,
// `clients/python/remotemedia/control/rpc_proxy.py`). See
// `openspec/changes/add-typed-rpc-nodejs/specs/typed-rpc/spec.md` for the full
// surface contract.
//
// This module exports only the public types — error classes, mode constants
// and protocol constants. The proxy machinery lives in `./rpc-proxy.js`.

const RpcMode = Object.freeze({
  FF: 'ff',
  REPLY: 'reply',
  STREAM: 'stream',
});

class RpcError extends Error {
  constructor(message) {
    super(message);
    this.name = 'RpcError';
  }
}

class RpcTimeoutError extends RpcError {
  constructor(message) {
    super(message);
    this.name = 'RpcTimeoutError';
  }
}

// Wire constants — must match the Python side verbatim.
const AUX_PORT_REPLY = '__reply__';
const RPC_META_METHOD = '__rpc_meta__';
const CANCEL_SUFFIX = '__cancel__';

// Defaults.
const DEFAULT_REPLY_TIMEOUT_MS = 30_000;
const DEFAULT_META_TIMEOUT_MS = 5_000;

module.exports = {
  RpcMode,
  RpcError,
  RpcTimeoutError,
  AUX_PORT_REPLY,
  RPC_META_METHOD,
  CANCEL_SUFFIX,
  DEFAULT_REPLY_TIMEOUT_MS,
  DEFAULT_META_TIMEOUT_MS,
};
