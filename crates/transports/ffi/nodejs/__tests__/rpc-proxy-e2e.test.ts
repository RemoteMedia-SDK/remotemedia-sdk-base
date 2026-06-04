/**
 * End-to-end test for the typed-RPC proxy against a real Python `@rpc`
 * plugin (`echo-rpc-python-loadable`).
 *
 * Skips when either:
 *   - the native binding (`remotemedia-native.*.node`) is not built, OR
 *   - the Python loadable plugin cdylib is not built at the expected
 *     `examples/echo-rpc-python-loadable/target/{release,debug}/...so` path.
 *
 * To build everything end-to-end:
 *   cd crates/transports/ffi/nodejs && npm run build:cargo
 *   cd examples/echo-rpc-python-loadable && cargo build --release
 *
 * Then `npm test -- rpc-proxy-e2e.test.ts` will exercise the live bus.
 *
 * The two tests share a single session via beforeAll/afterAll. Per-test
 * session spin-up cold-spawns Python + provisions a venv (5-10s the first
 * time, ~1s thereafter), which adds up fast across two tests and is
 * unrelated to what we're actually trying to assert (the proxy + bus
 * routing). Session teardown is left to jest's `forceExit: true`
 * because `sess.close()` on the loadable-plugin path waits for the
 * subprocess to terminate cleanly (it eventually does, just not within
 * the test deadline) and the test process exits anyway.
 */

import * as path from 'node:path';
import * as fs from 'node:fs';

function loadNative(): { native: any | null; loadError: Error | null } {
  try {
    const native = require('..');
    return { native, loadError: null };
  } catch (e) {
    return { native: null, loadError: e as Error };
  }
}

function findPluginSo(): string | null {
  const repoRoot = path.resolve(__dirname, '..', '..', '..', '..', '..');
  const pluginDir = path.join(
    repoRoot,
    'examples',
    'echo-rpc-python-loadable',
    'target',
  );
  const ext = process.platform === 'darwin' ? 'dylib' : 'so';
  const candidates = [
    path.join(pluginDir, 'release', `libecho_rpc_python_loadable_plugin.${ext}`),
    path.join(pluginDir, 'debug', `libecho_rpc_python_loadable_plugin.${ext}`),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  return null;
}

describe('Typed-RPC proxy — E2E against echo-rpc-python-loadable', () => {
  const { native, loadError } = loadNative();
  const pluginSo = findPluginSo();
  // Gated on RPC_E2E=1 because the loadable-plugin path spawns a real
  // Python subprocess + iceoryx2 services, which doesn't coexist well
  // with the other suites in a single jest worker (the next cold
  // session-spawn hangs as iceoryx2's service registry grows during
  // the run). The dedicated `npm run test:e2e-rpc` script sets this
  // var. Default `npm test` skips the suite.
  const e2eEnabled = process.env.RPC_E2E === '1';
  const enabled =
    e2eEnabled &&
    !!native &&
    typeof native.createStreamingSession === 'function' &&
    !!pluginSo;

  let sess: any = null;
  let ctrl: any = null;

  beforeAll(async () => {
    if (!enabled) {
      const reasons: string[] = [];
      if (!e2eEnabled)
        reasons.push('RPC_E2E env var not set (use `npm run test:e2e-rpc`)');
      if (!native) reasons.push('native binding unavailable');
      if (loadError) reasons.push(`native load error: ${loadError.message}`);
      if (e2eEnabled && native && !pluginSo)
        reasons.push(
          'echo-rpc-python-loadable cdylib not found; build with ' +
            '`cd examples/echo-rpc-python-loadable && cargo build --release`',
        );
      console.warn(
        'Skipping typed-RPC E2E tests:\n  - ' + reasons.join('\n  - '),
      );
      return;
    }
    // Clear iceoryx2 service registry state left over from earlier test
    // suites (control.test.ts, subscriber.test.ts, etc. all spin up
    // services that aren't always torn down). The Python plugin's
    // session uses content-hashed service names, so stale entries from
    // unrelated tests don't actively collide — but the iceoryx2 service
    // discovery itself slows to a crawl past a few hundred entries,
    // which adds 30s+ to our beforeAll. A fresh wipe restores ~1s
    // cold-spawn latency. The risk is zero: no other test runs in
    // parallel with this beforeAll (jest is serial by default).
    try {
      const sh = require('node:child_process');
      sh.execSync(
        'rm -rf /tmp/iceoryx2 /dev/shm/iox2_* && mkdir -p /tmp/iceoryx2/services /tmp/iceoryx2/nodes',
        { stdio: 'ignore' },
      );
    } catch (_) {
      // best-effort
    }
    // `REMOTEMEDIA_PYTHON_SRC` and `REMOTEMEDIA_PLUGIN_STDERR_FILE` must
    // both be in the shell environment BEFORE jest spawns the worker:
    //
    // - REMOTEMEDIA_PYTHON_SRC: Rust reads it via `std::env::var` from
    //   the plugin .so (libloading-dlopen'd) at venv-provisioning time.
    //   Jest's `process.env = ...` mutations inside a worker are JS-side
    //   only — they don't reach libc `environ` that std::env::var reads
    //   — so setting it in beforeAll is too late.
    // - REMOTEMEDIA_PLUGIN_STDERR_FILE: jest wraps the worker's stderr
    //   in a captured stream. The Python runner subprocess inherits
    //   that stderr by default; the inherited fd eventually goes broken
    //   under jest's capture (test deadline, worker teardown, …) and
    //   SIGPIPE-kills Python mid-startup with exit status 1. The new
    //   subprocess-death watchdog in `plugin-sdk/src/python_ipc.rs`
    //   then surfaces "runner process exited before publishing READY".
    //   Pointing the runner at a file sidesteps jest's stderr capture.
    //
    // Both vars are set by `scripts/run-e2e-rpc.js` (the npm
    // `test:e2e-rpc` entry point). We assert here so a missing setup
    // surfaces as a clear failure instead of the deceptive
    // "managed venv missing remotemedia" error.
    if (!process.env.REMOTEMEDIA_PYTHON_SRC) {
      throw new Error(
        'REMOTEMEDIA_PYTHON_SRC unset — run via `npm run test:e2e-rpc` ' +
          '(scripts/run-e2e-rpc.js sets it before spawning jest).',
      );
    }
    const { attachInproc } = require('../control');
    const manifest = JSON.stringify({
      version: 'v1',
      metadata: { name: 'typed-rpc-e2e' },
      plugins: [pluginSo!],
      nodes: [
        { id: 'echo', node_type: 'EchoRpcPythonNode', params: {} },
      ],
      connections: [],
    });
    sess = await native.createStreamingSession(manifest);
    ctrl = attachInproc(sess, { attachId: 'e2e' });
  }, 90_000);

  afterAll(() => {
    // Session teardown deliberately left to jest's `forceExit: true` —
    // see the file-level note on `sess.close()` blocking on subprocess
    // termination.
    if (ctrl) {
      try {
        ctrl.close();
      } catch (_) {
        // best-effort
      }
    }
  });

  test('reply round-trip (echo)', async () => {
    if (!enabled) return;
    const result = await ctrl.echo.echo('hello');
    expect(result).toBe('hello');
  }, 30_000);

  test('fire-and-forget + reply read-back (set_context / get_context)', async () => {
    if (!enabled) return;
    await ctrl.echo.set_context('session-7');
    const ctx = await ctrl.echo.get_context();
    expect(ctx).toBe('session-7');
  }, 30_000);

  test('stream mode yields N values then terminates (watch_tokens)', async () => {
    if (!enabled) return;
    const collected: string[] = [];
    for await (const token of ctrl.echo.watch_tokens(['a', 'b', 'c'])) {
      collected.push(token);
    }
    expect(collected).toEqual(['a', 'b', 'c']);
  }, 30_000);
});
