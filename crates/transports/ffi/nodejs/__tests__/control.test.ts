/**
 * In-proc control bus tests.
 *
 * Mirrors `clients/python/remotemedia/control/tests/test_inproc.py`.
 *
 * Each test creates an executor-backed streaming session via
 * `createStreamingSession` (NOT the legacy `createStreamSession`), opens an
 * `attachInproc` adapter, and exercises one slice of the control surface.
 */

import {
  Data,
  AttachedSession,
  Subscription,
  InterceptSession,
  SessionNotFoundError,
  ControlAddressError,
  NodeState,
  InterceptDecision,
  attachInproc,
} from '../control';

// Late-load the native module so tests stay skippable when the binding isn't built.
function loadNative(): { native: any | null; loadError: Error | null } {
  try {
    const native = require('..');
    return { native, loadError: null };
  } catch (e) {
    return { native: null, loadError: e as Error };
  }
}

const PASSTHROUGH_MANIFEST = JSON.stringify({
  version: 'v1',
  metadata: { name: 'control-test' },
  nodes: [
    { id: 'echo', node_type: 'PassThrough', params: {} },
  ],
  connections: [],
});

describe('FFI in-proc control surface (Node.js)', () => {
  const { native, loadError } = loadNative();
  const enabled = !!native && typeof native.createStreamingSession === 'function';

  beforeAll(() => {
    if (!enabled) {
      console.warn(
        'Skipping in-proc control tests: native module unavailable.',
        'Build with: cargo build -p remotemedia-ffi --features napi --no-default-features',
      );
      if (loadError) console.warn('Load error:', loadError.message);
    }
  });

  test('subscribe receives outputs published by a node', async () => {
    if (!enabled) return;

    const sess = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    const ctrl = attachInproc(sess);
    try {
      expect(ctrl).toBeInstanceOf(AttachedSession);
      expect(ctrl.sessionId).toBe(sess.sessionId);

      const tap = await ctrl.subscribe('echo.out');
      expect(tap).toBeInstanceOf(Subscription);

      await ctrl.publish('echo.in', Data.fromText('hello'));

      const out = await Promise.race([
        tap.recv(),
        new Promise<null>((_, rej) =>
          setTimeout(() => rej(new Error('timeout')), 2000),
        ),
      ]);
      expect(out).not.toBeNull();
      expect(out!.kind).toBe('text');
      expect(out!.textValue).toBe('hello');
    } finally {
      ctrl.close();
      await sess.close();
    }
  });

  test('text round-trip preserves kind and value', async () => {
    if (!enabled) return;

    const sess = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    const ctrl = attachInproc(sess);
    try {
      const tap = await ctrl.subscribe('echo.out');
      await ctrl.publish('echo.in', Data.fromText('round-trip-me'));

      const out = await tap.recv();
      expect(out).not.toBeNull();
      expect(out!.kind).toBe('text');
      expect(out!.textValue).toBe('round-trip-me');
    } finally {
      ctrl.close();
      await sess.close();
    }
  });

  test('json round-trip preserves kind and value (not downgraded to text)', async () => {
    if (!enabled) return;

    const sess = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    const ctrl = attachInproc(sess);
    try {
      const tap = await ctrl.subscribe('echo.out');
      const payload = { foo: 42, nested: { ok: true, list: [1, 2, 3] } };
      await ctrl.publish('echo.in', Data.fromJson(payload));

      const out = await tap.recv();
      expect(out).not.toBeNull();
      expect(out!.kind).toBe('json');
      expect(out!.jsonValue).toEqual(payload);
    } finally {
      ctrl.close();
      await sess.close();
    }
  });

  test('setNodeState transitions a node to Bypass and back', async () => {
    if (!enabled) return;

    const sess = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    const ctrl = attachInproc(sess);
    try {
      // Sanity: bypass + clear should not throw.
      await ctrl.setNodeState('echo', NodeState.BYPASS);
      await ctrl.clearNodeState('echo');

      // Bypass + publish should still deliver to subscriber (bypass = pass-through).
      const tap = await ctrl.subscribe('echo.out');
      await ctrl.setNodeState('echo', NodeState.BYPASS);
      await ctrl.publish('echo.in', Data.fromText('bypassed'));

      const out = await Promise.race([
        tap.recv(),
        new Promise<null>((_, rej) =>
          setTimeout(() => rej(new Error('timeout')), 2000),
        ),
      ]);
      expect(out).not.toBeNull();
      expect(out!.textValue).toBe('bypassed');
    } finally {
      ctrl.close();
      await sess.close();
    }
  });

  test('intercept receives request with original payload and replace() does not throw', async () => {
    // Architecture: taps see PRE-intercept data per the bus spec — `on_node_output`
    // broadcasts to tap subscribers BEFORE consulting the intercept slot. So we
    // verify (a) the intercept slot delivers the original frame to the handler,
    // (b) `complete(REPLACE, ...)` round-trips to the bus without raising.
    // Mirror of `test_intercept_inproc_replace` in
    // `clients/python/remotemedia/control/tests/test_inproc.py`.
    if (!enabled) return;

    const sess = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    const ctrl = attachInproc(sess);
    try {
      await ctrl.subscribe('echo.out'); // pre-arm a tap; not asserted on
      const intercept = await ctrl.intercept('echo.out', 500);
      expect(intercept).toBeInstanceOf(InterceptSession);

      await ctrl.publish('echo.in', Data.fromText('original'));

      const item = await Promise.race([
        intercept.recv(),
        new Promise<null>((_, rej) =>
          setTimeout(() => rej(new Error('intercept recv timeout')), 3000),
        ),
      ]);
      expect(item).not.toBeNull();
      expect(item!.data.kind).toBe('text');
      expect(item!.data.textValue).toBe('original');

      // Replace decision — must not throw.
      await intercept.complete(
        item!.correlationId,
        InterceptDecision.REPLACE,
        Data.fromText('rewritten'),
      );

      intercept.close();
    } finally {
      ctrl.close();
      await sess.close();
    }
  });

  test('subscribing to an in-address raises ControlAddressError', async () => {
    if (!enabled) return;

    const sess = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    const ctrl = attachInproc(sess);
    try {
      await expect(ctrl.subscribe('echo.in')).rejects.toBeInstanceOf(
        ControlAddressError,
      );
      await expect(ctrl.publish('echo.out', Data.fromText('x'))).rejects.toBeInstanceOf(
        ControlAddressError,
      );
    } finally {
      ctrl.close();
      await sess.close();
    }
  });

  test('attachInproc on a closed session raises SessionNotFoundError', async () => {
    if (!enabled) return;

    const sess = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    await sess.close();
    // The session id is no longer in the bus; attachInproc should bounce.
    expect(() => attachInproc(sess)).toThrow(SessionNotFoundError);
  });
});
