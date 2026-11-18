/**
 * `close()` on a streaming session with a receive pending.
 *
 * A receive holds the session's output receivers while it waits. `close()`
 * used to wait for those same receivers, so a session whose pipeline had
 * nothing more to say could not be closed while anything was reading it
 * (which is how an abort stops a run in `@remotemedia/ai`'s `native()`).
 */

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
  metadata: { name: 'streaming-close-test' },
  nodes: [{ id: 'echo', node_type: 'PassThrough', params: {} }],
  connections: [],
});

function within<T>(ms: number, what: string, promise: Promise<T>): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(`${what} did not finish within ${ms} ms`)), ms),
    ),
  ]);
}

describe('closing a streaming session with a pending receive (Node.js)', () => {
  const { native, loadError } = loadNative();
  const enabled = !!native && typeof native.createStreamingSession === 'function';

  beforeAll(() => {
    if (!enabled) {
      console.warn(
        'Skipping streaming close tests: native module unavailable.',
        'Build with: cargo build -p remotemedia-ffi --features napi --no-default-features',
      );
      if (loadError) console.warn('Load error:', loadError.message);
    }
  });

  test('close() returns and ends a pending recvOutput() with null', async () => {
    if (!enabled) return;
    const session = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    await session.sendInput(native.NapiRuntimeData.text('x'));
    const first = await within(5000, 'the echo', session.recvOutput());
    expect(first).not.toBeNull();

    // Nothing more will arrive: this receive waits.
    const pending = session.recvOutput();
    await within(5000, 'close()', session.close());
    await expect(within(5000, 'the pending recvOutput()', pending)).resolves.toBeNull();
    // And a receive after close ends at once.
    await expect(within(5000, 'recvOutput() after close', session.recvOutput())).resolves.toBeNull();
  });

  test('close() ends a pending per-kind receive', async () => {
    if (!enabled) return;
    const session = await native.createStreamingSession(PASSTHROUGH_MANIFEST);
    const pending = session.recvData();
    await within(5000, 'close()', session.close());
    await expect(within(5000, 'the pending recvData()', pending)).resolves.toBeNull();
  });
});
