/**
 * Unit tests for the typed-RPC proxy.
 *
 * These tests mock the `./control/data` module so they DO NOT require the
 * native binding to be built. Wire encoding / demultiplexing / mode
 * discovery / timeout / cancellation are all covered against a fake
 * `AttachedSession` that records publishes and lets the test emit synthetic
 * reply frames.
 *
 * The end-to-end test against a real Python `@rpc` plugin lives in
 * `rpc-proxy-e2e.test.ts`.
 */

// Mock the Data module before requiring anything that imports it.
jest.mock('../control/data', () => {
  class FakeData {
    private _payload: any;
    private _kind: string;
    constructor(payload: any, kind: string) {
      this._payload = payload;
      this._kind = kind;
    }
    static fromJson(obj: any): FakeData {
      return new FakeData(obj, 'json');
    }
    static fromText(s: string): FakeData {
      return new FakeData(s, 'text');
    }
    static fromBytes(_b: Buffer): FakeData {
      throw new Error('fromBytes not needed by unit tests');
    }
    static fromAudio(): FakeData {
      throw new Error('fromAudio not needed by unit tests');
    }
    static _fromNative(_n: any): FakeData {
      throw new Error('_fromNative not used by unit tests');
    }
    get kind(): string {
      return this._kind;
    }
    get textValue(): string {
      if (this._kind !== 'text') throw new Error('not text');
      return String(this._payload);
    }
    get jsonValue(): any {
      if (this._kind !== 'json') throw new Error('not json');
      return this._payload;
    }
    _toNative(): any {
      return this._payload;
    }
  }
  return { Data: FakeData };
});

import {
  CorrIdGen,
  ReplyDemux,
  _NodeProxyCore,
  _makeNodeProxy,
} from '../control/rpc-proxy';
import {
  RpcError,
  RpcTimeoutError,
  AUX_PORT_REPLY,
  RPC_META_METHOD,
} from '../control/rpc';
// Pull the *mocked* Data so we can build reply frames in tests.
const { Data: FakeData } = require('../control/data');

// ─── FakeAttachedSession ─────────────────────────────────────────────────────

interface RecordedPublish {
  address: string;
  payload: any; // The plain object passed to Data.fromJson.
}

class FakeSubscription {
  private _queue: Array<any> = [];
  private _waiters: Array<(v: any) => void> = [];
  private _closed = false;
  public _wasClosed = false;

  push(data: any) {
    if (this._closed) return;
    const w = this._waiters.shift();
    if (w) w(data);
    else this._queue.push(data);
  }

  async recv() {
    if (this._queue.length > 0) return this._queue.shift();
    if (this._closed) throw new Error('tap channel closed');
    return new Promise<any>((resolve) => {
      this._waiters.push(resolve);
    });
  }

  close() {
    this._closed = true;
    this._wasClosed = true;
    while (this._waiters.length > 0) {
      const w = this._waiters.shift()!;
      w(null);
    }
  }
}

class FakeAttachedSession {
  public publishes: RecordedPublish[] = [];
  public subscribes: string[] = [];
  public _corrIdGen = new CorrIdGen('test');
  private _subs = new Map<string, FakeSubscription>();

  async publish(address: string, data: any): Promise<void> {
    // The proxy always passes the result of Data.fromJson(...), which our
    // mock stores under .jsonValue (or .textValue for legacy).
    let payload: any;
    if (data && typeof data.jsonValue !== 'undefined') {
      try {
        payload = data.jsonValue;
      } catch {
        payload = data._payload;
      }
    } else if (data && typeof data.textValue !== 'undefined') {
      try {
        payload = data.textValue;
      } catch {
        payload = data._payload;
      }
    } else {
      payload = data;
    }
    this.publishes.push({ address, payload });
  }

  async subscribe(address: string): Promise<FakeSubscription> {
    this.subscribes.push(address);
    const sub = new FakeSubscription();
    this._subs.set(address, sub);
    return sub;
  }

  /** Helper for tests — emit a reply envelope to the demux of `nodeId`. */
  emitReply(nodeId: string, envelope: { corr_id: any; kind: string; data?: any }): void {
    const address = `${nodeId}.out.${AUX_PORT_REPLY}`;
    const sub = this._subs.get(address);
    if (!sub) throw new Error(`no subscription on ${address}`);
    // The demux decodes JSON frames; wrap as our FakeData.
    const wrapped = FakeData.fromJson({
      __aux_port__: AUX_PORT_REPLY,
      payload: envelope,
    });
    sub.push(wrapped);
  }

  /** Helper — find the corr_id of the published frame matching `address`. */
  lastCorrIdAt(address: string): string {
    for (let i = this.publishes.length - 1; i >= 0; i--) {
      if (this.publishes[i].address === address) {
        const cid = this.publishes[i].payload?.corr_id;
        return String(cid);
      }
    }
    throw new Error(`no publish on ${address}`);
  }

  closeSub(address: string): void {
    const s = this._subs.get(address);
    if (s) s.close();
  }
}

// ─── Tiny test helper ────────────────────────────────────────────────────────

function makeProxy(ctrl: FakeAttachedSession, nodeId: string) {
  // _makeNodeProxy returns a Proxy whose `get` returns _RpcCallable instances.
  return _makeNodeProxy(ctrl as any, nodeId);
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe('CorrIdGen', () => {
  test('produces "<attachId>:N" monotonic ids', () => {
    const g = new CorrIdGen('a1');
    expect(g.next()).toBe('a1:1');
    expect(g.next()).toBe('a1:2');
    expect(g.next()).toBe('a1:3');
  });
});

describe('RpcCallable + DeferredCall — fire-and-forget mode', () => {
  test('mode override skips meta and publishes once, resolves undefined', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'audio');
    const result = await (node as any).setContext.options({ mode: 'ff' })(
      'hello',
    );
    expect(result).toBeUndefined();
    expect(ctrl.publishes).toEqual([
      {
        address: 'audio.in.setContext',
        payload: { corr_id: null, args: ['hello'], kwargs: {} },
      },
    ]);
    // No subscription was ever opened for replies.
    expect(ctrl.subscribes).toEqual([]);
  });
});

describe('RpcCallable + DeferredCall — reply mode', () => {
  test('publishes and resolves when reply arrives', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'tts');
    // Promise.resolve on a thenable kicks off `.then(resolve, reject)` →
    // _DeferredCall._runUnary → demux.ensureStarted → ctrl.subscribe + publish.
    const promise = Promise.resolve(
      (node as any).listVoices.options({ mode: 'reply' })(),
    );
    // The demux opens its subscription before publishing, then publishes.
    await new Promise((r) => setTimeout(r, 10));
    expect(ctrl.subscribes).toEqual(['tts.out.__reply__']);
    expect(ctrl.publishes.length).toBe(1);
    expect(ctrl.publishes[0].address).toBe('tts.in.listVoices');
    expect(ctrl.publishes[0].payload.corr_id).toBe('test:1');
    expect(ctrl.publishes[0].payload.args).toEqual([]);
    expect(ctrl.publishes[0].payload.kwargs).toEqual({});
    // Emit a value reply.
    ctrl.emitReply('tts', {
      corr_id: 'test:1',
      kind: 'value',
      data: ['alice', 'bob'],
    });
    await expect(promise).resolves.toEqual(['alice', 'bob']);
  });

  test('rejects with RpcError on error reply', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'tts');
    const promise = Promise.resolve(
      (node as any).m.options({ mode: 'reply' })(),
    );
    await new Promise((r) => setTimeout(r, 10));
    ctrl.emitReply('tts', {
      corr_id: 'test:1',
      kind: 'error',
      data: 'ValueError: bad',
    });
    await expect(promise).rejects.toBeInstanceOf(RpcError);
    // The same promise can be asserted against multiple matchers because
    // jest's `rejects` adopts the underlying value, not the promise itself.
  });

  test('rejects with RpcTimeoutError when no reply arrives in window', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'x');
    const promise = (node as any).slow.options({ mode: 'reply', timeout: 25 })();
    await expect(promise).rejects.toBeInstanceOf(RpcTimeoutError);
    // The unary entry should be cleared.
    const core = (node as any) as { _demux: ReplyDemux };
    expect((core._demux as any)._unary.size).toBe(0);
  });

  test('kwargs via .invoke() preserves the kwargs envelope', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'tts');
    const promise = Promise.resolve(
      (node as any).synthesize.invoke({
        mode: 'reply',
        args: ['hi'],
        kwargs: { lang: 'ja', voice: 'alice' },
      }),
    );
    await new Promise((r) => setTimeout(r, 10));
    const pub = ctrl.publishes.find((p) => p.address === 'tts.in.synthesize')!;
    expect(pub).toBeDefined();
    expect(pub.payload).toEqual({
      corr_id: 'test:1',
      args: ['hi'],
      kwargs: { lang: 'ja', voice: 'alice' },
    });
    ctrl.emitReply('tts', { corr_id: 'test:1', kind: 'value', data: 'ok' });
    await expect(promise).resolves.toBe('ok');
  });

  test('bare positional args produce empty kwargs', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'x');
    const promise = Promise.resolve(
      (node as any).m.options({ mode: 'reply' })('a', 'b', 'c'),
    );
    await new Promise((r) => setTimeout(r, 10));
    const pub = ctrl.publishes.find((p) => p.address === 'x.in.m')!;
    expect(pub).toBeDefined();
    expect(pub.payload).toEqual({
      corr_id: 'test:1',
      args: ['a', 'b', 'c'],
      kwargs: {},
    });
    ctrl.emitReply('x', { corr_id: 'test:1', kind: 'value', data: null });
    await promise;
  });
});

describe('RpcCallable + DeferredCall — stream mode', () => {
  test('for-await yields N values then end', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'stt');
    const iter = (node as any).watchTokens.options({ mode: 'stream' })();

    // Start consuming asynchronously, then push values.
    const collected: any[] = [];
    const consumer = (async () => {
      for await (const v of iter) collected.push(v);
    })();

    // Wait for the publish + subscription to materialize.
    await new Promise((r) => setTimeout(r, 0));
    expect(ctrl.publishes[0].address).toBe('stt.in.watchTokens');
    const cid = ctrl.publishes[0].payload.corr_id;

    ctrl.emitReply('stt', { corr_id: cid, kind: 'value', data: 'a' });
    ctrl.emitReply('stt', { corr_id: cid, kind: 'value', data: 'b' });
    ctrl.emitReply('stt', { corr_id: cid, kind: 'value', data: 'c' });
    ctrl.emitReply('stt', { corr_id: cid, kind: 'end' });

    await consumer;
    expect(collected).toEqual(['a', 'b', 'c']);
  });

  test('break in for-await publishes __cancel__ exactly once', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'stt');
    const iter = (node as any).watchTokens.options({ mode: 'stream' })();

    const consumer = (async () => {
      for await (const v of iter) {
        // Break after the first value.
        void v;
        break;
      }
    })();

    await new Promise((r) => setTimeout(r, 0));
    const cid = ctrl.publishes[0].payload.corr_id;
    ctrl.emitReply('stt', { corr_id: cid, kind: 'value', data: 1 });
    await consumer;

    const cancelPublishes = ctrl.publishes.filter(
      (p) => p.address === 'stt.in.watchTokens.__cancel__',
    );
    expect(cancelPublishes.length).toBe(1);
    expect(cancelPublishes[0].payload).toEqual({ corr_id: cid });
  });

  test('error frame mid-stream throws RpcError out of for-await', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'stt');
    const iter = (node as any).watchTokens.options({ mode: 'stream' })();

    const consumer = (async () => {
      const collected: any[] = [];
      for await (const v of iter) collected.push(v);
      return collected;
    })();

    await new Promise((r) => setTimeout(r, 0));
    const cid = ctrl.publishes[0].payload.corr_id;
    ctrl.emitReply('stt', { corr_id: cid, kind: 'value', data: 1 });
    ctrl.emitReply('stt', { corr_id: cid, kind: 'error', data: 'boom' });

    await expect(consumer).rejects.toBeInstanceOf(RpcError);
    await expect(consumer).rejects.toThrow('boom');
  });

  test('AbortSignal cancels stream and publishes __cancel__', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'stt');
    const ctlAbort = new AbortController();
    const iter = (node as any).watchTokens.options({
      mode: 'stream',
      signal: ctlAbort.signal,
    })();

    const consumer = (async () => {
      const collected: any[] = [];
      for await (const v of iter) collected.push(v);
      return collected;
    })();

    await new Promise((r) => setTimeout(r, 0));
    const cid = ctrl.publishes[0].payload.corr_id;
    ctrl.emitReply('stt', { corr_id: cid, kind: 'value', data: 1 });
    // Let the value land, then abort.
    await new Promise((r) => setTimeout(r, 5));
    ctlAbort.abort();

    const result = await consumer;
    expect(result).toEqual([1]);
    const cancelPublishes = ctrl.publishes.filter(
      (p) => p.address === 'stt.in.watchTokens.__cancel__',
    );
    expect(cancelPublishes.length).toBe(1);
  });

  test('return() before first next() is a no-op (no publish)', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'stt');
    const deferred = (node as any).watchTokens.options({ mode: 'stream' })();
    const iter = deferred[Symbol.asyncIterator]();
    const r = await iter.return(undefined);
    expect(r).toEqual({ value: undefined, done: true });
    expect(ctrl.publishes).toEqual([]);
  });
});

describe('Mode discovery — __rpc_meta__', () => {
  test('first call to a node triggers one meta lookup; subsequent reuse cache', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'tts');

    const promise = Promise.resolve((node as any).listVoices());
    await new Promise((r) => setTimeout(r, 10));

    // Meta publish should have happened first.
    const metaPub = ctrl.publishes.find(
      (p) => p.address === `tts.in.${RPC_META_METHOD}`,
    );
    expect(metaPub).toBeDefined();
    expect(metaPub!.payload.args).toEqual([]);
    expect(metaPub!.payload.kwargs).toEqual({});

    // Reply with a meta map.
    const metaCid = metaPub!.payload.corr_id;
    ctrl.emitReply('tts', {
      corr_id: metaCid,
      kind: 'value',
      data: { listVoices: 'reply', stream_thing: 'stream' },
    });

    // The proxy uses the cached mode to publish the actual call.
    await new Promise((r) => setTimeout(r, 10));
    const listPub = ctrl.publishes.find(
      (p) => p.address === 'tts.in.listVoices',
    );
    expect(listPub).toBeDefined();
    ctrl.emitReply('tts', {
      corr_id: listPub!.payload.corr_id,
      kind: 'value',
      data: ['x'],
    });
    await expect(promise).resolves.toEqual(['x']);

    // A second reply-mode call must NOT republish the meta frame.
    const before = ctrl.publishes.filter(
      (p) => p.address === `tts.in.${RPC_META_METHOD}`,
    ).length;
    const p2 = Promise.resolve((node as any).listVoices());
    await new Promise((r) => setTimeout(r, 10));
    const after = ctrl.publishes.filter(
      (p) => p.address === `tts.in.${RPC_META_METHOD}`,
    ).length;
    expect(after).toBe(before);
    const pub2 = ctrl.publishes
      .filter((p) => p.address === 'tts.in.listVoices')
      .pop()!;
    ctrl.emitReply('tts', {
      corr_id: pub2.payload.corr_id,
      kind: 'value',
      data: ['y'],
    });
    await expect(p2).resolves.toEqual(['y']);
  });

  test('mode override skips meta lookup entirely', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'audio');
    await (node as any).setContext.options({ mode: 'ff' })('hello');
    const metaPub = ctrl.publishes.filter(
      (p) => p.address === `audio.in.${RPC_META_METHOD}`,
    );
    expect(metaPub).toEqual([]);
  });
});

describe('DeferredCall — one-shot semantics', () => {
  test('await then iterate (or vice versa) on the same DeferredCall throws', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'x');
    const deferred = (node as any).m.options({ mode: 'ff' })();
    await deferred;
    expect(() => deferred[Symbol.asyncIterator]()).toThrow(RpcError);
  });
});

describe('Reply demux — single subscription per node', () => {
  test('multiple concurrent reply calls share one subscription', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'tts');
    const p1 = Promise.resolve(
      (node as any).a.options({ mode: 'reply' })(),
    );
    const p2 = Promise.resolve(
      (node as any).b.options({ mode: 'reply' })(),
    );
    const p3 = Promise.resolve(
      (node as any).c.options({ mode: 'reply' })(),
    );
    await new Promise((r) => setTimeout(r, 10));
    expect(ctrl.subscribes).toEqual(['tts.out.__reply__']);
    const aPub = ctrl.publishes.find((p) => p.address === 'tts.in.a')!;
    const bPub = ctrl.publishes.find((p) => p.address === 'tts.in.b')!;
    const cPub = ctrl.publishes.find((p) => p.address === 'tts.in.c')!;
    ctrl.emitReply('tts', { corr_id: cPub.payload.corr_id, kind: 'value', data: 3 });
    ctrl.emitReply('tts', { corr_id: aPub.payload.corr_id, kind: 'value', data: 1 });
    ctrl.emitReply('tts', { corr_id: bPub.payload.corr_id, kind: 'value', data: 2 });
    await expect(p1).resolves.toBe(1);
    await expect(p2).resolves.toBe(2);
    await expect(p3).resolves.toBe(3);
  });

  test('demux.close() rejects in-flight unary calls', async () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'x');
    const p1 = (node as any).slow.options({ mode: 'reply' })();
    await new Promise((r) => setTimeout(r, 0));
    const core = (node as any) as { _demux: ReplyDemux };
    core._demux.close();
    await expect(p1).rejects.toBeInstanceOf(RpcError);
    await expect(p1).rejects.toThrow('attach closed');
  });
});

describe('NodeProxy Proxy semantics', () => {
  test('private and dunder access passes through to core', () => {
    const ctrl = new FakeAttachedSession();
    const node = makeProxy(ctrl, 'x');
    expect((node as any)._nodeId).toBe('x');
    expect((node as any)._demux).toBeInstanceOf(ReplyDemux);
    // `then` is reserved so the NodeProxy itself isn't accidentally thenable.
    expect((node as any).then).toBeUndefined();
  });
});
