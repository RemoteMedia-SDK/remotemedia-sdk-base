'use strict';
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { setTimeout: delay } = require('node:timers/promises');
const { Worker } = require('node:worker_threads');
const { once } = require('node:events');
const runtime = require(process.env.REMOTEMEDIA_NAPI_EXTENSION);
let sequence = 0;
function setup(strict = true, capacity = 2) {
  const session = runtime.createSession({
    id: `recv_${process.pid}_${sequence++}`, requireZeroCopy: strict,
    defaultChannelConfig: { capacity, maxPayloadSize: 4096 }
  });
  const channel = session.channel('bytes');
  const publisher = channel.createPublisher();
  const subscriber = channel.createSubscriber(capacity);
  return { session, channel, publisher, subscriber,
    close() { subscriber.close(); publisher.close(); session.close(); } };
}
function send(publisher, value, size = 8) {
  const loan = publisher.loan(size); loan.fill(value); loan.send();
}
function counts(session, reason = 'borrowed_input') {
  return JSON.parse(session.copyReport()).entries.find(([name]) => name === reason)[1];
}
function total(session) {
  return JSON.parse(session.copyReport()).entries.reduce((sum, [, c]) => sum + c.copied_bytes, 0);
}
async function until(check, message) {
  for (let i = 0; i < 200; i++) {
    if (check()) return;
    global.gc(); await delay(5);
  }
  assert.fail(message);
}
const manifest = JSON.stringify({version:'v1', metadata:{}, nodes:[
  {id:'echo', node_type:'PassThrough', params:{}}
], connections:[]});

test('strict native receive retains slices and hands the same storage to a pipeline', {timeout:15000}, async () => {
  const ipc = setup();
  const pipeline = await runtime.createStreamingSession(manifest);
  try {
    assert.equal(ipc.subscriber.receive(), null);
    send(ipc.publisher, 37);
    const sample = await ipc.subscriber.receiveAsync();
    assert.equal(sample.size, 8);
    assert.equal(typeof sample.timestampNs, 'number');
    assert.ok(sample.timestampNs > 1e18);
    assert.throws(() => sample.buffer, /copy/i);
    assert.throws(() => sample.readByte(8), /bounds/);
    assert.throws(() => sample.slice(8, 9));
    const view = sample.slice(2, 6);
    let source = view.toBinary();
    await pipeline.sendInput(source);
    let event = await pipeline.recvOutput();
    assert.equal(event.kind, 'data');
    let output = event.data;
    assert.equal(source.sharesBufferWith(output), true);
    sample.release(); view.release();
    assert.throws(() => sample.release(), /released/);
    assert.throws(() => sample.readByte(0), /released/);
    ipc.close(); await pipeline.close();
    assert.equal(output.readByte(3), 37);
    assert.equal(ipc.subscriber.borrowedCount, 1);
    assert.equal(total(ipc.session), 0);
    source = null; output = null; event = null;
    await until(() => ipc.subscriber.borrowedCount === 0, 'pipeline native owner was not reclaimed');
  } finally { ipc.close(); await pipeline.close(); }
});

test('retained aliases apply borrow backpressure until the final owner releases', async () => {
  const ipc = setup(true, 1);
  try {
    assert.equal(ipc.subscriber.maxBorrowedSamples, 1);
    send(ipc.publisher, 1);
    const first = await ipc.subscriber.receiveAsync();
    const view = first.slice(0, 1); first.release();
    send(ipc.publisher, 2);
    assert.equal(ipc.subscriber.receive(), null);
    assert.equal(await ipc.subscriber.receiveTimeout(10), null);
    view.release();
    const next = await ipc.subscriber.receiveAsync();
    assert.equal(next.readByte(0), 2); next.release();
    assert.equal(ipc.subscriber.borrowedCount, 0);
  } finally { ipc.close(); }
});

test('async receive gets actual bytes, times out, and rejects after channel close', async () => {
  const ipc = setup();
  try {
    assert.equal(await ipc.subscriber.receiveTimeout(0), null);
    const receiving = ipc.subscriber.receiveTimeout(1000);
    await delay(10); send(ipc.publisher, 19);
    const sample = await receiving; assert.equal(sample.readByte(7), 19); sample.release();
    const pending = ipc.subscriber.receiveAsync();
    const rejected = assert.rejects(pending, /closed/);
    ipc.channel.close(); await rejected;
    assert.equal(ipc.subscriber.isValid, false);
  } finally { ipc.close(); }
});

test('receive Buffer export is one independent session-attributed copy', async () => {
  const ipc = setup(false);
  try {
    send(ipc.publisher, 5);
    const sample = await ipc.subscriber.receiveAsync();
    assert.equal(total(ipc.session), 0);
    const bytes = sample.buffer;
    assert.equal(counts(ipc.session).copied_bytes, 8);
    assert.equal(counts(ipc.session).completed, 1);
    bytes.fill(9); assert.equal(sample.readByte(0), 5);
    sample.release(); ipc.close();
    assert.deepEqual([...bytes], Array(8).fill(9));
  } finally { ipc.close(); }
});

test('MessagePack decode uses receiver authority and strict policy rejects copies', async () => {
  for (const strict of [false, true]) {
    const ipc = setup(strict);
    // Generate portable wire through the compatibility publisher or directly
    // initialize known MessagePack bytes in a strict producer loan.
    try {
      if (strict) {
        // rmp-serde externally tagged Binary encoded as a one-entry map of byte values.
        const wire = [0x81, 0xa6, ...Buffer.from('Binary'), 0x94, 1, 2, 3, 4];
        const loan = ipc.publisher.loan(wire.length);
        wire.forEach((byte, index) => loan.set(index, byte)); loan.send();
      } else {
        ipc.publisher.publish(runtime.NapiRuntimeData.binary(Buffer.from([1,2,3,4])));
      }
      const sample = await ipc.subscriber.receiveAsync();
      const before = total(ipc.session);
      if (strict) {
        assert.throws(() => sample.decode(), /copy/i);
        assert.equal(total(ipc.session), before);
      } else {
        const data = sample.decode(); assert.equal(data.readByte(3), 4);
        assert.equal(total(ipc.session) - before, 4);
      }
      sample.release();
    } finally { ipc.close(); }
  }
});

test('native metadata parsing borrows payload and rejects truncated envelopes', async () => {
  const ipc = setup();
  try {
    const header = Buffer.alloc(18); header[0] = 3;
    header.writeUInt32LE(3, 11); header.set(Buffer.from('abc'), 15);
    const loan = ipc.publisher.loan(header.length);
    header.forEach((value, index) => loan.set(index, value)); loan.send();
    const sample = await ipc.subscriber.receiveAsync();
    const metadata = sample.toRuntimeData();
    assert.equal(metadata.payloadLen, 3); assert.equal(metadata.payloadOffset, 15);
    const truncated = sample.slice(0, 17);
    assert.throws(() => truncated.toRuntimeData(), /Truncated/);
    assert.equal(total(ipc.session), 0);
    truncated.release(); sample.release();
    const shared = Buffer.from(new SharedArrayBuffer(32));
    for (const parser of [runtime.parseRuntimeDataHeader, runtime.parseAudioMetadata,
      runtime.parseVideoMetadata, runtime.parseFileMetadata]) {
      assert.throws(() => parser(shared, 0), /SharedArrayBuffer/);
    }
  } finally { ipc.close(); }
});

test('callback queue is bounded, unsubscribe recovers queued owners and retained sample stays valid', {timeout:15000}, async () => {
  const ipc = setup(true, 2);
  let retained;
  try {
    let resolve, reject;
    const delivered = new Promise((res, rej) => { resolve = res; reject = rej; });
    const unsubscribe = ipc.subscriber.onData((sample, error) => {
      if (error) { reject(error); return; }
      retained = sample; unsubscribe(); resolve();
    });
    assert.throws(() => ipc.subscriber.receive(), /Unsubscribe/);
    assert.throws(() => ipc.subscriber.onData(() => {}), /already registered/);
    for (let i = 0; i < 8; i++) send(ipc.publisher, 42);
    // Deliberately keep JS busy while the receive worker queues native owners.
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
    assert.ok(ipc.subscriber.pendingCount <= ipc.subscriber.maxBorrowedSamples);
    assert.ok(ipc.subscriber.borrowedCount <= ipc.subscriber.maxBorrowedSamples);
    await delivered;
    assert.equal(ipc.subscriber.pendingCount, 0);
    assert.equal(ipc.subscriber.borrowedCount, 1);
    ipc.close(); assert.equal(retained.readByte(0), 42);
    retained.release(); assert.equal(ipc.subscriber.borrowedCount, 0);
    assert.equal(total(ipc.session), 0);
  } finally { ipc.close(); }
});

test('audio video and file metadata use the retained native view without payload snapshots', async () => {
  const ipc = setup();
  try {
    const bytes = Buffer.alloc(64);
    const loan = ipc.publisher.loan(bytes.length); loan.fill(0); loan.send();
    const sample = await ipc.subscriber.receiveAsync();
    for (const [method, parse] of [
      ['audioMetadata', runtime.parseAudioMetadata],
      ['videoMetadata', runtime.parseVideoMetadata],
      ['fileMetadata', runtime.parseFileMetadata],
    ]) {
      assert.deepEqual(sample[method](4), parse(bytes, 4));
      assert.throws(() => sample[method](63), /small|Invalid/);
    }
    assert.equal(total(ipc.session), 0); sample.release();
    assert.throws(() => sample.audioMetadata(0), /released/);
  } finally { ipc.close(); }
});

test('closing a channel reports a terminal callback error', {timeout:10000}, async () => {
  const ipc = setup();
  try {
    const failed = new Promise((resolve, reject) => {
      ipc.subscriber.onData((sample, error) => {
        if (sample !== null || !(error instanceof Error)) reject(new Error('missing terminal error'));
        else resolve(error);
      });
    });
    ipc.channel.close();
    assert.match((await failed).message, /closed/);
    assert.throws(() => ipc.subscriber.receive(), /closed/);
  } finally { ipc.close(); }
});

test('unsubscribe before JS delivery clears queued native owners and can register again', async () => {
  const ipc = setup();
  try {
    let calls = 0;
    ipc.subscriber.onData(() => { calls++; });
    send(ipc.publisher, 7);
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
    assert.equal(ipc.subscriber.borrowedCount, 1);
    ipc.subscriber.unsubscribe();
    assert.equal(ipc.subscriber.pendingCount, 0);
    assert.equal(ipc.subscriber.borrowedCount, 0);
    await delay(10); assert.equal(calls, 0);
    const next = new Promise((resolve, reject) => {
      ipc.subscriber.onData((sample, error) => {
        if (error) reject(error);
        else { sample.release(); ipc.subscriber.close(); resolve(); }
      });
    });
    send(ipc.publisher, 8); await next;
    assert.equal(ipc.subscriber.borrowedCount, 0);
  } finally { ipc.close(); }
});

test('worker environment termination joins receive worker with queued native samples', {timeout:15000}, async () => {
  const worker = new Worker(`
    const {parentPort} = require('node:worker_threads');
    const runtime = require(process.env.REMOTEMEDIA_NAPI_EXTENSION);
    const session = runtime.createSession({id:'shutdown_'+process.pid, requireZeroCopy:true});
    const channel = session.channel('bytes');
    const subscriber = channel.createSubscriber(2);
    const publisher = channel.createPublisher();
    subscriber.onData(() => { throw new Error('delivery must stay queued'); });
    const loan = publisher.loan(8); loan.fill(5); loan.send();
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
    parentPort.postMessage({borrowed:subscriber.borrowedCount, pending:subscriber.pendingCount});
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10000);
  `, {eval:true});
  try {
    const [queued] = await once(worker, 'message');
    assert.equal(queued.borrowed, 1); assert.equal(queued.pending, 1);
    await worker.terminate();
    // Surviving environment still creates and receives native data.
    const ipc = setup();
    try {
      send(ipc.publisher, 31); const sample = await ipc.subscriber.receiveAsync();
      assert.equal(sample.readByte(0), 31); sample.release();
    } finally { ipc.close(); }
  } finally { await worker.terminate(); }
});

test('throwing callbacks surface their exception and terminate delivery safely', {timeout:15000}, async () => {
  const worker = new Worker(`
    const runtime = require(process.env.REMOTEMEDIA_NAPI_EXTENSION);
    const session = runtime.createSession({id:'throw_'+process.pid, requireZeroCopy:true});
    const channel = session.channel('bytes');
    const subscriber = channel.createSubscriber();
    const publisher = channel.createPublisher();
    subscriber.onData(() => { throw new Error('receive callback sentinel'); });
    const loan = publisher.loan(4); loan.fill(9); loan.send();
  `, {eval:true});
  const exited = new Promise(resolve => worker.once('exit', resolve));
  try {
    const [error] = await once(worker, 'error');
    assert.match(error.message, /receive callback sentinel/);
    assert.equal(await exited, 1);
  } finally { await worker.terminate(); }
});
