'use strict';
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { fork } = require('node:child_process');
const { once } = require('node:events');
const runtime = require(process.env.REMOTEMEDIA_NAPI_EXTENSION);
let sequence = 0;
function setup(strict = false, capacity = 2) {
  const session = runtime.createSession({
    id: `ipc_${process.pid}_${sequence++}`, requireZeroCopy: strict,
    defaultChannelConfig: { capacity, maxPayloadSize: 1024 }
  });
  const channel = session.channel('bytes');
  const publisher = channel.createPublisher();
  return { session, channel, publisher };
}
function counts(session, reason = 'borrowed_input') {
  return JSON.parse(session.copyReport()).entries.find(([name]) => name === reason)[1];
}

// The receiving process deliberately uses the existing copying callback adapter.
// This proves native delivery across a process, not zero-copy receive (S3).
if (process.argv[2] === '--receiver') {
  const session = runtime.createSession({id: process.argv[3]});
  const subscriber = session.channel('bytes').createSubscriber();
  subscriber.onData(sample => {
    const bytes = [...sample.buffer];
    sample.release();
    subscriber.close();
    session.close();
    process.send({ bytes }, () => process.exit(0));
  });
  process.send({ready: true});
} else {
  test('strict native loan publishes bytes to another Node process', {timeout: 30000}, async () => {
    const { session, channel, publisher } = setup(true, 1);
    const loan = publisher.loan(8); // Creates service before the child opens it.
    let child;
    try {
      loan.fill(7);
      loan.set(3, 99);
      assert.equal(publisher.loanedCount, 1);
      assert.equal(publisher.tryLoan(1), null);
      assert.throws(() => loan.set(8, 1), /bounds/);
      assert.throws(() => loan.set(0, 256), /255/);
      assert.throws(() => channel.createSubscriber(), /strict receive is unsupported/);
      const env = {...process.env};
      delete env.NODE_TEST_CONTEXT;
      child = fork(__filename, ['--receiver', session.id], {
        execArgv: [], env, stdio: ['ignore', 'ignore', 'pipe', 'ipc']
      });
      let errors = '';
      child.stderr.on('data', chunk => { errors += chunk.toString().replaceAll('\0', ''); });
      const [ready] = await once(child, 'message', {signal: AbortSignal.timeout(10000)});
      assert.equal(ready.ready, true, errors);
      const received = once(child, 'message', {signal: AbortSignal.timeout(10000)});
      const exited = once(child, 'exit');
      loan.send();
      assert.equal(publisher.loanedCount, 0);
      assert.equal(loan.isConsumed, true);
      assert.throws(() => loan.send(), /consumed/);
      assert.throws(() => loan.fill(0), /consumed/);
      try {
        assert.deepEqual((await received)[0].bytes, [7, 7, 7, 99, 7, 7, 7, 7]);
      } catch (error) {
        throw new Error(`Native receive failed: ${error}; child stderr: ${errors.slice(-4000)}`);
      }
      assert.equal((await exited)[0], 0, errors);
      assert.equal(counts(session).copied_bytes, 0);
    } finally {
      if (child && child.exitCode === null) child.kill();
      publisher.close(); session.close();
    }
  });

  test('mutable imports and exports count one copy and snapshots survive send', () => {
    const { session, publisher } = setup();
    try {
      const loan = publisher.loan(4);
      const source = Buffer.from([3, 4]);
      loan.write(source, 1);
      source.fill(99);
      const snapshot = loan.buffer;
      assert.deepEqual([...snapshot], [0, 3, 4, 0]);
      snapshot[0] = 100;
      loan.fill(8);
      loan.send();
      publisher.publishRaw(Buffer.from([1, 2, 3]));
      assert.equal(counts(session).copied_bytes, 9);
      assert.equal(counts(session).completed, 3);
      publisher.close();
      assert.deepEqual([...snapshot], [100, 3, 4, 0]);
    } finally { publisher.close(); session.close(); }
  });

  test('strict rejects mutable copies and portable serialization without consuming loan', () => {
    const { session, publisher } = setup(true);
    try {
      const loan = publisher.loan(4);
      assert.throws(() => loan.write(Buffer.from([1])), /zero-copy/);
      assert.throws(() => loan.buffer, /zero-copy/);
      assert.throws(() => publisher.publishRaw(Buffer.from([1, 2])), /zero-copy/);
      const data = runtime.NapiRuntimeData.binary(Buffer.from([1, 2, 3]));
      assert.throws(() => publisher.publish(data), /zero-copy/);
      assert.equal(counts(session).copied_bytes, 0);
      assert.equal(counts(session).rejected_bytes, 7);
      assert.equal(counts(session, 'portable_serialization').rejected_bytes, 3);
      loan.fill(42);
      loan.send();
      assert.equal(publisher.loanedCount, 0);
    } finally { publisher.close(); session.close(); }
  });

  test('release, finalization, session close and invalid input return credits safely', async () => {
    const { session, channel, publisher } = setup(false, 1);
    try {
      assert.throws(() => publisher.loan(1025), /exceeds/);
      assert.equal(publisher.loanedCount, 0);
      let loan = publisher.loan(4);
      const shared = Buffer.from(new SharedArrayBuffer(4));
      assert.throws(() => loan.write(shared), /SharedArrayBuffer/);
      assert.throws(() => publisher.publishRaw(shared), /SharedArrayBuffer/);
      assert.throws(() => loan.write(Buffer.from([1, 2]), 3), /exceeds/);
      loan.release();
      assert.throws(() => loan.release(), /consumed/);
      assert.equal(publisher.loanedCount, 0);
      loan = publisher.loan(0);
      assert.equal(loan.buffer.length, 0);
      loan = null;
      for (let i = 0; i < 30 && publisher.loanedCount; ++i) {
        global.gc(); await new Promise(resolve => setTimeout(resolve, 10));
      }
      assert.equal(publisher.loanedCount, 0, 'GC must release native loan');
      loan = publisher.loan(4);
      assert.throws(() => session.channel('bytes', {capacity: 5}), /reconfigure/);
      assert.equal(session.channel('bytes').config.capacity, 1);
      session.close();
      assert.equal(channel.isOpen, false);
      assert.equal(publisher.isValid, false);
      assert.equal(loan.isConsumed, true);
      assert.throws(() => loan.send(), /closed/);
      const fresh = runtime.createSession({id: session.id, requireZeroCopy: true});
      try {
        session.close();
        assert.equal(JSON.parse(runtime.getSession(session.id).copyReport()).policy, 'require_zero_copy');
      } finally { fresh.close(); }
    } finally { publisher.close(); session.close(); }
  });

  test('compatibility serializer uses session ledger and enforces final wire limit', () => {
    const { session, publisher } = setup();
    try {
      publisher.publish(runtime.NapiRuntimeData.binary(Buffer.from([1, 2, 3, 4])));
      assert.equal(counts(session, 'portable_serialization').copied_bytes, 4);
      assert.equal(counts(session, 'private_ipc_ingress').copied_bytes, 0);
      assert.throws(() => publisher.publish(runtime.NapiRuntimeData.binary(Buffer.alloc(1024, 255))), /limit/);
      publisher.publishRaw(Buffer.from([5]));
    } finally { publisher.close(); session.close(); }
  });
}
