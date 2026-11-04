// Runs without a native addon: only the unused Data wrapper is substituted.
const { readFileSync } = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const test = require('node:test');

function loadProxy() {
  const module = { exports: {} };
  const control = path.join(__dirname, '../control');
  vm.runInNewContext(readFileSync(path.join(control, 'rpc-proxy.js'), 'utf8'), {
    module,
    setTimeout,
    clearTimeout,
    require(name) {
      if (name === './data') return { Data: {} };
      if (name === './rpc') return require(path.join(control, 'rpc.js'));
      throw new Error(`Unexpected dependency ${name}`);
    },
  });
  return module.exports;
}

test('reply failure rejects unary and streaming callers and closes registration', async () => {
  const { ReplyDemux } = loadProxy();
  let fail;
  const demux = new ReplyDemux({
    subscribe: async () => ({ recv: () => new Promise((_, reject) => { fail = reject; }) }),
  }, 'manager');
  await demux.ensureStarted();
  const unary = demux.registerUnary('unary', 5000);
  const queue = demux.registerStream('stream');
  const rejected = assert.rejects(unary, /lagged by 7/);
  fail(new Error('RPC reply subscription lagged by 7 frames'));
  await demux._consumerPromise;
  await rejected;
  const event = await queue.take();
  assert.equal(event.kind, 'error');
  assert.match(event.payload, /lagged by 7/);
  assert.equal(await queue.take(), null);
  await assert.rejects(demux.ensureStarted(), /closed/);
  await assert.rejects(demux.registerUnary('late', 5000), /closed/);
  assert.throws(() => demux.registerStream('late-stream'), /closed/);
  assert.equal(demux._unary.size, 0);
  assert.equal(demux._streams.size, 0);
});
