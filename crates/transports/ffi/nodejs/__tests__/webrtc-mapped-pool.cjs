// Real Node addon boundary; construction validates configuration without listening.
const assert = require('node:assert/strict');
const { WebRtcServer } = require(require('node:path').resolve(process.argv[2]));
const available = process.argv[3] !== 'unavailable';

async function main() {
  let accepted = 0, rejected = 0;
  const pool = { classes: [{ bytes: 3840, slots: 2 }, { bytes: 8192, slots: 1 }], maxBytes: 15872, maxLeases: 8 };
  const invalid = [
    {}, { ...pool, classes: [] }, { ...pool, maxBytes: 1 },
    { ...pool, classes: [{ bytes: 3840 }] },
    { ...pool, classes: [{ slots: 2 }] },
    { ...pool, classes: [{ bytes: Number.MAX_SAFE_INTEGER, slots: Number.MAX_SAFE_INTEGER }] },
    { ...pool, classes: [{ bytes: 1, slots: 2 ** 32 }], maxBytes: Number.MAX_SAFE_INTEGER },
  ];
  for (const key of ['bytes', 'slots', 'maxBytes', 'maxLeases']) {
    for (const value of [undefined, null, true, '8', 0, -1, 0.5, NaN, Infinity, 2 ** 53]) {
      invalid.push(key === 'bytes' || key === 'slots'
        ? { ...pool, classes: [{ bytes: 3840, slots: 1, [key]: value }] }
        : { ...pool, [key]: value });
    }
  }
  for (const factory of ['create', 'connect']) {
    const config = {
      ...(factory === 'create' ? { port: 50199 } : { signalingUrl: 'grpc://127.0.0.1:50199' }),
      manifest: JSON.stringify({ nodes: [], connections: [] }),
      stunServers: ['stun:127.0.0.1:3478'],
    };
    const selection = { policy: 'require_zero_copy', nodeRetentionOwners: 64, nodeRetentionBytes: 4194304 };
    for (const mappedPool of [undefined, pool]) {
      const input = { ...config, pipelineBuffers: { ...selection, mappedPool } };
      if (mappedPool && !available) {
        await assert.rejects(async () => WebRtcServer[factory](input), /mapped-buffers/);
        rejected++;
      } else {
        const server = await WebRtcServer[factory](input);
        try { assert.equal(await server.state, 'created'); accepted++; }
        finally { await server.shutdown(); }
      }
    }
    for (const mappedPool of invalid) {
      await assert.rejects(async () => WebRtcServer[factory]({ ...config, pipelineBuffers: { ...selection, mappedPool } }));
      rejected++;
    }
  }
  console.log(JSON.stringify({ available, accepted, rejected, scope: 'real addon pool configuration; no network started' }));
}
main().catch(error => { console.error(error); process.exitCode = 1; });
