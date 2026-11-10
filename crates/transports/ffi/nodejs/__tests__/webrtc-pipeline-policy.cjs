// Real addon boundary validation; factories do not start network listeners.
const assert = require('node:assert/strict');
const { WebRtcServer } = require(require('node:path').resolve(process.argv[2]));

async function main() {
  let accepted = 0;
  let rejected = 0;
  for (const factory of ['create', 'connect']) {
    const config = {
      ...(factory === 'create' ? { port: 50199 } : { signalingUrl: 'grpc://127.0.0.1:50199' }),
      manifest: JSON.stringify({ nodes: [], connections: [] }),
      stunServers: ['stun:127.0.0.1:3478'],
      transportMaterializationPolicy: 'allow_copies',
    };
    for (const policy of ['allow_copies', 'require_zero_copy']) {
      for (const budget of [0, 4194304, Number.MAX_SAFE_INTEGER]) {
        const server = await WebRtcServer[factory]({
          ...config,
          pipelineBuffers: { policy, nodeRetentionOwners: budget, nodeRetentionBytes: budget },
        });
        try {
          assert.equal(await server.state, 'created');
          accepted++;
        } finally {
          await server.shutdown();
        }
      }
    }
    const selection = { policy: 'require_zero_copy', nodeRetentionOwners: 64, nodeRetentionBytes: 4194304 };
    const invalid = [null, {}, { policy: 'require_zero_copy' }, { ...selection, policy: 'strict' }];
    for (const key of ['nodeRetentionOwners', 'nodeRetentionBytes']) {
      for (const value of [undefined, null, true, '8', -1, 0.5, NaN, Infinity, 2 ** 53]) {
        invalid.push({ ...selection, [key]: value });
      }
    }
    for (const pipelineBuffers of invalid) {
      await assert.rejects(async () => WebRtcServer[factory]({ ...config, pipelineBuffers }));
      rejected++;
    }
  }
  console.log(JSON.stringify({ accepted, rejected, scope: 'real addon config; no network started' }));
}

main().catch(error => { console.error(error); process.exitCode = 1; });
