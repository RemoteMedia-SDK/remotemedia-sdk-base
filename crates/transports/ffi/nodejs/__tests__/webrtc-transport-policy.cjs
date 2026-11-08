// Run directly against the built addon: node webrtc-transport-policy.cjs /path/addon.node
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
    };
    for (const policy of [undefined, 'allow_copies', 'require_zero_copy']) {
      const options = { ...config };
      if (policy !== undefined) options.transportMaterializationPolicy = policy;
      const server = await WebRtcServer[factory](options);
      try {
        assert.equal(await server.state, 'created');
        accepted++;
      } finally {
        await server.shutdown();
      }
    }
    for (const policy of [null, '', 'strict', 'RequireZeroCopy', true, 42]) {
      await assert.rejects(async () => WebRtcServer[factory]({
        ...config, transportMaterializationPolicy: policy,
      }));
      rejected++;
    }
  }
  console.log(JSON.stringify({ accepted, rejected, scope: 'addon configuration; no network started' }));
}

main().catch(error => { console.error(error); process.exitCode = 1; });
