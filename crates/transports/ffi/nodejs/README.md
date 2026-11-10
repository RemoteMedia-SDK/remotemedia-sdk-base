# @matbee/remotemedia-native

Native Node.js bindings for RemoteMedia zero-copy IPC with iceoryx2 shared memory.

## Overview

This package provides high-performance Node.js bindings for the RemoteMedia pipeline execution framework. It enables:

- **Zero-copy IPC**: Share data between Node.js, Python, and Rust processes via shared memory
- **Pipeline execution**: Run audio/video/ML pipelines with native performance
- **Type-safe API**: Full TypeScript definitions generated from Rust schemas

## Installation

```bash
npm install @matbee/remotemedia-native
```

## Requirements

- Node.js >= 18
- Linux (x64, arm64) or macOS (x64, arm64)
- For WebRTC features: see `@matbee/remotemedia-native-webrtc`

## Quick Start

```typescript
import { createSession } from '@matbee/remotemedia-native';

const session = createSession({
  id: 'my_session', requireZeroCopy: true,
  defaultChannelConfig: { capacity: 2, maxPayloadSize: 1_048_576 },
});
const channel = session.channel('bytes');
const subscriber = channel.createSubscriber();
const publisher = channel.createPublisher();
try {
  const loan = publisher.loan(16);
  loan.fill(7); // Generate directly in the native loan.
  loan.send();
  const sample = await subscriber.receiveAsync();
  try {
    console.log(sample.readByte(0)); // 7; no Buffer export.
    // sample.toBinary() hands retained native storage to a pipeline.
  } finally { sample.release(); }
} finally {
  subscriber.close(); publisher.close(); session.close();
}
```

## API Reference

### Session Management

```typescript
// Create a new IPC session
const session = createSession({
  id: 'session_id',        // 8-64 chars, alphanumeric + underscore/hyphen
  default_channel_config: {
    capacity: 64,          // Message queue size
    max_payload_size: 1_048_576,
    backpressure: false,
    history_size: 0,
  },
});

// Get existing session
const existing = getSession('session_id');

// List all sessions
const sessions = listSessions();
```

### RuntimeData Types

```typescript
// Audio data
const audio = NapiRuntimeData.audio(samples: Float32Array, sampleRate: number, channels: number);

// Video data
const video = NapiRuntimeData.video(
  pixels: Uint8Array,
  width: number,
  height: number,
  format: 'yuv420p' | 'rgb24' | 'rgba32' | 'gray8',
  codec?: string,
  frameNumber?: number,
  isKeyframe?: boolean
);

// Text/JSON
const text = NapiRuntimeData.text('hello world');
const json = NapiRuntimeData.json({ key: 'value' });

// Binary/Tensor
const binary = NapiRuntimeData.binary(buffer: Uint8Array);
const tensor = NapiRuntimeData.tensor(data: Uint8Array, shape: number[], dtype: string);
```

### Publisher

```typescript
const publisher = channel.createPublisher();

// Counted compatibility serialization into a native loan
publisher.publish(runtimeData);

// Direct producer writes into native shared memory
const loaned = publisher.loan(bufferSize);
loaned.fill(0);
loaned.set(0, 42);
loaned.send();

publisher.close();
```

`send()` waits for actual native publication. `release()` cancels a loan; either
operation consumes it exactly once and returns its producer credit. Dropping a
loan also releases it, but explicit release avoids waiting for garbage collection.

Use `loaned.write(buffer, offset)` or `publisher.publishRaw(buffer)` to import
existing mutable bytes with one counted copy. SharedArrayBuffer inputs reject.
`loaned.buffer` returns a counted, independent snapshot; modifying it does not
write into the loan, and it remains valid after sending or releasing the loan.

Create an IPC session with `requireZeroCopy: true` to reject nonempty copying
operations. Direct `loan`/`set`/`fill`/`send` remains available. `session.copyReport()`
returns that IPC session's JSON accounting report, separate from core pipeline
accounting. Native receive, readByte, retained slices and toBinary pipeline
handoff also work under strict policy. Synchronous per-byte setters and the 2 ms
receive polling interval are not a throughput or latency acceptance result.

### Subscriber

```typescript
const subscriber = channel.createSubscriber();
const unsubscribe = subscriber.onData((sample, error) => {
  if (error) { console.error(error); return; }
  if (!sample) return;
  try {
    console.log(sample.readByte(0));
    // const owned = sample.toBinary(); // independently retains the native sample
  } finally { sample.release(); }
});
// Later: unsubscribe(); subscriber.close();
```

`receive()` returns null when empty or when retained owners fill the native borrow
budget. `receiveTimeout(ms)` waits up to the deadline; `receiveAsync()` waits for
data. Both reject on close. Pull receives reject while a callback is registered.
Callbacks deliver `(sample, null)` or a terminal `(null, error)`; unsubscribe,
close and environment teardown stop delivery and release queued owners. A thrown
callback exception remains visible to Node's uncaught-exception handling.

`borrowedCount` counts native samples held by wrappers, slices, callback deliveries
and pipeline owners. `maxBorrowedSamples` is the smaller of the native service's
negotiated borrow limit and the requested receive buffer size. It is distinct from
native queue capacity. Retaining all slots stops further borrowing; the bounded
native queue retains its negotiated overflow behavior. `pendingCount` counts only
queued JS callback deliveries. GC releases abandoned owners; explicit `release()`
returns wrapper ownership promptly. Retained slices and `toBinary()` results stay
valid after wrapper release and publisher/subscriber/session close.

`sample.buffer` is an independent, counted mutable snapshot and rejects nonempty
exports in strict sessions. `decode()` explicitly decodes MessagePack under the
receiver session's copy policy. `toRuntimeData()` parses the legacy Python wire
header; `audioMetadata(offset)`, `videoMetadata(offset)` and `fileMetadata(offset)`
parse metadata without snapshotting the native payload. Metadata offsets use the
current sample view. These are different wire formats. `timestampNs` is local
arrival time represented as a JS number, so nanosecond precision is approximate.
Public Buffer-based metadata parsers reject SharedArrayBuffer storage.


## Pipeline Execution

```typescript
import { executePipeline, createStreamSession } from '@matbee/remotemedia-native';

// One-shot pipeline execution
const manifest = {
  version: '1.0',
  nodes: [
    { id: 'input', nodeType: 'AudioInput' },
    { id: 'vad', nodeType: 'SileroVAD', config: { threshold: 0.5 } },
  ],
  connections: [
    { source: 'input', destination: 'vad' },
  ],
};

const output = await executePipeline(JSON.stringify(manifest), inputs);

// Streaming session
const session = await createStreamSession(JSON.stringify(manifest));
await session.sendInput(audioData);
const result = await session.recvOutput();
await session.close();
```

## Type Generation

TypeScript types are auto-generated from the Rust node registry:

```bash
npm run generate-types
```

This creates:
- `node-schemas.ts` - TypeScript definitions for all nodes
- `node-schemas.json` - JSON schema for runtime validation

## Building from Source

```bash
# Install dependencies
npm install

# Build native addon
npm run build

# Build with WebRTC support
npm run build:webrtc

# Run tests
npm test
```

## Platform Support

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux    | x64         | ✅     |
| Linux    | arm64       | ✅     |
| macOS    | x64         | ✅     |
| macOS    | arm64       | ✅     |
| Windows  | x64         | 🚧     |

## License

MIT
