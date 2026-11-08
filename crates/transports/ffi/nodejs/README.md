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
import {
  createSession,
  NapiRuntimeData,
  isNativeLoaded,
} from '@matbee/remotemedia-native';

// Check if native bindings loaded successfully
if (!isNativeLoaded()) {
  console.error('Native bindings failed to load');
  process.exit(1);
}

// Create an IPC session for zero-copy communication
const session = createSession({
  id: 'my_session',
  default_channel_config: {
    capacity: 64,
    max_payload_size: 1_048_576, // 1MB
  },
});

// Create a channel for audio data
const channel = session.channel('audio_data');

// Publisher side
const publisher = channel.createPublisher();
const audioData = NapiRuntimeData.audio(
  new Float32Array(16000), // 1 second of silence at 16kHz
  16000, // sample rate
  1      // channels
);
publisher.publish(audioData);

// Subscriber side
const subscriber = channel.createSubscriber();
const unsubscribe = subscriber.onData((sample) => {
  try {
    const buffer = sample.buffer;
    // Process audio data...
  } finally {
    sample.release(); // IMPORTANT: Return sample to pool
  }
});

// Cleanup
unsubscribe();
subscriber.close();
publisher.close();
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
accounting. Strict subscriber creation currently rejects because receive-side
ownership migration is incomplete. The compatibility subscriber below still
copies its payload. Synchronous per-byte setters are not a throughput benchmark.

### Subscriber

```typescript
const subscriber = channel.createSubscriber();

// Register callback
const unsubscribe = subscriber.onData((sample) => {
  const buffer = sample.buffer;
  const timestamp = sample.timestampNs;
  
  // Process data...
  
  sample.release(); // Required!
});

// Cleanup
unsubscribe();
subscriber.close();
```

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
