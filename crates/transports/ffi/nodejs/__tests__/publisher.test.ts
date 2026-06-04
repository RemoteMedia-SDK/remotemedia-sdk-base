/**
 * Publisher Tests (T039)
 *
 * Tests Node.js publisher API for sending data to Rust runtime.
 * Note: Cross-language tests with Python are not included here because
 * Node.js communicates with the Rust runtime, which handles IPC to Python.
 */

// Type imports from the native module
interface LoanedSample {
  readonly buffer: Buffer;
  readonly size: number;
  readonly isConsumed: boolean;
  write(data: Buffer, offset?: number): void;
  send(): void;
  release(): void;
}

interface NapiPublisher {
  readonly channelName: string;
  readonly isValid: boolean;
  readonly loanedCount: number;
  readonly maxLoans: number;
  loan(size: number): LoanedSample;
  tryLoan(size: number): LoanedSample | null;
  publish(data: Buffer): void;
  close(): void;
}

interface NapiChannel {
  readonly name: string;
  readonly isOpen: boolean;
  createPublisher(): NapiPublisher;
  createSubscriber(bufferSize?: number): unknown;
  close(): void;
}

interface NapiSession {
  readonly id: string;
  readonly isActive: boolean;
  channel(name: string, config?: unknown): NapiChannel;
  listChannels(): string[];
  close(): void;
}

interface NativeModule {
  createSession(config: { id: string }): NapiSession;
  isNativeLoaded(): boolean;
  getLoadError(): Error | null;
}

// Attempt to load the native module
let publisherNative: NativeModule | null = null;
let publisherLoadError: Error | null = null;

try {
  publisherNative = require('..') as NativeModule;
} catch (e) {
  publisherLoadError = e as Error;
}

// Helper to create test audio payload
function createTestAudioPayload(sampleNum: number, numSamples: number = 480): Buffer {
  const sessionId = Buffer.from('test_session_01');
  const timestampNs = BigInt(Date.now()) * BigInt(1000000);
  const sampleRate = 16000;
  const channels = 1;

  // Calculate total size
  const headerSize = 1 + 2 + sessionId.length + 8 + 4 + 2 + 8;
  const samplesSize = numSamples * 4; // f32 = 4 bytes
  const totalSize = headerSize + samplesSize;

  const buffer = Buffer.alloc(totalSize);
  let offset = 0;

  // Type (1 byte) - Audio = 1
  buffer.writeUInt8(1, offset);
  offset += 1;

  // Session ID length (2 bytes LE)
  buffer.writeUInt16LE(sessionId.length, offset);
  offset += 2;

  // Session ID
  sessionId.copy(buffer, offset);
  offset += sessionId.length;

  // Timestamp (8 bytes LE)
  buffer.writeBigUInt64LE(timestampNs, offset);
  offset += 8;

  // Sample rate (4 bytes LE)
  buffer.writeUInt32LE(sampleRate, offset);
  offset += 4;

  // Channels (2 bytes LE)
  buffer.writeUInt16LE(channels, offset);
  offset += 2;

  // Number of samples (8 bytes LE)
  buffer.writeBigUInt64LE(BigInt(numSamples), offset);
  offset += 8;

  // Generate sine wave samples (440 Hz tone at 50% amplitude)
  for (let i = 0; i < numSamples; i++) {
    const t = (sampleNum * numSamples + i) / sampleRate;
    const sample = Math.sin(2 * Math.PI * 440 * t) * 0.5;
    buffer.writeFloatLE(sample, offset);
    offset += 4;
  }

  return buffer;
}

describe('NapiPublisher', () => {
  beforeAll(() => {
    if (!publisherNative || !publisherNative.isNativeLoaded()) {
      console.warn(
        'Native module not loaded, skipping publisher tests.',
        'Build with: cargo build --features napi --no-default-features'
      );
      if (publisherLoadError) {
        console.warn('Load error:', publisherLoadError.message);
      }
    }
  });

  describe('when native module is loaded', () => {
    let session: NapiSession | null = null;

    beforeEach(() => {
      if (publisherNative?.isNativeLoaded()) {
        const sessionId = `test_pub_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        session = publisherNative.createSession({ id: sessionId });
      }
    });

    afterEach(() => {
      if (session) {
        session.close();
        session = null;
      }
    });

    test('should create publisher with correct properties', () => {
      if (!publisherNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('pub_test');
      const publisher = channel.createPublisher();

      expect(publisher).toBeDefined();
      expect(publisher.channelName).toContain('pub_test');
      expect(publisher.isValid).toBe(true);
      expect(publisher.loanedCount).toBe(0);
      expect(publisher.maxLoans).toBeGreaterThan(0);

      publisher.close();
      channel.close();
    });

    test('should loan buffer with correct size', () => {
      if (!publisherNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('loan_test');
      const publisher = channel.createPublisher();

      const sample = publisher.loan(1024);

      expect(sample).toBeDefined();
      expect(sample.size).toBe(1024);
      expect(sample.isConsumed).toBe(false);

      // Clean up without sending
      sample.release();
      publisher.close();
      channel.close();
    });

    test('should track loaned count', () => {
      if (!publisherNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('loan_count_test', {
        capacity: 4,
      });
      const publisher = channel.createPublisher();

      expect(publisher.loanedCount).toBe(0);

      const sample1 = publisher.loan(100);
      expect(publisher.loanedCount).toBe(1);

      const sample2 = publisher.loan(100);
      expect(publisher.loanedCount).toBe(2);

      // Release samples
      sample1.release();
      sample2.release();

      publisher.close();
      channel.close();
    });

    test('should reject loan exceeding max payload size', () => {
      if (!publisherNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('size_limit_test', {
        maxPayloadSize: 1024,
      });
      const publisher = channel.createPublisher();

      // This should throw or return error
      expect(() => publisher.loan(2048)).toThrow();

      publisher.close();
      channel.close();
    });

    test('should return null from tryLoan when pool exhausted', () => {
      if (!publisherNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('pool_exhaust_test', {
        capacity: 2, // Small pool
      });
      const publisher = channel.createPublisher();

      const sample1 = publisher.tryLoan(100);
      expect(sample1).not.toBeNull();

      const sample2 = publisher.tryLoan(100);
      expect(sample2).not.toBeNull();

      // Pool should now be exhausted
      const sample3 = publisher.tryLoan(100);
      expect(sample3).toBeNull();

      // Cleanup
      sample1?.release();
      sample2?.release();
      publisher.close();
      channel.close();
    });

    test('should become invalid after close', () => {
      if (!publisherNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('close_test');
      const publisher = channel.createPublisher();

      expect(publisher.isValid).toBe(true);
      publisher.close();
      expect(publisher.isValid).toBe(false);

      channel.close();
    });

    test('should throw when publishing on closed publisher', () => {
      if (!publisherNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('closed_pub_test');
      const publisher = channel.createPublisher();
      publisher.close();

      const payload = createTestAudioPayload(0);
      expect(() => publisher.publish(payload)).toThrow();

      channel.close();
    });
  });

  // Note: Cross-language tests with Python removed.
  // Node.js communicates with Rust runtime, which handles IPC to Python.
  // See US2 (Phase 4) for full pipeline chain tests that go through gRPC.
});

describe('LoanedSample', () => {
  let session: NapiSession | null = null;

  beforeEach(() => {
    if (publisherNative?.isNativeLoaded()) {
      const sessionId = `loaned_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      session = publisherNative.createSession({ id: sessionId });
    }
  });

  afterEach(() => {
    if (session) {
      session.close();
      session = null;
    }
  });

  test('should have correct lifecycle (send marks as consumed)', () => {
    if (!publisherNative?.isNativeLoaded() || !session) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const channel = session.channel('lifecycle_send');
    const publisher = channel.createPublisher();

    const sample = publisher.loan(100);
    expect(sample.isConsumed).toBe(false);

    sample.send();
    expect(sample.isConsumed).toBe(true);

    // Double send should throw
    expect(() => sample.send()).toThrow();

    publisher.close();
    channel.close();
  });

  test('should have correct lifecycle (release marks as consumed)', () => {
    if (!publisherNative?.isNativeLoaded() || !session) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const channel = session.channel('lifecycle_release');
    const publisher = channel.createPublisher();

    const sample = publisher.loan(100);
    expect(sample.isConsumed).toBe(false);

    sample.release();
    expect(sample.isConsumed).toBe(true);

    // Can't send after release
    expect(() => sample.send()).toThrow();

    publisher.close();
    channel.close();
  });

  test('should allow writing data to buffer', () => {
    if (!publisherNative?.isNativeLoaded() || !session) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const channel = session.channel('write_test');
    const publisher = channel.createPublisher();

    const sample = publisher.loan(100);
    const data = Buffer.from([1, 2, 3, 4, 5]);

    // Write without offset
    sample.write(data);

    // Write with offset
    sample.write(Buffer.from([10, 11, 12]), 50);

    // Clean up
    sample.release();
    publisher.close();
    channel.close();
  });

  test('should reject write exceeding buffer size', () => {
    if (!publisherNative?.isNativeLoaded() || !session) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const channel = session.channel('write_overflow');
    const publisher = channel.createPublisher();

    const sample = publisher.loan(10);
    const largeData = Buffer.alloc(20);

    expect(() => sample.write(largeData)).toThrow();

    sample.release();
    publisher.close();
    channel.close();
  });

  test('should reject write after send', () => {
    if (!publisherNative?.isNativeLoaded() || !session) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const channel = session.channel('write_after_send');
    const publisher = channel.createPublisher();

    const sample = publisher.loan(100);
    sample.send();

    expect(() => sample.write(Buffer.from([1, 2, 3]))).toThrow();

    publisher.close();
    channel.close();
  });
});
