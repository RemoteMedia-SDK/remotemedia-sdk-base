/**
 * Subscriber Tests (T023)
 *
 * Tests Node.js subscriber API for receiving data from Rust runtime.
 * Note: Cross-language tests with Python are not included here because
 * Node.js communicates with the Rust runtime, which handles IPC to Python.
 */

// Type imports from the native module
interface ReceivedSample {
  readonly buffer: Buffer;
  readonly size: number;
  readonly isReleased: boolean;
  readonly timestampNs: bigint;
  release(): void;
  toRuntimeData(): unknown;
}

interface NapiSubscriber {
  readonly channelName: string;
  readonly isValid: boolean;
  readonly pendingCount: number;
  readonly bufferSize: number;
  receive(): ReceivedSample | null;
  receiveTimeout(timeoutMs: number): Promise<ReceivedSample | null>;
  receiveAsync(): Promise<ReceivedSample>;
  onData(callback: (sample: ReceivedSample) => void): boolean;
  unsubscribe(): void;
  close(): void;
}

interface NapiChannel {
  readonly name: string;
  readonly isOpen: boolean;
  createPublisher(): unknown;
  createSubscriber(bufferSize?: number): NapiSubscriber;
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
let subscriberNative: NativeModule | null = null;
let subscriberLoadError: Error | null = null;

try {
  subscriberNative = require('..') as NativeModule;
} catch (e) {
  subscriberLoadError = e as Error;
}

describe('NapiSubscriber', () => {
  // Skip all tests if native module failed to load
  beforeAll(() => {
    if (!subscriberNative || !subscriberNative.isNativeLoaded()) {
      console.warn(
        'Native module not loaded, skipping subscriber tests.',
        'Build with: cargo build --features napi --no-default-features'
      );
      if (subscriberLoadError) {
        console.warn('Load error:', subscriberLoadError.message);
      }
    }
  });

  describe('when native module is loaded', () => {
    let session: NapiSession | null = null;

    beforeEach(() => {
      if (subscriberNative?.isNativeLoaded()) {
        const sessionId = `test_sub_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        session = subscriberNative.createSession({ id: sessionId });
      }
    });

    afterEach(() => {
      if (session) {
        session.close();
        session = null;
      }
    });

    test('should create subscriber with correct properties', () => {
      if (!subscriberNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('audio_test');
      const subscriber = channel.createSubscriber(32);

      expect(subscriber).toBeDefined();
      expect(subscriber.channelName).toContain('audio_test');
      expect(subscriber.isValid).toBe(true);
      expect(subscriber.bufferSize).toBe(32);
      expect(subscriber.pendingCount).toBe(0);

      subscriber.close();
      channel.close();
    });

    test('should return null when no samples available (non-blocking receive)', () => {
      if (!subscriberNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('empty_channel');
      const subscriber = channel.createSubscriber();

      // Non-blocking receive should return null immediately
      const sample = subscriber.receive();
      expect(sample).toBeNull();

      subscriber.close();
      channel.close();
    });

    test('should become invalid after close', () => {
      if (!subscriberNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('close_test');
      const subscriber = channel.createSubscriber();

      expect(subscriber.isValid).toBe(true);
      subscriber.close();
      expect(subscriber.isValid).toBe(false);

      channel.close();
    });

    test('should throw when receiving on closed subscriber', () => {
      if (!subscriberNative?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('closed_receive_test');
      const subscriber = channel.createSubscriber();
      subscriber.close();

      expect(() => subscriber.receive()).toThrow();

      channel.close();
    });
  });

  // Note: Cross-language tests with Python removed.
  // Node.js communicates with Rust runtime, which handles IPC to Python.
  // See US2 (Phase 4) for full pipeline chain tests that go through gRPC.
});
