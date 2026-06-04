/**
 * Session Tests (T054)
 *
 * Tests session management and multi-session isolation.
 * Validates that sessions provide proper namespace isolation for channels.
 */

// Type imports from the native module
interface NapiRuntimeData {
  dataType: number;
  getText(): string;
}

interface NapiRuntimeDataFactory {
  text(text: string): NapiRuntimeData;
  audio(samplesBuffer: Buffer, sampleRate: number, channels: number): NapiRuntimeData;
  binary(data: Buffer): NapiRuntimeData;
}

interface NapiPublisher {
  readonly channelName: string;
  readonly isValid: boolean;
  publish(data: NapiRuntimeData): void;
  close(): void;
}

interface NapiSubscriber {
  readonly channelName: string;
  readonly isValid: boolean;
  receive(): unknown | null;
  onData(callback: (sample: unknown) => void): boolean;
  unsubscribe(): void;
  close(): void;
}

interface NapiChannel {
  readonly name: string;
  readonly isOpen: boolean;
  readonly config: ChannelConfig;
  createPublisher(): NapiPublisher;
  createSubscriber(bufferSize?: number): NapiSubscriber;
  close(): void;
}

interface ChannelConfig {
  capacity?: number;
  maxPayloadSize?: number;
  backpressure?: boolean;
  historySize?: number;
}

interface SessionConfig {
  id: string;
  defaultChannelConfig?: ChannelConfig;
}

interface NapiSession {
  readonly id: string;
  readonly isActive: boolean;
  channel(name: string, config?: ChannelConfig): NapiChannel;
  listChannels(): string[];
  close(): void;
}

interface NativeModule {
  createSession(config: SessionConfig): NapiSession;
  getSession(sessionId: string): NapiSession | null;
  listSessions(): string[];
  isNativeLoaded(): boolean;
  getLoadError(): Error | null;
  NapiRuntimeData: NapiRuntimeDataFactory;
}

// Attempt to load the native module
let native: NativeModule | null = null;
let loadError: Error | null = null;

try {
  native = require('..') as NativeModule;
} catch (e) {
  loadError = e as Error;
}

describe('NapiSession', () => {
  beforeAll(() => {
    if (!native || !native.isNativeLoaded()) {
      console.warn(
        'Native module not loaded, skipping session tests.',
        'Build with: cargo build --features napi --no-default-features'
      );
      if (loadError) {
        console.warn('Load error:', loadError.message);
      }
    }
  });

  describe('session creation', () => {
    test('should create session with valid ID', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const sessionId = `session_${Date.now()}_abc12345`;
      const session = native.createSession({ id: sessionId });

      expect(session).toBeDefined();
      expect(session.id).toBe(sessionId);
      expect(session.isActive).toBe(true);

      session.close();
    });

    test('should reject session ID that is too short', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      expect(() => native!.createSession({ id: 'short' })).toThrow();
    });

    test('should reject session ID with invalid characters', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      expect(() =>
        native!.createSession({ id: 'session:invalid' })
      ).toThrow();

      expect(() =>
        native!.createSession({ id: 'session/invalid' })
      ).toThrow();

      expect(() =>
        native!.createSession({ id: 'session with spaces' })
      ).toThrow();
    });

    test('should reject duplicate session IDs', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const sessionId = `duplicate_${Date.now()}_test123`;
      const session1 = native.createSession({ id: sessionId });

      expect(() => native!.createSession({ id: sessionId })).toThrow();

      session1.close();
    });

    test('should allow reusing session ID after close', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const sessionId = `reuse_${Date.now()}_test1234`;
      const session1 = native.createSession({ id: sessionId });
      session1.close();

      // Should be able to create again with same ID
      const session2 = native.createSession({ id: sessionId });
      expect(session2.id).toBe(sessionId);
      expect(session2.isActive).toBe(true);

      session2.close();
    });
  });

  describe('session lifecycle', () => {
    test('should become inactive after close', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const session = native.createSession({
        id: `lifecycle_${Date.now()}_12345678`,
      });

      expect(session.isActive).toBe(true);
      session.close();
      expect(session.isActive).toBe(false);
    });

    test('should reject channel creation on closed session', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const session = native.createSession({
        id: `closed_${Date.now()}_12345678`,
      });
      session.close();

      expect(() => session.channel('test')).toThrow();
    });
  });

  describe('channel management', () => {
    let session: NapiSession | null = null;

    beforeEach(() => {
      if (native?.isNativeLoaded()) {
        session = native.createSession({
          id: `channel_mgmt_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        });
      }
    });

    afterEach(() => {
      if (session) {
        session.close();
        session = null;
      }
    });

    test('should create channel with session-prefixed name', () => {
      if (!native?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const channel = session.channel('audio');

      expect(channel).toBeDefined();
      expect(channel.name).toContain(session.id);
      expect(channel.name).toContain('audio');
      expect(channel.isOpen).toBe(true);

      channel.close();
    });

    test('should track created channels', () => {
      if (!native?.isNativeLoaded() || !session) {
        console.log('Skipping: native module not loaded');
        return;
      }

      expect(session.listChannels().length).toBe(0);

      const channel1 = session.channel('channel_a');
      expect(session.listChannels()).toContain('channel_a');

      const channel2 = session.channel('channel_b');
      expect(session.listChannels()).toContain('channel_a');
      expect(session.listChannels()).toContain('channel_b');
      expect(session.listChannels().length).toBe(2);

      channel1.close();
      channel2.close();
    });

    test('should apply default channel config', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const sessionWithDefaults = native.createSession({
        id: `defaults_${Date.now()}_12345678`,
        defaultChannelConfig: {
          capacity: 128,
          maxPayloadSize: 2048,
        },
      });

      const channel = sessionWithDefaults.channel('with_defaults');

      // Config should be applied
      expect(channel.config.capacity).toBe(128);
      expect(channel.config.maxPayloadSize).toBe(2048);

      channel.close();
      sessionWithDefaults.close();
    });

    test('should allow overriding default config per channel', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const sessionWithDefaults = native.createSession({
        id: `override_${Date.now()}_12345678`,
        defaultChannelConfig: {
          capacity: 64,
        },
      });

      const channel = sessionWithDefaults.channel('overridden', {
        capacity: 256,
      });

      expect(channel.config.capacity).toBe(256);

      channel.close();
      sessionWithDefaults.close();
    });
  });

  describe('global session registry', () => {
    test('should list active sessions', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const initialSessions = native.listSessions();

      const session1 = native.createSession({
        id: `list_a_${Date.now()}_12345678`,
      });
      const session2 = native.createSession({
        id: `list_b_${Date.now()}_12345678`,
      });

      const activeSessions = native.listSessions();
      expect(activeSessions.length).toBe(initialSessions.length + 2);
      expect(activeSessions).toContain(session1.id);
      expect(activeSessions).toContain(session2.id);

      session1.close();
      session2.close();

      // After close, sessions should be removed
      const finalSessions = native.listSessions();
      expect(finalSessions).not.toContain(session1.id);
      expect(finalSessions).not.toContain(session2.id);
    });

    test('should get session by ID', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const sessionId = `get_test_${Date.now()}_12345678`;
      const session = native.createSession({ id: sessionId });

      const retrieved = native.getSession(sessionId);
      expect(retrieved).not.toBeNull();
      expect(retrieved?.id).toBe(sessionId);

      session.close();

      // After close, should return null
      const afterClose = native.getSession(sessionId);
      expect(afterClose).toBeNull();
    });

    test('should return null for non-existent session', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const result = native.getSession('non_existent_session_id_12345678');
      expect(result).toBeNull();
    });
  });
});

describe('Session Isolation', () => {
  beforeAll(() => {
    if (!native || !native.isNativeLoaded()) {
      console.warn('Native module not loaded, skipping isolation tests.');
    }
  });

  test('should isolate channels between sessions', () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    // Create two sessions
    const session1 = native.createSession({
      id: `iso_a_${Date.now()}_12345678`,
    });
    const session2 = native.createSession({
      id: `iso_b_${Date.now()}_12345678`,
    });

    // Create channels with same name in both sessions
    const channel1 = session1.channel('shared_name');
    const channel2 = session2.channel('shared_name');

    // Channel names should be different (prefixed with session ID)
    expect(channel1.name).not.toBe(channel2.name);
    expect(channel1.name).toContain(session1.id);
    expect(channel2.name).toContain(session2.id);

    channel1.close();
    channel2.close();
    session1.close();
    session2.close();
  });

  test('should not allow cross-session channel access', async () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const session1 = native.createSession({
      id: `cross_a_${Date.now()}_12345678`,
    });
    const session2 = native.createSession({
      id: `cross_b_${Date.now()}_12345678`,
    });

    // Create channel and publisher in session1
    const channel1 = session1.channel('isolated');
    const publisher = channel1.createPublisher();

    // Create subscriber in session2 with same channel name
    const channel2 = session2.channel('isolated');
    const subscriber = channel2.createSubscriber();

    // Track received samples
    let receivedCount = 0;
    subscriber.onData(() => {
      receivedCount++;
    });

    // Publish from session1
    const testData = native.NapiRuntimeData.text('test message');
    publisher.publish(testData);

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Subscriber in session2 should NOT receive the data (different channels)
    expect(receivedCount).toBe(0);

    subscriber.unsubscribe();
    subscriber.close();
    publisher.close();
    channel1.close();
    channel2.close();
    session1.close();
    session2.close();
  });

  // Note: Same-session IPC communication test removed.
  // Node.js communicates with Rust runtime, which handles IPC to Python.
  // See US2 (Phase 4) for full pipeline chain tests that go through gRPC.

  test('should clean up all channels on session close', () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const session = native.createSession({
      id: `cleanup_${Date.now()}_12345678`,
    });

    // Create multiple channels
    const channel1 = session.channel('cleanup_a');
    const channel2 = session.channel('cleanup_b');
    const channel3 = session.channel('cleanup_c');

    expect(channel1.isOpen).toBe(true);
    expect(channel2.isOpen).toBe(true);
    expect(channel3.isOpen).toBe(true);

    // Close session
    session.close();

    // All channels should be closed
    expect(channel1.isOpen).toBe(false);
    expect(channel2.isOpen).toBe(false);
    expect(channel3.isOpen).toBe(false);
  });
});

describe('Multiple Sessions Concurrent', () => {
  test('should handle multiple concurrent sessions', () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const sessions: NapiSession[] = [];
    const numSessions = 5;

    // Create multiple sessions
    for (let i = 0; i < numSessions; i++) {
      const session = native.createSession({
        id: `concurrent_${i}_${Date.now()}_12345678`,
      });
      sessions.push(session);
    }

    // All sessions should be active
    for (const session of sessions) {
      expect(session.isActive).toBe(true);
    }

    // Create channels in each session
    const channels: NapiChannel[] = [];
    for (const session of sessions) {
      const channel = session.channel('test_channel');
      channels.push(channel);
    }

    // All channels should be open and have unique names
    const channelNames = new Set<string>();
    for (const channel of channels) {
      expect(channel.isOpen).toBe(true);
      expect(channelNames.has(channel.name)).toBe(false);
      channelNames.add(channel.name);
    }

    // Clean up
    for (const channel of channels) {
      channel.close();
    }
    for (const session of sessions) {
      session.close();
    }
  });
});
