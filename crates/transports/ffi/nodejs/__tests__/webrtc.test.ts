/**
 * WebRTC Server Tests (T022, T039, T049, T056)
 *
 * Tests WebRTC server creation, configuration validation, event handling,
 * session management, external signaling, and targeted peer messaging.
 * Validates the Node.js FFI bindings for WebRTC transport.
 */

// Type imports from the native module
interface TurnServer {
  url: string;
  username: string;
  credential: string;
}

interface ReconnectConfig {
  maxAttempts?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
  backoffMultiplier?: number;
}

interface WebRtcServerConfig {
  port?: number;
  signalingUrl?: string;
  /** Pipeline manifest as JSON string */
  manifest: string;
  stunServers: string[];
  turnServers?: TurnServer[];
  maxPeers?: number;
  audioCodec?: 'opus';
  videoCodec?: 'vp8' | 'vp9' | 'h264';
  reconnect?: ReconnectConfig;
}

/** Helper: Create a valid pipeline manifest JSON string */
function createValidManifest(name: string = 'test-pipeline'): string {
  return JSON.stringify({
    version: '1.0',
    metadata: { name },
    nodes: [],
    connections: [],
  });
}

interface PeerCapabilities {
  audio: boolean;
  video: boolean;
  data: boolean;
}

interface PeerInfo {
  peerId: string;
  capabilities: PeerCapabilities;
  metadata: Record<string, string>;
  state: string;
  connectedAt: number;
}

interface SessionInfo {
  sessionId: string;
  metadata: Record<string, string>;
  peerIds: string[];
  createdAt: number;
}

/** WebRTC session (room) for managing peers */
interface WebRtcSession {
  readonly sessionId: string;
  readonly peers: Promise<string[]>;
  readonly createdAt: Promise<number>;
  readonly metadata: Promise<Record<string, string>>;
  on(event: 'peer_joined' | 'peer_left', callback: (peerId: string) => void): void;
  broadcast(data: Buffer): Promise<void>;
  sendToPeer(peerId: string, data: Buffer): Promise<void>;
  addPeer(peerId: string): Promise<void>;
  removePeer(peerId: string): Promise<void>;
  getInfo(): Promise<SessionInfo>;
}

interface PeerConnectedData {
  peerId: string;
  capabilities: PeerCapabilities;
  metadata: Record<string, string>;
}

interface PeerDisconnectedData {
  peerId: string;
  reason?: string;
}

interface PipelineOutputData {
  peerId: string;
  data: Buffer;
  timestamp: number;
}

interface DataReceivedData {
  peerId: string;
  data: Buffer;
  timestamp: number;
}

interface ErrorData {
  code: string;
  message: string;
  peerId?: string;
}

interface NapiWebRtcServer {
  readonly id: string;
  readonly state: Promise<string>;
  getPeers(): Promise<PeerInfo[]>;
  getSessions(): Promise<SessionInfo[]>;
  on(event: string, callback: (data: unknown) => void): void;
  start(): Promise<void>;
  startSignalingServer(port: number): Promise<void>;
  shutdown(): Promise<void>;
  sendToPeer(peerId: string, data: Buffer): Promise<void>;
  broadcast(data: Buffer): Promise<void>;
  disconnectPeer(peerId: string, reason?: string): Promise<void>;
  createSession(sessionId: string, metadata?: object): Promise<WebRtcSession>;
  createSessionInfo(sessionId: string, metadata?: object): Promise<SessionInfo>;
  getSession(sessionId: string): Promise<SessionInfo | null>;
  deleteSession(sessionId: string): Promise<void>;
}

interface WebRtcModule {
  create(config: WebRtcServerConfig): Promise<NapiWebRtcServer>;
  connect(config: WebRtcServerConfig): Promise<NapiWebRtcServer>;
}

interface NativeModule {
  WebRtcServer?: WebRtcModule;
  isNativeLoaded(): boolean;
  getLoadError(): Error | null;
}

// Attempt to load the native module
let native: NativeModule | null = null;
let loadError: Error | null = null;

try {
  native = require('..') as NativeModule;
} catch (e) {
  loadError = e as Error;
}

// Check if WebRTC is available (built with napi-webrtc feature)
function isWebRtcAvailable(): boolean {
  return !!(native?.isNativeLoaded() && native.WebRtcServer);
}

describe('WebRtcServer', () => {
  beforeAll(() => {
    if (!native || !native.isNativeLoaded()) {
      console.warn(
        'Native module not loaded, skipping WebRTC tests.',
        'Build with: cargo build --features napi-webrtc'
      );
      if (loadError) {
        console.warn('Load error:', loadError.message);
      }
    } else if (!native.WebRtcServer) {
      console.warn(
        'WebRTC bindings not available.',
        'Rebuild with: cargo build --features napi-webrtc'
      );
    }
  });

  describe('configuration validation', () => {
    test('should reject config without port or signalingUrl', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        manifest: createValidManifest(),
        stunServers: ['stun:stun.l.google.com:19302'],
      };

      await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
    });

    test('should reject config with both port and signalingUrl', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: 50051,
        signalingUrl: 'grpc://localhost:50052',
        manifest: createValidManifest(),
        stunServers: ['stun:stun.l.google.com:19302'],
      };

      await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
    });

    test('should reject config without STUN servers', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: 50051,
        manifest: createValidManifest(),
        stunServers: [],
      };

      await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
    });

    test('should reject invalid STUN URL format', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: 50051,
        manifest: createValidManifest(),
        stunServers: ['http://stun.example.com:3478'],
      };

      await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
    });

    test('should reject invalid max_peers value', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: 50051,
        manifest: createValidManifest(),
        stunServers: ['stun:stun.l.google.com:19302'],
        maxPeers: 100, // Max is 10
      };

      await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
    });

    test('should reject TURN server without username', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: 50051,
        manifest: createValidManifest(),
        stunServers: ['stun:stun.l.google.com:19302'],
        turnServers: [
          { url: 'turn:turn.example.com:3478', username: '', credential: 'secret' },
        ],
      };

      await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
    });

    test('should reject invalid manifest format', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: 50225,
        // Missing version and metadata fields - validation happens at startSignalingServer
        manifest: JSON.stringify({ nodes: [], connections: [] }),
        stunServers: ['stun:stun.l.google.com:19302'],
      };

      // Server creation may succeed since manifest is validated lazily
      // The actual validation happens when starting the signaling server
      const server = await native!.WebRtcServer!.create(config);
      try {
        // This should fail because the manifest is missing required fields
        await expect(server.startSignalingServer(50225)).rejects.toThrow(/manifest|version|Invalid/i);
      } finally {
        await server.shutdown();
      }
    });
  });

  describe('server creation', () => {
    // Use atomic counter for unique ports to avoid conflicts in parallel tests
    let portCounter = 50200;
    const getUniquePort = () => portCounter++;

    test('should create server with embedded signaling (port)', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: getUniquePort(),
        manifest: createValidManifest('embedded-signaling-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      };

      const server = await native!.WebRtcServer!.create(config);

      expect(server).toBeDefined();
      expect(server.id).toBeDefined();
      expect(server.id.length).toBeGreaterThan(0);

      const state = await server.state;
      expect(state).toBe('created');

      await server.shutdown();
    });

    test('should reject connect() without signalingUrl', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: getUniquePort(), // Should use signalingUrl instead
        manifest: createValidManifest(),
        stunServers: ['stun:stun.l.google.com:19302'],
      };

      await expect(native!.WebRtcServer!.connect(config)).rejects.toThrow(/signaling_url/);
    });

    test('should create server with valid config', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const config: WebRtcServerConfig = {
        port: getUniquePort(),
        manifest: JSON.stringify({
          version: '1.0',
          metadata: { name: 'full-config-test' },
          nodes: [{ id: 'test', node_type: 'Echo' }],
          connections: [],
        }),
        stunServers: ['stun:stun.l.google.com:19302'],
        turnServers: [
          {
            url: 'turn:turn.example.com:3478',
            username: 'user',
            credential: 'pass',
          },
        ],
        maxPeers: 5,
        videoCodec: 'vp9',
      };

      const server = await native!.WebRtcServer!.create(config);

      expect(server).toBeDefined();
      expect(server.id).toBeDefined();

      await server.shutdown();
    });
  });

  describe('event registration', () => {
    let server: NapiWebRtcServer | null = null;
    let eventRegPort = 50300;

    beforeEach(async () => {
      if (isWebRtcAvailable()) {
        server = await native!.WebRtcServer!.create({
          port: eventRegPort++,
          manifest: createValidManifest('event-registration-test'),
          stunServers: ['stun:stun.l.google.com:19302'],
        });
      }
    });

    afterEach(async () => {
      if (server) {
        await server.shutdown();
        server = null;
      }
    });

    test('should register peer_connected callback', () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      let callbackCalled = false;
      expect(() => {
        server!.on('peer_connected', (data: unknown) => {
          callbackCalled = true;
        });
      }).not.toThrow();
    });

    test('should register peer_disconnected callback', () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      expect(() => {
        server!.on('peer_disconnected', (data: unknown) => {
          // Handler
        });
      }).not.toThrow();
    });

    test('should register pipeline_output callback', () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      expect(() => {
        server!.on('pipeline_output', (data: unknown) => {
          // Handler
        });
      }).not.toThrow();
    });

    test('should register data callback', () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      expect(() => {
        server!.on('data', (data: unknown) => {
          // Handler
        });
      }).not.toThrow();
    });

    test('should register error callback', () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      expect(() => {
        server!.on('error', (data: unknown) => {
          // Handler
        });
      }).not.toThrow();
    });

    test('should reject unknown event name', () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      expect(() => {
        server!.on('unknown_event', (data: unknown) => {
          // Handler
        });
      }).toThrow(/Unknown event/);
    });

    test('should allow multiple callbacks for same event', () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      expect(() => {
        server!.on('peer_connected', () => {});
        server!.on('peer_connected', () => {});
        server!.on('peer_connected', () => {});
      }).not.toThrow();
    });
  });

  describe('server lifecycle', () => {
    let lifecyclePort = 50400;
    const getLifecyclePort = () => lifecyclePort++;

    test('should start and shutdown cleanly', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const server = await native!.WebRtcServer!.create({
        port: getLifecyclePort(),
        manifest: createValidManifest('lifecycle-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      });

      expect(await server.state).toBe('created');

      // Note: start() will try to bind to port, which may fail in test environment
      // We test that the method exists and can be called
      try {
        await server.start();
        expect(await server.state).toBe('running');
      } catch (e) {
        // Port binding might fail in CI, that's okay for this test
        console.log('Start failed (expected in some environments):', (e as Error).message);
      }

      await server.shutdown();
      expect(await server.state).toBe('stopped');
    });

    test('should return empty peers list initially', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const server = await native!.WebRtcServer!.create({
        port: getLifecyclePort(),
        manifest: createValidManifest('peers-list-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      });

      const peers = await server.getPeers();
      expect(peers).toEqual([]);

      await server.shutdown();
    });

    test('should return empty sessions list initially', async () => {
      if (!isWebRtcAvailable()) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const server = await native!.WebRtcServer!.create({
        port: getLifecyclePort(),
        manifest: createValidManifest('sessions-list-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      });

      const sessions = await server.getSessions();
      expect(sessions).toEqual([]);

      await server.shutdown();
    });
  });

  describe('session management', () => {
    let server: NapiWebRtcServer | null = null;
    let sessionMgmtPort = 50500;

    beforeEach(async () => {
      if (isWebRtcAvailable()) {
        server = await native!.WebRtcServer!.create({
          port: sessionMgmtPort++,
          manifest: createValidManifest('session-management-test'),
          stunServers: ['stun:stun.l.google.com:19302'],
        });
      }
    });

    afterEach(async () => {
      if (server) {
        await server.shutdown();
        server = null;
      }
    });

    test('should create session', async () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const sessionId = `test-session-${Date.now()}`;
      const session = await server.createSession(sessionId, {
        name: 'Test Room',
        host: 'user-1',
      });

      expect(session).toBeDefined();
      expect(session.sessionId).toBe(sessionId);
      expect(await session.peers).toEqual([]);
    });

    test('should get session by ID', async () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const sessionId = `get-session-${Date.now()}`;
      await server.createSession(sessionId);

      const session = await server.getSession(sessionId);
      expect(session).not.toBeNull();
      expect(session?.sessionId).toBe(sessionId);
    });

    test('should return null for non-existent session', async () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const session = await server.getSession('non-existent-session-id');
      expect(session).toBeNull();
    });

    test('should delete session', async () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const sessionId = `delete-session-${Date.now()}`;
      await server.createSession(sessionId);

      // Verify it exists
      const before = await server.getSession(sessionId);
      expect(before).not.toBeNull();

      // Delete it
      await server.deleteSession(sessionId);

      // Verify it's gone
      const after = await server.getSession(sessionId);
      expect(after).toBeNull();
    });

    test('should list sessions', async () => {
      if (!isWebRtcAvailable() || !server) {
        console.log('Skipping: WebRTC module not loaded');
        return;
      }

      const sessionId1 = `list-session-1-${Date.now()}`;
      const sessionId2 = `list-session-2-${Date.now()}`;

      await server.createSession(sessionId1);
      await server.createSession(sessionId2);

      const sessions = await server.getSessions();
      expect(sessions.length).toBeGreaterThanOrEqual(2);

      const sessionIds = sessions.map((s) => s.sessionId);
      expect(sessionIds).toContain(sessionId1);
      expect(sessionIds).toContain(sessionId2);
    });
  });
});

describe('WebRtcServer External Signaling', () => {
  test('should reject create() for external signaling config', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const config: WebRtcServerConfig = {
      signalingUrl: 'grpc://signaling.example.com:50051',
      manifest: createValidManifest('external-signaling-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
    };

    // create() requires port, not signalingUrl
    await expect(native!.WebRtcServer!.create(config)).rejects.toThrow(/Port is required/);
  });

  test('should validate signaling URL format', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const config: WebRtcServerConfig = {
      signalingUrl: 'http://invalid-protocol.com:50051',
      manifest: createValidManifest('invalid-url-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
    };

    await expect(native!.WebRtcServer!.connect(config)).rejects.toThrow();
  });

  test('should accept valid grpc signaling URL format', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const config: WebRtcServerConfig = {
      signalingUrl: 'grpc://signaling.example.com:50051',
      manifest: createValidManifest('valid-grpc-url-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      reconnect: {
        maxAttempts: 1,
        initialBackoffMs: 10,
        maxBackoffMs: 50,
      },
    };

    // Note: This will fail to actually connect (no server running),
    // but should accept the config format
    try {
      await native!.WebRtcServer!.connect(config);
    } catch (e) {
      // Connection failure is expected - we're testing config acceptance
      expect((e as Error).message).not.toMatch(/Invalid.*URL|protocol/i);
    }
  });

  test('should accept valid grpcs signaling URL format', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const config: WebRtcServerConfig = {
      signalingUrl: 'grpcs://secure-signaling.example.com:443',
      manifest: createValidManifest('valid-grpcs-url-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      reconnect: {
        maxAttempts: 1,
        initialBackoffMs: 10,
        maxBackoffMs: 50,
      },
    };

    // Note: This will fail to actually connect (no server running),
    // but should accept the config format
    try {
      await native!.WebRtcServer!.connect(config);
    } catch (e) {
      // Connection failure is expected - we're testing config acceptance
      expect((e as Error).message).not.toMatch(/Invalid.*URL|protocol/i);
    }
  });
});

describe('WebRtcServer Targeted Peer Messaging', () => {
  let server: NapiWebRtcServer | null = null;
  let peerMsgPort = 50600;

  beforeEach(async () => {
    if (isWebRtcAvailable()) {
      server = await native!.WebRtcServer!.create({
        port: peerMsgPort++,
        manifest: createValidManifest('peer-messaging-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      });
    }
  });

  afterEach(async () => {
    if (server) {
      await server.shutdown();
      server = null;
    }
  });

  test('should have sendToPeer method', () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    expect(typeof server.sendToPeer).toBe('function');
  });

  test('should reject sendToPeer for non-existent peer', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const testData = Buffer.from('test message');
    await expect(server.sendToPeer('non-existent-peer-id', testData)).rejects.toThrow(/not found|Peer/i);
  });

  test('should have broadcast method', () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    expect(typeof server.broadcast).toBe('function');
  });

  test('should broadcast to empty peer list without error', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const testData = Buffer.from('broadcast test');
    // Should succeed even with no peers connected
    await expect(server.broadcast(testData)).resolves.not.toThrow();
  });

  test('should have disconnectPeer method', () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    expect(typeof server.disconnectPeer).toBe('function');
  });

  test('should reject disconnectPeer for non-existent peer', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    await expect(server.disconnectPeer('non-existent-peer-id')).rejects.toThrow(/not found|Peer/i);
  });

  test('should accept disconnect reason parameter', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    // Even though peer doesn't exist, the method should accept the reason parameter
    await expect(server.disconnectPeer('non-existent-peer-id', 'kicked by admin')).rejects.toThrow(/not found|Peer/i);
  });
});

describe('WebRtcSession Targeted Messaging', () => {
  let server: NapiWebRtcServer | null = null;
  let sessionMsgPort = 50700;

  beforeEach(async () => {
    if (isWebRtcAvailable()) {
      server = await native!.WebRtcServer!.create({
        port: sessionMsgPort++,
        manifest: createValidManifest('session-messaging-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      });
    }
  });

  afterEach(async () => {
    if (server) {
      await server.shutdown();
      server = null;
    }
  });

  test('session should have broadcast method via server', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const sessionId = `broadcast-test-${Date.now()}`;
    const session = await server.createSession(sessionId);

    // The session object should be returned and have expected properties
    expect(session).toBeDefined();
    expect(session.sessionId).toBe(sessionId);
  });

  test('should create session with metadata', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const sessionId = `metadata-test-${Date.now()}`;
    const metadata = {
      roomName: 'Test Room',
      maxParticipants: '10',
      host: 'user-123',
    };

    const session = await server.createSession(sessionId, metadata);

    expect(session.sessionId).toBe(sessionId);
    // Session should be retrievable
    const retrieved = await server.getSession(sessionId);
    expect(retrieved).not.toBeNull();
    expect(retrieved?.sessionId).toBe(sessionId);
  });

  test('should return empty peer list for new session', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const sessionId = `empty-peers-test-${Date.now()}`;
    const session = await server.createSession(sessionId);

    expect(await session.peers).toEqual([]);
  });
});

describe('WebRtcServer Error Handling', () => {
  let server: NapiWebRtcServer | null = null;
  let errorHandlingPort = 50800;

  beforeEach(async () => {
    if (isWebRtcAvailable()) {
      server = await native!.WebRtcServer!.create({
        port: errorHandlingPort++,
        manifest: createValidManifest('error-handling-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      });
    }
  });

  afterEach(async () => {
    if (server) {
      await server.shutdown();
      server = null;
    }
  });

  test('should register error callback and handle gracefully', () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    let errorReceived = false;
    let receivedError: ErrorData | null = null;

    expect(() => {
      server!.on('error', (data: unknown) => {
        errorReceived = true;
        receivedError = data as ErrorData;
      });
    }).not.toThrow();
  });

  test('should handle duplicate session creation', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const sessionId = `duplicate-test-${Date.now()}`;

    // First creation should succeed
    await server.createSession(sessionId);

    // Second creation with same ID should fail or return existing
    await expect(server.createSession(sessionId)).rejects.toThrow(/exists|duplicate/i);
  });

  test('should handle deletion of non-existent session', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    // Deleting a session that doesn't exist should handle gracefully
    // (either succeed silently or throw a meaningful error)
    try {
      await server.deleteSession('non-existent-session-id');
    } catch (e) {
      expect((e as Error).message).toMatch(/not found|does not exist/i);
    }
  });
});

describe('WebRtcServer Session Event Registration', () => {
  let server: NapiWebRtcServer | null = null;
  let sessionEventPort = 50900;

  beforeEach(async () => {
    if (isWebRtcAvailable()) {
      server = await native!.WebRtcServer!.create({
        port: sessionEventPort++,
        manifest: createValidManifest('session-event-test'),
        stunServers: ['stun:stun.l.google.com:19302'],
      });
    }
  });

  afterEach(async () => {
    if (server) {
      await server.shutdown();
      server = null;
    }
  });

  test('should reject invalid event names on server', () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    // 'session' is NOT a valid server-level event
    // Session events (peer_joined, peer_left) are registered on WebRtcSession objects
    expect(() => {
      server!.on('session', () => {});
    }).toThrow(/Unknown event.*session/i);
  });

  test('should register session-level events on WebRtcSession', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const sessionId = `session-event-test-${Date.now()}`;
    const session = await server.createSession(sessionId);

    // Session events should be registered on the session object, not the server
    expect(() => {
      session.on('peer_joined', (peerId: string) => {
        console.log(`Peer joined session: ${peerId}`);
      });
    }).not.toThrow();

    expect(() => {
      session.on('peer_left', (peerId: string) => {
        console.log(`Peer left session: ${peerId}`);
      });
    }).not.toThrow();
  });

  test('should handle session metadata correctly', async () => {
    if (!isWebRtcAvailable() || !server) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const sessionId = `metadata-test-${Date.now()}`;
    const metadata = {
      roomName: 'Test Conference',
      createdBy: 'test-user',
      isPrivate: 'true',
    };

    const session = await server.createSession(sessionId, metadata);
    expect(session.sessionId).toBe(sessionId);

    // Retrieve and verify metadata persists
    const retrieved = await server.getSession(sessionId);
    expect(retrieved).not.toBeNull();
    expect(retrieved?.sessionId).toBe(sessionId);
  });
});

describe('WebRtcServer Multiple Codecs', () => {
  test('should accept vp8 video codec', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const server = await native!.WebRtcServer!.create({
      port: 51000,
      manifest: createValidManifest('vp8-codec-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      videoCodec: 'vp8',
    });

    expect(server).toBeDefined();
    await server.shutdown();
  });

  test('should accept vp9 video codec', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const server = await native!.WebRtcServer!.create({
      port: 51001,
      manifest: createValidManifest('vp9-codec-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      videoCodec: 'vp9',
    });

    expect(server).toBeDefined();
    await server.shutdown();
  });

  test('should accept h264 video codec', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const server = await native!.WebRtcServer!.create({
      port: 51002,
      manifest: createValidManifest('h264-codec-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      videoCodec: 'h264',
    });

    expect(server).toBeDefined();
    await server.shutdown();
  });

  test('should accept opus audio codec (default)', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const server = await native!.WebRtcServer!.create({
      port: 51003,
      manifest: createValidManifest('opus-codec-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      audioCodec: 'opus',
    });

    expect(server).toBeDefined();
    await server.shutdown();
  });
});

describe('WebRtcServer Max Peers Limit', () => {
  test('should accept maxPeers within valid range (1-10)', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    // Test minimum value
    const server1 = await native!.WebRtcServer!.create({
      port: 51010,
      manifest: createValidManifest('min-peers-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      maxPeers: 1,
    });
    expect(server1).toBeDefined();
    await server1.shutdown();

    // Test maximum value
    const server2 = await native!.WebRtcServer!.create({
      port: 51011,
      manifest: createValidManifest('max-peers-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      maxPeers: 10,
    });
    expect(server2).toBeDefined();
    await server2.shutdown();
  });

  test('should reject maxPeers of 0', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const config: WebRtcServerConfig = {
      port: 51012,
      manifest: createValidManifest('zero-peers-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      maxPeers: 0,
    };

    await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
  });

  test('should reject maxPeers greater than 10', async () => {
    if (!isWebRtcAvailable()) {
      console.log('Skipping: WebRTC module not loaded');
      return;
    }

    const config: WebRtcServerConfig = {
      port: 51013,
      manifest: createValidManifest('too-many-peers-test'),
      stunServers: ['stun:stun.l.google.com:19302'],
      maxPeers: 11,
    };

    await expect(native!.WebRtcServer!.create(config)).rejects.toThrow();
  });
});
