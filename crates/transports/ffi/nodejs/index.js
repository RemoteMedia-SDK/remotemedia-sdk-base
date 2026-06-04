// @matbee/remotemedia-native - Node.js bindings for RemoteMedia zero-copy IPC
//
// This module provides zero-copy IPC between Node.js, Python, and Rust
// via iceoryx2 shared memory.

const { existsSync, readFileSync } = require('fs');
const { join } = require('path');

const { platform, arch } = process;

let nativeBinding = null;
let loadError = null;

// Platform-specific binary name mapping
function getPlatformTriple() {
  const archMap = {
    'x64': 'x86_64',
    'arm64': 'aarch64',
  };

  const platformMap = {
    'darwin': 'apple-darwin',
    'linux': 'unknown-linux-gnu',
    'win32': 'pc-windows-msvc',
  };

  const mappedArch = archMap[arch];
  const mappedPlatform = platformMap[platform];

  if (!mappedArch || !mappedPlatform) {
    return null;
  }

  return `${mappedArch}-${mappedPlatform}`;
}

// Try to load the native binding
function loadNativeBinding() {
  const triple = getPlatformTriple();

  if (!triple) {
    throw new Error(
      `Unsupported platform: ${platform}-${arch}. ` +
      `Supported platforms: darwin-x64, darwin-arm64, linux-x64, linux-arm64`
    );
  }

  // Try loading from various locations
  const candidates = [
    // Local development build (with .node extension renamed from .so)
    join(__dirname, '..', '..', '..', 'target', 'release', 'remotemedia_native.node'),
    join(__dirname, '..', '..', '..', 'target', 'debug', 'remotemedia_native.node'),
    // Original .so file (Linux)
    join(__dirname, '..', '..', '..', 'target', 'release', 'libremotemedia_ffi.so'),
    join(__dirname, '..', '..', '..', 'target', 'debug', 'libremotemedia_ffi.so'),
    // Platform-specific builds (for npm distribution)
    join(__dirname, `remotemedia-native.${triple}.node`),
    join(__dirname, 'remotemedia-native.node'),
    // Fallback to generic name
    join(__dirname, 'index.node'),
  ];

  for (const candidate of candidates) {
    try {
      if (existsSync(candidate)) {
        return require(candidate);
      }
    } catch (e) {
      // Continue to next candidate
    }
  }

  // Try requiring without path (for globally installed)
  try {
    return require(`@matbee/remotemedia-native-${triple}`);
  } catch (e) {
    // Ignore
  }

  throw new Error(
    `Failed to load native binding for ${platform}-${arch}. ` +
    `Make sure you have built the native addon with 'npm run build'. ` +
    `Tried: ${candidates.join(', ')}`
  );
}

try {
  nativeBinding = loadNativeBinding();
} catch (e) {
  loadError = e;
}

// Export all bindings
if (nativeBinding) {
  module.exports = nativeBinding;
} else {
  // Create stub exports that throw helpful errors
  const createStub = (name) => () => {
    throw loadError || new Error(`Native binding not loaded: ${name}`);
  };

  module.exports = {
    // Session management
    createSession: createStub('createSession'),
    getSession: createStub('getSession'),
    listSessions: createStub('listSessions'),

    // Node management
    createIpcNode: createStub('createIpcNode'),

    // Classes (will throw on instantiation)
    NapiSession: class { constructor() { createStub('NapiSession')(); } },
    NapiChannel: class { constructor() { createStub('NapiChannel')(); } },
    NapiPublisher: class { constructor() { createStub('NapiPublisher')(); } },
    NapiSubscriber: class { constructor() { createStub('NapiSubscriber')(); } },
    ReceivedSample: class { constructor() { createStub('ReceivedSample')(); } },
    LoanedSample: class { constructor() { createStub('LoanedSample')(); } },
    IpcNode: class { constructor() { createStub('IpcNode')(); } },

    // RuntimeData helpers
    parseRuntimeDataHeader: createStub('parseRuntimeDataHeader'),

    // Error for debugging
    _loadError: loadError,
  };
}

// Export type helpers
module.exports.isNativeLoaded = () => nativeBinding !== null;
module.exports.getLoadError = () => loadError;

// Export proto-utils (browser/Node.js compatible)
const protoUtils = require('./proto-utils');
module.exports.protoUtils = protoUtils;
module.exports.encodeTextData = protoUtils.encodeTextData;
module.exports.encodeJsonData = protoUtils.encodeJsonData;
module.exports.decodeDataBuffer = protoUtils.decodeDataBuffer;
module.exports.parseJsonFromDataBuffer = protoUtils.parseJsonFromDataBuffer;
