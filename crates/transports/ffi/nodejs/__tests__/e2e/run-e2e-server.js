#!/usr/bin/env node
/**
 * E2E Test Server Runner
 *
 * Starts the WebRTC server and HTTP server for browser-based E2E testing.
 * This script can be run directly without Jest.
 *
 * Usage: node __tests__/e2e/run-e2e-server.js
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

// Configuration
const WS_PORT = parseInt(process.env.WS_PORT || '55000', 10);
const HTTP_PORT = parseInt(process.env.HTTP_PORT || '55001', 10);

// Load native module
let native = null;
let loadError = null;

try {
  native = require('../..');
} catch (e) {
  loadError = e;
}

function isWebRtcAvailable() {
  return !!(native?.isNativeLoaded() && native.WebRtcServer);
}

function createValidManifest(name = 'e2e-test-pipeline') {
  return JSON.stringify({
    version: '1.0',
    metadata: { name },
    nodes: [{ id: 'echo', node_type: 'Echo' }],
    connections: [],
  });
}

// Create HTTP server to serve the test HTML
function createTestServer(port) {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const htmlPath = path.join(__dirname, 'webrtc-browser-client.html');
      
      // Enable CORS
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
      
      if (req.url === '/' || req.url?.startsWith('/index') || req.url?.includes('?')) {
        fs.readFile(htmlPath, (err, content) => {
          if (err) {
            res.writeHead(500);
            res.end('Error loading test page: ' + err.message);
            return;
          }
          res.writeHead(200, { 'Content-Type': 'text/html' });
          res.end(content);
        });
      } else {
        res.writeHead(404);
        res.end('Not found');
      }
    });

    server.listen(port, '0.0.0.0', () => {
      resolve(server);
    });

    server.on('error', reject);
  });
}

async function main() {
  console.log('');
  console.log('🚀 Starting E2E Test Server...');
  console.log('');

  if (!isWebRtcAvailable()) {
    console.error('❌ WebRTC module not available.');
    console.error('   Build with: cargo build --release -p remotemedia-ffi --features napi-webrtc');
    if (loadError) {
      console.error('   Load error:', loadError.message);
    }
    process.exit(1);
  }

  console.log('✅ Native module loaded successfully');

  // Start HTTP server
  let httpServer;
  try {
    httpServer = await createTestServer(HTTP_PORT);
    console.log(`✅ HTTP server started on http://0.0.0.0:${HTTP_PORT}`);
  } catch (err) {
    console.error(`❌ Failed to start HTTP server: ${err.message}`);
    process.exit(1);
  }

  // Create WebRTC server
  const config = {
    port: WS_PORT,
    manifest: createValidManifest('e2e-manual-test'),
    stunServers: ['stun:stun.l.google.com:19302'],
    audioCodec: 'opus',
  };

  let server;
  try {
    server = await native.WebRtcServer.create(config);
    console.log(`✅ WebRTC server created (ID: ${server.id})`);
  } catch (err) {
    console.error(`❌ Failed to create WebRTC server: ${err.message}`);
    httpServer.close();
    process.exit(1);
  }

  // Register event handlers
  server.on('peer_connected', (data) => {
    console.log('');
    console.log('═'.repeat(60));
    console.log('✅ PEER CONNECTED');
    console.log('═'.repeat(60));
    console.log(`   Peer ID: ${data.peerId}`);
    console.log(`   Capabilities: audio=${data.capabilities?.audio}, video=${data.capabilities?.video}, data=${data.capabilities?.data}`);
    console.log(`   Metadata:`, data.metadata || {});
    console.log('═'.repeat(60));
    console.log('');
  });

  server.on('peer_disconnected', (data) => {
    console.log('');
    console.log('═'.repeat(60));
    console.log('❌ PEER DISCONNECTED');
    console.log('═'.repeat(60));
    console.log(`   Peer ID: ${data.peerId}`);
    console.log(`   Reason: ${data.reason || 'No reason provided'}`);
    console.log('═'.repeat(60));
    console.log('');
  });

  server.on('pipeline_output', (data) => {
    console.log('📤 PIPELINE OUTPUT:', data.peerId, '- timestamp:', data.timestamp);
  });

  server.on('data', (data) => {
    console.log('📦 DATA RECEIVED:', data.peerId, '- bytes:', data.data?.length);
  });

  server.on('error', (data) => {
    console.log('⚠️ ERROR:', data);
  });

  // Start the signaling server
  try {
    await server.startSignalingServer(WS_PORT);
    console.log(`✅ WebSocket signaling server started on ws://0.0.0.0:${WS_PORT}`);
  } catch (err) {
    console.error(`❌ Failed to start signaling server: ${err.message}`);
    httpServer.close();
    process.exit(1);
  }

  // Print instructions
  console.log('');
  console.log('═'.repeat(60));
  console.log('🎉 E2E TEST SERVER READY');
  console.log('═'.repeat(60));
  console.log('');
  console.log('Open this URL in your browser to test:');
  console.log('');
  console.log(`  📌 http://localhost:${HTTP_PORT}/?port=${WS_PORT}&autoConnect=true`);
  console.log('');
  console.log('Or with a custom peer ID:');
  console.log(`  📌 http://localhost:${HTTP_PORT}/?port=${WS_PORT}&peerId=my-test-peer`);
  console.log('');
  console.log('Expected events:');
  console.log('  1. ✅ PEER CONNECTED - when browser connects and announces');
  console.log('  2. ❌ PEER DISCONNECTED - when browser closes tab');
  console.log('');
  console.log('Press Ctrl+C to stop the server');
  console.log('═'.repeat(60));
  console.log('');

  // Keep server running until Ctrl+C
  process.on('SIGINT', async () => {
    console.log('');
    console.log('🛑 Shutting down...');
    await server.shutdown();
    httpServer.close();
    console.log('✅ Server stopped.');
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
