#!/usr/bin/env node

/**
 * Test client for the signaling server
 *
 * Demonstrates the complete signaling flow:
 * 1. Announce peer
 * 2. List peers
 * 3. Send offer to another peer
 * 4. Handle incoming offer/answer
 * 5. Exchange ICE candidates
 */

import WebSocket from 'ws';

const SIGNALING_URL = process.env.SIGNALING_URL || 'ws://localhost:8080';
const PEER_ID = process.argv[2] || `test-peer-${Math.floor(Math.random() * 10000)}`;

let ws;
let requestId = 1;
const pendingRequests = new Map();

console.log(`🧪 Test Client: ${PEER_ID}`);
console.log(`   Connecting to ${SIGNALING_URL}\n`);

/**
 * Connect to signaling server
 */
function connect() {
  ws = new WebSocket(SIGNALING_URL);

  ws.on('open', () => {
    console.log('✅ Connected to signaling server\n');
    announce();
  });

  ws.on('message', (data) => {
    const message = JSON.parse(data.toString());
    handleMessage(message);
  });

  ws.on('close', () => {
    console.log('\n👋 Disconnected from signaling server');
    process.exit(0);
  });

  ws.on('error', (error) => {
    console.error('❌ WebSocket error:', error.message);
    process.exit(1);
  });
}

/**
 * Handle incoming messages
 */
function handleMessage(message) {
  // Handle response to our request
  if (message.id && pendingRequests.has(message.id)) {
    const { method, resolve, reject } = pendingRequests.get(message.id);
    pendingRequests.delete(message.id);

    if (message.result) {
      console.log(`📥 Response to ${method}:`, JSON.stringify(message.result, null, 2));
      resolve(message.result);
    } else if (message.error) {
      console.error(`❌ Error response to ${method}:`, message.error);
      reject(message.error);
    }
    return;
  }

  // Handle notifications (server-initiated messages)
  if (message.method) {
    console.log(`🔔 Notification: ${message.method}`, JSON.stringify(message.params, null, 2));

    switch (message.method) {
      case 'offer':
        handleOffer(message.params);
        break;

      case 'answer':
        handleAnswer(message.params);
        break;

      case 'ice_candidate':
        handleIceCandidate(message.params);
        break;

      case 'peer_joined':
        console.log(`   New peer joined: ${message.params.peer_id}`);
        break;

      case 'peer_left':
        console.log(`   Peer left: ${message.params.peer_id}`);
        break;

      case 'peer_disconnected':
        console.log(`   Peer disconnected: ${message.params.peer_id}`);
        break;

      default:
        console.log(`   Unknown notification: ${message.method}`);
    }
  }
}

/**
 * Send JSON-RPC request
 */
function sendRequest(method, params = {}) {
  return new Promise((resolve, reject) => {
    const id = requestId++;
    const request = {
      jsonrpc: '2.0',
      method,
      params,
      id
    };

    console.log(`📤 Request ${id}: ${method}`, params);

    pendingRequests.set(id, { method, resolve, reject });
    ws.send(JSON.stringify(request));

    // Timeout after 10 seconds
    setTimeout(() => {
      if (pendingRequests.has(id)) {
        pendingRequests.delete(id);
        reject(new Error(`Request timeout: ${method}`));
      }
    }, 10000);
  });
}

/**
 * Announce this peer
 */
async function announce() {
  try {
    const result = await sendRequest('announce', {
      peer_id: PEER_ID,
      capabilities: {
        audio: true,
        video: true,
        data: true
      }
    });

    console.log(`\n✅ Announced as ${PEER_ID}`);
    console.log(`   Total peers: ${result.peers_count}`);
    console.log(`   Other peers: ${result.other_peers.join(', ') || 'none'}\n`);

    // Wait a bit, then list peers
    setTimeout(() => listPeers(), 1000);
  } catch (error) {
    console.error('Failed to announce:', error);
    process.exit(1);
  }
}

/**
 * List all connected peers
 */
async function listPeers() {
  try {
    const result = await sendRequest('list_peers');

    console.log(`\n📋 Peer List (${result.count} peers):`);
    result.peers.forEach(peer => {
      const isMe = peer.peer_id === PEER_ID;
      console.log(`   ${isMe ? '👤' : '👥'} ${peer.peer_id} - ${JSON.stringify(peer.capabilities)}`);
    });

    // If there are other peers, offer to connect to the first one
    const otherPeers = result.peers.filter(p => p.peer_id !== PEER_ID);
    if (otherPeers.length > 0) {
      console.log(`\n💡 Tip: To test offer/answer flow, run this in another terminal:`);
      console.log(`   node test-client.js ${otherPeers[0].peer_id}`);

      // If a target peer was specified, send offer
      const targetPeer = process.argv[3];
      if (targetPeer) {
        setTimeout(() => sendOffer(targetPeer), 1000);
      }
    } else {
      console.log(`\n💡 Tip: Start another client to test peer-to-peer signaling`);
      console.log(`   node test-client.js peer-2`);
    }

    console.log('\n⏳ Waiting for events (Ctrl+C to exit)...\n');
  } catch (error) {
    console.error('Failed to list peers:', error);
  }
}

/**
 * Send offer to another peer
 */
async function sendOffer(toPeerId) {
  try {
    console.log(`\n📞 Sending offer to ${toPeerId}...`);

    const mockSdp = `v=0
o=- ${Date.now()} 2 IN IP4 127.0.0.1
s=-
t=0 0
a=group:BUNDLE 0
a=msid-semantic: WMS
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=rtcp:9 IN IP4 0.0.0.0
a=ice-ufrag:test
a=ice-pwd:testpassword
a=fingerprint:sha-256 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00
a=setup:actpass
a=mid:0
a=sendrecv
a=rtcp-mux
a=rtpmap:111 opus/48000/2`;

    const result = await sendRequest('offer', {
      to_peer_id: toPeerId,
      sdp: mockSdp,
      type: 'offer'
    });

    console.log(`✅ Offer sent, offer_id: ${result.offer_id}`);
  } catch (error) {
    console.error('Failed to send offer:', error);
  }
}

/**
 * Handle incoming offer
 */
async function handleOffer(params) {
  const { from_peer_id, offer_id, sdp } = params;

  console.log(`\n📞 Received offer from ${from_peer_id}`);
  console.log(`   Offer ID: ${offer_id}`);
  console.log(`   SDP length: ${sdp.length} bytes`);

  // Send answer
  try {
    const mockAnswerSdp = sdp.replace('a=setup:actpass', 'a=setup:active');

    const result = await sendRequest('answer', {
      to_peer_id: from_peer_id,
      sdp: mockAnswerSdp,
      type: 'answer'
    });

    console.log(`✅ Sent answer to ${from_peer_id}`);

    // Simulate ICE candidate exchange
    setTimeout(() => sendIceCandidate(from_peer_id), 500);
  } catch (error) {
    console.error('Failed to send answer:', error);
  }
}

/**
 * Handle incoming answer
 */
function handleAnswer(params) {
  const { from_peer_id, sdp } = params;

  console.log(`\n✅ Received answer from ${from_peer_id}`);
  console.log(`   SDP length: ${sdp.length} bytes`);
  console.log(`\n🎉 Connection established! (in real scenario, WebRTC would now be connected)`);

  // Simulate ICE candidate exchange
  setTimeout(() => sendIceCandidate(from_peer_id), 500);
}

/**
 * Send ICE candidate
 */
async function sendIceCandidate(toPeerId) {
  try {
    const mockCandidate = `candidate:1 1 UDP 2130706431 192.168.1.100 ${Math.floor(Math.random() * 50000 + 10000)} typ host`;

    const result = await sendRequest('ice_candidate', {
      to_peer_id: toPeerId,
      candidate: mockCandidate,
      sdp_mid: '0',
      sdp_mline_index: 0
    });

    console.log(`🧊 Sent ICE candidate to ${toPeerId}`);
  } catch (error) {
    console.error('Failed to send ICE candidate:', error);
  }
}

/**
 * Handle incoming ICE candidate
 */
function handleIceCandidate(params) {
  const { from_peer_id, candidate } = params;
  console.log(`🧊 Received ICE candidate from ${from_peer_id}`);
  console.log(`   ${candidate}`);
}

/**
 * Graceful shutdown
 */
process.on('SIGINT', async () => {
  console.log('\n\n🛑 Shutting down...');

  if (ws && ws.readyState === WebSocket.OPEN) {
    // Optionally send disconnect notification
    try {
      // Just close the connection, server will handle cleanup
      ws.close();
    } catch (error) {
      console.error('Error during shutdown:', error);
    }
  }

  process.exit(0);
});

// Start the client
connect();
