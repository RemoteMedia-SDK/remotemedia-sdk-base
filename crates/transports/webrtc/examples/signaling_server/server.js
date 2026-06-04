#!/usr/bin/env node

/**
 * RemoteMedia WebRTC Signaling Server
 *
 * JSON-RPC 2.0 over WebSocket signaling server for peer discovery and SDP exchange.
 *
 * Supported Methods:
 * - announce: Announce peer with capabilities
 * - offer: Send SDP offer to peer
 * - answer: Send SDP answer to peer
 * - ice_candidate: Exchange ICE candidates
 * - disconnect: Notify peer disconnection
 */

import { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';

/**
 * Peer registry
 * Maps peer_id -> { ws, capabilities, state }
 */
const peers = new Map();

/**
 * Pending offers
 * Maps offer_id -> { from_peer_id, to_peer_id, sdp, timestamp }
 */
const pendingOffers = new Map();

/**
 * Create WebSocket server
 */
const wss = new WebSocketServer({
  host: HOST,
  port: PORT
});

console.log(`🚀 RemoteMedia WebRTC Signaling Server`);
console.log(`   Listening on ws://${HOST}:${PORT}`);
console.log(`   Protocol: JSON-RPC 2.0 over WebSocket\n`);

/**
 * Handle new WebSocket connections
 */
wss.on('connection', (ws, request) => {
  const clientIp = request.socket.remoteAddress;
  console.log(`📡 New connection from ${clientIp}`);

  let peerId = null;

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());

      // Validate JSON-RPC 2.0 format
      if (message.jsonrpc !== '2.0') {
        sendError(ws, null, -32600, 'Invalid Request: jsonrpc must be "2.0"');
        return;
      }

      if (!message.method) {
        sendError(ws, message.id, -32600, 'Invalid Request: method is required');
        return;
      }

      console.log(`📨 ${peerId || 'unknown'} -> ${message.method}`, message.params || '');

      // Route to handler
      switch (message.method) {
        case 'announce':
          handleAnnounce(ws, message);
          peerId = message.params?.peer_id;
          break;

        case 'offer':
          handleOffer(ws, message, peerId);
          break;

        case 'answer':
          handleAnswer(ws, message, peerId);
          break;

        case 'ice_candidate':
          handleIceCandidate(ws, message, peerId);
          break;

        case 'disconnect':
          handleDisconnect(ws, message, peerId);
          break;

        case 'list_peers':
          handleListPeers(ws, message);
          break;

        default:
          sendError(ws, message.id, -32601, `Method not found: ${message.method}`);
      }
    } catch (error) {
      console.error('❌ Error processing message:', error);
      sendError(ws, null, -32700, 'Parse error');
    }
  });

  ws.on('close', () => {
    if (peerId) {
      console.log(`👋 Peer disconnected: ${peerId}`);
      peers.delete(peerId);

      // Notify other peers
      broadcastPeerLeft(peerId);
    } else {
      console.log(`👋 Connection closed from ${clientIp}`);
    }
  });

  ws.on('error', (error) => {
    console.error(`❌ WebSocket error for ${peerId || clientIp}:`, error);
  });
});

/**
 * Handle announce method
 * Registers a peer with its capabilities
 */
function handleAnnounce(ws, message) {
  const { peer_id, capabilities } = message.params || {};

  if (!peer_id) {
    sendError(ws, message.id, -32602, 'Invalid params: peer_id is required');
    return;
  }

  // Check if peer already exists
  if (peers.has(peer_id)) {
    sendError(ws, message.id, -32000, `Peer ${peer_id} already announced`);
    return;
  }

  // Register peer
  peers.set(peer_id, {
    ws,
    capabilities: capabilities || {},
    state: 'available',
    connectedAt: Date.now()
  });

  console.log(`✅ Peer announced: ${peer_id}`, capabilities || {});

  // Send success response
  sendResult(ws, message.id, {
    peer_id,
    peers_count: peers.size,
    other_peers: Array.from(peers.keys()).filter(id => id !== peer_id)
  });

  // Notify other peers about new peer
  broadcastPeerJoined(peer_id, capabilities);
}

/**
 * Handle offer method
 * Forwards SDP offer to target peer
 */
function handleOffer(ws, message, fromPeerId) {
  const { to_peer_id, sdp, type } = message.params || {};

  if (!fromPeerId) {
    sendError(ws, message.id, -32000, 'Not announced: call announce first');
    return;
  }

  if (!to_peer_id || !sdp) {
    sendError(ws, message.id, -32602, 'Invalid params: to_peer_id and sdp are required');
    return;
  }

  const targetPeer = peers.get(to_peer_id);
  if (!targetPeer) {
    sendError(ws, message.id, -32000, `Peer not found: ${to_peer_id}`);
    return;
  }

  // Create offer ID
  const offerId = `${fromPeerId}_${to_peer_id}_${Date.now()}`;

  // Store pending offer
  pendingOffers.set(offerId, {
    from_peer_id: fromPeerId,
    to_peer_id,
    sdp,
    type: type || 'offer',
    timestamp: Date.now()
  });

  console.log(`📤 Forwarding offer: ${fromPeerId} -> ${to_peer_id}`);

  // Forward offer to target peer
  sendNotification(targetPeer.ws, 'offer', {
    from_peer_id: fromPeerId,
    offer_id: offerId,
    sdp,
    type: type || 'offer'
  });

  // Send success to sender
  sendResult(ws, message.id, {
    offer_id: offerId,
    to_peer_id
  });
}

/**
 * Handle answer method
 * Forwards SDP answer to original offerer
 */
function handleAnswer(ws, message, fromPeerId) {
  const { to_peer_id, sdp, type } = message.params || {};

  if (!fromPeerId) {
    sendError(ws, message.id, -32000, 'Not announced: call announce first');
    return;
  }

  if (!to_peer_id || !sdp) {
    sendError(ws, message.id, -32602, 'Invalid params: to_peer_id and sdp are required');
    return;
  }

  const targetPeer = peers.get(to_peer_id);
  if (!targetPeer) {
    sendError(ws, message.id, -32000, `Peer not found: ${to_peer_id}`);
    return;
  }

  console.log(`📤 Forwarding answer: ${fromPeerId} -> ${to_peer_id}`);

  // Forward answer to target peer
  sendNotification(targetPeer.ws, 'answer', {
    from_peer_id: fromPeerId,
    sdp,
    type: type || 'answer'
  });

  // Send success to sender
  sendResult(ws, message.id, {
    to_peer_id
  });
}

/**
 * Handle ice_candidate method
 * Forwards ICE candidate to target peer
 */
function handleIceCandidate(ws, message, fromPeerId) {
  const { to_peer_id, candidate, sdp_mid, sdp_mline_index } = message.params || {};

  if (!fromPeerId) {
    sendError(ws, message.id, -32000, 'Not announced: call announce first');
    return;
  }

  if (!to_peer_id || !candidate) {
    sendError(ws, message.id, -32602, 'Invalid params: to_peer_id and candidate are required');
    return;
  }

  const targetPeer = peers.get(to_peer_id);
  if (!targetPeer) {
    sendError(ws, message.id, -32000, `Peer not found: ${to_peer_id}`);
    return;
  }

  console.log(`🧊 Forwarding ICE candidate: ${fromPeerId} -> ${to_peer_id}`);

  // Forward ICE candidate to target peer
  sendNotification(targetPeer.ws, 'ice_candidate', {
    from_peer_id: fromPeerId,
    candidate,
    sdp_mid,
    sdp_mline_index
  });

  // Send success to sender
  sendResult(ws, message.id, {
    to_peer_id
  });
}

/**
 * Handle disconnect method
 * Notifies target peer of disconnection
 */
function handleDisconnect(ws, message, fromPeerId) {
  const { peer_id } = message.params || {};

  if (!fromPeerId) {
    sendError(ws, message.id, -32000, 'Not announced: call announce first');
    return;
  }

  if (!peer_id) {
    sendError(ws, message.id, -32602, 'Invalid params: peer_id is required');
    return;
  }

  const targetPeer = peers.get(peer_id);
  if (targetPeer) {
    console.log(`🔌 Peer disconnecting: ${fromPeerId} -> ${peer_id}`);

    // Notify target peer
    sendNotification(targetPeer.ws, 'peer_disconnected', {
      peer_id: fromPeerId
    });
  }

  // Send success to sender
  sendResult(ws, message.id, {
    peer_id
  });
}

/**
 * Handle list_peers method
 * Returns list of available peers
 */
function handleListPeers(ws, message) {
  const peerList = Array.from(peers.entries()).map(([id, peer]) => ({
    peer_id: id,
    capabilities: peer.capabilities,
    state: peer.state
  }));

  sendResult(ws, message.id, {
    peers: peerList,
    count: peerList.length
  });
}

/**
 * Broadcast peer joined notification
 */
function broadcastPeerJoined(peerId, capabilities) {
  const notification = {
    jsonrpc: '2.0',
    method: 'peer_joined',
    params: {
      peer_id: peerId,
      capabilities: capabilities || {}
    }
  };

  peers.forEach((peer, id) => {
    if (id !== peerId && peer.ws.readyState === 1) {
      peer.ws.send(JSON.stringify(notification));
    }
  });
}

/**
 * Broadcast peer left notification
 */
function broadcastPeerLeft(peerId) {
  const notification = {
    jsonrpc: '2.0',
    method: 'peer_left',
    params: {
      peer_id: peerId
    }
  };

  peers.forEach((peer) => {
    if (peer.ws.readyState === 1) {
      peer.ws.send(JSON.stringify(notification));
    }
  });
}

/**
 * Send JSON-RPC 2.0 success result
 */
function sendResult(ws, id, result) {
  const response = {
    jsonrpc: '2.0',
    id,
    result
  };
  ws.send(JSON.stringify(response));
}

/**
 * Send JSON-RPC 2.0 error
 */
function sendError(ws, id, code, message) {
  const response = {
    jsonrpc: '2.0',
    id,
    error: {
      code,
      message
    }
  };
  ws.send(JSON.stringify(response));
}

/**
 * Send JSON-RPC 2.0 notification (no response expected)
 */
function sendNotification(ws, method, params) {
  const notification = {
    jsonrpc: '2.0',
    method,
    params
  };
  ws.send(JSON.stringify(notification));
}

/**
 * Periodic cleanup of stale pending offers
 */
setInterval(() => {
  const now = Date.now();
  const timeout = 60000; // 60 seconds

  for (const [offerId, offer] of pendingOffers.entries()) {
    if (now - offer.timestamp > timeout) {
      console.log(`🧹 Cleaning up stale offer: ${offerId}`);
      pendingOffers.delete(offerId);
    }
  }
}, 30000); // Run every 30 seconds

/**
 * Graceful shutdown
 */
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down signaling server...');

  // Close all peer connections
  peers.forEach((peer, id) => {
    console.log(`   Closing connection to ${id}`);
    peer.ws.close(1001, 'Server shutting down');
  });

  // Close WebSocket server
  wss.close(() => {
    console.log('✅ Server shut down gracefully');
    process.exit(0);
  });

  // Force exit after 5 seconds
  setTimeout(() => {
    console.log('⚠️  Forced shutdown after timeout');
    process.exit(1);
  }, 5000);
});

/**
 * Log server stats periodically
 */
setInterval(() => {
  console.log(`📊 Stats: ${peers.size} peers, ${pendingOffers.size} pending offers`);
}, 60000); // Every 60 seconds
