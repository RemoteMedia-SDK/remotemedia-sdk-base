# RemoteMedia WebRTC Signaling Server

A JSON-RPC 2.0 over WebSocket signaling server for the RemoteMedia WebRTC transport.

## Features

- **JSON-RPC 2.0 Protocol**: Standards-compliant JSON-RPC over WebSocket
- **Peer Discovery**: Automatic peer registration and discovery
- **SDP Exchange**: Offer/answer pattern for WebRTC connection establishment
- **ICE Candidate Exchange**: NAT traversal support
- **Peer Notifications**: Real-time notifications for peer join/leave events
- **Lightweight**: Pure Node.js implementation with minimal dependencies

## Quick Start

### Installation

```bash
cd transports/webrtc/examples/signaling_server
npm install
```

### Running the Server

```bash
# Start with default settings (localhost:8080)
npm start

# Or with custom host/port
HOST=0.0.0.0 PORT=8080 npm start

# Development mode with auto-reload
npm run dev
```

Expected output:
```
🚀 RemoteMedia WebRTC Signaling Server
   Listening on ws://0.0.0.0:8080
   Protocol: JSON-RPC 2.0 over WebSocket
```

## Protocol Specification

### Connection

Connect via WebSocket:
```javascript
const ws = new WebSocket('ws://localhost:8080');
```

### Supported Methods

All requests/responses follow JSON-RPC 2.0 format:

```json
{
  "jsonrpc": "2.0",
  "method": "method_name",
  "params": { ... },
  "id": 1
}
```

#### 1. announce

Register a peer with the signaling server.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "announce",
  "params": {
    "peer_id": "peer-12345",
    "capabilities": {
      "audio": true,
      "video": true,
      "data": true
    }
  },
  "id": 1
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "peer_id": "peer-12345",
    "peers_count": 3,
    "other_peers": ["peer-67890", "peer-11111"]
  }
}
```

#### 2. offer

Send SDP offer to another peer.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "offer",
  "params": {
    "to_peer_id": "peer-67890",
    "sdp": "v=0\r\no=- 123456789 2 IN IP4 127.0.0.1\r\n...",
    "type": "offer"
  },
  "id": 2
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "offer_id": "peer-12345_peer-67890_1234567890",
    "to_peer_id": "peer-67890"
  }
}
```

**Target Peer Receives (Notification):**
```json
{
  "jsonrpc": "2.0",
  "method": "offer",
  "params": {
    "from_peer_id": "peer-12345",
    "offer_id": "peer-12345_peer-67890_1234567890",
    "sdp": "v=0\r\no=- 123456789 2 IN IP4 127.0.0.1\r\n...",
    "type": "offer"
  }
}
```

#### 3. answer

Send SDP answer to the peer that sent the offer.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "answer",
  "params": {
    "to_peer_id": "peer-12345",
    "sdp": "v=0\r\no=- 987654321 2 IN IP4 127.0.0.1\r\n...",
    "type": "answer"
  },
  "id": 3
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "to_peer_id": "peer-12345"
  }
}
```

**Target Peer Receives (Notification):**
```json
{
  "jsonrpc": "2.0",
  "method": "answer",
  "params": {
    "from_peer_id": "peer-67890",
    "sdp": "v=0\r\no=- 987654321 2 IN IP4 127.0.0.1\r\n...",
    "type": "answer"
  }
}
```

#### 4. ice_candidate

Exchange ICE candidates for NAT traversal.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "ice_candidate",
  "params": {
    "to_peer_id": "peer-67890",
    "candidate": "candidate:1 1 UDP 2130706431 192.168.1.100 54321 typ host",
    "sdp_mid": "0",
    "sdp_mline_index": 0
  },
  "id": 4
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "result": {
    "to_peer_id": "peer-67890"
  }
}
```

**Target Peer Receives (Notification):**
```json
{
  "jsonrpc": "2.0",
  "method": "ice_candidate",
  "params": {
    "from_peer_id": "peer-12345",
    "candidate": "candidate:1 1 UDP 2130706431 192.168.1.100 54321 typ host",
    "sdp_mid": "0",
    "sdp_mline_index": 0
  }
}
```

#### 5. disconnect

Notify peer of disconnection.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "disconnect",
  "params": {
    "peer_id": "peer-67890"
  },
  "id": 5
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "result": {
    "peer_id": "peer-67890"
  }
}
```

#### 6. list_peers

Get list of all connected peers.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "list_peers",
  "params": {},
  "id": 6
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "result": {
    "peers": [
      {
        "peer_id": "peer-12345",
        "capabilities": { "audio": true, "video": true },
        "state": "available"
      },
      {
        "peer_id": "peer-67890",
        "capabilities": { "audio": true, "video": false },
        "state": "available"
      }
    ],
    "count": 2
  }
}
```

### Notifications (Server-Initiated)

The server sends notifications (no response expected) for events:

#### peer_joined

```json
{
  "jsonrpc": "2.0",
  "method": "peer_joined",
  "params": {
    "peer_id": "peer-99999",
    "capabilities": { "audio": true, "video": true }
  }
}
```

#### peer_left

```json
{
  "jsonrpc": "2.0",
  "method": "peer_left",
  "params": {
    "peer_id": "peer-99999"
  }
}
```

#### peer_disconnected

```json
{
  "jsonrpc": "2.0",
  "method": "peer_disconnected",
  "params": {
    "peer_id": "peer-12345"
  }
}
```

### Error Responses

Standard JSON-RPC 2.0 error format:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32602,
    "message": "Invalid params: peer_id is required"
  }
}
```

**Error Codes:**
- `-32700` - Parse error
- `-32600` - Invalid Request
- `-32601` - Method not found
- `-32602` - Invalid params
- `-32000` - Application error (peer not found, etc.)

## Example Client

### Browser (JavaScript)

```javascript
const ws = new WebSocket('ws://localhost:8080');

// Request counter for JSON-RPC
let requestId = 1;

// Send JSON-RPC request
function sendRequest(method, params) {
  const request = {
    jsonrpc: '2.0',
    method,
    params,
    id: requestId++
  };
  ws.send(JSON.stringify(request));
  console.log('Sent:', request);
}

ws.onopen = () => {
  console.log('Connected to signaling server');

  // Announce this peer
  sendRequest('announce', {
    peer_id: 'browser-client-1',
    capabilities: {
      audio: true,
      video: true,
      data: true
    }
  });
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  console.log('Received:', message);

  if (message.method === 'offer') {
    // Received offer from another peer
    handleOffer(message.params);
  } else if (message.method === 'answer') {
    // Received answer from another peer
    handleAnswer(message.params);
  } else if (message.method === 'ice_candidate') {
    // Received ICE candidate
    handleIceCandidate(message.params);
  } else if (message.method === 'peer_joined') {
    console.log(`Peer joined: ${message.params.peer_id}`);
  } else if (message.method === 'peer_left') {
    console.log(`Peer left: ${message.params.peer_id}`);
  }
};

// Send offer to another peer
function sendOffer(toPeerId, sdp) {
  sendRequest('offer', {
    to_peer_id: toPeerId,
    sdp,
    type: 'offer'
  });
}

// Send answer to another peer
function sendAnswer(toPeerId, sdp) {
  sendRequest('answer', {
    to_peer_id: toPeerId,
    sdp,
    type: 'answer'
  });
}

// Send ICE candidate
function sendIceCandidate(toPeerId, candidate) {
  sendRequest('ice_candidate', {
    to_peer_id: toPeerId,
    candidate: candidate.candidate,
    sdp_mid: candidate.sdpMid,
    sdp_mline_index: candidate.sdpMLineIndex
  });
}
```

### Rust Client (Using tokio-tungstenite)

See the WebRTC transport's SignalingClient implementation:
- [src/signaling/client.rs](../../src/signaling/client.rs)

## Testing

### Automated Testing with Test Client

A test client (`test-client.js`) is provided to demonstrate the complete signaling flow:

**Terminal 1 (Server):**
```bash
npm start
```

**Terminal 2 (Peer 1):**
```bash
npm test
# or: node test-client.js peer-1
```

**Terminal 3 (Peer 2) - Connect and send offer:**
```bash
node test-client.js peer-2 peer-1
```

The test client will:
1. Connect and announce itself
2. List all connected peers
3. Send offer to target peer (if specified)
4. Respond to incoming offers with answers
5. Exchange ICE candidates

**Example output:**
```
🧪 Test Client: peer-1
   Connecting to ws://localhost:8080

✅ Connected to signaling server

📤 Request 1: announce { peer_id: 'peer-1', capabilities: { audio: true, video: true, data: true } }
📥 Response to announce: {
  "peer_id": "peer-1",
  "peers_count": 1,
  "other_peers": []
}

✅ Announced as peer-1
   Total peers: 1
   Other peers: none

📋 Peer List (1 peers):
   👤 peer-1 - {"audio":true,"video":true,"data":true}

⏳ Waiting for events (Ctrl+C to exit)...
```

### Manual Testing with wscat

Install wscat:
```bash
npm install -g wscat
```

Connect and test:
```bash
# Connect to server
wscat -c ws://localhost:8080

# Announce peer
> {"jsonrpc":"2.0","method":"announce","params":{"peer_id":"test-peer-1"},"id":1}

# List peers
> {"jsonrpc":"2.0","method":"list_peers","params":{},"id":2}
```

### End-to-End Test

Test the complete offer/answer flow with multiple clients:

**Terminal 1 (Server):**
```bash
npm start
```

**Terminal 2 (Peer 1):**
```bash
node test-client.js peer-1
```

**Terminal 3 (Peer 2 - sends offer to peer-1):**
```bash
node test-client.js peer-2 peer-1
```

You should see:
- Peer 2 sends offer → Server forwards to Peer 1
- Peer 1 receives offer → Sends answer to Peer 2
- Peer 2 receives answer → Connection established
- Both peers exchange ICE candidates

## Production Deployment

### Environment Variables

```bash
# Server configuration
HOST=0.0.0.0        # Bind address
PORT=8080           # WebSocket port

# Node.js configuration
NODE_ENV=production
```

### Using PM2

```bash
# Install PM2
npm install -g pm2

# Start server with PM2
pm2 start server.js --name webrtc-signaling

# View logs
pm2 logs webrtc-signaling

# Monitor
pm2 monit

# Restart on code changes
pm2 restart webrtc-signaling
```

### Docker Deployment

Create `Dockerfile`:
```dockerfile
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY server.js ./

EXPOSE 8080

CMD ["node", "server.js"]
```

Build and run:
```bash
docker build -t remotemedia-signaling .
docker run -p 8080:8080 -e HOST=0.0.0.0 remotemedia-signaling
```

### Nginx Reverse Proxy (WSS)

For production, use WSS (WebSocket Secure) with nginx:

```nginx
upstream signaling {
    server localhost:8080;
}

server {
    listen 443 ssl;
    server_name signaling.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://signaling;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket timeout
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
```

## Features and Limitations

### Current Features

- ✅ JSON-RPC 2.0 over WebSocket
- ✅ Peer registration and discovery
- ✅ SDP offer/answer exchange
- ✅ ICE candidate exchange
- ✅ Peer join/leave notifications
- ✅ Automatic cleanup of stale offers
- ✅ Graceful shutdown

### Limitations

- ⚠️ **No Authentication**: All peers can connect (add auth middleware for production)
- ⚠️ **No Persistence**: Peer state lost on server restart
- ⚠️ **In-Memory Storage**: Not suitable for large-scale deployments
- ⚠️ **Single Process**: No clustering support

### Future Enhancements

- [ ] Authentication and authorization
- [ ] Redis-backed peer storage for scaling
- [ ] Rate limiting
- [ ] Metrics and monitoring (Prometheus)
- [ ] TURN server integration
- [ ] Room-based signaling

## Troubleshooting

### Connection Refused

```
Error: connect ECONNREFUSED 127.0.0.1:8080
```

**Solution:** Ensure server is running with `npm start`

### Port Already in Use

```
Error: listen EADDRINUSE: address already in use :::8080
```

**Solution:** Use different port: `PORT=8081 npm start`

### Peer Not Found

```
{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Peer not found: peer-xyz"}}
```

**Solution:** Ensure target peer has called `announce` method first

## Related Documentation

- [WebRTC Transport README](../../README.md)
- [Integration Guide](../../INTEGRATION.md)
- [JSON-RPC 2.0 Specification](https://www.jsonrpc.org/specification)
- [WebSocket Protocol](https://datatracker.ietf.org/doc/html/rfc6455)

## License

MIT OR Apache-2.0 (same as parent project)
