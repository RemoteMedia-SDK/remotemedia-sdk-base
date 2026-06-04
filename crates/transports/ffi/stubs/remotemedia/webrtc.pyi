"""
Python type stubs for RemoteMedia WebRTC FFI bindings.

These stubs provide type hints for the WebRTC module when built
with the python-webrtc feature.
"""

from typing import Any, Awaitable, Callable, Optional, TypeVar
from collections.abc import Mapping

# Type variables
T = TypeVar('T')

class TurnServer:
    """TURN server configuration."""
    url: str
    username: str
    credential: str

    def __init__(self, url: str, username: str, credential: str) -> None: ...

class WebRtcServerConfig:
    """WebRTC server configuration."""
    port: Optional[int]
    signaling_url: Optional[str]
    manifest: Any
    stun_servers: list[str]
    turn_servers: list[TurnServer]
    max_peers: int
    audio_codec: str
    video_codec: str

    def __init__(
        self,
        *,
        port: Optional[int] = None,
        signaling_url: Optional[str] = None,
        manifest: Any = None,
        stun_servers: Optional[list[str]] = None,
        turn_servers: Optional[list[TurnServer]] = None,
        max_peers: int = 10,
        audio_codec: str = "opus",
        video_codec: str = "vp9",
    ) -> None: ...

class PeerCapabilities:
    """Peer media capabilities."""
    audio: bool
    video: bool
    data: bool

class PeerInfo:
    """Connected peer information."""
    peer_id: str
    capabilities: PeerCapabilities
    metadata: dict[str, str]
    state: str
    connected_at: int

class SessionInfo:
    """Session (room) information."""
    session_id: str
    peer_ids: list[str]
    created_at: int
    metadata: dict[str, str]

class PeerConnectedEvent:
    """Event emitted when a peer connects."""
    peer_id: str
    capabilities: PeerCapabilities
    metadata: dict[str, str]

class PeerDisconnectedEvent:
    """Event emitted when a peer disconnects."""
    peer_id: str
    reason: Optional[str]

class PipelineOutputEvent:
    """Event emitted when pipeline produces output."""
    peer_id: str
    data: bytes
    timestamp: int

class DataReceivedEvent:
    """Event emitted when raw data is received from a peer."""
    peer_id: str
    data: bytes
    timestamp: int

class ErrorEvent:
    """Error event."""
    code: str
    message: str
    peer_id: Optional[str]

class SessionEvent:
    """Session event (peer join/leave)."""
    session_id: str
    event_type: str  # "peer_joined" | "peer_left"
    peer_id: str

class WebRtcSession:
    """
    WebRTC session (room) for peer grouping.

    Created via WebRtcServer.create_session().
    """
    @property
    def session_id(self) -> str:
        """Session identifier."""
        ...

    async def get_peers(self) -> list[str]:
        """Get peer IDs in this session."""
        ...

    async def get_created_at(self) -> int:
        """Get session creation timestamp."""
        ...

    async def get_metadata(self) -> dict[str, str]:
        """Get session metadata."""
        ...

    def on_peer_joined(self, callback: Callable[[str], Any]) -> Callable[[str], Any]:
        """
        Decorator to register peer joined callback.

        Usage:
            @session.on_peer_joined
            async def handle_join(peer_id: str):
                print(f"Peer {peer_id} joined")
        """
        ...

    def on_peer_left(self, callback: Callable[[str], Any]) -> Callable[[str], Any]:
        """
        Decorator to register peer left callback.

        Usage:
            @session.on_peer_left
            async def handle_leave(peer_id: str):
                print(f"Peer {peer_id} left")
        """
        ...

    async def broadcast(self, data: bytes) -> None:
        """Broadcast data to all peers in the session."""
        ...

    async def send_to_peer(self, peer_id: str, data: bytes) -> None:
        """Send data to a specific peer in the session."""
        ...

    async def add_peer(self, peer_id: str) -> None:
        """Add a peer to this session."""
        ...

    async def remove_peer(self, peer_id: str) -> None:
        """Remove a peer from this session."""
        ...

class WebRtcServer:
    """
    WebRTC server for real-time media streaming.

    Supports both embedded and external signaling modes.
    """
    @property
    def id(self) -> str:
        """Server unique identifier."""
        ...

    @staticmethod
    async def create(config: WebRtcServerConfig) -> "WebRtcServer":
        """
        Create a server with embedded signaling.

        Args:
            config: Server configuration with port set

        Returns:
            WebRtcServer instance
        """
        ...

    @staticmethod
    async def connect(config: WebRtcServerConfig) -> "WebRtcServer":
        """
        Connect to an external signaling server.

        Args:
            config: Server configuration with signaling_url set

        Returns:
            WebRtcServer instance
        """
        ...

    async def get_state(self) -> str:
        """
        Get current server state.

        Returns:
            One of: "created", "starting", "running", "stopping", "stopped"
        """
        ...

    async def get_peers(self) -> list[PeerInfo]:
        """Get connected peers."""
        ...

    async def get_sessions(self) -> list[SessionInfo]:
        """Get active sessions."""
        ...

    def on_peer_connected(
        self, callback: Callable[[PeerConnectedEvent], Any]
    ) -> Callable[[PeerConnectedEvent], Any]:
        """
        Decorator to register peer connected callback.

        Usage:
            @server.on_peer_connected
            async def handle_peer(event: PeerConnectedEvent):
                print(f"Peer {event.peer_id} connected")
        """
        ...

    def on_peer_disconnected(
        self, callback: Callable[[PeerDisconnectedEvent], Any]
    ) -> Callable[[PeerDisconnectedEvent], Any]:
        """Decorator to register peer disconnected callback."""
        ...

    def on_pipeline_output(
        self, callback: Callable[[PipelineOutputEvent], Any]
    ) -> Callable[[PipelineOutputEvent], Any]:
        """Decorator to register pipeline output callback."""
        ...

    def on_data(
        self, callback: Callable[[DataReceivedEvent], Any]
    ) -> Callable[[DataReceivedEvent], Any]:
        """Decorator to register raw data callback."""
        ...

    def on_error(
        self, callback: Callable[[ErrorEvent], Any]
    ) -> Callable[[ErrorEvent], Any]:
        """Decorator to register error callback."""
        ...

    def on_session(
        self, callback: Callable[[SessionEvent], Any]
    ) -> Callable[[SessionEvent], Any]:
        """Decorator to register session event callback."""
        ...

    async def start(self) -> None:
        """
        Start the server.

        For embedded mode: binds to the configured port.
        For external mode: connects to the signaling server.
        """
        ...

    async def shutdown(self) -> None:
        """Stop the server gracefully."""
        ...

    async def send_to_peer(self, peer_id: str, data: bytes) -> None:
        """
        Send data to a specific peer.

        Args:
            peer_id: Target peer ID
            data: Data to send
        """
        ...

    async def broadcast(self, data: bytes) -> None:
        """
        Broadcast data to all connected peers.

        Args:
            data: Data to broadcast
        """
        ...

    async def disconnect_peer(self, peer_id: str, reason: Optional[str] = None) -> None:
        """
        Disconnect a peer.

        Args:
            peer_id: Peer to disconnect
            reason: Optional disconnect reason
        """
        ...

    async def create_session(
        self, session_id: str, metadata: Optional[dict[str, str]] = None
    ) -> WebRtcSession:
        """
        Create a new session (room).

        Args:
            session_id: Unique session identifier
            metadata: Optional session metadata

        Returns:
            WebRtcSession instance for managing the room
        """
        ...

    async def create_session_info(
        self, session_id: str, metadata: Optional[dict[str, str]] = None
    ) -> SessionInfo:
        """
        Create a new session and return info only.

        Args:
            session_id: Unique session identifier
            metadata: Optional session metadata

        Returns:
            SessionInfo with session details
        """
        ...

    async def get_session(self, session_id: str) -> Optional[SessionInfo]:
        """
        Get an existing session.

        Args:
            session_id: Session identifier

        Returns:
            SessionInfo or None if not found
        """
        ...

    async def delete_session(self, session_id: str) -> None:
        """
        Delete a session.

        Args:
            session_id: Session to delete
        """
        ...

    async def __aenter__(self) -> "WebRtcServer":
        """Context manager entry - starts the server."""
        ...

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> bool:
        """Context manager exit - shuts down the server."""
        ...
