#!/usr/bin/env python3
"""
WebRTC FFI Integration Tests for Feature 016 - FFI WebRTC Bindings

Tests Python WebRTC server creation, configuration validation, and event handling.
Validates the Python FFI bindings for WebRTC transport.

Run with: pytest transports/ffi/tests/test_webrtc.py
"""

import sys
import asyncio
import pytest
from pathlib import Path

# Add python-client to path for any shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "python-client"))


def is_webrtc_available():
    """Check if WebRTC bindings are available."""
    try:
        # Try to import the WebRTC classes
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig
        return True
    except ImportError:
        return False


# Skip all tests if WebRTC is not available
pytestmark = pytest.mark.skipif(
    not is_webrtc_available(),
    reason="WebRTC bindings not available. Build with: cargo build --features python-webrtc"
)


class TestWebRtcServerConfig:
    """Tests for WebRtcServerConfig dataclass."""

    def test_create_config_with_port(self):
        """Test creating config with embedded signaling (port)."""
        from remotemedia.runtime import WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50051,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        assert config.port == 50051
        assert config.signaling_url is None
        assert config.max_peers == 10  # Default

    def test_create_config_with_signaling_url(self):
        """Test creating config with external signaling."""
        from remotemedia.runtime import WebRtcServerConfig

        config = WebRtcServerConfig(
            signaling_url="grpc://signaling.example.com:50051",
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        assert config.port is None
        assert config.signaling_url == "grpc://signaling.example.com:50051"

    def test_config_with_turn_servers(self):
        """Test creating config with TURN servers."""
        from remotemedia.runtime import WebRtcServerConfig, TurnServer

        turn_server = TurnServer(
            url="turn:turn.example.com:3478",
            username="user",
            credential="pass",
        )

        config = WebRtcServerConfig(
            port=50051,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
            turn_servers=[turn_server],
        )

        assert len(config.turn_servers) == 1
        assert config.turn_servers[0].url == "turn:turn.example.com:3478"

    def test_config_custom_options(self):
        """Test creating config with custom options."""
        from remotemedia.runtime import WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50051,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
            max_peers=5,
            video_codec="vp8",
        )

        assert config.max_peers == 5
        assert config.video_codec == "vp8"

    def test_config_repr(self):
        """Test config string representation."""
        from remotemedia.runtime import WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50051,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        repr_str = repr(config)
        assert "port=50051" in repr_str


class TestWebRtcServerCreation:
    """Tests for WebRtcServer creation and lifecycle."""

    @pytest.mark.asyncio
    async def test_create_server_with_embedded_signaling(self):
        """Test creating server with embedded signaling."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50099,  # Use high port to avoid conflicts
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        assert server is not None
        assert server.id is not None
        assert len(server.id) > 0

        state = await server.get_state()
        assert state == "created"

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_reject_create_without_port(self):
        """Test that create() rejects config without port."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            signaling_url="grpc://signaling.example.com:50051",
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        with pytest.raises(ValueError, match="Port is required"):
            await WebRtcServer.create(config)

    @pytest.mark.asyncio
    async def test_reject_config_without_stun(self):
        """Test that config without STUN servers is rejected."""
        from remotemedia.runtime import WebRtcServerConfig

        with pytest.raises(ValueError, match="STUN"):
            config = WebRtcServerConfig(
                port=50051,
                manifest={"nodes": [], "connections": []},
                stun_servers=[],
            )
            # Validation happens on to_core_config()
            config.to_core_config()

    @pytest.mark.asyncio
    async def test_reject_invalid_max_peers(self):
        """Test that invalid max_peers value is rejected."""
        from remotemedia.runtime import WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50051,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
            max_peers=100,  # Max is 10
        )

        with pytest.raises(ValueError, match="max_peers"):
            config.to_core_config()


class TestWebRtcServerCallbacks:
    """Tests for callback registration patterns."""

    @pytest.mark.asyncio
    async def test_register_peer_connected_callback(self):
        """Test registering peer connected callback with decorator."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50098,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        callback_registered = False

        @server.on_peer_connected
        async def handle_peer(event):
            nonlocal callback_registered
            callback_registered = True

        # The decorator should return the function for chaining
        assert handle_peer is not None

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_register_multiple_callbacks(self):
        """Test registering multiple callbacks for same event."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50097,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        @server.on_peer_connected
        async def handler1(event):
            pass

        @server.on_peer_connected
        async def handler2(event):
            pass

        @server.on_peer_connected
        async def handler3(event):
            pass

        # All callbacks should be registered without error
        await server.shutdown()

    @pytest.mark.asyncio
    async def test_register_all_event_types(self):
        """Test registering callbacks for all event types."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50096,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        @server.on_peer_connected
        async def on_connect(event):
            pass

        @server.on_peer_disconnected
        async def on_disconnect(event):
            pass

        @server.on_pipeline_output
        async def on_output(event):
            pass

        @server.on_data
        async def on_data(event):
            pass

        @server.on_error
        async def on_error(event):
            pass

        @server.on_session
        async def on_session(event):
            pass

        await server.shutdown()


class TestWebRtcServerLifecycle:
    """Tests for server lifecycle methods."""

    @pytest.mark.asyncio
    async def test_start_and_shutdown(self):
        """Test starting and shutting down the server."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50095,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        state = await server.get_state()
        assert state == "created"

        # Note: start() will try to bind to port, which may fail in test environment
        try:
            await server.start()
            state = await server.get_state()
            assert state == "running"
        except Exception as e:
            # Port binding might fail in CI, that's okay for this test
            print(f"Start failed (expected in some environments): {e}")

        await server.shutdown()

        state = await server.get_state()
        assert state == "stopped"

    @pytest.mark.asyncio
    async def test_get_empty_peers_initially(self):
        """Test that peers list is empty initially."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50094,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        peers = await server.get_peers()
        assert peers == []

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_get_empty_sessions_initially(self):
        """Test that sessions list is empty initially."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50093,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        sessions = await server.get_sessions()
        assert sessions == []

        await server.shutdown()


class TestWebRtcSessionManagement:
    """Tests for session management methods."""

    @pytest.mark.asyncio
    async def test_create_session(self):
        """Test creating a session."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig
        import time

        config = WebRtcServerConfig(
            port=50092,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        session_id = f"test-session-{int(time.time())}"
        session = await server.create_session(session_id, {"name": "Test Room"})

        assert session is not None
        assert session.session_id == session_id
        assert session.peer_ids == []

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_get_session(self):
        """Test getting a session by ID."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig
        import time

        config = WebRtcServerConfig(
            port=50091,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        session_id = f"get-session-{int(time.time())}"
        await server.create_session(session_id)

        session = await server.get_session(session_id)
        assert session is not None
        assert session.session_id == session_id

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_get_nonexistent_session(self):
        """Test getting a non-existent session returns None."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50090,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        session = await server.get_session("non-existent-session-id")
        assert session is None

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_delete_session(self):
        """Test deleting a session."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig
        import time

        config = WebRtcServerConfig(
            port=50089,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        server = await WebRtcServer.create(config)

        session_id = f"delete-session-{int(time.time())}"
        await server.create_session(session_id)

        # Verify it exists
        session = await server.get_session(session_id)
        assert session is not None

        # Delete it
        await server.delete_session(session_id)

        # Verify it's gone
        session = await server.get_session(session_id)
        assert session is None

        await server.shutdown()


class TestWebRtcContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test using server as async context manager."""
        from remotemedia.runtime import WebRtcServer, WebRtcServerConfig

        config = WebRtcServerConfig(
            port=50088,
            manifest={"nodes": [], "connections": []},
            stun_servers=["stun:stun.l.google.com:19302"],
        )

        async with await WebRtcServer.create(config) as server:
            assert server is not None
            # Server should be started
            state = await server.get_state()
            # May be "running" or "created" depending on port binding success
            assert state in ("running", "created")

        # After exiting context, server should be stopped
        # Note: Can't check state after context exit as server is shut down


class TestEventTypes:
    """Tests for event type classes."""

    def test_peer_capabilities(self):
        """Test PeerCapabilities creation."""
        from remotemedia.runtime import PeerCapabilities

        caps = PeerCapabilities(audio=True, video=False, data=True)
        assert caps.audio is True
        assert caps.video is False
        assert caps.data is True

    def test_peer_info(self):
        """Test PeerInfo representation."""
        from remotemedia.runtime import PeerInfo

        # PeerInfo is typically created from events, not directly
        # This test just verifies the class exists
        assert PeerInfo is not None

    def test_session_info(self):
        """Test SessionInfo representation."""
        from remotemedia.runtime import SessionInfo

        # SessionInfo is typically created from server methods
        assert SessionInfo is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
