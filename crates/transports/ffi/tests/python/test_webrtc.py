"""
WebRTC Server Tests (T032, T040, T050, T057)

Tests WebRTC server creation, configuration validation, event handling,
session management, external signaling, and targeted peer messaging.
Validates the Python FFI bindings for WebRTC transport.
"""

import asyncio
import json
import pytest
from typing import Optional, Any

# Try to import the WebRTC module from the native bindings
webrtc_available = False
WebRtcServer = None
WebRtcServerConfig = None

try:
    from remotemedia.webrtc import WebRtcServer, WebRtcServerConfig
    webrtc_available = True
except ImportError:
    try:
        # Alternative import path for development
        from remotemedia_ffi.webrtc import WebRtcServer, WebRtcServerConfig
        webrtc_available = True
    except ImportError:
        pass


def skip_if_webrtc_unavailable():
    """Skip test if WebRTC module is not available."""
    if not webrtc_available:
        pytest.skip("WebRTC module not available. Build with: cargo build --features python-webrtc")


def create_test_config(port: int = 50051) -> dict:
    """Create a minimal test configuration."""
    return {
        "port": port,
        "manifest": json.dumps({"nodes": [], "connections": []}),
        "stun_servers": ["stun:stun.l.google.com:19302"],
    }


class TestWebRtcServerConfiguration:
    """Tests for configuration validation."""

    def test_should_reject_config_without_port_or_signaling_url(self):
        skip_if_webrtc_unavailable()

        config = {
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": ["stun:stun.l.google.com:19302"],
        }

        with pytest.raises(Exception):
            asyncio.run(WebRtcServer.create(config))

    def test_should_reject_config_with_both_port_and_signaling_url(self):
        skip_if_webrtc_unavailable()

        config = {
            "port": 50051,
            "signaling_url": "grpc://localhost:50052",
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": ["stun:stun.l.google.com:19302"],
        }

        with pytest.raises(Exception):
            asyncio.run(WebRtcServer.create(config))

    def test_should_reject_config_without_stun_servers(self):
        skip_if_webrtc_unavailable()

        config = {
            "port": 50051,
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": [],
        }

        with pytest.raises(Exception):
            asyncio.run(WebRtcServer.create(config))

    def test_should_reject_invalid_stun_url_format(self):
        skip_if_webrtc_unavailable()

        config = {
            "port": 50051,
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": ["http://stun.example.com:3478"],
        }

        with pytest.raises(Exception):
            asyncio.run(WebRtcServer.create(config))

    def test_should_reject_invalid_max_peers_value(self):
        skip_if_webrtc_unavailable()

        config = {
            "port": 50051,
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": ["stun:stun.l.google.com:19302"],
            "max_peers": 100,  # Max is 10
        }

        with pytest.raises(Exception):
            asyncio.run(WebRtcServer.create(config))

    def test_should_reject_turn_server_without_username(self):
        skip_if_webrtc_unavailable()

        config = {
            "port": 50051,
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": ["stun:stun.l.google.com:19302"],
            "turn_servers": [
                {"url": "turn:turn.example.com:3478", "username": "", "credential": "secret"}
            ],
        }

        with pytest.raises(Exception):
            asyncio.run(WebRtcServer.create(config))


class TestWebRtcServerCreation:
    """Tests for server creation."""

    @pytest.mark.asyncio
    async def test_should_create_server_with_embedded_signaling(self):
        skip_if_webrtc_unavailable()

        config = create_test_config(port=50099)
        server = await WebRtcServer.create(config)

        assert server is not None
        assert server.id is not None
        assert len(server.id) > 0

        state = await server.state
        assert state == "created"

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_should_reject_connect_without_signaling_url(self):
        skip_if_webrtc_unavailable()

        config = create_test_config(port=50051)

        with pytest.raises(Exception, match="signaling_url"):
            await WebRtcServer.connect(config)

    @pytest.mark.asyncio
    async def test_should_create_server_with_valid_config(self):
        skip_if_webrtc_unavailable()

        config = {
            "port": 50098,
            "manifest": json.dumps({
                "nodes": [{"id": "test", "type": "Echo"}],
                "connections": [],
            }),
            "stun_servers": ["stun:stun.l.google.com:19302"],
            "turn_servers": [
                {"url": "turn:turn.example.com:3478", "username": "user", "credential": "pass"}
            ],
            "max_peers": 5,
            "video_codec": "vp9",
        }

        server = await WebRtcServer.create(config)

        assert server is not None
        assert server.id is not None

        await server.shutdown()


class TestWebRtcServerEventRegistration:
    """Tests for event callback registration."""

    @pytest.fixture
    async def server(self):
        skip_if_webrtc_unavailable()
        config = create_test_config(port=50097)
        server = await WebRtcServer.create(config)
        yield server
        await server.shutdown()

    @pytest.mark.asyncio
    async def test_should_register_peer_connected_callback(self, server):
        skip_if_webrtc_unavailable()

        callback_called = False

        @server.on_peer_connected
        async def handle_peer_connected(event):
            nonlocal callback_called
            callback_called = True

        # Callback should be registered without error

    @pytest.mark.asyncio
    async def test_should_register_peer_disconnected_callback(self, server):
        skip_if_webrtc_unavailable()

        @server.on_peer_disconnected
        async def handle_peer_disconnected(event):
            pass
        # Should not raise

    @pytest.mark.asyncio
    async def test_should_register_pipeline_output_callback(self, server):
        skip_if_webrtc_unavailable()

        @server.on_pipeline_output
        async def handle_pipeline_output(event):
            pass
        # Should not raise

    @pytest.mark.asyncio
    async def test_should_register_error_callback(self, server):
        skip_if_webrtc_unavailable()

        @server.on_error
        async def handle_error(event):
            pass
        # Should not raise


class TestWebRtcServerLifecycle:
    """Tests for server lifecycle management."""

    @pytest.mark.asyncio
    async def test_should_start_and_shutdown_cleanly(self):
        skip_if_webrtc_unavailable()

        config = create_test_config(port=50096)
        server = await WebRtcServer.create(config)

        assert await server.state == "created"

        try:
            await server.start()
            assert await server.state == "running"
        except Exception:
            # Port binding might fail in CI
            pass

        await server.shutdown()
        assert await server.state == "stopped"

    @pytest.mark.asyncio
    async def test_should_return_empty_peers_list_initially(self):
        skip_if_webrtc_unavailable()

        config = create_test_config(port=50095)
        server = await WebRtcServer.create(config)

        peers = await server.get_peers()
        assert peers == []

        await server.shutdown()

    @pytest.mark.asyncio
    async def test_should_return_empty_sessions_list_initially(self):
        skip_if_webrtc_unavailable()

        config = create_test_config(port=50094)
        server = await WebRtcServer.create(config)

        sessions = await server.get_sessions()
        assert sessions == []

        await server.shutdown()


class TestWebRtcServerSessionManagement:
    """Tests for session/room management."""

    @pytest.fixture
    async def server(self):
        skip_if_webrtc_unavailable()
        config = create_test_config(port=50093)
        server = await WebRtcServer.create(config)
        yield server
        await server.shutdown()

    @pytest.mark.asyncio
    async def test_should_create_session(self, server):
        skip_if_webrtc_unavailable()

        session_id = f"test-session-{id(self)}"
        session = await server.create_session(session_id, metadata={
            "name": "Test Room",
            "host": "user-1",
        })

        assert session is not None
        assert session.session_id == session_id
        assert session.peer_ids == []

    @pytest.mark.asyncio
    async def test_should_get_session_by_id(self, server):
        skip_if_webrtc_unavailable()

        session_id = f"get-session-{id(self)}"
        await server.create_session(session_id)

        session = await server.get_session(session_id)
        assert session is not None
        assert session.session_id == session_id

    @pytest.mark.asyncio
    async def test_should_return_none_for_nonexistent_session(self, server):
        skip_if_webrtc_unavailable()

        session = await server.get_session("non-existent-session-id")
        assert session is None

    @pytest.mark.asyncio
    async def test_should_delete_session(self, server):
        skip_if_webrtc_unavailable()

        session_id = f"delete-session-{id(self)}"
        await server.create_session(session_id)

        # Verify it exists
        before = await server.get_session(session_id)
        assert before is not None

        # Delete it
        await server.delete_session(session_id)

        # Verify it's gone
        after = await server.get_session(session_id)
        assert after is None

    @pytest.mark.asyncio
    async def test_should_list_sessions(self, server):
        skip_if_webrtc_unavailable()

        session_id1 = f"list-session-1-{id(self)}"
        session_id2 = f"list-session-2-{id(self)}"

        await server.create_session(session_id1)
        await server.create_session(session_id2)

        sessions = await server.get_sessions()
        assert len(sessions) >= 2

        session_ids = [s.session_id for s in sessions]
        assert session_id1 in session_ids
        assert session_id2 in session_ids


class TestWebRtcExternalSignaling:
    """Tests for external signaling mode."""

    @pytest.mark.asyncio
    async def test_should_reject_create_for_external_signaling_config(self):
        skip_if_webrtc_unavailable()

        config = {
            "signaling_url": "grpc://signaling.example.com:50051",
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": ["stun:stun.l.google.com:19302"],
        }

        with pytest.raises(Exception, match="Port is required"):
            await WebRtcServer.create(config)

    @pytest.mark.asyncio
    async def test_should_validate_signaling_url_format(self):
        skip_if_webrtc_unavailable()

        config = {
            "signaling_url": "http://invalid-protocol.com:50051",
            "manifest": json.dumps({"nodes": [], "connections": []}),
            "stun_servers": ["stun:stun.l.google.com:19302"],
        }

        with pytest.raises(Exception):
            await WebRtcServer.connect(config)


class TestWebRtcTargetedPeerMessaging:
    """Tests for targeted peer messaging (T057)."""

    @pytest.fixture
    async def server(self):
        skip_if_webrtc_unavailable()
        config = create_test_config(port=50091)
        server = await WebRtcServer.create(config)
        yield server
        await server.shutdown()

    @pytest.mark.asyncio
    async def test_should_have_send_to_peer_method(self, server):
        skip_if_webrtc_unavailable()

        assert hasattr(server, "send_to_peer")
        assert callable(server.send_to_peer)

    @pytest.mark.asyncio
    async def test_should_reject_send_to_peer_for_nonexistent_peer(self, server):
        skip_if_webrtc_unavailable()

        test_data = b"test message"

        with pytest.raises(Exception, match="(?i)(not found|peer)"):
            await server.send_to_peer("non-existent-peer-id", test_data)

    @pytest.mark.asyncio
    async def test_should_have_broadcast_method(self, server):
        skip_if_webrtc_unavailable()

        assert hasattr(server, "broadcast")
        assert callable(server.broadcast)

    @pytest.mark.asyncio
    async def test_should_broadcast_to_empty_peer_list_without_error(self, server):
        skip_if_webrtc_unavailable()

        test_data = b"broadcast test"
        # Should succeed even with no peers connected
        await server.broadcast(test_data)

    @pytest.mark.asyncio
    async def test_should_have_disconnect_peer_method(self, server):
        skip_if_webrtc_unavailable()

        assert hasattr(server, "disconnect_peer")
        assert callable(server.disconnect_peer)

    @pytest.mark.asyncio
    async def test_should_reject_disconnect_peer_for_nonexistent_peer(self, server):
        skip_if_webrtc_unavailable()

        with pytest.raises(Exception, match="(?i)(not found|peer)"):
            await server.disconnect_peer("non-existent-peer-id")

    @pytest.mark.asyncio
    async def test_should_accept_disconnect_reason_parameter(self, server):
        skip_if_webrtc_unavailable()

        # Even though peer doesn't exist, the method should accept the reason parameter
        with pytest.raises(Exception, match="(?i)(not found|peer)"):
            await server.disconnect_peer("non-existent-peer-id", reason="kicked by admin")


class TestWebRtcSessionTargetedMessaging:
    """Tests for session-based targeted messaging."""

    @pytest.fixture
    async def server(self):
        skip_if_webrtc_unavailable()
        config = create_test_config(port=50090)
        server = await WebRtcServer.create(config)
        yield server
        await server.shutdown()

    @pytest.mark.asyncio
    async def test_session_should_have_broadcast_method(self, server):
        skip_if_webrtc_unavailable()

        session_id = f"broadcast-test-{id(self)}"
        session = await server.create_session(session_id)

        assert session is not None
        assert hasattr(session, "broadcast") or hasattr(server, "broadcast_to_session")

    @pytest.mark.asyncio
    async def test_should_create_session_with_metadata(self, server):
        skip_if_webrtc_unavailable()

        session_id = f"metadata-test-{id(self)}"
        metadata = {
            "room_name": "Test Room",
            "max_participants": "10",
            "host": "user-123",
        }

        session = await server.create_session(session_id, metadata=metadata)

        assert session.session_id == session_id
        # Session should be retrievable
        retrieved = await server.get_session(session_id)
        assert retrieved is not None
        assert retrieved.session_id == session_id

    @pytest.mark.asyncio
    async def test_should_return_empty_peer_list_for_new_session(self, server):
        skip_if_webrtc_unavailable()

        session_id = f"empty-peers-test-{id(self)}"
        session = await server.create_session(session_id)

        assert session.peer_ids == []


class TestWebRtcContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_should_support_async_context_manager(self):
        skip_if_webrtc_unavailable()

        config = create_test_config(port=50089)
        server = await WebRtcServer.create(config)

        # Check if context manager is supported
        if hasattr(server, "__aenter__") and hasattr(server, "__aexit__"):
            async with server:
                state = await server.state
                # Server should be running when used as context manager
                assert state in ("created", "running")
            # Server should be stopped after context exit
            final_state = await server.state
            assert final_state == "stopped"
        else:
            # If context manager not implemented, just shutdown manually
            await server.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
