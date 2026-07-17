"""Tests for tunnel.py — HostPortTunnel, ResilientHostPortTunnel."""
from __future__ import annotations

import queue
import threading

import pytest

from tenki_sandbox._pb import pb
from tenki_sandbox.errors import CapabilityUnavailableError, PrimitiveTimeoutError
from tenki_sandbox.models import TunnelTermination
from tenki_sandbox.tunnel import (
    HostPortTunnel,
    ResilientHostPortTunnel,
    _map_termination,
    _should_reconnect,
    jittered_backoff,
    open_host_port_tunnel,
    open_resilient_host_port_tunnel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tunnel(frames=None, host_addr="127.0.0.1:9000"):
    """Build a HostPortTunnel with a fake response_iter, no real sockets."""
    outbound = queue.Queue()
    response_iter = iter(frames or [])
    t = HostPortTunnel(
        sandbox_port=8080,
        sandbox_address="10.0.0.1:8080",
        outbound=outbound,
        response_iter=response_iter,
        host_addr=host_addr,
    )
    return t, outbound


def _make_tunnel_no_thread(frames=None, host_addr="127.0.0.1:9000"):
    """Build a HostPortTunnel without starting the forward thread."""
    outbound = queue.Queue()
    t = HostPortTunnel.__new__(HostPortTunnel)
    t.sandbox_port = 8080
    t.sandbox_address = "10.0.0.1:8080"
    t._outbound = outbound
    t._host_addr = host_addr
    t.terminated = threading.Event()
    t._termination = None
    t._finish_lock = threading.Lock()
    t._cb_fired = False
    t._callbacks = []
    t._closed = False
    return t, outbound


class FakeRPC:
    def __init__(self, frames):
        self._frames = frames

    def host_port_tunnel(self, request_iter):
        # drain in background
        threading.Thread(target=lambda: list(request_iter), daemon=True).start()
        return iter(self._frames)


class ErrorRPC:
    def __init__(self, err):
        self._err = err

    def host_port_tunnel(self, request_iter):
        raise self._err


# ---------------------------------------------------------------------------
# TunnelTermination model
# ---------------------------------------------------------------------------

def test_tunnel_termination_fields():
    t = TunnelTermination(reason="sdk_closed", message="by user")
    assert t.reason == "sdk_closed"
    assert t.message == "by user"


def test_tunnel_termination_default_message():
    t = TunnelTermination(reason="timeout")
    assert t.message == ""


def test_tunnel_termination_frozen():
    t = TunnelTermination(reason="timeout")
    with pytest.raises((AttributeError, TypeError)):
        t.reason = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _map_termination
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("proto_reason,expected", [
    (pb.HostPortTunnelTerminated.REASON_KEEPALIVE_TIMEOUT, "timeout"),
    (pb.HostPortTunnelTerminated.REASON_SESSION_LOST, "engine_terminated"),
    (pb.HostPortTunnelTerminated.REASON_ENGINE_DRAINING, "engine_draining"),
    (pb.HostPortTunnelTerminated.REASON_BIND_FAILED, "host_error"),
    (pb.HostPortTunnelTerminated.REASON_PLATFORM_ERROR, "host_error"),
    (pb.HostPortTunnelTerminated.REASON_CAPABILITY_UNAVAILABLE, "host_error"),
    (pb.HostPortTunnelTerminated.REASON_UNSPECIFIED, "engine_terminated"),
])
def test_map_termination_reason(proto_reason, expected):
    msg = pb.HostPortTunnelTerminated(reason=proto_reason, detail="detail")
    term = _map_termination(msg)
    assert term.reason == expected
    assert term.message == "detail"


# ---------------------------------------------------------------------------
# HostPortTunnel._finish / terminated event
# ---------------------------------------------------------------------------

def test_finish_sets_terminated_event():
    t, _ = _make_tunnel_no_thread()
    t._finish(TunnelTermination(reason="sdk_closed"))
    assert t.terminated.is_set()


def test_finish_fires_callbacks():
    t, _ = _make_tunnel_no_thread()
    fired = []
    done = threading.Event()
    def cb(term):
        fired.append(term)
        done.set()
    t._callbacks.append(cb)
    t._finish(TunnelTermination(reason="timeout"))
    assert done.wait(timeout=1)
    assert len(fired) == 1
    assert fired[0].reason == "timeout"


def test_finish_is_idempotent():
    t, _ = _make_tunnel_no_thread()
    call_count = [0]
    done = threading.Event()
    def cb(_):
        call_count[0] += 1
        done.set()
    t._callbacks.append(cb)
    t._finish(TunnelTermination(reason="timeout"))
    t._finish(TunnelTermination(reason="timeout"))
    assert done.wait(timeout=1)
    assert call_count[0] == 1


# ---------------------------------------------------------------------------
# on_terminated
# ---------------------------------------------------------------------------

def test_on_terminated_before_termination():
    t, _ = _make_tunnel_no_thread()
    fired = threading.Event()
    t.on_terminated(lambda _: fired.set())
    t._finish(TunnelTermination(reason="sdk_closed"))
    assert fired.wait(timeout=1)


def test_on_terminated_after_termination_fires_immediately():
    t, _ = _make_tunnel_no_thread()
    t._finish(TunnelTermination(reason="sdk_closed"))
    fired = threading.Event()
    t.on_terminated(lambda _: fired.set())
    assert fired.wait(timeout=1)


# ---------------------------------------------------------------------------
# HostPortTunnel.close()
# ---------------------------------------------------------------------------

def test_host_port_tunnel_close_sets_terminated():
    # Empty iterator: _forward exits immediately without setting terminated.
    # No wait needed — go straight to close().
    t, outbound = _make_tunnel(frames=[])
    t.close()
    assert t.terminated.is_set()
    assert t._termination is not None
    assert t._termination.reason == "sdk_closed"


# ---------------------------------------------------------------------------
# open_host_port_tunnel: happy path
# ---------------------------------------------------------------------------

def test_open_host_port_tunnel_happy_path():
    frames = [
        pb.HostPortTunnelResponse(
            opened=pb.HostPortTunnelOpened(sandbox_port=9999, sandbox_address="10.0.0.1:9999")
        ),
    ]
    rpc = FakeRPC(frames)
    t = open_host_port_tunnel(rpc, "sess-1", "localhost:3000")
    assert t.sandbox_port == 9999
    assert t.sandbox_address == "10.0.0.1:9999"
    t.close()


# ---------------------------------------------------------------------------
# open_host_port_tunnel: error cases
# ---------------------------------------------------------------------------

def test_open_host_port_tunnel_capability_unavailable():
    frames = [
        pb.HostPortTunnelResponse(
            terminated=pb.HostPortTunnelTerminated(
                reason=pb.HostPortTunnelTerminated.REASON_CAPABILITY_UNAVAILABLE,
                detail="not supported",
            )
        )
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(CapabilityUnavailableError):
        open_host_port_tunnel(rpc, "sess", "host:1234")


def test_open_host_port_tunnel_primitive_timeout():
    frames = [
        pb.HostPortTunnelResponse(
            terminated=pb.HostPortTunnelTerminated(
                reason=pb.HostPortTunnelTerminated.REASON_CAPABILITY_UNAVAILABLE,
                detail="first_frame_timeout: agent not responding",
            )
        )
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(PrimitiveTimeoutError):
        open_host_port_tunnel(rpc, "sess", "host:1234")


def test_open_host_port_tunnel_other_terminated():
    frames = [
        pb.HostPortTunnelResponse(
            terminated=pb.HostPortTunnelTerminated(
                reason=pb.HostPortTunnelTerminated.REASON_BIND_FAILED,
                detail="port in use",
            )
        )
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(RuntimeError, match="host_port_tunnel failed"):
        open_host_port_tunnel(rpc, "sess", "host:1234")


def test_open_host_port_tunnel_stream_empty():
    rpc = FakeRPC([])
    with pytest.raises(RuntimeError, match="closed before open"):
        open_host_port_tunnel(rpc, "sess", "host:1234")


def test_open_host_port_tunnel_unexpected_first_frame():
    frames = [pb.HostPortTunnelResponse(ping=pb.HostPortTunnelKeepalivePing(ping_id=1))]
    rpc = FakeRPC(frames)
    with pytest.raises(RuntimeError, match="unexpected first frame"):
        open_host_port_tunnel(rpc, "sess", "host:1234")


def test_open_host_port_tunnel_rpc_raises():
    class GrpcError(Exception):
        def code(self):
            class C:
                name = "UNAUTHENTICATED"
            return C()
        def details(self):
            return "bad token"

    rpc = ErrorRPC(GrpcError())
    from tenki_sandbox.errors import SandboxError
    with pytest.raises(SandboxError):
        open_host_port_tunnel(rpc, "sess", "host:1234")


# ---------------------------------------------------------------------------
# HostPortTunnel forward: ping -> pong
# ---------------------------------------------------------------------------

def test_forward_ping_sends_pong():
    t, outbound = _make_tunnel_no_thread()
    frames = [
        pb.HostPortTunnelResponse(ping=pb.HostPortTunnelKeepalivePing(ping_id=42)),
    ]
    t._forward(iter(frames))
    # outbound should have pong
    frame = outbound.get_nowait()
    assert frame.WhichOneof("payload") == "pong"
    assert frame.pong.ping_id == 42


# ---------------------------------------------------------------------------
# HostPortTunnel forward: terminated frame -> finish
# ---------------------------------------------------------------------------

def test_forward_terminated_fires_finish():
    t, _ = _make_tunnel_no_thread()
    frames = [
        pb.HostPortTunnelResponse(
            terminated=pb.HostPortTunnelTerminated(
                reason=pb.HostPortTunnelTerminated.REASON_KEEPALIVE_TIMEOUT,
                detail="keepalive missed",
            )
        ),
    ]
    t._forward(iter(frames))
    assert t.terminated.is_set()
    assert t._termination is not None
    assert t._termination.reason == "timeout"


# ---------------------------------------------------------------------------
# _should_reconnect
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reason", ["transport_error", "engine_terminated", "engine_draining", "timeout"])
def test_should_reconnect_true(reason):
    assert _should_reconnect(reason) is True


@pytest.mark.parametrize("reason", ["sdk_closed", "host_error", "host_closed"])
def test_should_reconnect_false(reason):
    assert _should_reconnect(reason) is False


# ---------------------------------------------------------------------------
# jittered_backoff
# ---------------------------------------------------------------------------

def test_jittered_backoff_within_bounds():
    for attempt in range(10):
        val = jittered_backoff(0.25, 10.0, attempt)
        assert val >= 0
        assert val <= 10.0 * 1.25 + 0.01  # max * 1.25 headroom for jitter


def test_jittered_backoff_does_not_exceed_max():
    for _ in range(50):
        val = jittered_backoff(1.0, 5.0, 100)
        assert val <= 5.0 * 1.25 + 0.01


def test_jittered_backoff_attempt_zero_uses_initial():
    # attempt=0 → 2^max(0,-1)=1, base=initial*1, so between 0.75*initial and 1.25*initial
    for _ in range(20):
        val = jittered_backoff(1.0, 10.0, 0)
        assert 0.75 <= val <= 1.25 + 0.01


# ---------------------------------------------------------------------------
# ResilientHostPortTunnel: initial state
# ---------------------------------------------------------------------------

def test_resilient_initial_state():
    frames = [
        pb.HostPortTunnelResponse(
            opened=pb.HostPortTunnelOpened(sandbox_port=7777, sandbox_address="10.0.0.2:7777")
        ),
    ]
    rpc = FakeRPC(frames)
    r = open_resilient_host_port_tunnel(rpc, "sess", "host:9000")
    assert r.state == "connected"
    assert r.sandbox_port == 7777
    assert r.sandbox_address == "10.0.0.2:7777"
    r.close()


# ---------------------------------------------------------------------------
# ResilientHostPortTunnel: close -> terminated with sdk_closed
# ---------------------------------------------------------------------------

def test_resilient_close_terminates():
    frames = [
        pb.HostPortTunnelResponse(
            opened=pb.HostPortTunnelOpened(sandbox_port=1, sandbox_address="")
        ),
    ]
    rpc = FakeRPC(frames)
    r = open_resilient_host_port_tunnel(rpc, "sess", "host:9000")
    r.close()
    assert r.terminated.is_set()
    assert r._termination is not None
    assert r._termination.reason == "sdk_closed"


# ---------------------------------------------------------------------------
# ResilientHostPortTunnel: on_terminated
# ---------------------------------------------------------------------------

def test_resilient_on_terminated():
    frames = [
        pb.HostPortTunnelResponse(
            opened=pb.HostPortTunnelOpened(sandbox_port=1, sandbox_address="")
        ),
    ]
    rpc = FakeRPC(frames)
    r = open_resilient_host_port_tunnel(rpc, "sess", "host:9000")
    fired = threading.Event()
    r.on_terminated(lambda _: fired.set())
    r.close()
    assert fired.wait(timeout=1)


# ---------------------------------------------------------------------------
# ResilientHostPortTunnel: on_state_change
# ---------------------------------------------------------------------------

def test_resilient_on_state_change_reconnecting_then_connected():
    """Simulate: first tunnel terminates with engine_draining -> reconnect -> connected."""
    call_log = []
    state_event = threading.Event()
    call_count = [0]
    ready_for_second = threading.Event()

    class SequencedRPC:
        def host_port_tunnel(self, request_iter):
            threading.Thread(target=lambda: list(request_iter), daemon=True).start()
            n = call_count[0]
            call_count[0] += 1
            if n == 0:
                def gen():
                    yield pb.HostPortTunnelResponse(
                        opened=pb.HostPortTunnelOpened(sandbox_port=1, sandbox_address="a")
                    )
                    # Wait until listener is registered before terminating
                    ready_for_second.wait(timeout=3)
                    yield pb.HostPortTunnelResponse(
                        terminated=pb.HostPortTunnelTerminated(
                            reason=pb.HostPortTunnelTerminated.REASON_ENGINE_DRAINING,
                        )
                    )
                return gen()
            else:
                def gen2():
                    yield pb.HostPortTunnelResponse(
                        opened=pb.HostPortTunnelOpened(sandbox_port=2, sandbox_address="b")
                    )
                    ev = threading.Event()
                    ev.wait(timeout=5)
                return gen2()

    rpc = SequencedRPC()
    r = open_resilient_host_port_tunnel(rpc, "sess", "host:1234", initial_backoff=0.01, max_backoff=0.1)

    def track_state(new_state):
        call_log.append(new_state)
        if new_state == "connected" and len(call_log) >= 2:
            state_event.set()

    r.on_state_change(track_state)
    # Signal first tunnel to terminate now that listener is registered
    ready_for_second.set()

    assert state_event.wait(timeout=5), f"state changes: {call_log}"
    r.close()

    assert "reconnecting" in call_log
    assert call_log.count("connected") >= 1


# ---------------------------------------------------------------------------
# ResilientHostPortTunnel: non-retryable reason terminates
# ---------------------------------------------------------------------------

def test_resilient_non_retryable_terminates():
    frames = [
        pb.HostPortTunnelResponse(
            opened=pb.HostPortTunnelOpened(sandbox_port=1, sandbox_address="")
        ),
        pb.HostPortTunnelResponse(
            terminated=pb.HostPortTunnelTerminated(
                reason=pb.HostPortTunnelTerminated.REASON_BIND_FAILED,
                detail="port in use",
            )
        ),
    ]
    rpc = FakeRPC(frames)
    r = open_resilient_host_port_tunnel(rpc, "sess", "host:1234", initial_backoff=0.01)
    # Should terminate without reconnect
    assert r.terminated.wait(timeout=3)
    assert r._termination is not None
    assert r._termination.reason == "host_error"


# ---------------------------------------------------------------------------
# ResilientHostPortTunnel: reconnect updates sandbox_port/address
# ---------------------------------------------------------------------------

def test_resilient_reconnect_updates_port_and_address():
    call_count = [0]
    connected_event = threading.Event()

    class SequencedRPC2:
        def host_port_tunnel(self, request_iter):
            threading.Thread(target=lambda: list(request_iter), daemon=True).start()
            n = call_count[0]
            call_count[0] += 1
            if n == 0:
                def gen():
                    yield pb.HostPortTunnelResponse(
                        opened=pb.HostPortTunnelOpened(sandbox_port=1111, sandbox_address="a:1111")
                    )
                    yield pb.HostPortTunnelResponse(
                        terminated=pb.HostPortTunnelTerminated(
                            reason=pb.HostPortTunnelTerminated.REASON_SESSION_LOST,
                        )
                    )
                return gen()
            else:
                def gen2():
                    yield pb.HostPortTunnelResponse(
                        opened=pb.HostPortTunnelOpened(sandbox_port=2222, sandbox_address="b:2222")
                    )
                    ev = threading.Event()
                    ev.wait(timeout=5)
                return gen2()

    rpc = SequencedRPC2()
    r = open_resilient_host_port_tunnel(rpc, "sess", "host:1234", initial_backoff=0.01, max_backoff=0.1)

    def on_state(s):
        if s == "connected" and call_count[0] >= 2:
            connected_event.set()

    r.on_state_change(on_state)
    assert connected_event.wait(timeout=5)
    assert r.sandbox_port == 2222
    assert r.sandbox_address == "b:2222"
    r.close()


# ---------------------------------------------------------------------------
# sandbox.expose_host_port_resilient (with mocked tunnel)
# ---------------------------------------------------------------------------

def test_sandbox_expose_host_port_resilient_delegation():
    """Ensure Sandbox.expose_host_port_resilient delegates correctly."""
    from unittest.mock import MagicMock, patch

    from tenki_sandbox import Client
    from tenki_sandbox._pb import pb as _pb

    def _session():
        return _pb.SandboxSession(
            id="sess-x",
            state=_pb.SESSION_STATE_RUNNING,
            owner_type="SERVICE",
            owner_id="self",
            name="test",
            project_id="proj-1",
            workspace_id="ws-1",
        )

    class MinimalRPC:
        def create_session(self, req):
            return _pb.CreateSessionResponse(session=_session())

    rpc = MinimalRPC()
    client = Client(rpc=rpc)
    sandbox = client.create(wait=False)

    fake_tunnel = MagicMock(spec=ResilientHostPortTunnel)
    fake_tunnel.sandbox_port = 9876
    fake_tunnel.sandbox_address = "10.0.0.1:9876"

    with patch("tenki_sandbox.tunnel.open_resilient_host_port_tunnel", return_value=fake_tunnel) as mock_fn:
        result = sandbox.expose_host_port_resilient(
            "host:3000",
            sandbox_port=8080,
            initial_backoff=0.5,
        )
        assert result is fake_tunnel
        mock_fn.assert_called_once()
        call_kwargs = mock_fn.call_args
        assert call_kwargs.args[2] == "host:3000"
        assert call_kwargs.kwargs.get("sandbox_port") == 8080
        assert call_kwargs.kwargs.get("initial_backoff") == 0.5
