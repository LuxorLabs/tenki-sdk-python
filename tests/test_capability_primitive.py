from __future__ import annotations

import pytest

from tenki_sandbox._pb import pb
from tenki_sandbox.aio.dial import open_dial_conn as open_dial_conn_async
from tenki_sandbox.aio.tunnel import open_host_port_tunnel as open_host_port_tunnel_async
from tenki_sandbox.dial import open_dial_conn
from tenki_sandbox.errors import CapabilityUnavailableError, PrimitiveTimeoutError
from tenki_sandbox.process import _command_result_from_exit
from tenki_sandbox.tunnel import open_host_port_tunnel

# Go and TypeScript name these primitives "dial", "host_port_tunnel" and "run".
UNAVAILABLE = "not supported"
TIMEOUT = "first_frame_timeout: no guest agent response"


class _RPC:
    def __init__(self, frame) -> None:
        self._frame = frame

    def dial(self, _requests, timeout=None):
        return iter([self._frame])

    def host_port_tunnel(self, _requests, timeout=None):
        return iter([self._frame])


class _AsyncRPC(_RPC):
    def _stream(self):
        async def frames():
            yield self._frame

        return frames()

    def dial(self, _requests, timeout=None):
        return self._stream()

    def host_port_tunnel(self, _requests, timeout=None):
        return self._stream()


def _dial_closed(detail: str):
    return pb.DialResponse(closed=pb.DialClosed(reason=pb.DialClosed.REASON_CAPABILITY_UNAVAILABLE, detail=detail))


def _tunnel_terminated(detail: str):
    return pb.HostPortTunnelResponse(
        terminated=pb.HostPortTunnelTerminated(
            reason=pb.HostPortTunnelTerminated.REASON_CAPABILITY_UNAVAILABLE, detail=detail
        )
    )


@pytest.mark.parametrize(("detail", "error"), [(UNAVAILABLE, CapabilityUnavailableError), (TIMEOUT, PrimitiveTimeoutError)])
def test_dial_names_its_primitive(detail, error):
    with pytest.raises(error) as excinfo:
        open_dial_conn(_RPC(_dial_closed(detail)), "sess", "/sock")
    assert excinfo.value.primitive == "dial"
    assert str(excinfo.value) == detail


@pytest.mark.parametrize(("detail", "error"), [(UNAVAILABLE, CapabilityUnavailableError), (TIMEOUT, PrimitiveTimeoutError)])
async def test_async_dial_names_its_primitive(detail, error):
    with pytest.raises(error) as excinfo:
        await open_dial_conn_async(_AsyncRPC(_dial_closed(detail)), "sess", "/sock")
    assert excinfo.value.primitive == "dial"
    assert str(excinfo.value) == detail


@pytest.mark.parametrize(("detail", "error"), [(UNAVAILABLE, CapabilityUnavailableError), (TIMEOUT, PrimitiveTimeoutError)])
def test_tunnel_names_its_primitive(detail, error):
    with pytest.raises(error) as excinfo:
        open_host_port_tunnel(_RPC(_tunnel_terminated(detail)), "sess", "host:1234")
    assert excinfo.value.primitive == "host_port_tunnel"
    assert str(excinfo.value) == detail


@pytest.mark.parametrize(("detail", "error"), [(UNAVAILABLE, CapabilityUnavailableError), (TIMEOUT, PrimitiveTimeoutError)])
async def test_async_tunnel_names_its_primitive(detail, error):
    with pytest.raises(error) as excinfo:
        await open_host_port_tunnel_async(_AsyncRPC(_tunnel_terminated(detail)), "sess", "host:1234")
    assert excinfo.value.primitive == "host_port_tunnel"
    assert str(excinfo.value) == detail


@pytest.mark.parametrize(
    ("reason", "error"),
    [("capability_unavailable: guest lacks run", CapabilityUnavailableError), ("first_frame_timeout: no reply", PrimitiveTimeoutError)],
)
def test_run_names_its_primitive(reason, error):
    with pytest.raises(error) as excinfo:
        _command_result_from_exit(pb.RunExit(reason=reason), ["true"], b"", b"")
    assert excinfo.value.primitive == "run"


def test_unavailable_without_detail_gets_the_typescript_default_message():
    with pytest.raises(CapabilityUnavailableError) as excinfo:
        open_dial_conn(_RPC(_dial_closed("")), "sess", "/sock")
    assert str(excinfo.value) == "sandbox capability unavailable: dial"
