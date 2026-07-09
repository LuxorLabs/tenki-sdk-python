from __future__ import annotations

import pytest

from tenki_sandbox import Client, VolumeNotFoundError
from tenki_sandbox._pb import pb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _vol(state=pb.VOLUME_STATE_AVAILABLE) -> pb.Volume:
    return pb.Volume(id="vol-1", workspace_id="ws-1", name="cache", size_bytes=1024, state=state)


class FakeRPC:
    def __init__(self, states):
        self._states = iter(states)

    def get_volume(self, req):
        state = next(self._states)
        return pb.GetVolumeResponse(volume=_vol(state=state))


# ---------------------------------------------------------------------------
# volumes.wait_ready
# ---------------------------------------------------------------------------

def test_wait_ready_returns_when_available():
    rpc = FakeRPC([pb.VOLUME_STATE_AVAILABLE])
    vol = Client(rpc=rpc).volumes.wait_ready("vol-1", timeout=5)
    assert vol.state == "AVAILABLE"


def test_wait_ready_polls_until_available():
    rpc = FakeRPC([
        pb.VOLUME_STATE_UNSPECIFIED,
        pb.VOLUME_STATE_UNSPECIFIED,
        pb.VOLUME_STATE_AVAILABLE,
    ])
    vol = Client(rpc=rpc).volumes.wait_ready("vol-1", timeout=10)
    assert vol.state == "AVAILABLE"


def test_wait_ready_raises_on_deleted():
    rpc = FakeRPC([pb.VOLUME_STATE_DELETED])
    with pytest.raises(VolumeNotFoundError):
        Client(rpc=rpc).volumes.wait_ready("vol-1", timeout=5)


def test_wait_ready_raises_on_deleting():
    rpc = FakeRPC([pb.VOLUME_STATE_DELETING])
    with pytest.raises(RuntimeError, match="terminal state: DELETING"):
        Client(rpc=rpc).volumes.wait_ready("vol-1", timeout=5)


def test_wait_ready_timeout():
    rpc = FakeRPC([pb.VOLUME_STATE_UNSPECIFIED] * 100)
    with pytest.raises(TimeoutError, match="vol-1"):
        Client(rpc=rpc).volumes.wait_ready("vol-1", timeout=0.01)
