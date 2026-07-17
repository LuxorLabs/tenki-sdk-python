from __future__ import annotations

import pytest

from tenki_sandbox import AsyncClient, VolumeNotFoundError
from tenki_sandbox._pb import pb

from _async_fakes import FakeControlRPC


def _vol(state=pb.VOLUME_STATE_AVAILABLE) -> pb.Volume:
    return pb.Volume(id="vol-1", workspace_id="ws-1", name="cache", size_bytes=1024, state=state)


class VolumeRPC(FakeControlRPC):
    def __init__(self, states):
        super().__init__()
        self._states = iter(states)

    async def create_volume(self, req):
        self._record("create_volume", req)
        return pb.CreateVolumeResponse(volume=_vol())

    async def get_volume(self, req):
        return pb.GetVolumeResponse(volume=_vol(state=next(self._states)))


async def test_async_volume_create():
    rpc = VolumeRPC([])
    vol = await AsyncClient(rpc=rpc).volumes.create(workspace_id="ws-1", name="cache", size_bytes=1024)
    assert vol.id == "vol-1"
    assert rpc.requests["create_volume"][-1].name == "cache"


async def test_async_volume_wait_ready_polls():
    rpc = VolumeRPC(
        [pb.VOLUME_STATE_UNSPECIFIED, pb.VOLUME_STATE_UNSPECIFIED, pb.VOLUME_STATE_AVAILABLE]
    )
    vol = await AsyncClient(rpc=rpc).volumes.wait_ready("vol-1", timeout=10)
    assert vol.state == "AVAILABLE"


async def test_async_volume_wait_ready_deleted_raises():
    rpc = VolumeRPC([pb.VOLUME_STATE_DELETED])
    with pytest.raises(VolumeNotFoundError):
        await AsyncClient(rpc=rpc).volumes.wait_ready("vol-1", timeout=5)


async def test_async_volume_wait_ready_timeout():
    rpc = VolumeRPC([pb.VOLUME_STATE_UNSPECIFIED] * 100)
    with pytest.raises(TimeoutError, match="vol-1"):
        await AsyncClient(rpc=rpc).volumes.wait_ready("vol-1", timeout=0.01)


async def test_async_snapshot_update_normalizes_tags():
    rpc = FakeControlRPC()
    await AsyncClient(rpc=rpc).snapshots.update("snap-1", tags=["  Prod ", "EU", "  "])
    req = rpc.requests["update_snapshot"][-1]
    assert list(req.tags) == ["prod", "eu"]
    assert req.clear_tags is False


async def test_async_template_build_forwards_environment_and_secrets():
    rpc = FakeControlRPC()
    build_env = {"NODE_ENV": "production"}
    build_secrets = {"GIT_TOKEN": "token", "NPM_TOKEN": "npm"}

    build = await AsyncClient(rpc=rpc).templates.build(
        "template-1", build_env=build_env, build_secrets=build_secrets
    )
    build_env["NODE_ENV"] = "mutated"
    build_secrets["GIT_TOKEN"] = "mutated"

    assert build.state == "READY"
    assert dict(rpc.requests["build_template"][-1].build_env) == {
        "NODE_ENV": "production"
    }
    assert dict(rpc.requests["build_template"][-1].build_secrets) == {
        "GIT_TOKEN": "token",
        "NPM_TOKEN": "npm",
    }
