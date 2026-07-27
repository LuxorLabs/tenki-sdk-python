from __future__ import annotations

import asyncio

from tenki_sandbox import Client
from tenki_sandbox._pb import pb
from tenki_sandbox.aio import AsyncClient


class FakeRPC:
    def __init__(self):
        self.list_req = None

    def list_workspace_sandboxes(self, req):
        self.list_req = req
        return pb.ListWorkspaceSandboxesResponse(sessions=[])


def test_include_terminated_defaults_to_false():
    rpc = FakeRPC()
    Client(rpc=rpc).list()
    assert rpc.list_req.include_terminated is False


def test_include_terminated_forwarded():
    rpc = FakeRPC()
    Client(rpc=rpc).list(include_terminated=True)
    assert rpc.list_req.include_terminated is True


class AsyncFakeRPC:
    def __init__(self):
        self.list_req = None

    async def list_workspace_sandboxes(self, req):
        self.list_req = req
        return pb.ListWorkspaceSandboxesResponse(sessions=[])


def test_async_include_terminated_forwarded():
    async def run():
        rpc = AsyncFakeRPC()
        await AsyncClient(rpc=rpc).list(include_terminated=True)
        assert rpc.list_req.include_terminated is True

        rpc2 = AsyncFakeRPC()
        await AsyncClient(rpc=rpc2).list()
        assert rpc2.list_req.include_terminated is False

    asyncio.run(run())
