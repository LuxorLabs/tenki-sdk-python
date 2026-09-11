from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tenki_sandbox.aio.sandbox import AsyncSandbox
from tenki_sandbox.models import SandboxInfo, SessionEgressPolicy
from tenki_sandbox.sandbox import Sandbox


def test_egress_tracks_refresh():
    policy = SessionEgressPolicy(allow_domains=("example.com",))
    client = SimpleNamespace(
        get=Mock(
            return_value=SimpleNamespace(
                info=SandboxInfo(id="session", egress=policy),
            )
        )
    )
    sandbox = Sandbox(client, SandboxInfo(id="session"))
    assert sandbox.egress == SessionEgressPolicy()
    sandbox.refresh()
    assert sandbox.egress == policy
    client.get.return_value.info = SandboxInfo(id="session")
    sandbox.refresh()
    assert sandbox.egress == SessionEgressPolicy()


@pytest.mark.asyncio
async def test_async_egress_tracks_refresh():
    policy = SessionEgressPolicy(allow_cidrs=("203.0.113.0/24",))
    client = SimpleNamespace(
        get=AsyncMock(
            return_value=SimpleNamespace(
                info=SandboxInfo(id="session", egress=policy),
            )
        )
    )
    sandbox = AsyncSandbox(client, SandboxInfo(id="session"))
    assert sandbox.egress == SessionEgressPolicy()
    await sandbox.refresh()
    assert sandbox.egress == policy
    client.get.return_value.info = SandboxInfo(id="session")
    await sandbox.refresh()
    assert sandbox.egress == SessionEgressPolicy()
