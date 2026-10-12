from __future__ import annotations

import asyncio
import pprint

from tenki_sandbox import (
    AsyncClient,
    AsyncWorkspaceClient,
    Client,
    Sandbox,
    WorkspaceClient,
)
from tenki_sandbox._pb import pb
from tenki_sandbox._transport import GrpcConfig
from tenki_sandbox.aio.data_plane import AsyncDataPlaneCredentialManager
from tenki_sandbox.aio.sandbox import AsyncSandbox
from tenki_sandbox.data_plane import DataPlaneCredentialManager
from tenki_sandbox.models import SandboxInfo

AUTH_TOKEN = "tk_dummy-auth-token-private"
CREDENTIAL = "dummy-session-credential-private"


def rendered(*values: object) -> str:
    return "\n".join(f"{value!s} {value!r} {pprint.pformat(value)}" for value in values)


def test_sync_objects_redact_credentials():
    client = Client(auth_token=AUTH_TOKEN, base_url="http://127.0.0.1:1")
    workspace = WorkspaceClient(auth_token=AUTH_TOKEN, base_url="http://127.0.0.1:1")
    credential = pb.SessionCredential(credential=CREDENTIAL)
    sandbox = Sandbox(
        client,
        SandboxInfo(id="session-privacy", state="RUNNING"),
        data_plane_credential=credential,
    )
    manager = DataPlaneCredentialManager(sandbox, credential)
    try:
        assert ("authorization", f"Bearer {AUTH_TOKEN}") in workspace.secrets._metadata
        assert sandbox._data_plane_credentials.current() == CREDENTIAL
        output = rendered(
            client,
            workspace,
            workspace.secrets,
            vars(workspace),
            sandbox,
            manager,
            GrpcConfig(base_url="http://127.0.0.1:1", auth_token=AUTH_TOKEN),
        )
        assert AUTH_TOKEN not in output
        assert CREDENTIAL not in output
        assert "REDACTED" in repr(client)
        assert "session-privacy" in repr(sandbox)
    finally:
        manager.stop()
        workspace.close()
        client.close()


def test_async_objects_redact_credentials():
    async def scenario() -> str:
        client = AsyncClient(auth_token=AUTH_TOKEN, base_url="http://127.0.0.1:1")
        workspace = AsyncWorkspaceClient(
            auth_token=AUTH_TOKEN, base_url="http://127.0.0.1:1"
        )
        credential = pb.SessionCredential(credential=CREDENTIAL)
        sandbox = AsyncSandbox(
            client,
            SandboxInfo(id="session-privacy", state="RUNNING"),
            data_plane_credential=credential,
        )
        manager = AsyncDataPlaneCredentialManager(sandbox, credential)
        try:
            assert ("authorization", f"Bearer {AUTH_TOKEN}") in workspace.secrets._metadata
            assert manager.current() == CREDENTIAL
            return rendered(
                client, workspace, workspace.secrets, vars(workspace), sandbox, manager
            )
        finally:
            manager.stop()
            await workspace.close()
            await client.close()

    output = asyncio.run(scenario())
    assert AUTH_TOKEN not in output
    assert CREDENTIAL not in output
    assert "REDACTED" in output
