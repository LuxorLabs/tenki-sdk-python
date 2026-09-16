from __future__ import annotations

import os

import grpc

from ._transport import auth_metadata, grpc_target, resolve_auth_token
from .workspace_secrets import _AsyncSecrets, _Secrets


class _WorkspaceClientBase:
    _async = False

    def __init__(
        self,
        *,
        workspace_id: str = "",
        auth_token: str | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float = 30,
        root_certificates: bytes | None = None,
    ):
        metadata = auth_metadata(resolve_auth_token(auth_token, api_key))
        endpoint = (
            base_url or os.getenv("TENKI_CLOUD_API_URL") or "https://api.tenki.cloud"
        )
        self._target, self._secure = grpc_target(endpoint)
        self._root_certificates = root_certificates
        self._channel = None
        self._closed = False
        if not self._async:
            self._ensure_channel()
        resource = _AsyncSecrets if self._async else _Secrets
        self.secrets = resource(self._ensure_channel, metadata, timeout, workspace_id)

    def _ensure_channel(self):
        # Async channel creation must happen on the first RPC's event loop.
        if self._closed:
            raise RuntimeError("Workspace client is closed")
        if self._channel is None:
            api = grpc.aio if self._async else grpc
            self._channel = (
                api.secure_channel(
                    self._target, grpc.ssl_channel_credentials(self._root_certificates)
                )
                if self._secure
                else api.insecure_channel(self._target)
            )
        return self._channel


class WorkspaceClient(_WorkspaceClientBase):
    """Owns workspace scope, authentication, and connection lifetime."""

    secrets: _Secrets

    def close(self) -> None:
        self._closed = True
        if self._channel is not None:
            self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class AsyncWorkspaceClient(_WorkspaceClientBase):
    """Async workspace client with the same resources as WorkspaceClient."""

    _async = True
    secrets: _AsyncSecrets

    async def close(self) -> None:
        self._closed = True
        if self._channel is not None:
            await self._channel.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()
