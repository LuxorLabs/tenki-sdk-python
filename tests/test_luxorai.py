"""LUXORAI_* environment precedence, lx_ API keys and SDK identity headers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from importlib.metadata import version

import grpc
import luxor_ai
import pytest
import tenki_sandbox
from _client_metadata import EXPECTED_CLIENT_METADATA
from tenki_sandbox import AsyncClient, Client, WorkspaceClient
from tenki_sandbox._internal.proto.tenki.cloud.workspace.v1beta1 import secrets_pb2 as pb
from tenki_sandbox._internal.proto.tenki.cloud.workspace.v1beta1 import secrets_pb2_grpc
from tenki_sandbox._pb import pb as sandbox_pb
from tenki_sandbox._transport import (
    CHANNEL_OPTIONS,
    auth_metadata,
    resolve_auth_token,
    resolve_gateway_url,
)
from tenki_sandbox.aio.data_plane import (
    AsyncDataPlaneCredentialManager,
    AsyncGrpcDataPlaneRPC,
)
from tenki_sandbox.data_plane import DataPlaneCredentialManager, GrpcDataPlaneRPC

SDK_VERSION = version("tenki")
ENV_NAMES = (
    "LUXORAI_AUTH_TOKEN",
    "LUXORAI_API_KEY",
    "LUXORAI_API_ENDPOINT",
    "LUXORAI_API_URL",
    "LUXORAI_SANDBOX_GATEWAY_URL",
    "LUXORAI_CLOUD_API_URL",
    "TENKI_AUTH_TOKEN",
    "TENKI_API_KEY",
    "TENKI_API_ENDPOINT",
    "TENKI_API_URL",
    "TENKI_SANDBOX_GATEWAY_URL",
    "TENKI_CLOUD_API_URL",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_luxorai_auth_token_wins_over_tenki(monkeypatch):
    monkeypatch.setenv("TENKI_AUTH_TOKEN", "tk_tenki_auth")
    monkeypatch.setenv("TENKI_API_KEY", "tk_tenki_key")
    monkeypatch.setenv("LUXORAI_API_KEY", "lx_luxorai_key")
    assert resolve_auth_token(None) == "lx_luxorai_key"
    monkeypatch.setenv("LUXORAI_AUTH_TOKEN", "lx_luxorai_auth")
    assert resolve_auth_token(None) == "lx_luxorai_auth"


def test_tenki_auth_token_is_the_fallback(monkeypatch):
    monkeypatch.setenv("TENKI_API_KEY", "tk_tenki_key")
    assert resolve_auth_token(None) == "tk_tenki_key"


def test_whitespace_luxorai_auth_token_falls_back_to_tenki(monkeypatch):
    monkeypatch.setenv("LUXORAI_AUTH_TOKEN", " \t ")
    monkeypatch.setenv("TENKI_AUTH_TOKEN", "tk_tenki_auth")
    assert resolve_auth_token(None) == "tk_tenki_auth"


@pytest.mark.parametrize("client_type", [Client, AsyncClient])
def test_luxorai_urls_win_over_tenki(monkeypatch, client_type):
    monkeypatch.setenv("TENKI_API_ENDPOINT", "https://api.tenki.example")
    monkeypatch.setenv("TENKI_SANDBOX_GATEWAY_URL", "wss://gateway.tenki.example")
    monkeypatch.setenv("LUXORAI_API_URL", "https://api.luxor.example")
    monkeypatch.setenv("LUXORAI_SANDBOX_GATEWAY_URL", "wss://gateway.luxor.example")
    client = client_type(auth_token="lx_test", rpc=object())
    assert client.base_url == "https://api.luxor.example"
    assert client.gateway_url == "wss://gateway.luxor.example"
    assert client._gateway_explicit


@pytest.mark.parametrize("client_type", [Client, AsyncClient])
def test_tenki_urls_are_the_fallback(monkeypatch, client_type):
    monkeypatch.setenv("TENKI_API_URL", "https://api.tenki.example")
    client = client_type(auth_token="tk_test", rpc=object())
    assert client.base_url == "https://api.tenki.example"
    assert client.gateway_url == "wss://sandbox-gateway.tenki.example"
    assert not client._gateway_explicit


def test_luxorai_api_endpoint_derives_luxor_gateway(monkeypatch):
    monkeypatch.setenv("LUXORAI_API_ENDPOINT", "https://api.luxor.ai")
    monkeypatch.setenv("LUXORAI_API_URL", "https://ignored.luxor.example")
    client = Client(auth_token="lx_test", rpc=object())
    assert client.base_url == "https://api.luxor.ai"
    assert client.gateway_url == "wss://sandbox-gateway.luxor.ai"
    assert resolve_gateway_url(None, "https://api.luxor.ai") == "wss://sandbox-gateway.luxor.ai"


@pytest.mark.parametrize("token", ["lx_key", "lx_svc_token", "tk_key", "tk_svc_token"])
def test_accepts_lx_and_tk_prefixes(token):
    assert resolve_auth_token(token) == token


def test_rejects_other_prefixes():
    with pytest.raises(tenki_sandbox.InvalidAuthTokenError, match="lx_ or tk_"):
        resolve_auth_token("lxr_token")


def test_luxor_ai_reexports_tenki_sandbox():
    assert luxor_ai.Sandbox is tenki_sandbox.Sandbox
    assert luxor_ai.Client is tenki_sandbox.Client
    assert set(luxor_ai.__all__) == set(tenki_sandbox.__all__)


def test_control_plane_metadata_and_user_agent():
    assert auth_metadata("lx_test") == (
        ("authorization", "Bearer lx_test"),
        *EXPECTED_CLIENT_METADATA.items(),
    )
    assert dict(CHANNEL_OPTIONS)["grpc.primary_user_agent"] == f"luxor-sandbox-python/{SDK_VERSION}"


def _credential(value: str) -> sandbox_pb.SessionCredential:
    credential = sandbox_pb.SessionCredential(credential=value)
    credential.expires_at.FromDatetime(datetime.now(timezone.utc) + timedelta(hours=1))
    return credential


class _FakeSandbox:
    id = "sess-1"
    _client = None


def test_sync_data_plane_sends_client_metadata():
    seen = []

    class Stub:
        def Stat(self, request, *, metadata, **kwargs):
            seen.append(dict(metadata))
            return sandbox_pb.SandboxSessionDataPlaneServiceStatResponse(
                response=sandbox_pb.StatResponse()
            )

    endpoint = "http://127.0.0.1:1"
    credentials = DataPlaneCredentialManager(_FakeSandbox(), _credential("cred"), endpoint)
    rpc = GrpcDataPlaneRPC(endpoint, credentials, "sess-1")
    try:
        rpc._stub = Stub()
        rpc.stat(sandbox_pb.StatRequest(session_id="sess-1", path="/"))
    finally:
        rpc.close()
    assert seen[0]["x-luxor-client-family"] == "python"
    assert seen[0]["x-tenki-session-cert"] == "cred"


def test_async_data_plane_sends_client_metadata():
    endpoint = "http://127.0.0.1:1"
    credentials = AsyncDataPlaneCredentialManager(_FakeSandbox(), _credential("cred"), endpoint)
    rpc = AsyncGrpcDataPlaneRPC(endpoint, credentials, "sess-1")
    metadata = dict(rpc._auth_metadata({}, sandbox_pb.StatRequest(session_id="sess-1", path="/")))
    assert metadata["x-luxor-client-family"] == "python"
    assert metadata["x-tenki-session-cert"] == "cred"


@pytest.fixture
def workspace_endpoint():
    seen = []

    class Handler(secrets_pb2_grpc.WorkspaceSecretsServiceServicer):
        def GetSecret(self, request, context):
            seen.append(dict(context.invocation_metadata()))
            return pb.GetSecretResponse(secret=pb.Secret(id="secret"))

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    secrets_pb2_grpc.add_WorkspaceSecretsServiceServicer_to_server(Handler(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"http://127.0.0.1:{port}", seen
    server.stop(0).wait()


def test_workspace_client_reads_luxorai_env_and_sends_sdk_identity(monkeypatch, workspace_endpoint):
    endpoint, seen = workspace_endpoint
    monkeypatch.setenv("TENKI_CLOUD_API_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("LUXORAI_CLOUD_API_URL", endpoint)
    monkeypatch.setenv("TENKI_AUTH_TOKEN", "tk_tenki")
    monkeypatch.setenv("LUXORAI_AUTH_TOKEN", "lx_luxorai")
    with WorkspaceClient() as client:
        client.secrets.get("secret")
    assert seen[0]["authorization"] == "Bearer lx_luxorai"
    assert seen[0]["x-luxor-client-family"] == "python"
    assert seen[0]["user-agent"].startswith(f"luxor-sandbox-python/{SDK_VERSION} ")


def test_workspace_client_falls_back_to_tenki_env(monkeypatch, workspace_endpoint):
    endpoint, seen = workspace_endpoint
    monkeypatch.setenv("TENKI_CLOUD_API_URL", endpoint)
    monkeypatch.setenv("TENKI_API_KEY", "tk_tenki")
    with WorkspaceClient() as client:
        client.secrets.get("secret")
    assert seen[0]["authorization"] == "Bearer tk_tenki"
