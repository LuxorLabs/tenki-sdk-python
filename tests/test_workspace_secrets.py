import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from uuid import UUID

import grpc
import pytest
from tenki import (
    AsyncWorkspaceClient,
    SecretPolicy,
    WorkspaceClient,
    WorkspaceSecretError,
)
from tenki_sandbox._internal.proto.tenki.cloud.workspace.v1beta1 import (
    secrets_pb2 as pb,
)
from tenki_sandbox._internal.proto.tenki.cloud.workspace.v1beta1 import secrets_pb2_grpc


def metadata():
    return pb.Secret(
        id="secret",
        workspace_id="workspace",
        name="TOKEN",
        active_version=1,
        revision=2,
        policy=pb.SecretPolicy(
            delivery_mode=pb.SECRET_DELIVERY_MODE_GUEST_AND_INJECTION,
            destination_mode=pb.SECRET_DESTINATION_MODE_UNSET,
        ),
    )


@pytest.fixture
def endpoint():
    requests = []
    headers = []
    failure = []

    class Handler(secrets_pb2_grpc.WorkspaceSecretsServiceServicer):
        pass

    def method(name):
        def call(self, request, context):
            requests.append(request)
            headers.append(dict(context.invocation_metadata()))
            if failure:
                context.abort(failure[0], "revision conflict")
            if name == "ListSecrets":
                return pb.ListSecretsResponse(secrets=[metadata()], next_cursor="next")
            if name == "ListSecretVersions":
                return pb.ListSecretVersionsResponse(
                    versions=[pb.SecretVersion(version=1)], next_cursor="versions-next"
                )
            return getattr(pb, name + "Response")(secret=metadata())

        return call

    for name in (
        "CreateSecret",
        "UpdateSecret",
        "GetSecret",
        "ListSecrets",
        "ListSecretVersions",
        "RevokeSecret",
        "DeleteSecret",
    ):
        setattr(Handler, name, method(name))
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    secrets_pb2_grpc.add_WorkspaceSecretsServiceServicer_to_server(Handler(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"http://127.0.0.1:{port}", requests, headers, failure
    server.stop(0).wait()


async def invoke(client, name, *args, **kwargs):
    resource = client if name == "close" else client.secrets
    result = getattr(resource, name)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.mark.parametrize("client_type", [WorkspaceClient, AsyncWorkspaceClient])
@pytest.mark.parametrize("workspace_id", ["workspace", ""])
async def test_wire_contract(endpoint, client_type, workspace_id):
    url, requests, headers, _ = endpoint
    client = client_type(workspace_id=workspace_id, auth_token="tk_test", base_url=url)
    policy = SecretPolicy("guest_and_injection", "unset")
    try:
        result = await invoke(
            client,
            "create",
            "TOKEN",
            b"\x00\xff\n\x80",
            policy,
            request_id="00000000-0000-4000-8000-000000000001",
        )
        await invoke(client, "create", "EMPTY", b"", policy)
        await invoke(client, "update", "secret", expected_revision=2)
        await invoke(
            client,
            "update",
            "secret",
            expected_revision=2,
            value=b"",
            request_id="00000000-0000-4000-8000-000000000002",
        )
        await invoke(
            client,
            "update",
            "secret",
            expected_revision=2,
            active_version=1,
            request_id="00000000-0000-4000-8000-000000000003",
        )
        await invoke(client, "get", "secret")
        page = await invoke(client, "list", page_size=2, cursor="current")
        versions = await invoke(client, "list_versions", "secret", cursor="current")
        await invoke(client, "revoke", "secret", expected_revision=2)
        await invoke(
            client,
            "revoke",
            "secret",
            expected_revision=2,
            version=1,
            request_id="00000000-0000-4000-8000-000000000004",
        )
        await invoke(
            client,
            "delete",
            "secret",
            expected_revision=2,
            request_id="00000000-0000-4000-8000-000000000005",
        )
        assert all(header["authorization"] == "Bearer tk_test" for header in headers)
        assert requests[0].value == b"\x00\xff\n\x80"
        assert requests[0].request_id == "00000000-0000-4000-8000-000000000001"
        assert requests[0].workspace_id == workspace_id
        assert requests[1].HasField("value") and requests[1].value == b""
        assert all(request.workspace_id == workspace_id for request in requests)
        UUID(requests[1].request_id)
        assert not requests[2].HasField("value")
        assert requests[3].HasField("value") and requests[3].value == b""
        assert requests[3].expected_revision == 2
        assert requests[3].request_id == "00000000-0000-4000-8000-000000000002"
        assert requests[4].request_id == "00000000-0000-4000-8000-000000000003"
        assert requests[4].active_version == 1
        assert page.next_cursor == "next" and len(page.secrets) == 1
        assert (
            versions.next_cursor == "versions-next"
            and versions.versions[0].version == 1
        )
        assert not requests[8].HasField("version")
        assert requests[9].version == 1 and requests[9].expected_revision == 2
        assert requests[10].request_id == "00000000-0000-4000-8000-000000000005"
        assert "value" not in asdict(result) and "ciphertext" not in asdict(result)
        assert result.policy == policy
    finally:
        await invoke(client, "close")


@pytest.mark.parametrize("client_type", [WorkspaceClient, AsyncWorkspaceClient])
@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.ALREADY_EXISTS,
    ],
)
async def test_conflict_replay(endpoint, client_type, code):
    url, requests, _, failure = endpoint
    failure.append(code)
    client = client_type(workspace_id="workspace", auth_token="tk_test", base_url=url)
    try:
        for _ in range(2):
            with pytest.raises(WorkspaceSecretError) as caught:
                await invoke(
                    client,
                    "update",
                    "secret",
                    expected_revision=1,
                    request_id="00000000-0000-4000-8000-000000000002",
                )
            assert caught.value.code == code
        assert [request.request_id for request in requests] == [
            "00000000-0000-4000-8000-000000000002",
            "00000000-0000-4000-8000-000000000002",
        ]
    finally:
        await invoke(client, "close")


def test_sync_async_signature_parity():
    from tenki_sandbox.workspace_secrets import _AsyncSecrets, _Secrets

    for name in (
        "create",
        "update",
        "get",
        "list",
        "list_versions",
        "revoke",
        "delete",
    ):
        assert inspect.signature(getattr(_Secrets, name)) == inspect.signature(
            getattr(_AsyncSecrets, name)
        )
        assert inspect.iscoroutinefunction(getattr(_AsyncSecrets, name))


@pytest.mark.parametrize("client_type", [WorkspaceClient, AsyncWorkspaceClient])
async def test_parent_context_closes_resource_connection(endpoint, client_type):
    from unittest.mock import AsyncMock, Mock

    url, _, _, _ = endpoint
    client = client_type(workspace_id="workspace", auth_token="tk_test", base_url=url)
    await invoke(client, "get", "secret")
    original_close = client._channel.close
    if isinstance(client, AsyncWorkspaceClient):
        close = AsyncMock(wraps=original_close)
        client._channel.close = close
        async with client as workspace:
            assert (await workspace.secrets.get("secret")).name == "TOKEN"
        close.assert_awaited_once()
    else:
        close = Mock(wraps=original_close)
        client._channel.close = close
        with client as workspace:
            assert workspace.secrets.get("secret").name == "TOKEN"
        close.assert_called_once()


@pytest.mark.parametrize("construction", ["outside-loop", "other-loop"])
def test_async_client_binds_on_first_use(endpoint, construction):
    url, requests, _, _ = endpoint
    asyncio.set_event_loop(None)

    def make_client():
        return AsyncWorkspaceClient(
            workspace_id="workspace", auth_token="tk_test", base_url=url
        )

    if construction == "other-loop":

        async def construct():
            return make_client()

        client = asyncio.run(construct())
    else:
        client = make_client()
    get_secret = client.secrets.get

    async def use_client():
        async with client:
            results = await asyncio.gather(get_secret("secret"), get_secret("secret"))
            assert [result.name for result in results] == ["TOKEN", "TOKEN"]

    asyncio.run(use_client())
    assert len(requests) == 2


def test_async_client_close_before_first_use():
    asyncio.set_event_loop(None)
    client = AsyncWorkspaceClient(auth_token="tk_test", base_url="http://127.0.0.1:1")

    async def close_unused():
        await client.close()
        await client.close()
        with pytest.raises(RuntimeError, match="closed"):
            await client.secrets.get("secret")

    asyncio.run(close_unused())
