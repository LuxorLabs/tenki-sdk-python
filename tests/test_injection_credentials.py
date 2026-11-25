import base64
import inspect
import os
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock
import traceback
import warnings

import grpc
import pytest
from google.protobuf.json_format import MessageToDict

from tenki_sandbox import (
    Client, AsyncClient, InjectionCredentialInput, InjectionRule,
    PermissionDeniedError, InvalidStateError, SandboxError, WaitReadyFailedError,
    TailnetAttachment, TailnetPort, TemplateSpec,
)
from tenki_sandbox._pb import pb
from tenki_sandbox.injection_credentials import credentials_pb

CANARY = b"SYNTHETIC_CANARY_P4_NOT_A_CREDENTIAL"


def input_value():
    return InjectionCredentialInput("SYNTHETIC", CANARY, [
        InjectionRule("https://example.invalid", ["GET"], "/repo", "Authorization"),
    ])


class RPCError(Exception):
    def __init__(self, code):
        self.status = code
        super().__init__(CANARY.decode())

    def code(self):
        return self.status

    def details(self):
        return CANARY.decode()


class RPC:
    _accepts_call_timeout = False
    code = None
    acknowledge = False
    public_egress = None

    def __init__(self):
        self.calls = []
        self.warnings = []

    def __getattr__(self, name):
        def call(request):
            self.calls.append((name, request))
            if self.code:
                raise RPCError(self.code)
            metadata = credentials_pb.SessionInjectionCredentialMetadata(
                credential_id="credential-synthetic", name="SYNTHETIC", revision=7,
                revoked=name == "revoke_session_injection_credential",
            )
            created = pb.CreateSessionResponse(
                session=pb.SandboxSession(
                    id="session-synthetic", state=pb.SESSION_STATE_RUNNING,
                ),
                warnings=self.warnings,
            )
            if self.public_egress is not None:
                created.session.egress.public_egress.CopyFrom(self.public_egress)
            if name == "create_session_with_public_egress":
                return pb.CreateSessionWithPublicEgressResponse(response=created)
            session_request = request.session if name == "create_session_with_injection_credentials" else request
            if self.acknowledge and name in {"create_session", "create_session_with_injection_credentials"}:
                if session_request.tailnet.HasField("accept_dns"):
                    created.session.tailnet_status.accept_dns = session_request.tailnet.accept_dns
                created.session.tailnet_status.exposed_ports.extend(session_request.tailnet.exposed_ports)
            if name == "terminate_session":
                return pb.TerminateSessionResponse()
            if name == "create_session":
                return created
            if name == "create_session_with_injection_credentials":
                return pb.CreateSessionWithInjectionCredentialsResponse(created=created, credentials=[metadata])
            if name == "set_session_injection_credential":
                return credentials_pb.SetSessionInjectionCredentialResponse(credential=metadata)
            if name == "revoke_session_injection_credential":
                return credentials_pb.RevokeSessionInjectionCredentialResponse(credential=metadata)
            raise AssertionError("unexpected RPC")
        return call


class AsyncRPC(RPC):
    def __getattr__(self, name):
        async def call(request):
            return super(AsyncRPC, self).__getattr__(name)(request)
        return call


@pytest.mark.parametrize("client_type", [Client, AsyncClient])
def test_credential_create_matches_ordinary_keyword_signature(client_type):
    ordinary = inspect.signature(client_type.create).parameters
    credential = inspect.signature(client_type.create_with_injection_credentials).parameters
    assert list(ordinary.values())[1:] == list(credential.values())[2:]
    names = list(ordinary)
    assert names[names.index("github_token") + 1] == "github_token_injection"
    assert ordinary["github_token_injection"].default is False


@pytest.mark.parametrize("client_type", [Client, AsyncClient])
def test_credential_create_rejects_unknown_keyword_at_call(client_type):
    rpc = RPC()
    with pytest.raises(TypeError, match="unexpected keyword argument 'unknown'"):
        client_type(rpc=rpc).create_with_injection_credentials([], unknown=True)
    assert rpc.calls == []


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["create", "credential_create", "owned_client_create"])
async def test_create_warning_points_at_callers_line(asynchronous, operation, monkeypatch):
    from tenki_sandbox import Sandbox, AsyncSandbox, SandboxWarningNotification

    rpc = AsyncRPC() if asynchronous else RPC()
    rpc.warnings = [pb.SandboxWarning(
        code=pb.SANDBOX_WARNING_CODE_STICKY_OVERRIDES_MAX_DURATION,
        message="synthetic create warning",
    )]
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    module = "tenki_sandbox.aio.client" if asynchronous else "tenki_sandbox.client"
    monkeypatch.setattr(f"{module}.{'AsyncClient' if asynchronous else 'Client'}", lambda **kwargs: client)

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", SandboxWarningNotification)
        if asynchronous:
            if operation == "create":
                line = inspect.currentframe().f_lineno + 1
                sandbox = await client.create(wait=False)
            elif operation == "credential_create":
                line = inspect.currentframe().f_lineno + 1
                sandbox, _ = await client.create_with_injection_credentials([], wait=False)
            else:
                line = inspect.currentframe().f_lineno + 1
                sandbox = await AsyncSandbox.create(wait=False)
        else:
            if operation == "create":
                line = inspect.currentframe().f_lineno + 1
                sandbox = client.create(wait=False)
            elif operation == "credential_create":
                line = inspect.currentframe().f_lineno + 1
                sandbox, _ = client.create_with_injection_credentials([], wait=False)
            else:
                line = inspect.currentframe().f_lineno + 1
                sandbox = Sandbox.create(wait=False)

    assert len(captured) == 1
    assert captured[0].filename == __file__
    assert captured[0].lineno == line
    assert sandbox._owns_client == (operation == "owned_client_create")


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_wire_revisions_and_ordinary_create(asynchronous):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)

    async def result(value):
        return await value if asynchronous else value

    options = dict(name="synthetic", env={"EXISTING": "unchanged"}, github_token="SYNTHETIC_GITHUB_CANARY", wait=False)
    await result(client.create(**options))
    ordinary = rpc.calls[-1][1]
    session, metadata = await result(client.create_with_injection_credentials([input_value()], **options))
    request = rpc.calls[-1][1]
    assert request.session == ordinary
    assert MessageToDict(request.credentials[0]) == {
        "name": "SYNTHETIC", "value": base64.b64encode(CANARY).decode(),
        "rules": [{"origin": "https://example.invalid", "methods": ["GET"], "pathPrefix": "/repo", "header": "Authorization"}],
    }
    assert metadata[0].revision == 7
    assert not hasattr(metadata[0], "value")
    for revision in [None, 0, 7]:
        assert await result(session.set_injection_credential(input_value(), revision)) == metadata[0]
        set_request = rpc.calls[-1][1]
        assert set_request.session_id == session.id
        assert set_request.credential == request.credentials[0]
        assert set_request.HasField("expected_revision") == (revision is not None)
        assert set_request.expected_revision == (revision or 0)
        assert (await result(session.revoke_injection_credential("SYNTHETIC", revision))).revoked
        revoke_request = rpc.calls[-1][1]
        assert revoke_request.session_id == session.id
        assert revoke_request.name == "SYNTHETIC"
        assert revoke_request.HasField("expected_revision") == (revision is not None)
        assert revoke_request.expected_revision == (revision or 0)
    await result(client.create_with_injection_credentials([], **options))
    assert rpc.calls[-1][0] == "create_session_with_injection_credentials"
    assert len(rpc.calls[-1][1].credentials) == 0


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("code,expected", [
    (grpc.StatusCode.PERMISSION_DENIED, PermissionDeniedError),
    (grpc.StatusCode.FAILED_PRECONDITION, InvalidStateError),
    (grpc.StatusCode.ABORTED, SandboxError),
    (grpc.StatusCode.UNIMPLEMENTED, SandboxError),
    (grpc.StatusCode.INVALID_ARGUMENT, SandboxError),
    (grpc.StatusCode.INTERNAL, SandboxError),
])
async def test_error_mapping_and_redaction(asynchronous, code, expected):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    session = await client.create() if asynchronous else client.create()
    rpc.code = code
    output = repr(input_value()) + str(input_value())
    for operation in [
        lambda: client.create_with_injection_credentials([input_value()]),
        lambda: session.set_injection_credential(input_value()),
        lambda: session.revoke_injection_credential("SYNTHETIC"),
    ]:
        with pytest.raises(expected) as caught:
            value = operation()
            if asynchronous:
                await value
        assert caught.value.code == code
        output += str(caught.value) + repr(caught.value) + "".join(traceback.format_exception(caught.value))
    assert CANARY.decode() not in output
    assert base64.b64encode(CANARY).decode() not in output


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_readiness_failure_retains_cleanup_identity(asynchronous, monkeypatch):
    from tenki_sandbox import Sandbox, AsyncSandbox
    rpc = AsyncRPC() if asynchronous else RPC()
    original = rpc.__class__.__getattr__

    if asynchronous:
        def getter(self, name):
            async def call(request):
                response = await original(self, name)(request)
                response.created.session.state = pb.SESSION_STATE_CREATING
                return response
            return call
        async def fail(self, timeout):
            raise TimeoutError("synthetic wait failure")
    else:
        def getter(self, name):
            def call(request):
                response = original(self, name)(request)
                response.created.session.state = pb.SESSION_STATE_CREATING
                return response
            return call
        def fail(self, timeout):
            raise TimeoutError("synthetic wait failure")
    monkeypatch.setattr(rpc.__class__, "__getattr__", getter)
    monkeypatch.setattr(AsyncSandbox if asynchronous else Sandbox, "wait_ready", fail)
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(WaitReadyFailedError) as caught:
        value = client.create_with_injection_credentials([])
        if asynchronous:
            await value
    assert caught.value.sandbox.id == "session-synthetic"
    assert [(item.credential_id, item.revision) for item in caught.value.credentials] == [("credential-synthetic", 7)]


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_authenticated_wire_transport(asynchronous):
    from concurrent.futures import ThreadPoolExecutor
    from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

    calls = []

    class Servicer(sandbox_pb2_grpc.SandboxServiceServicer):
        def CreateSessionWithInjectionCredentials(self, request, context):
            assert dict(context.invocation_metadata())["authorization"] == "Bearer tk_test"
            assert request.session.name == "synthetic"
            assert request.credentials[0].value == CANARY
            assert request.credentials[0].rules[0].path_prefix == "/repo"
            return RPC().create_session_with_injection_credentials(request)

        def SetSessionInjectionCredential(self, request, context):
            calls.append(request)
            return RPC().set_session_injection_credential(request)

        def RevokeSessionInjectionCredential(self, request, context):
            calls.append(request)
            return RPC().revoke_session_injection_credential(request)

    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        sandbox_pb2_grpc.add_SandboxServiceServicer_to_server(Servicer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        client_type = AsyncClient if asynchronous else Client
        client = client_type(auth_token="tk_test", base_url=f"http://127.0.0.1:{port}")

        async def result(value):
            return await value if asynchronous else value

        try:
            session, _ = await result(client.create_with_injection_credentials([input_value()], name="synthetic"))
            for revision in [None, 0, 7]:
                await result(session.set_injection_credential(input_value(), revision))
                await result(session.revoke_injection_credential("SYNTHETIC", revision))
                for request in calls[-2:]:
                    assert request.session_id == session.id
                    assert request.HasField("expected_revision") == (revision is not None)
                    assert request.expected_revision == (revision or 0)
        finally:
            await result(client.close())
            server.stop(0).wait()


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("public_egress", [None, {}])
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_runtime_admission_identity_redacts_details(asynchronous, public_egress, cleanup_fails):
    from tenki_sandbox import SandboxInfo, TemplateRuntimeFailedError
    rpc = AsyncRPC() if asynchronous else RPC()
    failure = TemplateRuntimeFailedError(CANARY.decode(), SandboxInfo(id="session-synthetic", metadata={"echo": CANARY.decode()}))
    if asynchronous:
        async def fail(request):
            raise failure
    else:
        def fail(request):
            raise failure
    rpc.create_session_with_injection_credentials = fail
    if cleanup_fails:
        rpc.code = grpc.StatusCode.UNAVAILABLE
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(TemplateRuntimeFailedError if public_egress is None else RuntimeError) as caught:
        value = client.create_with_injection_credentials([input_value()], public_egress=public_egress)
        if asynchronous:
            await value
    if public_egress is None:
        assert caught.value.sandbox.id == "session-synthetic"
    else:
        assert type(caught.value) is RuntimeError
        assert caught.value.credentials == []
        assert isinstance(caught.value.__cause__, TemplateRuntimeFailedError)
        assert caught.value.__cause__.sandbox.id == "session-synthetic"
        assert caught.value.__cause__.credentials == []
        assert "does not support public egress" in str(caught.value)
        assert [(name, req.session_id) for name, req in rpc.calls] == [("terminate_session", "session-synthetic")]
    output = str(caught.value) + repr(vars(caught.value)) + "".join(traceback.format_exception(caught.value))
    assert CANARY.decode() not in output


@pytest.mark.parametrize("revision", [-1, 1.5, 0x100000000, True])
def test_invalid_revision_rejected_before_transport(revision):
    rpc = RPC()
    session = Client(rpc=rpc).create()
    with pytest.raises(ValueError):
        session.set_injection_credential(input_value(), revision)
    with pytest.raises(ValueError):
        session.revoke_injection_credential("SYNTHETIC", revision)
    assert len(rpc.calls) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_github_injection_default_shape_rotation(asynchronous):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)

    async def result(value):
        return await value if asynchronous else value

    options = dict(github_token=CANARY.decode(), wait=False)
    await result(client.create(**options))
    golden = rpc.calls[-1][1]
    assert dict(golden.env) == {"GH_TOKEN": CANARY.decode(), "GIT_TOKEN": CANARY.decode()}
    await result(client.create(**options, github_token_injection=False))
    assert rpc.calls[-1][1] == golden
    session = await result(client.create(**options, github_token_injection=True))
    name, req = rpc.calls[-1]
    assert name == "create_session_with_injection_credentials"
    assert [c.name for c in req.credentials] == ["SDK_GITHUB_API", "SDK_GITHUB_GIT"]
    assert req.credentials[0].value == CANARY
    assert req.credentials[1].value == base64.b64encode(b"x-access-token:" + CANARY)
    assert CANARY.decode() not in str(req.session)
    assert req.session.env["GH_TOKEN"] == req.session.env["GIT_TOKEN"] == "secrets://SDK_GITHUB_API"
    assert req.session.env["GIT_CONFIG_VALUE_0"] == "Authorization: Basic secrets://SDK_GITHUB_GIT"
    assert req.session.env["GIT_CONFIG_VALUE_1"] == "HTTP/1.1"
    assert req.session.env["GIT_CONFIG_VALUE_2"] == ""
    assert MessageToDict(req.credentials[1])["rules"] == [dict(origin="https://github.com", methods=["GET", "POST"], pathPrefix="/", header="Authorization")]
    await result(client.create_with_injection_credentials([], **options, github_token_injection=True))
    assert rpc.calls[-1] == (name, req)
    await result(session.rotate_github_token("SYNTHETIC_ROTATED_CANARY"))
    assert [r.credential.name for _, r in rpc.calls[-2:]] == ["SDK_GITHUB_API", "SDK_GITHUB_GIT"]
    assert rpc.calls[-1][1].credential.value == base64.b64encode(b"x-access-token:SYNTHETIC_ROTATED_CANARY")
    rpc.code = grpc.StatusCode.FAILED_PRECONDITION
    with pytest.raises(InvalidStateError):
        await result(session.rotate_github_token(CANARY.decode()))
    assert rpc.calls[-1][1].credential.name == "SDK_GITHUB_API"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("code", [grpc.StatusCode.UNIMPLEMENTED, grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.PERMISSION_DENIED])
async def test_github_injection_refusal(asynchronous, code):
    rpc = AsyncRPC() if asynchronous else RPC()
    rpc.code = code
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(SandboxError):
        value = client.create(github_token=CANARY.decode(), github_token_injection=True)
        if asynchronous:
            await value
    assert [name for name, _ in rpc.calls] == ["create_session_with_injection_credentials"]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("options", [
    {}, dict(github_token=" "),
    dict(github_token=CANARY.decode(), env={"GH_HOST": "enterprise.invalid"}),
    dict(github_token=CANARY.decode(), env={"GIT_CONFIG_COUNT": "1"}),
    dict(github_token=CANARY.decode(), clone_repo_url="https://github.com/synthetic/repo"),
])
async def test_github_injection_invalid_options(asynchronous, options):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(ValueError):
        value = client.create(**options, github_token_injection=True)
        if asynchronous:
            await value
    assert rpc.calls == []


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_github_git_helpers_and_config(asynchronous, monkeypatch):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)

    async def result(value):
        return await value if asynchronous else value

    session = await result(client.create(github_token=CANARY.decode(), github_token_injection=True))
    env = dict(rpc.calls[-1][1].session.env)
    mock = AsyncMock if asynchronous else Mock
    git = mock(return_value=SimpleNamespace(output="synthetic"))
    rpc.git_operation = git
    execute = mock(return_value=SimpleNamespace(stdout_text="synthetic"))
    monkeypatch.setattr(session, "exec", execute)
    await result(session.git.clone("https://github.com/synthetic/repo.git"))
    await result(session.git.checkout("main"))
    await result(session.git.diff())
    await result(session.git.log())
    await result(session.git.fetch_pr(1))
    await result(session.exec("git", "fetch", "origin"))
    await result(session.exec("git", "push", "origin"))
    assert git.call_count == 4
    assert execute.call_count == 4
    wire = str((git.call_args_list, execute.call_args_list, env))
    assert CANARY.decode() not in wire
    assert base64.b64encode(b"x-access-token:" + CANARY).decode() not in wire
    for key, expected in [
        ("http.extraHeader", "Authorization: Basic secrets://SDK_GITHUB_GIT"),
        ("http.version", "HTTP/1.1"), ("credential.helper", ""), ("http.followRedirects", "false"),
    ]:
        output = subprocess.check_output(
            ["git", "config", "--get-urlmatch", key, "https://github.com/synthetic/repo.git"],
            env={**os.environ, **env, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}, text=True,
        )
        assert output.strip() == expected
    for url in ["https://enterprise.invalid/repo", "http://github.com/repo", "https://github.com.attacker.invalid/repo"]:
        denied = subprocess.run(
            ["git", "config", "--get-urlmatch", "http.extraHeader", url],
            env={**os.environ, **env, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}, capture_output=True,
        )
        assert denied.returncode == 1
        assert not denied.stdout


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("fail_at", [1, 2])
async def test_github_rotation_failure(asynchronous, fail_at):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)

    async def result(value):
        return await value if asynchronous else value

    session = await result(client.create(github_token=CANARY.decode(), github_token_injection=True))
    metadata = credentials_pb.SetSessionInjectionCredentialResponse(credential=credentials_pb.SessionInjectionCredentialMetadata(name="synthetic"))
    mock = AsyncMock if asynchronous else Mock
    rpc.set_session_injection_credential = mock(side_effect=[
        *([metadata] * (fail_at - 1)), RPCError(grpc.StatusCode.FAILED_PRECONDITION), metadata, metadata,
    ])
    expected = (
        r"^rotate SDK_GITHUB_API failed:"
        if fail_at == 1 else r"^rotate SDK_GITHUB_GIT failed \(SDK_GITHUB_API already rotated\):"
    )
    with pytest.raises(InvalidStateError, match=expected) as caught:
        await result(session.rotate_github_token(CANARY.decode()))
    assert CANARY.decode() not in "".join(traceback.format_exception(caught.value))
    assert rpc.set_session_injection_credential.call_count == fail_at
    await result(session.rotate_github_token(CANARY.decode()))
    assert [call.args[0].credential.name for call in rpc.set_session_injection_credential.call_args_list[fail_at:]] == ["SDK_GITHUB_API", "SDK_GITHUB_GIT"]


@pytest.mark.parametrize("cancel_at", [1, 2])
async def test_github_rotation_cancellation(cancel_at):
    import asyncio

    rpc = AsyncRPC()
    session = await AsyncClient(rpc=rpc).create(github_token=CANARY.decode(), github_token_injection=True)
    started = asyncio.Event()
    calls = []
    cancellations = []

    async def set_credential(request):
        calls.append(request.credential.name)
        if len(calls) == cancel_at:
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError as err:
                cancellations.append(err)
                raise
        return credentials_pb.SetSessionInjectionCredentialResponse(
            credential=credentials_pb.SessionInjectionCredentialMetadata(name=request.credential.name),
        )

    rpc.set_session_injection_credential = set_credential
    expected = (
        "rotate SDK_GITHUB_API failed: caller cancelled"
        if cancel_at == 1 else "rotate SDK_GITHUB_GIT failed (SDK_GITHUB_API already rotated): caller cancelled"
    )

    async def rotate():
        try:
            await session.rotate_github_token(CANARY.decode())
        except asyncio.CancelledError as err:
            assert err is cancellations[0]
            assert str(err) == expected
            raise

    task = asyncio.create_task(rotate())
    await started.wait()
    task.cancel("caller cancelled")
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert task.cancelled()
    assert calls == ["SDK_GITHUB_API", "SDK_GITHUB_GIT"][:cancel_at]
    formatted = "".join(traceback.format_exception(caught.value))
    assert expected in formatted
    assert CANARY.decode() not in formatted


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("wait", [False, True])
@pytest.mark.parametrize("accept_dns,ports", [(None, ()), (False, ()), (True, ()), (None, (TailnetPort(6081, "udp"),))])
@pytest.mark.parametrize("acknowledged", [False, True])
async def test_credential_tailnet_compatibility(asynchronous, wait, accept_dns, ports, acknowledged):
    rpc = AsyncRPC() if asynchronous else RPC()
    rpc.acknowledge = acknowledged
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    options = dict(wait=wait, tailnet=TailnetAttachment(
        profile_id="11111111-1111-4111-8111-111111111111", accept_dns=accept_dns, exposed_ports=ports,
    ))
    rejected = not acknowledged and (accept_dns is not None or ports)
    for credential in [False, True]:
        async def run():
            value = client.create_with_injection_credentials([], **options) if credential else client.create(**options)
            return await value if asynchronous else value
        if rejected:
            with pytest.raises(RuntimeError, match="acknowledge|does not support"):
                await run()
            assert rpc.calls[-1][0] == "terminate_session"
            assert rpc.calls[-1][1].session_id == "session-synthetic"
        else:
            await run()
            assert all(name != "terminate_session" for name, _ in rpc.calls)
    ordinary = next(request for name, request in rpc.calls if name == "create_session")
    request = next(request.session for name, request in rpc.calls if name == "create_session_with_injection_credentials")
    assert request == ordinary
    assert request.tailnet.HasField("accept_dns") == (accept_dns is not None)
    if accept_dns is not None:
        assert request.tailnet.accept_dns == accept_dns
    assert list(request.tailnet.exposed_ports) == [pb.TailnetPort(port=6081, protocol=pb.TAILNET_PORT_PROTOCOL_UDP)] * len(ports)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("wait", [False, True])
@pytest.mark.parametrize("requested,accepted,rejected", [
    (None, None, False),
    ({}, pb.PublicEgress(), False),
    ({"tcp_ports": [80, 443]}, pb.PublicEgress(tcp_ports=[443]), False),
    ({}, None, True),
    ({"tcp_ports": [443]}, pb.PublicEgress(tcp_ports=[22]), True),
], ids=["omitted", "deny_all", "narrowed", "missing", "wider"])
async def test_credential_public_egress_compatibility(asynchronous, wait, requested, accepted, rejected):
    rpc = AsyncRPC() if asynchronous else RPC()
    rpc.public_egress = accepted
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)

    async def result(value):
        return await value if asynchronous else value

    for operation in [
        lambda: client.create(public_egress=requested, wait=wait),
        lambda: client.create_with_injection_credentials([input_value()], public_egress=requested, wait=wait),
    ]:
        if rejected:
            with pytest.raises(RuntimeError, match="does not support public egress"):
                await result(operation())
        else:
            await result(operation())
    creates = [(name, req) for name, req in rpc.calls if name != "terminate_session"]
    ordinary_name = "create_session" if requested is None else "create_session_with_public_egress"
    assert [name for name, _ in creates] == [ordinary_name, "create_session_with_injection_credentials"]
    ordinary = creates[0][1] if requested is None else creates[0][1].request
    assert ordinary == creates[1][1].session
    assert ordinary.egress.HasField("public_egress") == (requested is not None)
    terminated = [req.session_id for name, req in rpc.calls if name == "terminate_session"]
    assert terminated == ["session-synthetic"] * (2 if rejected else 0)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("target", ["GH_TOKEN", "GIT_TOKEN", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"])
async def test_github_injection_secret_target_conflicts(asynchronous, target):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(ValueError, match="secret target conflicts with session environment"):
        value = client.create(
            github_token=CANARY.decode(), github_token_injection=True,
            direct_runtime=TemplateSpec().start("app", secret_env={target: "token"}),
        )
        if asynchronous:
            await value
    assert rpc.calls == []


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("value,rules", [(b"value", None), ("value", [])])
async def test_invalid_credential_input_is_local(asynchronous, value, rules):
    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(TypeError):
        value = client.create_with_injection_credentials([InjectionCredentialInput("bad", value, rules)])
        if asynchronous:
            await value
    assert rpc.calls == []


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("code,message,expected_name", [
    (grpc.StatusCode.NOT_FOUND, "template not found", "TemplateNotFoundError"),
    (grpc.StatusCode.NOT_FOUND, "egress policy not found", "EgressPolicyNotFoundError"),
    (grpc.StatusCode.FAILED_PRECONDITION, "session terminated", "SessionTerminatedError"),
    (grpc.StatusCode.RESOURCE_EXHAUSTED, "rate limit", "RateLimitedError"),
    (grpc.StatusCode.RESOURCE_EXHAUSTED, "capacity unavailable", "CapacityUnavailableError"),
])
async def test_typed_injection_errors_stay_redacted(asynchronous, code, message, expected_name):
    from tenki_sandbox import errors

    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    session = await client.create() if asynchronous else client.create()
    error = RPCError(code)
    error.details = lambda: message + CANARY.decode()
    if asynchronous:
        async def fail(request):
            raise error
    else:
        def fail(request):
            raise error
    rpc.create_session_with_injection_credentials = fail
    rpc.set_session_injection_credential = fail
    rpc.revoke_session_injection_credential = fail
    for operation in [
        lambda: client.create_with_injection_credentials([input_value()]),
        lambda: session.set_injection_credential(input_value()),
        lambda: session.revoke_injection_credential("SYNTHETIC"),
    ]:
        with pytest.raises(getattr(errors, expected_name)) as caught:
            value = operation()
            if asynchronous:
                await value
        assert CANARY.decode() not in repr(vars(caught.value)) + str(caught.value) + "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_runtime_credential_mutations_redact_identity(asynchronous):
    from tenki_sandbox import SandboxInfo, TemplateRuntimeFailedError

    rpc = AsyncRPC() if asynchronous else RPC()
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    session = await client.create() if asynchronous else client.create()
    failure = TemplateRuntimeFailedError(CANARY.decode(), SandboxInfo(id=session.id, metadata={"echo": CANARY.decode()}))
    if asynchronous:
        async def fail(request):
            raise failure
    else:
        def fail(request):
            raise failure
    rpc.set_session_injection_credential = fail
    rpc.revoke_session_injection_credential = fail
    for operation in [lambda: session.set_injection_credential(input_value()), lambda: session.revoke_injection_credential("SYNTHETIC")]:
        with pytest.raises(TemplateRuntimeFailedError) as caught:
            value = operation()
            if asynchronous:
                await value
        assert caught.value.sandbox.id == session.id
        assert caught.value.sandbox.metadata == {}
        assert CANARY.decode() not in repr(vars(caught.value)) + "".join(traceback.format_exception(caught.value))


def test_input_serialization_and_copy_isolation():
    from dataclasses import asdict, astuple
    import pickle
    from tenki_sandbox.injection_credentials import injection_input

    value = bytearray(CANARY)
    methods = ["GET"]
    rules = [InjectionRule("https://example.invalid", methods, "/repo", "Authorization")]
    credential = InjectionCredentialInput("SYNTHETIC", value, rules)
    value[:] = b"changed"
    methods.append("POST")
    rules.clear()
    assert credential.value == CANARY
    assert credential == InjectionCredentialInput("SYNTHETIC", b"different", list(credential.rules))
    output = repr(credential) + repr(asdict(credential)) + repr(astuple(credential)) + repr(vars(credential))
    assert CANARY.decode() not in output
    assert "value" not in asdict(credential)
    with pytest.raises(TypeError, match="cannot be pickled"):
        pickle.dumps(credential)
    wire = injection_input(credential)
    assert wire.value == CANARY
    assert list(wire.rules[0].methods) == ["GET"]
    wire.value = b"changed"
    wire.rules[0].methods.append("POST")
    assert injection_input(credential).value == CANARY
    assert list(injection_input(credential).rules[0].methods) == ["GET"]


@pytest.mark.parametrize("operation", ["create", "set", "revoke"])
async def test_async_cancellation_propagates(operation):
    import asyncio

    rpc = AsyncRPC()
    client = AsyncClient(rpc=rpc)
    session = await client.create()
    started = asyncio.Event()

    async def block(request):
        started.set()
        await asyncio.Future()

    rpc.create_session_with_injection_credentials = block
    rpc.set_session_injection_credential = block
    rpc.revoke_session_injection_credential = block
    coroutine = {
        "create": lambda: client.create_with_injection_credentials([input_value()]),
        "set": lambda: session.set_injection_credential(input_value()),
        "revoke": lambda: session.revoke_injection_credential("SYNTHETIC"),
    }[operation]()
    task = asyncio.create_task(coroutine)
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_runtime_failure_preserves_acknowledged_public_egress(asynchronous):
    from tenki_sandbox import SandboxInfo, TemplateRuntimeFailedError
    from tenki_sandbox.models import SessionEgressPolicy

    rpc = AsyncRPC() if asynchronous else RPC()
    failure = TemplateRuntimeFailedError(CANARY.decode(), SandboxInfo(
        id="session-synthetic", metadata={"echo": CANARY.decode()},
        egress=SessionEgressPolicy(public_egress={"tcp_ports": [443]}),
    ))
    if asynchronous:
        async def fail(request):
            raise failure
    else:
        def fail(request):
            raise failure
    rpc.create_session_with_injection_credentials = fail
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(TemplateRuntimeFailedError) as caught:
        value = client.create_with_injection_credentials([input_value()], public_egress={"tcp_ports": [443]})
        if asynchronous:
            await value
    assert caught.value.sandbox.id == "session-synthetic"
    assert caught.value.credentials == []
    assert rpc.calls == []
    assert CANARY.decode() not in repr(vars(caught.value)) + "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_egress_rejection_retains_credential_metadata(asynchronous, cleanup_fails):
    rpc = AsyncRPC() if asynchronous else RPC()
    if cleanup_fails:
        if asynchronous:
            async def terminate(request):
                raise RuntimeError("synthetic cleanup failure")
        else:
            def terminate(request):
                raise RuntimeError("synthetic cleanup failure")
        rpc.terminate_session = terminate
    client = AsyncClient(rpc=rpc) if asynchronous else Client(rpc=rpc)
    with pytest.raises(RuntimeError, match="does not support public egress") as caught:
        value = client.create_with_injection_credentials([input_value()], public_egress={})
        if asynchronous:
            await value
    assert type(caught.value) is RuntimeError
    assert [(item.credential_id, item.revision) for item in caught.value.credentials] == [("credential-synthetic", 7)]
    if cleanup_fails:
        assert str(caught.value.__cause__) == "synthetic cleanup failure"


def test_sanitized_capability_and_git_diagnostics():
    from tenki_sandbox.errors import CapabilityUnavailableError, GitOperationFailedError
    from tenki_sandbox.injection_credentials import safe_injection_error

    error = RPCError(grpc.StatusCode.UNIMPLEMENTED)
    error.details = lambda: "unknown request type: *sandboxv1.GuestRequest_Stat " + CANARY.decode()
    safe = safe_injection_error(error)
    assert isinstance(safe, CapabilityUnavailableError)
    assert safe.primitive == "Stat"
    assert CANARY.decode() not in str(safe) + repr(vars(safe))
    error = RPCError(grpc.StatusCode.FAILED_PRECONDITION)
    error.details = lambda: "git clone failed (exit_code=128); check the repository, ref and access token: " + CANARY.decode()
    safe = safe_injection_error(error)
    assert isinstance(safe, GitOperationFailedError)
    assert safe.stderr == ""
    assert safe.exit_code == 128
    assert CANARY.decode() not in str(safe) + repr(vars(safe))
