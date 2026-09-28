import inspect

import pytest
from tenki import AsyncClient, AsyncSandbox, Client, Sandbox, TemplateSpec
from tenki_sandbox._convert import snapshot
from tenki_sandbox._pb import pb
from tenki_sandbox.client import build_create_session_request

REFS = {"API_TOKEN": "shared-token"}


@pytest.mark.parametrize("kind", ["start", "argv", "compose"])
def test_runtime_references_roundtrip(kind):
    refs = dict(REFS)
    spec = (
        TemplateSpec().process_compose("compose.yaml", secret_env=refs)
        if kind == "compose"
        else TemplateSpec().start(["app"] if kind == "argv" else "app", secret_env=refs)
    )
    refs["API_TOKEN"] = "mutated"
    spec.validate()
    assert dict(spec.to_proto().runtime.secret_env) == REFS
    assert (
        dict(TemplateSpec.from_json(spec.to_json()).to_proto().runtime.secret_env)
        == REFS
    )
    assert (
        dict(
            TemplateSpec.from_proto(spec.to_proto())
            ._to_direct_runtime_proto()
            .secret_env
        )
        == REFS
    )


def test_runtime_reference_validation():
    for refs in [{"BAD-TARGET": "token"}, {"TOKEN": "not a name"}]:
        with pytest.raises(Exception):
            TemplateSpec().start("app", secret_env=refs).validate()
    with pytest.raises(Exception):
        TemplateSpec().runtime_env({"API_TOKEN": "plain"}).start(
            "app", secret_env=REFS
        ).validate()
    with pytest.raises(ValueError, match="runtime settings only"):
        TemplateSpec().run("setup").start("app")._to_direct_runtime_proto()
    with pytest.raises(ValueError, match="boot runtime"):
        TemplateSpec().start("app", run_at="manual")._to_direct_runtime_proto()
    with pytest.raises(ValueError, match="session environment"):
        build_create_session_request(
            direct_runtime=TemplateSpec().start(
                "app", secret_env={"GH_TOKEN": "token"}
            ),
            github_token="plain",
        )
    with pytest.raises(ValueError, match="from_template_spec"):
        build_create_session_request(
            direct_runtime=TemplateSpec().start("app"), from_template_spec="template"
        )


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("shortcut", [False, True])
async def test_sync_async_launch_contract(asynchronous, shortcut, monkeypatch):
    requests = []

    def create(request):
        wire = pb.CreateSessionRequest.FromString(request.SerializeToString())
        requests.append(wire)
        return pb.CreateSessionResponse(
            session=pb.SandboxSession(
                id="session", state=pb.SESSION_STATE_RUNNING, has_runtime_secrets=True
            )
        )

    class RPC:
        def create_session(self, request):
            return create(request)

    class AsyncRPC:
        async def create_session(self, request):
            return create(request)

    client = AsyncClient(rpc=AsyncRPC()) if asynchronous else Client(rpc=RPC())
    launcher = client
    if shortcut:
        monkeypatch.setattr(
            "tenki_sandbox.aio.client.AsyncClient"
            if asynchronous
            else "tenki_sandbox.client.Client",
            lambda **_kwargs: client,
        )
        launcher = AsyncSandbox if asynchronous else Sandbox
    result = launcher.create(
        image="team/base:v1",
        direct_runtime=TemplateSpec().workdir("/app").start("app", secret_env=REFS),
        secret_overrides={"shared-token": "local-token"},
        secret_files=[{"path":"/app/env.tpl-sandbox","content":"TOKEN=secrets://shared-token"}],
        wait=False,
    )
    sandbox = await result if inspect.isawaitable(result) else result
    assert sandbox.info.has_runtime_secrets
    assert sandbox.has_runtime_secrets
    request = requests[0]
    assert request.registry_ref == "team/base:v1"
    assert request.secret_files[0].content == "TOKEN=secrets://shared-token"
    assert request.runtime.start.workdir == "/app"
    assert dict(request.runtime.secret_env) == REFS
    assert dict(request.secret_overrides) == {"shared-token": "local-token"}
    assert snapshot(pb.Snapshot(has_runtime_secrets=True)).has_runtime_secrets
    result = launcher.create(
        from_template_spec="template",
        secret_overrides={"shared-token": "local-token"},
        wait=False,
    )
    if inspect.isawaitable(result):
        await result
    assert not requests[1].HasField("runtime")
    assert dict(requests[1].secret_overrides) == {"shared-token": "local-token"}

@pytest.mark.parametrize("kind", ["start", "argv", "compose"])
def test_runtime_file_contract(kind):
    files = [
        {"path":"/app/raw", "raw":{"name":"RAW"}},
        {"path":"/app/.env", "content":"TOKEN=secrets://TOKEN"},
        {"path":"/app/config.yaml", "source":"/app/config.tpl.yaml"},
    ]
    spec = (TemplateSpec().process_compose("compose.yaml", secret_files=files) if kind == "compose" else TemplateSpec().start(["app"] if kind == "argv" else "app", secret_files=files))
    files[0]["path"] = "/etc/unsafe"
    spec.validate()
    runtime = TemplateSpec.from_json(spec.to_json())._to_direct_runtime_proto()
    assert len(runtime.secret_files) == 3
    assert runtime.secret_files[0].path == "/app/raw"
    request = build_create_session_request(direct_runtime=spec)
    assert request.runtime.secret_files == runtime.secret_files
    with pytest.raises(Exception):
        TemplateSpec().start("app", secret_files=files).validate()


def test_source_marker_grammar():
    from tenki_sandbox.secret_files import secret_file_names
    assert secret_file_names(r"a=secrets://TOKEN,b=secrets://TOKEN,literal=\secrets://IGNORE,header=\secrets://API") == ["TOKEN"]
    for text in ["secrets://", "secrets://9bad", "secrets://"+"a"*129, "bad\x00"]:
        assert secret_file_names(text) is None

@pytest.mark.parametrize("kind", ["start", "argv", "compose"])
@pytest.mark.parametrize("location", [{"header": "Authorization"}, {"query_parameter": "token"}, {"json_pointer": "/auth/token"}])
def test_request_binding_roundtrip(kind, location):
    from tenki import SecretRequestBinding
    rule: SecretRequestBinding = {"name": "token", "secret_name": "API_TOKEN", "origin": "https://api.example.com", "methods": ["POST"], "path_prefix": "/v1/", **location}
    spec = TemplateSpec().process_compose("compose.yaml", secret_requests=[rule]) if kind == "compose" else TemplateSpec().start(["app"] if kind == "argv" else "app", secret_requests=[rule])
    rule["methods"].append("DELETE")
    runtime = TemplateSpec.from_json(spec.to_json())._to_direct_runtime_proto()
    assert list(runtime.secret_requests[0].methods) == ["POST"]
    assert runtime.secret_requests[0].secret_name == "API_TOKEN"
    for key, value in location.items():
        assert getattr(runtime.secret_requests[0], key) == value

@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("shortcut", [False, True])
@pytest.mark.parametrize("location", [{"header": "Authorization"}, {"query_parameter": "token"}, {"json_pointer": "/auth/token"}])
async def test_request_binding_launch_contract(asynchronous, shortcut, monkeypatch, location):
    seen = []
    def create(request):
        seen.append(pb.CreateSessionRequest.FromString(request.SerializeToString()))
        return pb.CreateSessionResponse(session=pb.SandboxSession(id="session", state=pb.SESSION_STATE_RUNNING))
    class RPC:
        def create_session(self, request): return create(request)
    class AsyncRPC:
        async def create_session(self, request): return create(request)
    client = AsyncClient(rpc=AsyncRPC()) if asynchronous else Client(rpc=RPC())
    launcher = client
    if shortcut:
        monkeypatch.setattr("tenki_sandbox.aio.client.AsyncClient" if asynchronous else "tenki_sandbox.client.Client", lambda **_kwargs: client)
        launcher = AsyncSandbox if asynchronous else Sandbox
    result = launcher.create(secret_requests=[{"name":"token", "secret_name":"API_TOKEN", "origin":"https://api.example.com", "methods":["POST"], "path_prefix":"/v1/", **location}], wait=False)
    if inspect.isawaitable(result): await result
    assert not seen[0].HasField("runtime")
    assert seen[0].secret_requests[0].secret_name == "API_TOKEN"
    for key, value in location.items():
        assert getattr(seen[0].secret_requests[0], key) == value

