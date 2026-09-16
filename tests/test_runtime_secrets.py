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
        wait=False,
    )
    sandbox = await result if inspect.isawaitable(result) else result
    assert sandbox.info.has_runtime_secrets
    assert sandbox.has_runtime_secrets
    request = requests[0]
    assert request.registry_ref == "team/base:v1"
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
