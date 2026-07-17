import threading

import pytest
from google.protobuf.any_pb2 import Any

from tenki_sandbox import Image, TemplateSpec
from tenki_sandbox._internal.proto.buf.validate import validate_pb2
from tenki_sandbox._pb import registry_pb, template_pb
from tenki_sandbox.aio.resources import AsyncTemplates
from tenki_sandbox.client import build_create_session_request
from tenki_sandbox.errors import (
    TemplateBuildFailedError,
    TemplateBuildWaitCancelledError,
    TemplateSpecValidationError,
    _grpc_status_message_class,
)
from tenki_sandbox.models import Template, TemplateBuild
from tenki_sandbox.resources import Templates

TEMPLATE_ID = "0190a5a0-0000-7000-8000-00000000000a"
BUILD_ID = "0190a5a0-0000-7000-8000-00000000000b"
DIGEST = "sha256:" + "a" * 64


def _template_proto(spec_proto=None):
    proto = template_pb.Template(
        id=TEMPLATE_ID, workspace_id="0190a5a0-0000-7000-8000-000000000001", name="node-api"
    )
    if spec_proto is not None:
        proto.builder_spec.CopyFrom(spec_proto)
        proto.spec_hash = DIGEST
        proto.definition_mode = template_pb.TEMPLATE_DEFINITION_MODE_TYPED
    return proto


def _build_proto(state, events=(), with_image=False, failure=None):
    proto = template_pb.TemplateBuild(id=BUILD_ID, template_id=TEMPLATE_ID, state=state)
    for i, data in enumerate(events):
        event = proto.events.add()
        event.log.phase = "execute"
        event.log.stream = template_pb.TEMPLATE_BUILD_LOG_STREAM_STDOUT
        event.log.data = data
        event.log.step.index = i
    if with_image:
        proto.image.CopyFrom(
            registry_pb.RegistryImage(
                id="0190a5a0-0000-7000-8000-000000000003",
                workspace_id="0190a5a0-0000-7000-8000-000000000001",
                workspace_slug="acme",
                name="node-api",
            )
        )
        proto.image_digest = DIGEST
        proto.image_digest_ref = f"acme/node-api@{DIGEST}"
        proto.spec_hash = DIGEST
    if failure is not None:
        proto.failure.code = failure
        proto.failure.message = "step failed"
    return proto


class FakeTemplatesRPC:
    def __init__(self, build_states=None, create_error=None):
        self.requests = []
        self.build_states = list(build_states or [])
        self.create_error = create_error

    def create_template(self, req):
        self.requests.append(("create_template", req))
        if self.create_error is not None:
            raise self.create_error
        return template_pb.CreateTemplateResponse(
            template=_template_proto(
                req.builder_spec if req.HasField("builder_spec") else None
            )
        )

    def update_template(self, req):
        self.requests.append(("update_template", req))
        return template_pb.UpdateTemplateResponse(
            template=_template_proto(
                req.builder_spec if req.HasField("builder_spec") else None
            )
        )

    def get_template(self, req):
        self.requests.append(("get_template", req))
        return template_pb.GetTemplateResponse(template=_template_proto())

    def delete_template(self, req):
        self.requests.append(("delete_template", req))
        return template_pb.DeleteTemplateResponse(template=_template_proto())

    def build_template(self, req):
        self.requests.append(("build_template", req))
        return template_pb.BuildTemplateResponse(
            build=_build_proto(template_pb.TEMPLATE_BUILD_STATE_PENDING)
        )

    def get_template_build(self, req):
        self.requests.append(("get_template_build", req))
        return template_pb.GetTemplateBuildResponse(build=self.build_states.pop(0))

    def list_active_template_builds(self, req):
        self.requests.append(("list_active_template_builds", req))
        return template_pb.ListActiveTemplateBuildsResponse(
            builds=[
                _build_proto(template_pb.TEMPLATE_BUILD_STATE_BUILDING),
                _build_proto(template_pb.TEMPLATE_BUILD_STATE_PENDING),
            ]
        )

    def cancel_template_build(self, req):
        self.requests.append(("cancel_template_build", req))
        return template_pb.CancelTemplateBuildResponse(
            build=_build_proto(
                template_pb.TEMPLATE_BUILD_STATE_FAILED, failure="BUILD_CANCELLED"
            )
        )


class AsyncFakeTemplatesRPC(FakeTemplatesRPC):
    def __init__(self, build_states=None, create_error=None):
        super().__init__(build_states, create_error)
        for name in (
            "create_template",
            "update_template",
            "get_template",
            "delete_template",
            "build_template",
            "get_template_build",
            "list_active_template_builds",
            "cancel_template_build",
        ):
            sync_method = getattr(FakeTemplatesRPC, name)

            async def call(req, _m=sync_method):
                return _m(self, req)

            setattr(self, name, call)


def _spec():
    return (
        TemplateSpec()
        .from_image("sandbox-v2")
        .with_git_context("https://github.com/acme/node-api", "main")
        .run("npm ci", name="Install dependencies")
        .runtime_env({"NODE_ENV": "production"})
        .start(["npm", "start"], ready_when=[{"http": "http://localhost:3000/health"}])
    )


def test_create_with_spec_sets_builder_spec_and_exposes_typed_fields():
    rpc = FakeTemplatesRPC()
    templates = Templates(rpc)
    created = templates.create(
        workspace_id="0190a5a0-0000-7000-8000-000000000001",
        name="node-api",
        spec=_spec(),
    )
    _, req = rpc.requests[0]
    assert req.HasField("builder_spec")
    assert req.builder_spec.base.image == "sandbox-v2"
    assert req.base_image_id == "" and req.setup_script == ""
    assert created.spec == _spec()
    assert created.spec_hash == DIGEST
    assert created.definition_mode == "TYPED"


def test_create_rejects_spec_with_legacy_fields():
    templates = Templates(FakeTemplatesRPC())
    with pytest.raises(ValueError, match="legacy template fields"):
        templates.create(
            workspace_id="0190a5a0-0000-7000-8000-000000000001",
            name="node-api",
            spec=_spec(),
            setup_script="apt install -y jq",
        )


def test_create_validates_spec_locally():
    templates = Templates(FakeTemplatesRPC())
    from tenki_sandbox.errors import TemplateSpecValidationError

    with pytest.raises(TemplateSpecValidationError):
        templates.create(
            workspace_id="0190a5a0-0000-7000-8000-000000000001",
            name="node-api",
            spec=TemplateSpec().copy("a", "/b"),  # copy without git context
        )


def _server_validation_error():
    violations = validate_pb2.Violations(
        violations=[
            validate_pb2.Violation(
                field=validate_pb2.FieldPath(
                    elements=[
                        validate_pb2.FieldPathElement(field_name="builder_spec"),
                        validate_pb2.FieldPathElement(field_name="steps", index=0),
                        validate_pb2.FieldPathElement(field_name="run"),
                    ]
                ),
                rule_id="template_run_step.command_or_argv",
                message="exactly one of command or argv is required",
            ),
            validate_pb2.Violation(
                field=validate_pb2.FieldPath(
                    elements=[validate_pb2.FieldPathElement(field_name="workdir")]
                ),
                rule_id="string.pattern",
                message="workdir must be absolute",
            ),
        ]
    )
    packed = Any()
    packed.Pack(violations)
    status = _grpc_status_message_class()(
        code=3,
        message="validation failed",
        details=[packed],
    )

    class FakeValidationError(Exception):
        def code(self):
            return type("Code", (), {"name": "INVALID_ARGUMENT"})()

        def details(self):
            return "validation failed"

        def trailing_metadata(self):
            return (("grpc-status-details-bin", status.SerializeToString()),)

    return FakeValidationError("validation failed")


def test_create_maps_all_server_spec_violations():
    templates = Templates(FakeTemplatesRPC(create_error=_server_validation_error()))
    with pytest.raises(TemplateSpecValidationError) as err:
        templates.create(
            workspace_id="0190a5a0-0000-7000-8000-000000000001",
            name="node-api",
            spec=_spec(),
        )
    assert err.value.violations == (
        "builder_spec.steps[0].run: exactly one of command or argv is required",
        "workdir: workdir must be absolute",
    )


def test_update_replaces_spec_atomically_and_patches_metadata():
    rpc = FakeTemplatesRPC()
    templates = Templates(rpc)
    template_obj = Template(id=TEMPLATE_ID, workspace_id="w", name="node-api")
    templates.update(template_obj, name="renamed", spec=_spec())
    _, req = rpc.requests[0]
    assert req.template_id == TEMPLATE_ID
    assert req.name == "renamed"
    assert req.HasField("builder_spec")
    with pytest.raises(ValueError, match="legacy template fields"):
        templates.update(template_obj, spec=_spec(), env={"A": "1"})
    templates.update(TEMPLATE_ID, tags=[])
    assert rpc.requests[-1][1].clear_tags is True


def test_follow_up_calls_accept_resource_objects_and_ids():
    rpc = FakeTemplatesRPC()
    templates = Templates(rpc)
    template_obj = Template(id=TEMPLATE_ID, workspace_id="w", name="node-api")
    build_obj = TemplateBuild(id=BUILD_ID, template_id=TEMPLATE_ID, state="PENDING")

    templates.get(template_obj)
    templates.build(template_obj)
    templates.delete(TEMPLATE_ID)
    rpc.build_states = [_build_proto(template_pb.TEMPLATE_BUILD_STATE_READY)]
    templates.get_build(build_obj)
    active = templates.list_active_builds(template_obj)
    templates.cancel_build(BUILD_ID)

    kinds = [kind for kind, _ in rpc.requests]
    assert kinds == [
        "get_template",
        "build_template",
        "delete_template",
        "get_template_build",
        "list_active_template_builds",
        "cancel_template_build",
    ]
    assert rpc.requests[0][1].template_id == TEMPLATE_ID
    assert rpc.requests[3][1].build_id == BUILD_ID
    assert rpc.requests[4][1].template_id == TEMPLATE_ID
    assert [build.state for build in active] == ["BUILDING", "PENDING"]

    with pytest.raises(ValueError):
        templates.get("   ")


def test_build_forwards_secrets_with_defensive_copy():
    rpc = FakeTemplatesRPC()
    templates = Templates(rpc)
    secrets = {"GITHUB_TOKEN": "token-1"}
    templates.build(TEMPLATE_ID, build_secrets=secrets)
    secrets["GITHUB_TOKEN"] = "mutated"
    _, req = rpc.requests[0]
    assert dict(req.build_secrets) == {"GITHUB_TOKEN": "token-1"}


def test_wait_build_replays_ordered_events_without_duplicates():
    building = _build_proto(template_pb.TEMPLATE_BUILD_STATE_BUILDING, events=["a", "b"])
    ready = _build_proto(
        template_pb.TEMPLATE_BUILD_STATE_READY,
        events=["a", "b", "c", "d"],
        with_image=True,
    )
    rpc = FakeTemplatesRPC(build_states=[building, ready])
    templates = Templates(rpc)
    seen = []
    result = templates.build(
        TEMPLATE_ID,
        wait_for_completion=True,
        on_event=lambda event: seen.append(event.data),
    )
    assert seen == ["a", "b", "c", "d"]
    assert result.state == "READY"
    assert result.image == Image(
        ref="acme/node-api",
        digest=DIGEST,
        digest_ref=f"acme/node-api@{DIGEST}",
        name="node-api",
        registry_image_id="0190a5a0-0000-7000-8000-000000000003",
        workspace_id="0190a5a0-0000-7000-8000-000000000001",
    )
    assert result.spec_hash == DIGEST


def test_wait_build_failure_raises_typed_error_with_final_build():
    failed = _build_proto(
        template_pb.TEMPLATE_BUILD_STATE_FAILED, failure="STEP_FAILED"
    )
    templates = Templates(FakeTemplatesRPC(build_states=[failed]))
    with pytest.raises(TemplateBuildFailedError) as err:
        templates.wait_build(BUILD_ID)
    assert err.value.build.state == "FAILED"
    assert err.value.build.failure.code == "STEP_FAILED"


def test_waited_build_preserves_local_timeout_error():
    templates = Templates(FakeTemplatesRPC())
    with pytest.raises(TimeoutError, match=BUILD_ID):
        templates.build(TEMPLATE_ID, wait_for_completion=True, timeout=0)


def test_wait_build_preserves_event_handler_error():
    templates = Templates(
        FakeTemplatesRPC(
            build_states=[
                _build_proto(template_pb.TEMPLATE_BUILD_STATE_BUILDING, events=["a"])
            ]
        )
    )

    def fail(_event):
        raise RuntimeError("handler failed")

    with pytest.raises(RuntimeError, match="handler failed"):
        templates.wait_build(BUILD_ID, on_event=fail)


def test_local_cancellation_never_cancels_remote_build():
    rpc = FakeTemplatesRPC(
        build_states=[_build_proto(template_pb.TEMPLATE_BUILD_STATE_BUILDING)]
    )
    templates = Templates(rpc)
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(TemplateBuildWaitCancelledError):
        templates.wait_build(BUILD_ID, cancel_event=cancel)
    assert all(kind != "cancel_template_build" for kind, _ in rpc.requests)


def test_cancel_build_reaches_terminal_build():
    rpc = FakeTemplatesRPC()
    templates = Templates(rpc)
    build = templates.cancel_build(BUILD_ID)
    assert build.state == "FAILED"
    assert build.failure.code == "BUILD_CANCELLED"


def test_create_session_accepts_image_object_pinning_digest_ref():
    image = Image(
        ref="acme/node-api",
        digest=DIGEST,
        digest_ref=f"acme/node-api@{DIGEST}",
    )
    req = build_create_session_request(image=image, wait_for_runtime=True)
    assert req.registry_ref == f"acme/node-api@{DIGEST}"
    assert req.wait_for_runtime is True
    untagged = build_create_session_request(image=Image(ref="acme/node-api"))
    assert untagged.registry_ref == "acme/node-api"
    assert build_create_session_request(image="acme/node-api:prod").registry_ref == (
        "acme/node-api:prod"
    )


async def test_async_templates_typed_flow_matches_sync():
    building = _build_proto(template_pb.TEMPLATE_BUILD_STATE_BUILDING, events=["a"])
    ready = _build_proto(
        template_pb.TEMPLATE_BUILD_STATE_READY, events=["a", "b"], with_image=True
    )
    rpc = AsyncFakeTemplatesRPC(build_states=[building, ready])
    templates = AsyncTemplates(rpc)

    created = await templates.create(
        workspace_id="0190a5a0-0000-7000-8000-000000000001",
        name="node-api",
        spec=_spec(),
    )
    assert created.spec == _spec()

    await templates.update(created, spec=_spec())

    seen = []

    async def on_event(event):
        seen.append(event.data)

    result = await templates.build(
        created, wait_for_completion=True, on_event=on_event
    )
    assert seen == ["a", "b"]
    assert result.image is not None and result.image.digest_ref == (
        f"acme/node-api@{DIGEST}"
    )

    active = await templates.list_active_builds(created)
    assert [build.state for build in active] == ["BUILDING", "PENDING"]

    cancelled = await templates.cancel_build(result)
    assert cancelled.state == "FAILED"


async def test_async_create_maps_server_spec_violations():
    templates = AsyncTemplates(
        AsyncFakeTemplatesRPC(create_error=_server_validation_error())
    )
    with pytest.raises(TemplateSpecValidationError) as err:
        await templates.create(
            workspace_id="0190a5a0-0000-7000-8000-000000000001",
            name="node-api",
            spec=_spec(),
        )
    assert len(err.value.violations) == 2


async def test_async_local_cancellation_is_local_only():
    rpc = AsyncFakeTemplatesRPC(
        build_states=[_build_proto(template_pb.TEMPLATE_BUILD_STATE_BUILDING)]
    )
    templates = AsyncTemplates(rpc)
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(TemplateBuildWaitCancelledError):
        await templates.wait_build(BUILD_ID, cancel_event=cancel)
    assert all(kind != "cancel_template_build" for kind, _ in rpc.requests)


async def test_async_waited_build_preserves_local_timeout_error():
    templates = AsyncTemplates(AsyncFakeTemplatesRPC())
    with pytest.raises(TimeoutError, match=BUILD_ID):
        await templates.build(TEMPLATE_ID, wait_for_completion=True, timeout=0)


async def test_async_wait_build_preserves_event_handler_error():
    templates = AsyncTemplates(
        AsyncFakeTemplatesRPC(
            build_states=[
                _build_proto(template_pb.TEMPLATE_BUILD_STATE_BUILDING, events=["a"])
            ]
        )
    )

    async def fail(_event):
        raise RuntimeError("handler failed")

    with pytest.raises(RuntimeError, match="handler failed"):
        await templates.wait_build(BUILD_ID, on_event=fail)
