from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


os.environ.setdefault("TENKI_WORKSPACE_ID", "01900000-0000-7000-8000-000000000001")

_EXAMPLE_PATH = Path(__file__).parents[1] / "examples" / "template_builder.py"
_SPEC = importlib.util.spec_from_file_location("template_builder_example", _EXAMPLE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
example = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(example)


class FakeTemplates:
    def __init__(self) -> None:
        self.spec = None

    def create(self, *, spec, **_kwargs):
        self.spec = spec
        return SimpleNamespace(name="node-api", spec_hash="sha256:test")

    def build(self, *_args, **_kwargs):
        image = SimpleNamespace(ref="acme/node-api", digest_ref="acme/node-api@sha256:test")
        return SimpleNamespace(image=image)


class FakeSandbox:
    def __init__(self, metadata: dict[str, str] | None = None) -> None:
        self.exec_args = None
        self.closed = False
        self.info = SimpleNamespace(runtime_state="READY", metadata=metadata or {})

    def exec(self, *args):
        self.exec_args = args
        return SimpleNamespace(stdout_text="ready\n")

    def refresh(self):
        return self.info

    def close(self) -> None:
        self.closed = True


class FakeClient:
    def __init__(self, metadata: dict[str, str] | None = None) -> None:
        self.templates = FakeTemplates()
        self.sandbox = FakeSandbox(metadata)
        self.create_kwargs = None

    def create(self, **kwargs):
        self.create_kwargs = kwargs
        return self.sandbox


def test_filesystem_example_declares_boot_runtime_and_uses_variadic_exec():
    client = FakeClient()

    example.filesystem_workflow(client)

    runtime = client.templates.spec.to_dict()["runtime"]
    assert runtime["runAt"] == "TEMPLATE_RUNTIME_RUN_AT_BOOT"
    assert "snapshotMode" not in runtime
    assert client.create_kwargs["wait_for_runtime"] is True
    assert client.sandbox.exec_args == ("cat", "/home/tenki/app/.template-built")
    assert client.sandbox.closed is True


@pytest.mark.parametrize(
    "metadata",
    [
        {"restore_mode": "memory", "restore_degraded": "false"},
        {
            "restore_mode": "cold_boot",
            "restore_degraded": "true",
            "restore_reason": "incompatible_cpu",
        },
    ],
)
def test_memory_example_accepts_memory_restore_and_attributed_cold_fallback(metadata):
    client = FakeClient(metadata)

    example.memory_workflow(client)

    runtime = client.templates.spec.to_dict()["runtime"]
    assert runtime["runAt"] == "TEMPLATE_RUNTIME_RUN_AT_BUILD"
    assert runtime["snapshotMode"] == "TEMPLATE_SNAPSHOT_MODE_MEMORY"
    assert client.sandbox.closed is True


def test_memory_example_rejects_missing_restore_attribution():
    client = FakeClient({})

    with pytest.raises(RuntimeError, match="unexpected restore mode"):
        example.memory_workflow(client)

    assert client.sandbox.closed is True
