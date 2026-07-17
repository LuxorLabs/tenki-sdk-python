import json

import pytest

from tenki_sandbox import Image, TemplateSpec, TemplateSpecValidationError
from tenki_sandbox.models import Snapshot, Template


def test_default_spec_uses_sandbox_base():
    spec = TemplateSpec()
    data = spec.to_dict()
    assert data["specVersion"] == "tenki.template.v1"
    assert data["base"] == {"image": "sandbox"}
    spec.validate()


def test_builder_operations_return_new_values():
    base = TemplateSpec().from_image("sandbox-v2")
    with_step = base.run("npm ci")
    other_branch = base.run("pip install -r requirements.txt")

    assert "steps" not in base.to_dict()
    assert len(with_step.to_dict()["steps"]) == 1
    assert with_step.to_dict()["steps"][0]["run"]["command"] == "npm ci"
    assert (
        other_branch.to_dict()["steps"][0]["run"]["command"]
        == "pip install -r requirements.txt"
    )


def test_base_selectors_accept_resource_objects():
    template = Template(id="0190a5a0-0000-7000-8000-000000000001", workspace_id="w", name="n")
    snapshot = Snapshot(
        id="0190a5a0-0000-7000-8000-000000000002", session_id="s", state="READY"
    )
    image = Image(ref="acme/node-api", digest_ref="acme/node-api@sha256:" + "a" * 64)

    assert TemplateSpec().from_template(template).to_dict()["base"] == {
        "templateId": template.id
    }
    assert TemplateSpec().from_snapshot(snapshot).to_dict()["base"] == {
        "snapshotId": snapshot.id
    }
    assert TemplateSpec().from_image(image).to_dict()["base"] == {
        "image": image.digest_ref
    }
    assert TemplateSpec().from_template(template.id).to_dict()["base"] == {
        "templateId": template.id
    }


def test_spec_matches_canonical_filesystem_example():
    spec = (
        TemplateSpec()
        .from_image("sandbox")
        .workdir("/home/tenki/app")
        .with_git_context(
            "https://github.com/acme/app",
            "main",
            dest="/home/tenki/app",
            mode="contents",
            ignore=[".git/cache"],
        )
        .build_env({"NODE_ENV": "production"})
        .run("npm ci", timeout=1800)
        .copy("ops/nginx.conf", "/etc/nginx/nginx.conf")
        .write_file("/home/tenki/app/.template-built", "ready\n")
        .runtime_env({"PORT": "3000"})
        .start(
            "npm run dev",
            run_at="build",
            workdir="/home/tenki/app",
            snapshot_mode="filesystem",
            ready_when={
                "timeout": 60,
                "poll_interval": 1,
                "checks": [
                    {
                        "http": {
                            "url": "http://127.0.0.1:3000/health",
                            "success_status_codes": [200, 204],
                        }
                    }
                ],
            },
            stop_grace_seconds=30,
        )
        .resources(cpu_cores=2, memory_mb=4096, disk_size_gb=10)
    )
    spec.validate()
    assert spec.to_dict() == {
        "specVersion": "tenki.template.v1",
        "base": {"image": "sandbox"},
        "workdir": "/home/tenki/app",
        "context": {
            "source": {
                "git": {"repo": "https://github.com/acme/app", "ref": "main"}
            },
            "checkout": {"dest": "/home/tenki/app", "mode": "TEMPLATE_CHECKOUT_MODE_CONTENTS"},
            "ignore": [".git/cache"],
        },
        "build": {"env": {"NODE_ENV": "production"}},
        "steps": [
            {"run": {"command": "npm ci", "timeoutSeconds": 1800}},
            {"copy": {"src": "ops/nginx.conf", "dest": "/etc/nginx/nginx.conf"}},
            {
                "writeFile": {
                    "path": "/home/tenki/app/.template-built",
                    "content": "cmVhZHkK",
                }
            },
        ],
        "runtime": {
            "env": {"PORT": "3000"},
            "runAt": "TEMPLATE_RUNTIME_RUN_AT_BUILD",
            "start": {"command": "npm run dev", "workdir": "/home/tenki/app"},
            "snapshotMode": "TEMPLATE_SNAPSHOT_MODE_FILESYSTEM",
            "readyWhen": {
                "timeoutSeconds": 60,
                "pollIntervalSeconds": 1,
                "checks": [
                    {
                        "http": {
                            "url": "http://127.0.0.1:3000/health",
                            "successStatusCodes": [200, 204],
                        }
                    }
                ],
            },
            "stopGraceSeconds": 30,
        },
        "resources": {"cpuCores": 2, "memoryMb": 4096, "diskSizeGb": 10},
    }


def test_json_round_trip_and_equality():
    spec = (
        TemplateSpec()
        .with_git_context("https://github.com/acme/app", "v1.2.3")
        .apt("git", "curl")
        .pip(["uv", "ruff"])
        .start(["npm", "start"], ready_when=[3000, {"exec": "test -f /ready"}])
    )
    restored = TemplateSpec.from_json(spec.to_json())
    assert restored == spec
    assert TemplateSpec.from_dict(spec.to_dict()) == spec


def test_from_json_rejects_unknown_fields_and_versions():
    with pytest.raises(TemplateSpecValidationError):
        TemplateSpec.from_json(
            json.dumps(
                {
                    "specVersion": "tenki.template.v1",
                    "base": {"image": "sandbox"},
                    "bogus": True,
                }
            )
        )
    with pytest.raises(TemplateSpecValidationError) as err:
        TemplateSpec.from_json(
            json.dumps({"specVersion": "tenki.template.v2", "base": {"image": "sandbox"}})
        )
    assert "specVersion" in str(err.value)


def test_validate_aggregates_violations():
    spec = (
        TemplateSpec()
        .copy("../escape", "relative/dest")
        .run("npm ci", timeout=99999999)
        .apt("-bad-flag")
    )
    with pytest.raises(TemplateSpecValidationError) as err:
        spec.validate()
    violations = err.value.violations
    assert any("copy steps require a Git context" in v for v in violations)
    assert any("copy.src" in v for v in violations)
    assert any("copy.dest" in v for v in violations)
    assert any("timeout_seconds" in v for v in violations)
    assert any("packages[0]" in v for v in violations)
    assert len(violations) >= 5


def test_validate_rejects_command_argv_ambiguity_from_json():
    payload = {
        "specVersion": "tenki.template.v1",
        "base": {"image": "sandbox"},
        "steps": [{"run": {"command": "npm ci", "argv": ["npm", "ci"]}}],
    }
    spec = TemplateSpec.from_dict(payload)
    with pytest.raises(TemplateSpecValidationError) as err:
        spec.validate()
    assert any("exactly one of command or argv" in v for v in err.value.violations)


def test_memory_snapshot_mode_requires_build_runtime_and_ready_when():
    # memory without run_at=build: run_at defaults to boot -> rejected
    boot = TemplateSpec().start("npm run dev", snapshot_mode="memory")
    with pytest.raises(TemplateSpecValidationError) as err:
        boot.validate()
    assert any("only valid for build runtime" in v for v in err.value.violations)

    # build runtime without ready_when -> rejected
    no_ready = TemplateSpec().start("npm run dev", run_at="build")
    with pytest.raises(TemplateSpecValidationError) as err:
        no_ready.validate()
    assert any("requires ready_when" in v for v in err.value.violations)

    # complete memory workflow spec is accepted
    memory = TemplateSpec().start(
        ["npm", "run", "dev"],
        run_at="build",
        snapshot_mode="memory",
        ready_when=[
            3000,
            {"http": "http://127.0.0.1:3000/health"},
            {"exec": {"command": "test -f /home/tenki/app/.ready", "timeout": 5}},
        ],
    )
    memory.validate()
    runtime = memory.to_dict()["runtime"]
    assert runtime["snapshotMode"] == "TEMPLATE_SNAPSHOT_MODE_MEMORY"
    assert runtime["readyWhen"]["timeoutSeconds"] == 60


def test_ready_when_rejects_non_localhost_http():
    spec = TemplateSpec().start(
        "npm start",
        run_at="build",
        ready_when=[{"http": "http://example.com/health"}],
    )
    with pytest.raises(TemplateSpecValidationError) as err:
        spec.validate()
    assert any("localhost" in v for v in err.value.violations)


def test_ready_when_mapping_kwargs_override_without_mutation():
    ready = {"timeout": 60, "poll_interval": 1, "checks": [3000]}
    original = dict(ready)

    runtime = TemplateSpec().ready_when(ready, timeout=120, poll_interval=5).to_dict()[
        "runtime"
    ]

    assert ready == original
    assert runtime["readyWhen"] == {
        "timeoutSeconds": 120,
        "pollIntervalSeconds": 5,
        "checks": [{"port": 3000}],
    }


def test_process_compose_runtime():
    spec = TemplateSpec().process_compose(
        "process-compose.yaml",
        workdir="/home/tenki/app",
        env_files=[".env.template"],
        run_at="boot",
    )
    spec.validate()
    runtime = spec.to_dict()["runtime"]
    assert runtime["processCompose"] == {
        "configPath": "process-compose.yaml",
        "workdir": "/home/tenki/app",
        "envFiles": [".env.template"],
    }
    escape = TemplateSpec().process_compose("../evil.yaml")
    with pytest.raises(TemplateSpecValidationError):
        escape.validate()


def test_resources_bounds():
    bad = TemplateSpec().resources(cpu_cores=64, memory_mb=513, disk_size_gb=2)
    with pytest.raises(TemplateSpecValidationError) as err:
        bad.validate()
    assert len(err.value.violations) >= 3


def test_env_merging_is_immutable_and_additive():
    base = TemplateSpec().build_env({"A": "1"})
    merged = base.build_env({"B": "2"})
    assert base.to_dict()["build"]["env"] == {"A": "1"}
    assert merged.to_dict()["build"]["env"] == {"A": "1", "B": "2"}


def test_directory_checkout_name_rules():
    spec = TemplateSpec().with_git_context(
        "https://github.com/acme/app", "main", mode="directory", name="app"
    )
    spec.validate()
    bad = TemplateSpec().with_git_context(
        "https://github.com/acme/app", "main", mode="contents", name="app"
    )
    with pytest.raises(TemplateSpecValidationError) as err:
        bad.validate()
    assert any("directory" in v for v in err.value.violations)


def test_file_operation_steps():
    spec = (
        TemplateSpec()
        .mkdir("/opt/app", parents=True, mode=0o755)
        .remove("/tmp/scratch", recursive=True)
        .rename("/opt/app/a", "/opt/app/b")
        .symlink("/opt/app/b", "/usr/local/bin/app", name="link it")
    )
    spec.validate()
    steps = spec.to_dict()["steps"]
    assert steps[0]["mkdir"] == {"path": "/opt/app", "parents": True, "mode": 0o755}
    assert steps[1]["remove"] == {"path": "/tmp/scratch", "recursive": True}
    assert steps[2]["rename"] == {"src": "/opt/app/a", "dest": "/opt/app/b"}
    assert steps[3] == {
        "name": "link it",
        "symlink": {"target": "/opt/app/b", "path": "/usr/local/bin/app"},
    }
