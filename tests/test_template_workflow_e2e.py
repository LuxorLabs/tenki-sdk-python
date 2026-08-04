from __future__ import annotations

import os
import subprocess
import time
from contextlib import contextmanager

import pytest

from tenki_sandbox import Client, TemplateSpec


pytestmark = pytest.mark.skipif(
    os.environ.get("SANDBOX_E2E") != "1",
    reason="set SANDBOX_E2E=1 to run sandbox SDK E2E tests",
)


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.fail(f"{name} is required")
    return value


def _psql(sql: str, **variables: str) -> str:
    dsn = os.environ.get(
        "TENKI_SANDBOX_E2E_DATABASE_URL",
        "postgresql://postgres@127.0.0.1:6444/tenki",
    )
    args = ["psql", dsn, "-Atq", "-v", "ON_ERROR_STOP=1"]
    for key, value in variables.items():
        args.extend(("-v", f"{key}={value}"))
    return subprocess.run(
        args,
        input=sql,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@contextmanager
def _force_incompatible_cpu(snapshot_id: str):
    original = _psql(
        """
        SELECT coalesce(cpu_profile, '') || '|' || coalesce(origin_cpu_signature, '')
        FROM sandbox_snapshots
        WHERE id = :'snapshot_id'::uuid;
        """,
        snapshot_id=snapshot_id,
    ).split("|", 1)
    if len(original) != 2:
        pytest.fail("snapshot CPU metadata was not found")
    _psql(
        """
        UPDATE sandbox_snapshots
        SET cpu_profile = 'legacy',
            origin_cpu_signature = 'sdk-e2e-forced-mismatch'
        WHERE id = :'snapshot_id'::uuid;
        """,
        snapshot_id=snapshot_id,
    )
    try:
        yield
    finally:
        _psql(
            """
            UPDATE sandbox_snapshots
            SET cpu_profile = nullif(:'cpu_profile', ''),
                origin_cpu_signature = nullif(:'origin_cpu_signature', '')
            WHERE id = :'snapshot_id'::uuid;
            """,
            snapshot_id=snapshot_id,
            cpu_profile=original[0],
            origin_cpu_signature=original[1],
        )


def _template_spec(label: str, *, memory: bool) -> TemplateSpec:
    spec = (
        TemplateSpec()
        .with_git_context(
            os.environ.get(
                "TENKI_SANDBOX_E2E_GIT_REPO",
                "https://github.com/octocat/Hello-World.git",
            ),
            os.environ.get("TENKI_SANDBOX_E2E_GIT_REF", "master"),
        )
        .build_env({"SDK_E2E_BUILD": label})
        .run("test -f README", name="verify git checkout")
        .run(f'test "$SDK_E2E_BUILD" = "{label}"', name="verify build env")
        .write_file("/home/tenki/app/sdk-e2e.txt", f"{label}\n")
        .runtime_env({"SDK_E2E_RUNTIME": f"{label}-template"})
        .start(
            [
                "python3",
                "-m",
                "http.server",
                "3000",
                "--directory",
                "/home/tenki/app",
            ],
            run_at="build" if memory else "boot",
            snapshot_mode="memory" if memory else None,
            ready_when={"timeout": 60, "checks": [3000]},
        )
        .resources(cpu_cores=2, memory_mb=2048, disk_size_gb=10)
    )
    spec.validate()
    return spec


def _verify_session(client: Client, build, label: str, expected_mode: str | None):
    session = client.create(
        workspace_id=_required_env("TENKI_SANDBOX_WORKSPACE_ID"),
        image=build.image,
        env={"SDK_E2E_RUNTIME": f"{label}-create"},
        wait_for_runtime=True,
        timeout=300,
    )
    try:
        info = session.refresh()
        assert info.runtime_state == "READY"
        assert (
            session.exec("cat", "/home/tenki/app/sdk-e2e.txt")
            .check()
            .stdout_text.strip()
            == label
        )
        assert (
            session.exec("printenv", "SDK_E2E_RUNTIME").check().stdout_text.strip()
            == f"{label}-create"
        )
        info = session.refresh()
        assert info.runtime_state == "READY"
        if expected_mode is not None:
            assert info.metadata.get("restore_mode") == expected_mode
        return session, info
    except Exception:
        session.close()
        raise


def test_template_workflow_filesystem_memory_and_forced_fallback():
    run_id = str(time.time_ns())
    workspace_id = _required_env("TENKI_SANDBOX_WORKSPACE_ID")
    templates = []
    sessions = []
    image_ids: list[str] = []

    with Client() as client:
        try:
            for mode in ("filesystem", "memory"):
                memory = mode == "memory"
                label = f"py-{mode}-{run_id}"
                template = client.templates.create(
                    workspace_id=workspace_id,
                    name=label,
                    spec=_template_spec(label, memory=memory),
                    tags=["sdk-e2e", "python"],
                )
                templates.append(template)
                build = client.templates.build(
                    template,
                    wait_for_completion=True,
                    timeout=600,
                )
                assert build.state == "READY"
                assert build.image is not None
                assert build.snapshot_id
                assert build.image.digest_ref
                assert build.image.digest.startswith("sha256:")
                assert len(build.image.digest) == len("sha256:") + 64
                assert build.image.digest_ref == f"{build.image.ref}@{build.image.digest}"
                assert build.snapshot_id not in build.image.digest_ref
                image_ids.append(build.image.registry_image_id)

                session, info = _verify_session(
                    client,
                    build,
                    label,
                    "memory" if memory else None,
                )
                sessions.append(session)
                if memory:
                    assert info.metadata.get("restore_degraded") == "false"
                session.close()

                if memory:
                    with _force_incompatible_cpu(build.snapshot_id):
                        cold, cold_info = _verify_session(
                            client,
                            build,
                            label,
                            "cold_boot",
                        )
                        sessions.append(cold)
                        assert cold_info.metadata.get("restore_degraded") == "true"
                        assert cold_info.metadata.get("restore_reason") == "incompatible_cpu"
                        cold.close()
        finally:
            for session in reversed(sessions):
                try:
                    session.close()
                except Exception:
                    pass
            for template in reversed(templates):
                try:
                    client.templates.delete(template)
                except Exception:
                    pass
            for image_id in reversed(image_ids):
                try:
                    client.registry.delete(image_id, reason="SDK E2E cleanup")
                except Exception:
                    pass
