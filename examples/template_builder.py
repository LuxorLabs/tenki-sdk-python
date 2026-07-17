"""Typed template builder workflow: recipe -> build -> image -> sandbox.

Covers the complete filesystem workflow plus a memory-snapshot template.
Requires TENKI_AUTH_TOKEN (and optionally TENKI_API_ENDPOINT) in the env.
"""

from __future__ import annotations

import os
import sys

from tenki_sandbox import Client, TemplateBuildFailedError, TemplateSpec

WORKSPACE_ID = os.environ["TENKI_WORKSPACE_ID"]
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")


def print_event(event) -> None:
    # One ordered handler receives both log and progress events.
    if event.type == "log":
        stream = sys.stderr if event.stream == "stderr" else sys.stdout
        stream.write(event.data)
    else:
        print(f"[{event.phase}] {event.state}: {event.message or ''}")


def filesystem_spec() -> TemplateSpec:
    # Immutable typed recipe: every builder call returns a new value.
    return (
        TemplateSpec()
        .from_image("sandbox")
        .with_git_context("https://github.com/acme/node-api", "main")
        .run("npm ci", name="Install dependencies")
        .copy("ops/nginx.conf", "/etc/nginx/nginx.conf")
        .write_file("/home/tenki/app/.template-built", "ready\n")
        .runtime_env({"NODE_ENV": "production"})
        .start(
            ["npm", "start"],
            run_at="boot",
            ready_when=[{"http": "http://127.0.0.1:3000/health"}],
        )
        .resources(cpu_cores=2, memory_mb=4096, disk_size_gb=10)
    )


def filesystem_workflow(client: Client) -> None:
    spec = filesystem_spec()
    spec.validate()  # aggregate local validation; the server stays authoritative

    template = client.templates.create(
        workspace_id=WORKSPACE_ID, name="node-api", spec=spec
    )
    print(f"template {template.name} spec_hash={template.spec_hash}")

    # build_secrets are explicit request-time values, never persisted.
    try:
        build = client.templates.build(
            template,
            build_secrets={"GITHUB_TOKEN": GITHUB_TOKEN} if GITHUB_TOKEN else None,
            wait_for_completion=True,
            on_event=print_event,
        )
    except TemplateBuildFailedError as err:
        print(f"build failed: {err}; failure={err.build.failure}")
        raise

    print(f"image ref={build.image.ref} digest_ref={build.image.digest_ref}")

    sandbox = client.create(image=build.image, wait_for_runtime=True)
    try:
        print(sandbox.exec("cat", "/home/tenki/app/.template-built").stdout_text)
    finally:
        sandbox.close()


def memory_spec() -> TemplateSpec:
    # Memory capture requires run_at="build" plus a ready_when deadline;
    # incompatible restores cold-boot the rootfs and report degraded attribution.
    return (
        TemplateSpec()
        .from_image("sandbox")
        .with_git_context("https://github.com/acme/node-api", "main")
        .run("npm ci")
        .start(
            ["npm", "run", "dev"],
            run_at="build",
            snapshot_mode="memory",
            ready_when={
                "timeout": 60,
                "checks": [3000, {"http": "http://127.0.0.1:3000/health"}],
            },
        )
    )


def memory_workflow(client: Client) -> None:
    spec = memory_spec()
    template = client.templates.create(
        workspace_id=WORKSPACE_ID, name="node-api-warm", spec=spec
    )
    build = client.templates.build(template, wait_for_completion=True)
    sandbox = client.create(image=build.image, wait_for_runtime=True)
    try:
        info = sandbox.refresh()
        restore_mode = info.metadata.get("restore_mode")
        if restore_mode == "memory":
            assert info.metadata.get("restore_degraded") == "false"
        elif restore_mode == "cold_boot":
            assert info.metadata.get("restore_degraded") == "true"
            assert info.metadata.get("restore_reason")
        else:
            raise RuntimeError(f"unexpected restore mode: {restore_mode!r}")
        print(f"runtime_state={info.runtime_state} metadata={dict(info.metadata)}")
    finally:
        sandbox.close()


def reconnect_and_cancel(client: Client, build_id: str) -> None:
    # Reconnect replays ordered events exactly once; Ctrl-C or cancel_event
    # stops observation only. Remote cancellation is always explicit.
    build = client.templates.wait_build(build_id, on_event=print_event)
    client.templates.cancel_build(build)


if __name__ == "__main__":
    with Client() as client:
        filesystem_workflow(client)
        memory_workflow(client)
