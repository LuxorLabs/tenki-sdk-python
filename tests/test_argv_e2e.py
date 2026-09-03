"""Live argv checks against a real guest (CLO-4671). Opt-in.

The unit tests in ``test_argv_normalization.py`` assert on the start frame, which
proves what the SDK puts on the wire but not what the guest does with it. These
run the same argv shapes against a real sandbox, so they are what to reach for
when debugging argv handling end to end.

Disabled by default. To run against local development::

    export SANDBOX_E2E=1
    export TENKI_API_URL=http://localhost:8090     # 8090, and http:// is REQUIRED
    export TENKI_AUTH_TOKEN=tk_...                 # or TENKI_API_KEY
    .venv/bin/python -m pytest tests/test_argv_e2e.py -v

The scheme is not optional. ``resolve_base_url`` feeds ``_parse_url``, which
prepends ``https://`` to anything without a scheme, so a bare
``localhost:8090`` makes the SDK dial TLS against a plaintext port and fail with
``CERTIFICATE_VERIFY_FAILED``. ``TENKI_API_ENDPOINT`` also outranks
``TENKI_API_URL``, so a stale endpoint silently wins over the URL you just set.

The port matters too: the sandbox engine serves ``SandboxService`` on ``:8090``
(``backend/configs/sandbox-engine.yaml``). ``:8080`` is traefik, which has no
route for it and answers ``404``. The fixture checks all three before creating
anything.

Optional overrides: ``TENKI_SANDBOX_E2E_CPU_CORES`` (default 1),
``TENKI_SANDBOX_E2E_MEMORY_MB`` (default 1024), ``TENKI_SANDBOX_E2E_TIMEOUT``
(per-command seconds, default 60), ``TENKI_SANDBOX_E2E_CREATE_TIMEOUT``
(default 180) so a misconfigured endpoint fails in minutes, not an hour.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request

import pytest

from tenki_sandbox import Sandbox

# Private, deliberately: reusing the real dial resolution keeps the endpoint
# diagnostic below from drifting away from what the transport actually does.
from tenki_sandbox._transport import grpc_target, resolve_base_url

pytestmark = pytest.mark.skipif(
    os.environ.get("SANDBOX_E2E") != "1",
    reason="set SANDBOX_E2E=1 to run sandbox SDK E2E tests",
)


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    return int(value) if value else default


COMMAND_TIMEOUT = _env_int("TENKI_SANDBOX_E2E_TIMEOUT", 60)
CREATE_TIMEOUT = _env_int("TENKI_SANDBOX_E2E_CREATE_TIMEOUT", 180)

_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


def _check_endpoint() -> str:
    """Fail fast on the endpoint mistakes that otherwise surface as TLS errors."""
    base_url = resolve_base_url(None)
    target, secure = grpc_target(base_url)
    if not target:
        pytest.fail(f"could not resolve a dial target from {base_url!r}")
    host = target.rsplit(":", 1)[0]
    if secure and host in _LOOPBACK_HOSTS:
        source = (
            "TENKI_API_ENDPOINT"
            if os.environ.get("TENKI_API_ENDPOINT")
            else "TENKI_API_URL"
        )
        pytest.fail(
            f"{source}={base_url!r} dials TLS at {target}, but local development "
            f"serves plaintext, so the handshake fails with "
            f"CERTIFICATE_VERIFY_FAILED. Set {source}=http://{target} instead "
            f"(a URL without a scheme is treated as https)."
        )
    return base_url


def _check_service_route(base_url: str) -> None:
    """A 404 means the port answers HTTP but does not mount SandboxService."""
    url = base_url.rstrip("/") + "/tenki.sandbox.v1.SandboxService/ListSessions"
    request = urllib.request.Request(
        url, data=b"{}", method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            status = response.status
    except urllib.error.HTTPError as err:
        status = err.code
    except OSError:
        # Unreachable is create()'s story to tell, with its own error mapping.
        return
    if status == 404:
        pytest.fail(
            f"{base_url} answers HTTP but serves no SandboxService route (404). "
            "In local development the sandbox engine listens on :8090 "
            "(backend/configs/sandbox-engine.yaml); :8080 is traefik, which has "
            "no route for it."
        )


@pytest.fixture(scope="module")
def sandbox():
    """One sandbox for the whole module; argv handling needs no isolation."""
    if not (os.environ.get("TENKI_AUTH_TOKEN") or os.environ.get("TENKI_API_KEY")):
        pytest.fail("TENKI_AUTH_TOKEN or TENKI_API_KEY is required")
    _check_service_route(_check_endpoint())
    sb = Sandbox.create(
        cpu_cores=_env_int("TENKI_SANDBOX_E2E_CPU_CORES", 1),
        memory_mb=_env_int("TENKI_SANDBOX_E2E_MEMORY_MB", 1024),
        # Bounds the RPC so a bad endpoint fails in minutes, not an hour.
        timeout=CREATE_TIMEOUT,
    )
    try:
        yield sb
    finally:
        sb.close()


def test_list_argv_runs_the_command(sandbox):
    """The exact call from the ticket: it used to ENOENT with empty stdout."""
    result = sandbox.exec(["sh", "-c", "echo hi"], timeout=COMMAND_TIMEOUT)
    result.check()
    assert result.stdout_text.strip() == "hi"
    assert result.argv == ["sh", "-c", "echo hi"]


def test_list_and_varargs_are_equivalent(sandbox):
    as_list = sandbox.exec(["sh", "-c", "echo hi"], timeout=COMMAND_TIMEOUT)
    as_varargs = sandbox.exec("sh", "-c", "echo hi", timeout=COMMAND_TIMEOUT)
    assert as_list.argv == as_varargs.argv
    assert as_list.stdout_text == as_varargs.stdout_text
    assert as_list.exit_code == as_varargs.exit_code == 0


def test_argv_parts_are_never_shell_split(sandbox):
    """A part with spaces is one argument, so argc is 2 and $1 keeps the spaces."""
    result = sandbox.exec(
        ["sh", "-c", 'printf "%s|%s" "$#" "$1"', "sh", "a b c"],
        timeout=COMMAND_TIMEOUT,
    )
    result.check()
    assert result.stdout_text == "1|a b c"


def test_exit_code_and_stderr_survive_the_list_form(sandbox):
    result = sandbox.exec(
        ["sh", "-c", "echo oops >&2; exit 3"], timeout=COMMAND_TIMEOUT
    )
    assert result.exit_code == 3
    assert result.ok is False
    assert "oops" in result.stderr_text


def test_missing_binary_is_diagnosable_not_silent(sandbox):
    """The half of the ticket about an empty result being unreadable."""
    result = sandbox.exec(["tenki-not-a-real-binary"], timeout=COMMAND_TIMEOUT)
    assert result.ok is False
    # Something has to say why: an exit code, a reason, or an errno.
    assert result.exit_code != 0 or result.reason or result.errno, (
        "a command that never started reported nothing: "
        f"exit_code={result.exit_code} reason={result.reason!r} errno={result.errno!r}"
    )


def test_container_argv_is_rejected_before_reaching_the_guest(sandbox):
    with pytest.raises(TypeError):
        sandbox.exec("sh", "-c", ["echo", "hi"], timeout=COMMAND_TIMEOUT)


def test_scalar_argv_parts_still_reach_the_guest_as_strings(sandbox):
    result = sandbox.exec(["sleep", 0], timeout=COMMAND_TIMEOUT)
    result.check()
    assert result.argv == ["sleep", "0"]
