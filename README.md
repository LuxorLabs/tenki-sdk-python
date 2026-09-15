# Tenki Python SDK

Python SDK for Tenki: cloud sandboxes (microVMs) for AI agents and code execution.

```bash
pip install tenki
```

> The SDK is published as `tenki`. Existing code that does
> `import tenki_sandbox` keeps working (that module ships inside `tenki`), but
> new code should use `import tenki`.

## Protobuf compatibility

The SDK supports `protobuf>=5.29.5` with no upper bound. Its Python bindings are
temporarily generated with protobuf 5.29.5 so applications that require
`protobuf<6` can install the SDK alongside dependencies such as Memori.

Protobuf 6.31.x emits a benign warning when it loads 5.29.5-generated bindings.
If a test suite pins that runtime and promotes warnings to errors, scope the
exception narrowly instead of disabling protobuf's runtime checks:

```toml
[tool.pytest.ini_options]
filterwarnings = [
  "ignore:Protobuf gencode version 5\\.29\\.5 is exactly one major version older than the runtime version 6\\.31\\..*:UserWarning:google\\.protobuf\\.runtime_version",
]
```

For local SDK tests, run `./scripts/test.sh`; it installs test dependencies in
a versioned user cache when needed.

```python
from tenki import Sandbox

# create() waits by default via a single server-held request and returns an
# exec-ready sandbox with data-plane access primed (wait=False to skip).
with Sandbox.create(cpu_cores=2, memory_mb=4096) as sb:
    result = sb.exec("python3", "-c", "print('hello')")
    result.check()
    print(result.stdout_text)

    # fs paths are relative to the sandbox workdir (absolute paths must stay inside it)
    sb.fs.write_text("input.txt", "data")
    print(sb.fs.read_text("input.txt"))

    preview = sb.expose_port(3000, ttl=3600)
    print(preview.url)
```

The API key determines the Workspace automatically; ordinary Sandbox calls do not require a Workspace ID.

## Durable pause

`pause()` preserves legacy completion semantics and returns only after the pause completes.
`pause_async()` returns after the service accepts the request and moves the sandbox to `PAUSING`.
Call `wait_paused()` after `pause_async()` when the caller needs a durable checkpoint before continuing.
It returns at `PAUSED` and raises `PauseFailedError` if a verified rollback restores `RUNNING`.
Pass `timeout=0` to wait without a deadline.
For explicit local cancellation, pass a `threading.Event` as `cancel_event`; cancellation raises `PauseWaitCancelledError` and leaves the remote pause running.

```python
sb.pause_async()
sb.wait_paused(timeout=0)
```

`AsyncSandbox` exposes the same operation through `await sb.pause_async()` and `await sb.wait_paused(timeout=0)`.
Its wait accepts an `asyncio.Event` or `threading.Event`, and normal task cancellation also stops local observation without cancelling the remote pause.
`DURABLE_CEPH` snapshots can resume on a capability-matched host in the same datacenter while R2 replication continues.
`DURABLE` snapshots are portable to eligible hosts.
`LOCAL_READY` is not resumable for pause operations.

`allow_inbound` and `allow_outbound` both default to `True` on `create()`, so a new sandbox reaches the
internet and can expose ports without passing either argument. Both are create-time settings and cannot be
changed on an existing sandbox; `sandbox.info.inbound_enabled` / `sandbox.info.outbound_enabled` report what
a sandbox was created with.

Restrict outbound access with `allow_domains` and `allow_cidrs`. A sandbox that may only reach PyPI:

```python
sb = Sandbox.create(
    allow_domains=["pypi.org", "*.pypi.org", "files.pythonhosted.org"],
)
```

`allow_outbound=False` still blocks everything, regardless of `allow_domains`/`allow_cidrs`.
Read the allowlist back from `sb.info.egress`.

## What survives a pause

Pause captures the VM's full memory, not only its disk: a resumed sandbox is the
same running machine, not a reboot. Processes keep their PIDs and their in-memory
state, and the guest clock stops for the duration of the pause.

`/tmp` is cleared across a pause. Keep durable state under `/home/tenki`, which
is preserved.

While a sandbox is paused it serves no traffic: preview requests return `404`
until it resumes. The client connection itself is not dropped — a held
keep-alive connection returns `200` again after resume — so treat a paused
sandbox as unavailable rather than disconnected.

SSH is different: a held transport fails on use while paused and does not
recover after resume, so reconnect rather than retry on it. A fresh
connection succeeds as soon as the sandbox is `RUNNING` again.

```python
sb.exec("bash", "-lc", "echo hello > ~/marker")
sb.exec("bash", "-lc", "setsid nohup sleep 3600 >/dev/null 2>&1 </dev/null &")
pid = sb.exec("bash", "-lc", "pgrep -n sleep").stdout.decode().strip()

sb.pause()
sb.resume()

assert sb.exec("bash", "-lc", "cat ~/marker").stdout == b"hello\n"
assert sb.exec("bash", "-lc", f"kill -0 {pid} && echo alive").stdout == b"alive\n"
```

`examples/pause_preserves_memory.py` extends this to prove the memory claim
directly: it hashes a multi-megabyte buffer that only ever exists in RAM and
re-reads it from the same process after resume.

## Async (asyncio)

`AsyncClient` / `AsyncSandbox` expose the same surface with native `async`/`await`,
built on `grpc.aio` — no `asyncio.to_thread` wrapping required. Use it inside any
asyncio server (FastAPI, aiohttp, etc.).

```python
import asyncio
from tenki import AsyncSandbox


async def main():
    async with await AsyncSandbox.create(cpu_cores=2) as sb:
        result = await sb.exec("python3", "-c", "print('hello')")
        result.check()
        print(result.stdout_text)

        await sb.fs.write_text("input.txt", "data")
        print(await sb.fs.read_text("input.txt"))

        proc = await sb.start("bash", "-lc", "read name; echo hi $name")
        await proc.write_stdin("tenki\n")
        await proc.close_stdin()
        async for chunk in proc.stdout:
            print(chunk.decode(), end="")
        (await proc.wait()).check()


asyncio.run(main())
```

The sync `Client` / `Sandbox` remain available for non-async consumers. SSH,
`dial`, and host-port tunnels are also async: `AsyncSandbox.ssh()` returns an
`AsyncSSHConn` (raw async `read`/`write`/`close`, matching the TS/Go SDKs; needs
the `tenki[async]` extra for `websockets`), `dial()` returns an
`AsyncDialConn`, and `expose_host_port()` / `expose_host_port_resilient()` return
async tunnels with `await tunnel.terminated.wait()` and `on_terminated` callbacks.
For sync and async resilient tunnels, `address, port = tunnel.endpoint()` reads
one established connection. It retains the last endpoint during reconnect and after close.

## Auth

Auth resolution:

1. `auth_token=` passed to `Client` or `Sandbox.create`
2. `TENKI_AUTH_TOKEN`
3. `TENKI_API_KEY`

Credentials must be a Tenki API key or service token starting with `tk_`. Missing
credentials raise `MissingAuthTokenError`; nonempty credentials with any other
prefix raise `InvalidAuthTokenError`. Requests authenticate with an
`Authorization: Bearer` header.

### Migrating to v0.7.0

Version 0.7.0 removes the generated workspace settings and pause-retention RPCs.
Use `get_usage()` for read-only workspace usage and limit data. Its shared
concurrency entry now uses the `max_concurrent_jobs` key.

### Migrating to v0.6.0

Version 0.6.0 removes the `cookie_name` client and sandbox option and support
for Ory session tokens and browser cookie values. Pass a `tk_` API key or
service token through `auth_token`; the SDK sends it as an
`Authorization: Bearer` credential.

`TENKI_API_ENDPOINT` overrides the API URL; legacy `TENKI_API_URL` is also accepted.

## Process API

`exec` collects stdout/stderr and returns a result:

```python
result = sb.exec("npm", "test", cwd="app", timeout=60, env={"CI": "1"})
print(result.stdout_text)
result.check()
```

Argv may be passed as separate arguments or as a single list:

```python
sb.exec("sh", "-c", "echo hi")
sb.exec(["sh", "-c", "echo hi"])  # equivalent
```

`start` returns a live process:

```python
proc = sb.start("bash", "-lc", "read name; echo hello $name")
proc.write_stdin("tenki\n")
proc.close_stdin()
for chunk in proc.stdout:
    print(chunk.decode(), end="")
proc.wait().check()
```

Signals are enqueued, not awaited: neither call waits for the process to die —
matching `subprocess.Popen.kill()` — and `wait()` is what blocks until it is
actually dead:

```python
proc = sb.start("sleep", "3600")
proc.signal("SIGTERM")  # returns once the frame is queued
result = proc.wait()  # returns once the process has exited
print(result.signal)  # "terminated"
```

The two differ before the process has started. `signal()` waits for the guest to
acknowledge the spawn first, so on a slow start it blocks for up to 30s and
raises `TimeoutError` if the acknowledgement never arrives. `kill()` skips that
wait and always returns immediately, which makes it the safer call during
startup.

`result.signal` carries the guest's own name for the signal, not the name you
passed: SIGTERM reports `"terminated"` and SIGKILL reports `"killed"`. Compare
against those values, not against `"TERM"`/`"KILL"`.

`kill()` takes no argument and always sends SIGKILL; use `signal(name)` for
anything else. `signal()` accepts `KILL`, `TERM`, `INT`, `HUP`, `USR1` and
`USR2` (with or without the `SIG` prefix) and raises `ValueError` for anything
else — note the Go and TypeScript SDKs silently downgrade an unknown signal to
SIGTERM instead. One name escapes the check: `signal("unspecified")` is accepted
and sends the protobuf zero value, which the guest maps to no signal at all, so
it is a silent no-op rather than a `ValueError`. Do not port that no-op to the
other SDKs — Go and TypeScript map `unspecified` to SIGTERM like any other
unrecognized name, so there it **terminates** the process.

After a normal exit both calls are no-ops rather than errors. The one exception:
if the handle already failed with a stream error, `signal()` re-raises that
error, while `kill()` stays silent unconditionally.

`wait(timeout=...)` sends SIGKILL and raises `TimeoutError` when the timeout
expires. Once the process is running the handle stays usable, so you can call
`wait()` again for the exit result. Do not rely on that during startup: if the
timeout expires while the run stream is still being established, the SIGKILL it
sends also cancels the establishment retry, and every later `wait()` raises the
underlying stream error instead of returning a result.

The asyncio client mirrors this exactly: `await proc.signal(...)` and
`await proc.kill()` resolve at enqueue, `await proc.wait()` at exit.

Process lifetime is bound to the Run stream, not to the handle. Once that stream
tears down — the connection breaks, or the edge times the idle connection out
(~30s) — the platform sends SIGTERM, escalates to SIGKILL after 5s and reaps
within 10s.

Dropping the handle does **not** close the stream on its own: the pump thread
keeps reading until the process exits or the stream fails. An abandoned process
can keep running, and one that keeps writing output keeps its own stream alive
indefinitely. To stop a process, signal it and `wait()` rather than abandoning
it. There is no reattach.

Use `shell()` when you want shell parsing:

```python
sb.shell("python3 -m http.server 3000 >/home/tenki/server.log 2>&1 &")
```

### Timeouts

Commands are unbounded by default — omit `timeout` or pass `0`. A positive
`timeout` is sent to the guest-agent, which enforces it: on expiry it signals the
process (`SIGTERM`, escalating to `SIGKILL`) and reports the run as timed out.

That is **not** an exception — it comes back as an ordinary `CommandResult` with
`timed_out` set and `ok` false, carrying the output captured before the budget
expired:

```python
result = sb.exec("npm", "ci", timeout=120)
if result.timed_out:
    # result.reason is "timeout", or "grace_timeout" when the guest could not
    # reap the process. Partial output usually shows where it stalled.
    print(result.reason, result.stdout_text)
```

Passing `check=True` raises `CommandFailedError` for a timed-out run, since
`ok` is false.

The guest caps the request at its own configured maximum command timeout, and
older guest-agents accept `timeout` but ignore it, leaving the run unbounded.

`Process.wait(timeout=...)` is a separate, local bound: it limits how long _your
call_ blocks and raises the builtin `TimeoutError`, saying nothing about the
command's own fate.

### Background processes and long-running services

Note the redirect in the `shell()` example above — it is required, not stylistic.
A command returns when its **stdout and stderr reach EOF**, not when the shell
exits. A backgrounded process inherits both streams and holds them open, so this
waits for the server rather than the shell:

```python
sb.shell("python3 -m http.server 3000 &")  # hangs
```

Redirecting only stdout is not enough — stderr still holds the stream open — and
`nohup` does not help, because it blocks `SIGHUP` rather than stream inheritance.
This is standard POSIX behavior, the same as `subprocess.run(...,
capture_output=True)`.

To keep hold of a service instead, use `sb.start(...)` and read from the returned
process; for services you always want running, start them from a template start
command.

Process `cwd` values follow the guest contract: relative paths are normalized
under the sandbox guest workdir (`/home/tenki` by default), absolute paths are
used unchanged, and missing or non-directory targets fail before the process
starts.

`cwd` sets the spawned process's directory. A login shell (`bash -lc`) sources
the guest's startup files first, so a `cd` in `~/.bashrc` or `~/.profile` runs
before your command and wins over `cwd`; use `bash -c` when the directory must
hold.

## Sandbox lifetime

Long-lived sandboxes are a parameter choice at `create()`, not a separate API:

```python
sb = client.create(
    sticky=True,              # long-lived session: no automatic lifetime limit
    pause_retention=24 * 3600,# how long a paused session is kept resumable
)
```

- `max_duration` caps total lifetime; `sb.extend(seconds)` pushes the deadline
  (`sb.info.timeout_at`) while running.
- `sticky=True` runs the session until it is terminated, for keep-warm use
  cases. It discards `max_duration`. Manual pause and resume do not clear
  sticky.
- `idle_timeout_minutes` is deprecated and ignored: sandboxes do not auto-pause
  on idle. Use `max_duration`, or `sb.pause()` to pause explicitly.
- `client.list(sticky=True)` filters for long-lived sessions in the API key's Workspace.

When the service discards a create option, `sb.warnings` retains the structured
warning. By default, `Client` and `AsyncClient` emit a `SandboxWarningNotification`.
Pass `warning_handler` to route warnings through your logger, or pass `None` to
suppress emission while still reading `sb.warnings`:

```python
import logging

from tenki import Client, SandboxWarningCode

logger = logging.getLogger(__name__)
client = Client(warning_handler=lambda warning: logger.warning(warning.message))
sb = client.create(sticky=True, max_duration=8 * 3600)
assert sb.warnings[0].code == SandboxWarningCode.STICKY_OVERRIDES_MAX_DURATION
```

## SSH

For tools that speak SSH (paramiko, scp, IDE remote dev), the SDK can mint a
short-lived OpenSSH user certificate for your session and open a transport to
the sandbox SSH gateway. No keys are provisioned into the guest; the engine
signs your local public key and the gateway verifies the certificate.

Requires `pip install websocket-client paramiko` (websocket-client for the
transport, paramiko if you want a client in-process).

```python
import subprocess
import paramiko

# 1. local keypair (any OpenSSH key works; ed25519 shown)
subprocess.run(["ssh-keygen", "-t", "ed25519", "-N", "", "-q", "-f", "id_tenki"], check=True)

# 2. engine signs a short-lived user cert for this sandbox
cert = sb.issue_ssh_cert(open("id_tenki.pub").read(), ttl=600)
open("id_tenki-cert.pub", "w").write(cert.ssh_cert)

# 3. open the gateway transport and run a paramiko session over it
pkey = paramiko.Ed25519Key.from_private_key_file("id_tenki")
pkey.load_certificate("id_tenki-cert.pub")

transport = paramiko.Transport(sb.ssh())  # WebSocket-backed socket
transport.connect(username="tenki", pkey=pkey)
session = transport.open_session()
session.exec_command("echo hello-over-ssh")
print(session.makefile().read().decode())
transport.close()
```

Notes:

- `sb.ssh()` / `client.ssh(session_id)` discover an active gateway and return
  `SSHConn`, an `io.RawIOBase` socket usable anywhere paramiko accepts one.
- The SSH username is `tenki`.
- `TENKI_SANDBOX_GATEWAY_URL` overrides the gateway WebSocket URL (it is
  otherwise derived from the API endpoint).
- Certificate RPCs use the Connect protocol over HTTPS (same as the Go SDK and
  the `tenki` CLI), so they work through standard HTTP load balancers.
- `sb.update_ssh_authorized_keys([...])` additionally plants long-lived public
  keys in the guest's `authorized_keys` if you prefer key-based auth for the
  in-guest sshd.

## Disk space

The root disk defaults to 5 GB and holds the base image as well as your work, so a fresh
sandbox already reports around 56% used. It is fixed at create time and cannot be grown in
place — pass `disk_size_gb` (5–100) to `create` if you need more. Budget for it before a
large install.

Every `CommandResult` carries `disk`, and `check=True` folds a full disk into the raised
`CommandFailedError`. `result.disk.exhausted` reports whether the sandbox
ran out of space at any point during the command — check it even when the command exited 0,
since npm reports `ENOSPC` as a warning and still exits successfully with a broken install.

## Resource APIs

```python
from tenki import Client, GiB

client = Client()

volume = client.volumes.create(
    name="cache",
    size_bytes=10 * GiB,
)

preview = client.preview_urls.create(
    slug="demo",
    session_id=sb.id,
    port=3000,
)
```

## Registry

Delete an untagged, non-latest, unshared registry version by image and snapshot
ID:

```python
result = client.registry.delete_version(
    "11111111-1111-1111-1111-111111111111",
    "55555555-5555-5555-5555-555555555555",
)
```

## Templates (typed builder)

`TemplateSpec` is an immutable typed recipe (every builder call returns a new
value). Builds freeze the normalized spec + `spec_hash` server-side and return
a private digest-addressed `Image`, the normal sandbox launch source.

```python
from tenki import Client, TemplateSpec

client = Client()

spec = (
    TemplateSpec()                     # defaults to base image "sandbox"
    .from_image("sandbox-v2")          # or .from_template(...) / .from_snapshot(...)
    .with_git_context("https://github.com/acme/node-api", "main")
    .run("npm ci", name="Install dependencies")
    .runtime_env({"NODE_ENV": "production"})
    .start(["npm", "start"], ready_when=[{"http": "http://localhost:3000/health"}])
)
spec.validate()  # aggregate local validation; server stays authoritative

template = client.templates.create(name="node-api", spec=spec)

build = client.templates.build(
    template,                          # resource objects in, IDs as fallback
    build_secrets={"GITHUB_TOKEN": token},  # explicit request-time values
    wait_for_completion=True,
    on_event=lambda e: print(e),       # one ordered log/progress handler
)

sb = client.create(image=build.image, wait_for_runtime=True)
```

`wait_build(build_id)` reconnects to an existing build (ordered, deduplicated
events). Local interruption (Ctrl-C or `cancel_event`) stops observation only;
`client.templates.cancel_build(build)` cancels remotely. Waited failures raise
`TemplateBuildFailedError` carrying the final redacted build; runtime waits
raise `TemplateRuntimeFailedError` without terminating the running sandbox.
Strict authored JSON import/export: `spec.to_json()` / `TemplateSpec.from_json(text)`.
Checkout `mode` uses `"contents"` or `"directory"`; runtime `runAt`,
`restartPolicy`, and `snapshotMode` use short values such as `"build"`,
`"on-failure"`, and `"memory"`. Full protobuf enum names remain accepted;
unknown fields and enum values are rejected.
See `examples/template_builder.py` for the full filesystem/memory workflow.

## Workspace secrets

Workspace clients manage secrets independently of sandbox Sessions:

```python
from tenki import WorkspaceClient, SecretPolicy

with WorkspaceClient(workspace_id=workspace_id) as workspace:
    secret = workspace.secrets.create(
        "TOKEN", value_bytes,
        SecretPolicy("guest_and_injection", "unset"),
        request_id=request_id,
    )
```

`AsyncWorkspaceClient` has the same methods and arguments. Use `async with`
and await each operation. Authentication uses `TENKI_AUTH_TOKEN` or `TENKI_API_KEY`.
Set `base_url` or `TENKI_CLOUD_API_URL` to override the cloud API endpoint
independently of sandbox configuration. Set `workspace_id` once on the client. An omitted or empty workspace ID uses the authenticated
workspace key's scope; other callers must specify one.

`create`, `update`, `get`, `list`, `list_versions`, `revoke`, and `delete` return
metadata only. Values are `bytes`; omitted or `None` on update retains the value,
while `b""` creates an empty value. Update, revoke, and delete require
`expected_revision`. To select an existing version, update `active_version` with
`value` omitted. Revoking without `version` irreversibly revokes the entire secret.

Mutations generate a request ID when none is supplied. For retries after an
uncertain result, supply and reuse the same request ID and identical arguments.
The SDK does not retry mutations automatically. `WorkspaceSecretError.code`
preserves the RPC status, including revision conflicts.
