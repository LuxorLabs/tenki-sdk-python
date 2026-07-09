# Tenki Sandbox Python SDK

Python SDK for Tenki Sandbox: cloud sandboxes for AI agents and code execution.

```bash
pip install tenki-sandbox
```

```python
from tenki_sandbox import Sandbox

# create() waits by default via a single server-held request and returns an
# exec-ready sandbox with data-plane access primed (wait=False to skip).
with Sandbox.create(project_id="proj_123", cpu_cores=2, memory_mb=4096) as sb:
    result = sb.exec("python3", "-c", "print('hello')")
    result.check()
    print(result.stdout_text)

    # fs paths are relative to the sandbox workdir (absolute paths must stay inside it)
    sb.fs.write_text("input.txt", "data")
    print(sb.fs.read_text("input.txt"))

    preview = sb.expose_port(3000, ttl=3600)
    print(preview.url)
```

## Async (asyncio)

`AsyncClient` / `AsyncSandbox` expose the same surface with native `async`/`await`,
built on `grpc.aio` — no `asyncio.to_thread` wrapping required. Use it inside any
asyncio server (FastAPI, aiohttp, etc.).

```python
import asyncio
from tenki_sandbox import AsyncSandbox


async def main():
    async with await AsyncSandbox.create(project_id="proj_123", cpu_cores=2) as sb:
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
the `tenki-sandbox[async]` extra for `websockets`), `dial()` returns an
`AsyncDialConn`, and `expose_host_port()` / `expose_host_port_resilient()` return
async tunnels with `await tunnel.terminated.wait()` and `on_terminated` callbacks.

## Auth

Auth resolution:

1. `auth_token=` passed to `Client` or `Sandbox.create`
2. `TENKI_AUTH_TOKEN`
3. `TENKI_API_KEY`

`TENKI_API_ENDPOINT` overrides the API URL; legacy `TENKI_API_URL` is also accepted.

## Process API

`exec` collects stdout/stderr and returns a result:

```python
result = sb.exec("npm", "test", timeout=60, env={"CI": "1"})
print(result.stdout_text)
result.check()
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

Use `shell()` when you want shell parsing:

```python
sb.shell("python3 -m http.server 3000 >/tmp/server.log 2>&1 &")
```

## Sandbox lifetime

Long-lived sandboxes are a parameter choice at `create()`, not a separate API:

```python
sb = client.create(
    project_id="proj_123",
    sticky=True,              # long-lived session: not reaped on idle
    idle_timeout_minutes=120, # or: generous idle window before auto-pause
    max_duration=8 * 3600,    # total lifetime cap (seconds)
    pause_retention=24 * 3600,# how long a paused session is kept resumable
)
```

- `max_duration` caps total lifetime; `sb.extend(seconds)` pushes the deadline
  (`sb.info.timeout_at`) while running.
- `idle_timeout_minutes` auto-pauses an idle sandbox; `sb.resume()` brings it
  back with the filesystem intact.
- `sticky=True` opts the session out of idle reaping for keep-warm use cases
  (workspaces cap concurrent sticky sessions).
- `client.list(sticky=True)` filters for long-lived sessions.

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

## Resource APIs

```python
from tenki_sandbox import Client, GiB

client = Client()

volume = client.volumes.create(
    workspace_id="ws_123",
    name="cache",
    size_bytes=10 * GiB,
    project_id="proj_123",
)

preview = client.preview_urls.create(
    project_id="proj_123",
    slug="demo",
    session_id=sb.id,
    port=3000,
)
```
