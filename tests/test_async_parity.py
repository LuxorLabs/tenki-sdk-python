"""Assert the async client mirrors the sync surface method-for-method."""

from __future__ import annotations

import inspect

import tenki_sandbox as sync
import tenki_sandbox.aio as aio
from tenki_sandbox import dial as sync_dial
from tenki_sandbox import fs as sync_fs
from tenki_sandbox import git as sync_git
from tenki_sandbox import preview_url as sync_preview
from tenki_sandbox import registry as sync_registry
from tenki_sandbox import resources as sync_resources
from tenki_sandbox import ssh as sync_ssh
from tenki_sandbox import tunnel as sync_tunnel
from tenki_sandbox.aio import client as aio_client
from tenki_sandbox.aio import resources as aio_resources


def _public_methods(cls) -> set[str]:
    return {
        name
        for name, _ in inspect.getmembers(cls, predicate=inspect.isfunction)
        if not name.startswith("_")
    }


def _assert_superset(sync_cls, async_cls, *, pending: set[str] = frozenset()):
    missing = _public_methods(sync_cls) - _public_methods(async_cls) - pending
    assert not missing, f"{async_cls.__name__} missing {sorted(missing)}"


def test_client_surface_parity():
    _assert_superset(sync.Client, aio.AsyncClient)


def test_sandbox_surface_parity():
    _assert_superset(sync.Sandbox, aio.AsyncSandbox)


def test_resource_clients_surface_parity():
    _assert_superset(sync_resources.Volumes, aio_resources.AsyncVolumes)
    _assert_superset(sync_resources.Snapshots, aio_resources.AsyncSnapshots)
    _assert_superset(sync_resources.Templates, aio_resources.AsyncTemplates)
    _assert_superset(sync_registry.Registry, aio.AsyncRegistry)
    _assert_superset(sync_preview.PreviewUrls, aio.AsyncPreviewUrls)


def test_sandbox_helper_surface_parity():
    _assert_superset(sync_fs.SandboxFS, aio.AsyncSandboxFS)
    _assert_superset(sync_git.SandboxGit, aio.AsyncSandboxGit)


def test_tunnel_surface_parity():
    _assert_superset(sync_tunnel.HostPortTunnel, aio.AsyncHostPortTunnel)
    _assert_superset(sync_tunnel.ResilientHostPortTunnel, aio.AsyncResilientHostPortTunnel)


def test_connection_surfaces_expose_raw_async_stream():
    # SSH and dial intentionally drop the sync io.RawIOBase / paramiko-socket
    # adapter shape (readinto/readable/recv/send/...) for a raw async byte stream
    # matching the TS/Go SDKs, so they are NOT sync supersets. Guard the async
    # shape positively instead so the surface can't silently regress.
    for name in ("read", "write", "close"):
        assert inspect.iscoroutinefunction(getattr(aio.AsyncSSHConn, name, None)), f"AsyncSSHConn.{name}"
    for name in ("read", "write", "aclose"):
        assert inspect.iscoroutinefunction(getattr(aio.AsyncDialConn, name, None)), f"AsyncDialConn.{name}"
    # sync counterparts still exist (divergence is intentional, not a missing port)
    assert sync_ssh.SSHConn is not None and sync_dial.DialConn is not None


def test_async_public_methods_are_coroutines_or_async_gen():
    # Every public AsyncClient method is awaitable (coroutine function).
    for name in _public_methods(aio.AsyncClient):
        fn = getattr(aio.AsyncClient, name)
        assert inspect.iscoroutinefunction(fn), f"AsyncClient.{name} is not async"


def test_top_level_reexports():
    assert sync.AsyncClient is aio.AsyncClient
    assert sync.AsyncSandbox is aio.AsyncSandbox
    assert aio_client.AsyncClient is aio.AsyncClient


# Sandbox.create is a typed shortcut, not a **kwargs passthrough, so a new
# Client.create argument no longer reaches it for free. Fail here on drift.
CLIENT_CONNECT_PARAMS = {
    "auth_token",
    "api_key",
    "base_url",
    "gateway_url",
    "timeout",
    "warning_handler",
}


def _keyword_params(fn) -> set[str]:
    return {
        name
        for name, param in inspect.signature(fn).parameters.items()
        if param.kind is inspect.Parameter.KEYWORD_ONLY
    }


def _assert_create_shortcut_covers(client_cls, sandbox_cls):
    expected = (_keyword_params(client_cls.create) - {"_owns_client"}) | CLIENT_CONNECT_PARAMS
    actual = _keyword_params(sandbox_cls.create)
    assert actual == expected, (
        f"{sandbox_cls.__name__}.create drifted from {client_cls.__name__}.create: "
        f"missing {sorted(expected - actual)}, extra {sorted(actual - expected)}"
    )


def test_sandbox_create_shortcut_exposes_every_client_create_param():
    _assert_create_shortcut_covers(sync.Client, sync.Sandbox)
    _assert_create_shortcut_covers(aio.AsyncClient, aio.AsyncSandbox)


def test_sandbox_create_shortcut_defaults_match_client_create():
    pairs = ((sync.Client, sync.Sandbox), (aio.AsyncClient, aio.AsyncSandbox))
    for client_cls, sandbox_cls in pairs:
        shortcut = inspect.signature(sandbox_cls.create).parameters
        for source in (client_cls.__init__, client_cls.create):
            for name, param in inspect.signature(source).parameters.items():
                if name not in shortcut or name in {"_owns_client", "rpc"}:
                    continue
                # timeout is deliberately the client's, not Client.create's.
                if name == "timeout" and source is client_cls.create:
                    continue
                assert shortcut[name].default == param.default, (
                    f"{sandbox_cls.__name__}.create({name}=) default drifted from "
                    f"{client_cls.__name__}.{source.__name__}"
                )


def test_sandbox_create_shortcut_is_keyword_only():
    for sandbox_cls in (sync.Sandbox, aio.AsyncSandbox):
        kinds = {p.kind for p in inspect.signature(sandbox_cls.create).parameters.values()}
        assert inspect.Parameter.VAR_KEYWORD not in kinds, f"{sandbox_cls.__name__}.create still takes **kwargs"
        assert inspect.Parameter.POSITIONAL_OR_KEYWORD not in kinds
