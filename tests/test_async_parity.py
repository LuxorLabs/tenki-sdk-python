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
