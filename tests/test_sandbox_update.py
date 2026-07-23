from __future__ import annotations

import pytest

from tenki_sandbox import Client
from tenki_sandbox._pb import pb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(**kwargs) -> pb.SandboxSession:
    defaults = dict(id="sess-1", name="my-box", state=pb.SESSION_STATE_RUNNING)
    defaults.update(kwargs)
    return pb.SandboxSession(**defaults)


class FakeRPC:
    def __init__(self):
        self.update_request = None
        self.ssh_keys_request = None

    def create_session(self, req):
        return pb.CreateSessionResponse(session=_make_session())

    def get_session(self, req):
        return pb.GetSessionResponse(session=_make_session())

    def update_session(self, req):
        self.update_request = req
        return pb.UpdateSessionResponse(session=_make_session(name=req.name or "my-box"))

    def update_ssh_authorized_keys(self, req):
        self.ssh_keys_request = req
        return pb.UpdateSSHAuthorizedKeysResponse()


# ---------------------------------------------------------------------------
# sandbox.update
# ---------------------------------------------------------------------------

def test_update_name():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(name="new-name")
    assert rpc.update_request.session_id == "sess-1"
    assert rpc.update_request.name == "new-name"


def test_update_tags_set():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(tags=["gpu", "python"])
    req = rpc.update_request
    assert list(req.tags) == ["gpu", "python"]
    assert not req.clear_tags


def test_update_tags_empty_clears():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(tags=[])
    req = rpc.update_request
    assert req.clear_tags is True
    assert list(req.tags) == []


def test_update_tags_normalized():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(tags=["  GPU ", "Python"])
    assert list(rpc.update_request.tags) == ["gpu", "python"]


def test_update_tags_none_not_touched():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(name="x")
    req = rpc.update_request
    assert not req.clear_tags
    assert list(req.tags) == []


def test_update_sticky():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(sticky=True)
    assert rpc.update_request.sticky is True


def test_update_mutates_info():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(name="updated")
    assert sandbox.name == "updated"


def test_update_tags_alias():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update_tags("a", "b")
    assert list(rpc.update_request.tags) == ["a", "b"]


# ---------------------------------------------------------------------------
# sandbox.update_ssh_authorized_keys
# ---------------------------------------------------------------------------

def test_update_ssh_authorized_keys():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    keys = ["ssh-ed25519 AAAA...", "ssh-rsa BBBB..."]
    sandbox.update_ssh_authorized_keys(keys)
    req = rpc.ssh_keys_request
    assert req.session_id == "sess-1"
    assert list(req.ssh_authorized_keys) == keys


def test_update_ssh_authorized_keys_empty():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update_ssh_authorized_keys([])
    assert list(rpc.ssh_keys_request.ssh_authorized_keys) == []
