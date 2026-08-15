from __future__ import annotations

import pytest

from tenki_sandbox import Client, Identity, IdentityWorkspace
from tenki_sandbox._pb import pb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _proto_workspace(workspace_id="ws-1", name="acme"):
    return pb.WhoAmIWorkspace(workspace_id=workspace_id, name=name)


class FakeRPC:
    def __init__(self, resp: pb.WhoAmIResponse):
        self._resp = resp
        self.called = False

    def who_am_i(self, req):
        self.called = True
        return self._resp


def _simple_resp(owner_type="USER", owner_id="user-1") -> pb.WhoAmIResponse:
    return pb.WhoAmIResponse(owner_type=owner_type, owner_id=owner_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_who_am_i_calls_rpc():
    rpc = FakeRPC(_simple_resp())
    Client(rpc=rpc).who_am_i()
    assert rpc.called


def test_who_am_i_returns_identity():
    rpc = FakeRPC(_simple_resp())
    result = Client(rpc=rpc).who_am_i()
    assert isinstance(result, Identity)


def test_who_am_i_owner_fields():
    rpc = FakeRPC(_simple_resp(owner_type="WORKSPACE", owner_id="ws-42"))
    ident = Client(rpc=rpc).who_am_i()
    assert ident.owner_type == "WORKSPACE"
    assert ident.owner_id == "ws-42"


def test_who_am_i_empty_workspaces():
    rpc = FakeRPC(_simple_resp())
    ident = Client(rpc=rpc).who_am_i()
    assert ident.workspaces == ()


def test_who_am_i_workspaces():
    resp = pb.WhoAmIResponse(owner_type="USER", owner_id="user-1")
    resp.workspaces.append(_proto_workspace(workspace_id="ws-1", name="acme"))
    resp.workspaces.append(_proto_workspace(workspace_id="ws-2", name="beta"))
    ident = Client(rpc=FakeRPC(resp)).who_am_i()
    assert len(ident.workspaces) == 2
    assert isinstance(ident.workspaces[0], IdentityWorkspace)
    assert ident.workspaces[0].id == "ws-1"
    assert ident.workspaces[0].name == "acme"
    assert ident.workspaces[1].id == "ws-2"


def test_identity_is_frozen():
    rpc = FakeRPC(_simple_resp())
    ident = Client(rpc=rpc).who_am_i()
    with pytest.raises(AttributeError):
        ident.owner_type = "WORKSPACE"  # type: ignore[misc]
