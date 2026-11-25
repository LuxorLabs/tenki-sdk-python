import pytest

from tenki_sandbox._convert import sandbox_info
from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2 as pb
from tenki_sandbox.models import SandboxInfo
from tenki_sandbox.sandbox import Sandbox
from tenki_sandbox.aio.sandbox import AsyncSandbox


@pytest.mark.parametrize("denied", [False, True])
def test_guest_control_capability(denied):
    info = sandbox_info(pb.SandboxSession(id="synthetic-session", guest_control_denied=denied))
    assert info.guest_control_denied is denied
    for cls in (Sandbox, AsyncSandbox):
        sandbox = object.__new__(cls)
        sandbox._info = info
        assert sandbox.guest_control_denied is denied
        sandbox._info = SandboxInfo(id="synthetic-session")
        assert sandbox.guest_control_denied is False
