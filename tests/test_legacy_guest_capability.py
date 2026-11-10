from __future__ import annotations

import pytest

from tenki_sandbox import CapabilityUnavailableError
from tenki_sandbox.errors import map_rpc_error
from tenki_sandbox.fs import SandboxFS

# Node-agent wire text for Stat on a guest agent that predates the fs primitives; the Go and TypeScript SDK tests use the same string.
LEGACY_GUEST_STAT_WIRE_TEXT = "guest_error: unknown request type: *sandboxv1.GuestRequest_Stat"
SESSION_ID = "00000000-0000-0000-0000-000000000001"


class _Code:
    def __init__(self, name: str) -> None:
        self.name = name


class _RpcError(Exception):
    def __init__(self, code: str, details: str) -> None:
        super().__init__(details)
        self._code = _Code(code)
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


class _LegacyGuestRpc:
    def stat(self, _req):
        raise _RpcError("UNIMPLEMENTED", LEGACY_GUEST_STAT_WIRE_TEXT)


@pytest.mark.parametrize("code", ["UNIMPLEMENTED", "INTERNAL"])
def test_map_rpc_error_legacy_guest_is_capability_unavailable(code):
    mapped = map_rpc_error(_RpcError(code, LEGACY_GUEST_STAT_WIRE_TEXT))
    assert isinstance(mapped, CapabilityUnavailableError)
    assert mapped.primitive == "Stat"


def test_map_rpc_error_guest_io_error_is_not_capability_unavailable():
    mapped = map_rpc_error(_RpcError("INTERNAL", "guest_error: read /home/tenki/f: input/output error"))
    assert not isinstance(mapped, CapabilityUnavailableError)


def test_stat_on_legacy_guest_raises_capability_unavailable():
    with pytest.raises(CapabilityUnavailableError) as excinfo:
        SandboxFS(_LegacyGuestRpc(), SESSION_ID).stat("f.bin")
    assert excinfo.value.primitive == "Stat"
    assert isinstance(excinfo.value.__cause__, _RpcError)
