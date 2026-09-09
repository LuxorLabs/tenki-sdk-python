"""Transport-level envelope coverage.

The fs/process tests drive a fake RPC that returns inner responses directly, so
they never exercise the envelope handling in `data_plane`. A unary RPC missing
from either map leaves callers holding the envelope, and attribute access on the
inner message then fails at runtime.
"""

import pytest

from tenki_sandbox._pb import pb
from tenki_sandbox.data_plane import (
    _UNARY_REQUEST_TYPES,
    GrpcDataPlaneRPC,
    _unwrap_response,
)

# Streamed in, streamed out, or both: these do not use the unary envelope maps.
_STREAM_IN_OR_OUT = {"run", "dial", "host_port_tunnel", "read_file_stream", "write_file_stream"}

_UNARY_RPCS = sorted(set(GrpcDataPlaneRPC._methods) - _STREAM_IN_OR_OUT)


class _FakeEnvelope:
    def __init__(self, response):
        self.response = response


@pytest.mark.parametrize("name", _UNARY_RPCS)
def test_unary_rpc_wraps_its_request(name):
    assert name in _UNARY_REQUEST_TYPES, f"{name} has no request envelope"


@pytest.mark.parametrize("name", _UNARY_RPCS)
def test_unary_rpc_unwraps_its_response(name):
    inner = pb.StatResponse(exists=True, size=7)
    assert _unwrap_response(name, _FakeEnvelope(inner)) is inner, (
        f"{name} response was not unwrapped from its envelope"
    )
