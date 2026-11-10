from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import grpc
import pytest

from tenki_sandbox import FileTooLargeError
from tenki_sandbox._pb import pb
from tenki_sandbox.aio.data_plane import AsyncGrpcDataPlaneRPC
from tenki_sandbox.aio.fs import AsyncSandboxFS
from tenki_sandbox.data_plane import DataPlaneCredentialManager, GrpcDataPlaneRPC
from tenki_sandbox.errors import QuotaExceededError, map_rpc_error
from tenki_sandbox.fs import SandboxFS

# Node-agent wire text for a refused unary ReadFile; the Go and TypeScript SDK tests use the same string.
FILE_TOO_LARGE_WIRE_TEXT = (
    "file too large for ReadFile: size_bytes=1100000000 max_bytes=33554432; use ReadFileStream"
)
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


class _RefusingRpc:
    def read_file(self, _req):
        raise _RpcError("RESOURCE_EXHAUSTED", FILE_TOO_LARGE_WIRE_TEXT)


class _AsyncRefusingRpc:
    async def read_file(self, _req):
        raise _RpcError("RESOURCE_EXHAUSTED", FILE_TOO_LARGE_WIRE_TEXT)


def _assert_too_large(err: BaseException) -> None:
    assert isinstance(err, FileTooLargeError)
    assert not isinstance(err, QuotaExceededError)
    assert err.operation == "ReadFile"
    assert err.size_bytes == 1100000000
    assert err.max_bytes == 33554432


def test_map_rpc_error_file_too_large():
    _assert_too_large(map_rpc_error(_RpcError("RESOURCE_EXHAUSTED", FILE_TOO_LARGE_WIRE_TEXT)))


def test_map_rpc_error_file_too_large_for_write():
    mapped = map_rpc_error(
        _RpcError(
            "RESOURCE_EXHAUSTED",
            "file too large for WriteFile: size_bytes=94371840 max_bytes=33554432; use WriteFileStream",
        )
    )
    assert isinstance(mapped, FileTooLargeError)
    assert mapped.operation == "WriteFile"
    assert mapped.size_bytes == 94371840


def test_overlong_size_stays_untyped():
    mapped = map_rpc_error(
        _RpcError(
            "RESOURCE_EXHAUSTED",
            "file too large for ReadFile: size_bytes=12345678901234567890 max_bytes=33554432; use ReadFileStream",
        )
    )
    assert not isinstance(mapped, FileTooLargeError)


def test_other_resource_exhaustion_stays_quota():
    mapped = map_rpc_error(_RpcError("RESOURCE_EXHAUSTED", "workspace quota exceeded"))
    assert isinstance(mapped, QuotaExceededError)
    assert not isinstance(mapped, FileTooLargeError)


def test_read_bytes_raises_file_too_large():
    with pytest.raises(FileTooLargeError) as excinfo:
        SandboxFS(_RefusingRpc(), SESSION_ID).read_bytes("big.bin")
    _assert_too_large(excinfo.value)


async def test_async_read_bytes_raises_file_too_large():
    async def resolve():
        return _AsyncRefusingRpc()

    with pytest.raises(FileTooLargeError) as excinfo:
        await AsyncSandboxFS(resolve, SESSION_ID).read_bytes("big.bin")
    _assert_too_large(excinfo.value)


# The server-side unary cap (backend sandbox.MaxUnaryFileBytes); grpc-core receives only 4 MiB by default.
UNARY_CAP_BYTES = 32 << 20


@contextmanager
def _data_plane(content: bytes | None):
    def read_file(_request, context):
        if content is None:
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, FILE_TOO_LARGE_WIRE_TEXT)
        return pb.SandboxSessionDataPlaneServiceReadFileResponse(response=pb.ReadFileResponse(content=content))

    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "tenki.sandbox.v1.SandboxSessionDataPlaneService",
                    {
                        "ReadFile": grpc.unary_unary_rpc_method_handler(
                            read_file,
                            request_deserializer=pb.SandboxSessionDataPlaneServiceReadFileRequest.FromString,
                            response_serializer=pb.SandboxSessionDataPlaneServiceReadFileResponse.SerializeToString,
                        )
                    },
                ),
            )
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        endpoint = f"http://127.0.0.1:{port}"
        credentials = DataPlaneCredentialManager(
            SimpleNamespace(id=SESSION_ID), pb.SessionCredential(credential="read-credential"), endpoint
        )
        try:
            yield endpoint, credentials
        finally:
            credentials.stop()
            server.stop(0).wait()


def _sync_read(content: bytes | None) -> bytes:
    with _data_plane(content) as (endpoint, credentials):
        rpc = GrpcDataPlaneRPC(endpoint, credentials, SESSION_ID)
        try:
            return SandboxFS(rpc, SESSION_ID).read_bytes("file.bin")
        finally:
            rpc.close()


async def _async_read(content: bytes | None) -> bytes:
    with _data_plane(content) as (endpoint, credentials):
        rpc = AsyncGrpcDataPlaneRPC(endpoint, credentials, SESSION_ID)

        async def resolve():
            return rpc

        try:
            return await AsyncSandboxFS(resolve, SESSION_ID).read_bytes("file.bin")
        finally:
            await rpc.close()


def test_read_bytes_up_to_the_cap_over_grpc():
    content = b"x" * UNARY_CAP_BYTES
    assert _sync_read(content) == content


async def test_async_read_bytes_up_to_the_cap_over_grpc():
    content = b"x" * UNARY_CAP_BYTES
    assert await _async_read(content) == content


def test_read_bytes_above_the_cap_over_grpc_raises_file_too_large():
    with pytest.raises(FileTooLargeError) as excinfo:
        _sync_read(None)
    _assert_too_large(excinfo.value)


async def test_async_read_bytes_above_the_cap_over_grpc_raises_file_too_large():
    with pytest.raises(FileTooLargeError) as excinfo:
        await _async_read(None)
    _assert_too_large(excinfo.value)


@contextmanager
def _write_data_plane(received: list):
    def write_file(request, _context):
        received.append(len(request.request.content))
        return pb.SandboxSessionDataPlaneServiceWriteFileResponse(response=pb.WriteFileResponse())

    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor, options=[("grpc.max_receive_message_length", 64 << 20)])
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "tenki.sandbox.v1.SandboxSessionDataPlaneService",
                    {
                        "WriteFile": grpc.unary_unary_rpc_method_handler(
                            write_file,
                            request_deserializer=pb.SandboxSessionDataPlaneServiceWriteFileRequest.FromString,
                            response_serializer=pb.SandboxSessionDataPlaneServiceWriteFileResponse.SerializeToString,
                        )
                    },
                ),
            )
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        endpoint = f"http://127.0.0.1:{port}"
        credentials = DataPlaneCredentialManager(
            SimpleNamespace(id=SESSION_ID), pb.SessionCredential(credential="write-credential"), endpoint
        )
        try:
            yield endpoint, credentials
        finally:
            credentials.stop()
            server.stop(0).wait()


def test_write_bytes_up_to_the_cap_over_grpc():
    received = []
    with _write_data_plane(received) as (endpoint, credentials):
        rpc = GrpcDataPlaneRPC(endpoint, credentials, SESSION_ID)
        try:
            SandboxFS(rpc, SESSION_ID).write_bytes("cap.bin", b"x" * UNARY_CAP_BYTES)
        finally:
            rpc.close()
    assert received == [UNARY_CAP_BYTES]


async def test_async_write_bytes_up_to_the_cap_over_grpc():
    received = []
    with _write_data_plane(received) as (endpoint, credentials):
        rpc = AsyncGrpcDataPlaneRPC(endpoint, credentials, SESSION_ID)

        async def resolve():
            return rpc

        try:
            await AsyncSandboxFS(resolve, SESSION_ID).write_bytes("cap.bin", b"x" * UNARY_CAP_BYTES)
        finally:
            await rpc.close()
    assert received == [UNARY_CAP_BYTES]
