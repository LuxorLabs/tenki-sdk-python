from __future__ import annotations

import asyncio

import pytest

from tenki_sandbox import FileNotFoundError, IncompleteWriteError, SandboxError
from tenki_sandbox._pb import pb
from tenki_sandbox.aio.fs import AsyncSandboxFS
from tenki_sandbox.errors import map_rpc_error, map_write_error
from tenki_sandbox.fs import SandboxFS

from _async_fakes import FakeRPCError

PATH = "/home/tenki/out.bin"
CHUNKS = [b"hello ", b"world"]


class SkewedRPC:
    """Acknowledges writes with ``delta`` bytes more (or fewer) than it received."""

    def __init__(self, delta: int = 0) -> None:
        self.delta = delta

    def write_file(self, req):
        return pb.WriteFileResponse(path=req.path, size_bytes=len(req.content) + self.delta)

    def write_file_stream(self, requests):
        try:
            received = sum(len(frame.data) for frame in requests if frame.WhichOneof("payload") == "data")
        except Exception as err:  # grpc swallows the source error into a generic one
            raise FakeRPCError("UNKNOWN", "Exception iterating requests!") from err
        return pb.WriteFileStreamResponse(path=PATH, bytes_written=received + self.delta)


class AsyncSkewedRPC(SkewedRPC):
    async def write_file(self, req):
        return super().write_file(req)

    async def write_file_stream(self, requests):
        received = 0
        try:
            async for frame in requests:
                if frame.WhichOneof("payload") == "data":
                    received += len(frame.data)
        except Exception:  # grpc.aio cancels the call when the request iterator raises
            raise asyncio.CancelledError()
        return pb.WriteFileStreamResponse(path=PATH, bytes_written=received + self.delta)


def _transport_data_loss() -> SandboxError:
    """A DATA_LOSS status as the transports raise it: pre-mapped, with the gRPC error as __cause__."""
    status = FakeRPCError("DATA_LOSS", "incomplete write: sandbox wrote 2 of 4 bytes")
    try:
        raise map_rpc_error(status) from status
    except SandboxError as err:
        return err


class DataLossRPC:
    def write_file(self, req):
        raise _transport_data_loss()

    def write_file_stream(self, requests):
        for _ in requests:
            pass
        raise _transport_data_loss()


class AsyncDataLossRPC:
    async def write_file(self, req):
        raise _transport_data_loss()

    async def write_file_stream(self, requests):
        async for _ in requests:
            pass
        raise _transport_data_loss()


def _async_fs(rpc) -> AsyncSandboxFS:
    async def resolve():
        return rpc

    return AsyncSandboxFS(resolve, "session-1")


def _failing_source():
    yield b"hello "
    raise RuntimeError("injected source failure")


async def _async_failing_source():
    yield b"hello "
    raise RuntimeError("injected source failure")


@pytest.mark.parametrize("delta", [0, -1, 1, -11])
def test_sync_writes_fail_when_the_sandbox_saves_a_different_byte_count(delta):
    fs = SandboxFS(SkewedRPC(delta), "session-1")
    for write in (lambda: fs.write_bytes(PATH, b"hello world"), lambda: fs.write_stream(PATH, CHUNKS)):
        if delta == 0:
            write()
            continue
        with pytest.raises(IncompleteWriteError, match="sent 11 bytes"):
            write()


@pytest.mark.parametrize("delta", [0, -1, 1, -11])
async def test_async_writes_fail_when_the_sandbox_saves_a_different_byte_count(delta):
    fs = _async_fs(AsyncSkewedRPC(delta))
    for write in (lambda: fs.write_bytes(PATH, b"hello world"), lambda: fs.write_stream(PATH, CHUNKS)):
        if delta == 0:
            await write()
            continue
        with pytest.raises(IncompleteWriteError, match="sent 11 bytes"):
            await write()


def test_sync_write_stream_raises_the_source_error():
    fs = SandboxFS(SkewedRPC(), "session-1")
    with pytest.raises(SandboxError, match="injected source failure") as raised:
        fs.write_stream(PATH, _failing_source())
    assert isinstance(raised.value.__cause__, RuntimeError)


@pytest.mark.parametrize("source", [_failing_source, _async_failing_source])
async def test_async_write_stream_raises_the_source_error_not_cancelled(source):
    fs = _async_fs(AsyncSkewedRPC())
    with pytest.raises(SandboxError, match="injected source failure") as raised:
        await fs.write_stream(PATH, source())
    assert isinstance(raised.value.__cause__, RuntimeError)


def _missing_source():
    yield b"hello "
    raise FileNotFoundError("/home/tenki/src.bin")


async def _async_missing_source():
    for chunk in _missing_source():
        yield chunk


def test_sync_write_stream_keeps_sandbox_source_errors():
    fs = SandboxFS(SkewedRPC(), "session-1")
    with pytest.raises(FileNotFoundError, match="src.bin"):
        fs.write_stream(PATH, _missing_source())


async def test_async_write_stream_keeps_sandbox_source_errors():
    fs = _async_fs(AsyncSkewedRPC())
    with pytest.raises(FileNotFoundError, match="src.bin"):
        await fs.write_stream(PATH, _async_missing_source())


def test_data_loss_maps_to_incomplete_write_only_for_writes():
    data_loss = FakeRPCError("DATA_LOSS", "incomplete write: sandbox wrote 2 of 4 bytes")
    assert isinstance(map_write_error(data_loss), IncompleteWriteError)
    assert not isinstance(map_rpc_error(data_loss), IncompleteWriteError)
    assert isinstance(map_write_error(_transport_data_loss()), IncompleteWriteError)
    try:
        raise SandboxError("wrapped") from _transport_data_loss()
    except SandboxError as wrapped:
        assert isinstance(map_write_error(wrapped), IncompleteWriteError)
    other = map_rpc_error(FakeRPCError("NOT_FOUND", "no such file"))
    assert map_write_error(other) is other


def test_sync_writes_raise_incomplete_write_on_transport_data_loss():
    fs = SandboxFS(DataLossRPC(), "session-1")
    with pytest.raises(IncompleteWriteError, match="2 of 4"):
        fs.write_bytes(PATH, b"data")
    with pytest.raises(IncompleteWriteError, match="2 of 4"):
        fs.write_stream(PATH, iter(CHUNKS))


async def test_async_writes_raise_incomplete_write_on_transport_data_loss():
    fs = _async_fs(AsyncDataLossRPC())
    with pytest.raises(IncompleteWriteError, match="2 of 4"):
        await fs.write_bytes(PATH, b"data")
    with pytest.raises(IncompleteWriteError, match="2 of 4"):
        await fs.write_stream(PATH, CHUNKS)
