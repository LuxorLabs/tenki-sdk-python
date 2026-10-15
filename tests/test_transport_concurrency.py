import asyncio
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from tenki_sandbox.data_plane import _acquire_shared_channel
from tenki_sandbox.aio.data_plane import _acquire_shared_channel as acquire_async


@pytest.fixture
def stream_target():
    chunk = bytes([42]) * (16 * 1024)

    def stream(request, context):
        for _ in range(16):
            yield chunk

    with ThreadPoolExecutor(max_workers=100) as workers:
        server = grpc.server(workers)
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            "test.Streams", {"Read": grpc.unary_stream_rpc_method_handler(stream)}
        ),))
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield f"127.0.0.1:{port}"
        finally:
            server.stop(0).wait()


def test_sync_transport_100_streams(stream_target):
    channel, release = _acquire_shared_channel(f"http://{stream_target}")
    try:
        call = channel.unary_stream("/test.Streams/Read")

        def read(_):
            assert b"".join(call(b"", timeout=15)) == bytes([42]) * (256 * 1024)

        with ThreadPoolExecutor(max_workers=100) as workers:
            list(workers.map(read, range(100)))
    finally:
        release()


async def test_async_transport_100_streams(stream_target):
    channel, release = acquire_async(stream_target, False)
    try:
        call = channel.unary_stream("/test.Streams/Read")

        async def read():
            chunks = [chunk async for chunk in call(b"", timeout=15)]
            assert b"".join(chunks) == bytes([42]) * (256 * 1024)

        await asyncio.gather(*(read() for _ in range(100)))
    finally:
        await release()
