from tenki_sandbox._pb import pb
from test_async_process_fs import FakeDataPlaneRPC, _sandbox
from test_async_process_fs import TransientRunError as AsyncTransientRunError
from test_process_fs_preview import FakeRPC, TransientRunError, create_test_sandbox

FRAMES = [
    pb.RunResponse(started=pb.RunStarted(pid=42)),
    pb.RunResponse(keepalive=True),
    pb.RunResponse(stdout=b"out\n"),
    pb.RunResponse(keepalive=True),
    pb.RunResponse(exit=pb.RunExit(reason="exit")),
]


class KeepaliveRunRPC(FakeRPC):
    def run(self, requests):
        return iter(FRAMES)


class AsyncKeepaliveRunRPC(FakeDataPlaneRPC):
    def run(self, requests):
        async def frames():
            for frame in FRAMES:
                yield frame

        return frames()


def test_run_ignores_keepalive_frames():
    result = create_test_sandbox(KeepaliveRunRPC()).start("true").wait()

    assert result.ok
    assert result.stdout == b"out\n"


async def test_async_run_ignores_keepalive_frames():
    process = await (await _sandbox(AsyncKeepaliveRunRPC())).start("true")
    result = await process.wait()

    assert result.ok
    assert result.stdout == b"out\n"


class KeepaliveThenResetRPC(FakeRPC):
    """First attempt drops after a lone keepalive, before any started frame."""

    def __init__(self):
        super().__init__()
        self.run_calls = 0

    def run(self, requests):
        next(requests)
        self.run_calls += 1
        if self.run_calls == 1:
            yield pb.RunResponse(keepalive=True)
            raise TransientRunError()
        yield from FRAMES


class AsyncKeepaliveThenResetRPC(FakeDataPlaneRPC):
    def __init__(self):
        super().__init__()
        self.run_calls = 0

    def run(self, requests):
        async def frames():
            await requests.__anext__()
            self.run_calls += 1
            if self.run_calls == 1:
                yield pb.RunResponse(keepalive=True)
                raise AsyncTransientRunError()
            for frame in FRAMES:
                yield frame

        return frames()


def test_run_keepalive_before_started_keeps_establishment_retry():
    rpc = KeepaliveThenResetRPC()
    result = create_test_sandbox(rpc).start("true").wait()

    assert result.ok
    assert result.stdout == b"out\n"
    assert rpc.run_calls == 2


async def test_async_run_keepalive_before_started_keeps_establishment_retry():
    rpc = AsyncKeepaliveThenResetRPC()
    process = await (await _sandbox(rpc)).start("true")
    result = await process.wait()

    assert result.ok
    assert result.stdout == b"out\n"
    assert rpc.run_calls == 2
