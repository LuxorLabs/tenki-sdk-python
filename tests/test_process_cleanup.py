import threading

import pytest

from tenki_sandbox import errors
from tenki_sandbox._pb import pb
from test_async_process_fs import FakeDataPlaneRPC, _sandbox
from test_process_fs_preview import FakeRPC, create_test_sandbox


class RetainedRunRPC(FakeRPC):
    def run(self, requests):
        def frames():
            try:
                yield pb.RunResponse(started=pb.RunStarted(pid=42))
                yield pb.RunResponse(exit=pb.RunExit(reason=self.reason))
            finally:
                self.closed.set()

        self.response = frames()
        return self.response


class AsyncRetainedRunRPC(FakeDataPlaneRPC):
    def run(self, requests):
        async def frames():
            try:
                yield pb.RunResponse(started=pb.RunStarted(pid=42))
                yield pb.RunResponse(exit=pb.RunExit(reason=self.reason))
            finally:
                self.closed.set()

        self.response = frames()
        return self.response


@pytest.mark.parametrize("reason", ["exit", "capability_unavailable"])
def test_run_closes_its_response_before_completion(reason):
    rpc = RetainedRunRPC()
    rpc.reason, rpc.closed = reason, threading.Event()
    sandbox = create_test_sandbox(rpc)
    try:
        if reason == "exit":
            assert sandbox.start("true").wait().ok
        else:
            with pytest.raises(errors.CapabilityUnavailableError):
                sandbox.start("true").wait()
        assert rpc.closed.wait(1), "completed command retained its response"
    finally:
        rpc.response.close()


@pytest.mark.parametrize("reason", ["exit", "capability_unavailable"])
async def test_async_run_closes_its_response_before_completion(reason):
    rpc = AsyncRetainedRunRPC()
    rpc.reason, rpc.closed = reason, threading.Event()
    sandbox = await _sandbox(rpc)
    try:
        if reason == "exit":
            process = await sandbox.start("true")
            assert (await process.wait()).ok
        else:
            with pytest.raises(errors.CapabilityUnavailableError):
                process = await sandbox.start("true")
                await process.wait()
        assert rpc.closed.is_set(), "completed command retained its response"
    finally:
        await rpc.response.aclose()
