from __future__ import annotations

import threading

import pytest

from tenki_sandbox._pb import pb
from tenki_sandbox.process import Process

SESSION_ID = "11111111-1111-1111-1111-111111111111"

class ExitFrameRPC:
    """Replays optional output and one exit frame, recording the start request."""

    def __init__(
        self,
        *,
        exit_code=0,
        signal="",
        timed_out=False,
        reason="exit",
        stdout=b"",
        stderr=b"",
    ):
        self.exit = dict(
            exit_code=exit_code, signal=signal, timed_out=timed_out, reason=reason
        )
        self.stdout = stdout
        self.stderr = stderr
        self.run_starts = []

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)
        yield pb.RunResponse(started=pb.RunStarted(pid=42))
        if self.stdout:
            yield pb.RunResponse(stdout=self.stdout)
        if self.stderr:
            yield pb.RunResponse(stderr=self.stderr)
        yield pb.RunResponse(exit=pb.RunExit(duration_ms=10, **self.exit))


def _result(**kwargs):
    proc = Process(ExitFrameRPC(**kwargs), SESSION_ID, ["cmd"])
    return proc.wait(timeout=5)


def test_start_frame_advertises_the_budget_to_the_guest():
    rpc = ExitFrameRPC()
    Process(rpc, SESSION_ID, ["start-server"], timeout=1.5).wait(timeout=5)

    assert rpc.run_starts[0].timeout_ms == 1500


@pytest.mark.parametrize("timeout", [None, 0, 0.0, -1])
def test_absent_or_non_positive_budget_is_unbounded_on_the_wire(timeout):
    rpc = ExitFrameRPC()
    Process(rpc, SESSION_ID, ["cmd"], timeout=timeout).wait(timeout=5)

    assert rpc.run_starts[0].timeout_ms == 0


def test_timed_out_frame_is_not_ok_even_with_exit_code_zero():
    # A timed-out run can arrive as exit 0.
    result = _result(exit_code=0, timed_out=True, reason="timeout")

    assert result.timed_out is True
    assert result.exit_code == 0
    assert result.reason == "timeout"
    assert result.ok is False


def test_grace_timeout_stays_distinguishable():
    result = _result(exit_code=-1, timed_out=True, reason="grace_timeout")

    assert result.timed_out is True
    assert result.reason == "grace_timeout"
    assert result.ok is False


def test_timed_out_result_keeps_the_output_captured_so_far():
    result = _result(
        exit_code=0,
        timed_out=True,
        reason="timeout",
        stdout=b"listening on :3000\n",
        stderr=b"warn: slow start\n",
    )

    assert result.stdout == b"listening on :3000\n"
    assert result.stderr == b"warn: slow start\n"


def test_timed_out_result_fails_check():
    result = _result(exit_code=0, timed_out=True, reason="timeout")

    with pytest.raises(Exception):
        result.check()


def test_clean_exit_is_still_ok():
    result = _result(exit_code=0, reason="exit")

    assert result.timed_out is False
    assert result.ok is True


def test_non_zero_exit_is_not_ok():
    result = _result(exit_code=3, reason="exit")

    assert result.ok is False


class StalledRunRPC:
    """A guest that starts the process and then goes quiet.

    An older guest-agent that ignores timeout_ms behaves this way when a
    backgrounded grandchild holds the run's stdout/stderr open. The SDK does not
    compensate, so exec() blocks - `Process.wait(timeout=...)` is the caller's own
    escape hatch, not the command's budget.
    """

    def __init__(self):
        self.signals = []
        self.release = threading.Event()

    def run(self, requests):
        next(requests)
        yield pb.RunResponse(started=pb.RunStarted(pid=42))
        for req in requests:
            if req.HasField("signal"):
                self.signals.append(req.signal.signal)
        self.release.wait(timeout=5)


def test_wait_timeout_is_a_local_bound_not_the_command_budget():
    rpc = StalledRunRPC()
    proc = Process(rpc, SESSION_ID, ["start-server"], timeout=60)
    try:
        with pytest.raises(TimeoutError):
            proc.wait(timeout=0.2)
    finally:
        rpc.release.set()
