"""Argv reaching the guest must be the argv the caller meant (CLO-4671).

`Sandbox.exec` / `Sandbox.start` are variadic, so the natural call
`exec(["sh", "-c", cmd])` binds to `argv == (["sh", "-c", cmd],)` and is handed
to `Process` verbatim. `Process.__init__` then does `str(part)` over it, so the
whole list is rendered into `cmd[0]`: the guest execs a binary literally named
`"['sh', '-c', ...]"`, ENOENTs, and the caller sees an empty result.

These tests assert on the start frame's `cmd` rather than on guest behavior, so
they pin the wire contract without needing a sandbox. The argv tuples below are
exactly what the variadic signatures produce, so a fix at either layer -- the
signature or the coercion -- is enough to satisfy them.
"""

from __future__ import annotations

from collections import UserList, UserString, deque
from pathlib import Path

import pytest

from tenki_sandbox._pb import pb
from tenki_sandbox.aio.process import AsyncProcess
from tenki_sandbox.process import Process


class CaptureRPC:
    """Guest that records the start frame it was sent, then exits cleanly."""

    def __init__(self) -> None:
        self.cmd: list[str] | None = None

    def run(self, requests):
        it = iter(requests)
        self.cmd = list(next(it).start.cmd)
        yield pb.RunResponse(started=pb.RunStarted(pid=1))
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, reason="exit"))


def wire_cmd(*argv: object) -> list[str]:
    """The `cmd` a `Sandbox.exec(*argv)` call would put on the wire."""
    rpc = CaptureRPC()
    proc = Process(rpc, "session-1", argv)  # type: ignore[arg-type]
    proc.close_stdin()
    proc.wait(timeout=5)
    assert rpc.cmd is not None
    return rpc.cmd


class AsyncCaptureRPC:
    """Async twin of CaptureRPC."""

    def __init__(self) -> None:
        self.cmd: list[str] | None = None

    async def run(self, requests):
        it = requests.__aiter__()
        self.cmd = list((await it.__anext__()).start.cmd)
        yield pb.RunResponse(started=pb.RunStarted(pid=1))
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, reason="exit"))


async def async_wire_cmd(*argv: object) -> list[str]:
    """The `cmd` an `AsyncSandbox.exec(*argv)` call would put on the wire."""
    rpc = AsyncCaptureRPC()
    proc = await AsyncProcess.start(rpc, "session-1", argv)  # type: ignore[arg-type]
    await proc.close_stdin()
    await proc.wait(timeout=5)
    assert rpc.cmd is not None
    return rpc.cmd


def test_single_list_argv_is_passed_through_as_argv():
    assert wire_cmd(["sh", "-c", "echo hi"]) == ["sh", "-c", "echo hi"]


def test_single_tuple_argv_is_passed_through_as_argv():
    assert wire_cmd(("sh", "-c", "echo hi")) == ["sh", "-c", "echo hi"]


def test_nested_container_inside_varargs_is_rejected():
    # Ambiguous by construction: reject it rather than stringify it.
    with pytest.raises(TypeError):
        wire_cmd("sh", "-c", ["echo", "hi"])


def test_varargs_argv_is_unchanged():
    assert wire_cmd("sh", "-c", "echo hi") == ["sh", "-c", "echo hi"]


# A closed list of container types missed these: they are not list/tuple/set/
# dict, so they used to stringify to a repr and ENOENT exactly like the lone
# list did. Rejection keys on "str() falls back to a repr" instead.
@pytest.mark.parametrize(
    "factory",
    [
        lambda: map(str, ["sh", "-c", "echo hi"]),
        lambda: (part for part in ["sh", "-c", "echo hi"]),
        lambda: iter(["sh", "-c", "echo hi"]),
        lambda: deque(["sh", "-c", "echo hi"]),
        lambda: UserList(["sh", "-c", "echo hi"]),
        lambda: {"sh"},
        lambda: frozenset(["sh"]),
        lambda: {"sh": "-c"},
        lambda: memoryview(b"sh"),
    ],
    ids=[
        "map",
        "generator",
        "iterator",
        "deque",
        "userlist",
        "set",
        "frozenset",
        "dict",
        "memoryview",
    ],
)
def test_other_iterables_are_rejected_not_stringified(factory):
    with pytest.raises(TypeError, match="pass argv as a list of strings"):
        wire_cmd(factory())


def test_rejected_iterable_names_its_type():
    with pytest.raises(TypeError, match=r"argv\[0\] must be a string, got map"):
        wire_cmd(map(str, ["sh"]))


class Flagish:
    """Iterable, but with a __str__ callers can reasonably rely on."""

    def __iter__(self):
        return iter(())

    def __str__(self) -> str:
        return "--flag"


# Every one of these worked before argv normalization existed, so every one has
# to keep its exact result: only the shapes that could never resolve to a binary
# are allowed to change behavior.
@pytest.mark.parametrize(
    ("part", "expected"),
    [
        (5, "5"),
        (0.5, "0.5"),
        (True, "True"),
        (None, "None"),
        (Path("/etc/hostname"), "/etc/hostname"),
        (UserString("-la"), "-la"),
        (Flagish(), "--flag"),
        (b"-la", "b'-la'"),
    ],
    ids=["int", "float", "bool", "none", "path", "userstring", "iterable_str", "bytes"],
)
def test_scalar_argv_parts_keep_their_historical_coercion(part, expected):
    assert wire_cmd("ls", part) == ["ls", expected]


def test_lone_bytes_is_not_split_into_byte_values():
    # Unwrapping any sequence would turn b"ls" into ["108", "115"].
    assert wire_cmd(b"ls") == ["b'ls'"]


def test_empty_argv_still_rejected():
    with pytest.raises(ValueError):
        wire_cmd()


async def test_async_single_list_argv_is_passed_through_as_argv():
    assert await async_wire_cmd(["sh", "-c", "echo hi"]) == ["sh", "-c", "echo hi"]


async def test_async_other_iterables_are_rejected():
    with pytest.raises(TypeError):
        await async_wire_cmd(map(str, ["sh", "-c", "echo hi"]))


async def test_async_nested_container_inside_varargs_is_rejected():
    with pytest.raises(TypeError):
        await async_wire_cmd("sh", "-c", ["echo", "hi"])


async def test_async_varargs_argv_is_unchanged():
    assert await async_wire_cmd("sh", "-c", "echo hi") == ["sh", "-c", "echo hi"]
