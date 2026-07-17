from __future__ import annotations

import asyncio
import inspect
from typing import Callable

from .._pb import pb
from ..errors import (
    CapabilityUnavailableError,
    PermissionDeniedError,
    PrimitiveTimeoutError,
    SessionNotFoundError,
    SessionTerminatedError,
    UnauthorizedError,
    map_rpc_error,
)
from ..models import TunnelTermination
from ..tunnel import _map_termination, _resolve_host, _should_reconnect, jittered_backoff


# Strong references to in-flight async callback tasks. asyncio only keeps a weak
# reference to a bare create_task result, so without this a not-yet-run callback
# could be garbage-collected and silently never fire (missed on_terminated /
# on_state_change cleanup). Discard each task once it completes.
_cb_tasks: set[asyncio.Task] = set()


def _discard_cb_task(task: asyncio.Task) -> None:
    _cb_tasks.discard(task)
    # Retrieve and swallow any exception from an async callback so it doesn't
    # leak a "Task exception was never retrieved" traceback — matching the
    # try/except swallow applied to sync callbacks below.
    if not task.cancelled():
        task.exception()


def _invoke_cb(cb: Callable, *args) -> None:
    try:
        result = cb(*args)
        if inspect.isawaitable(result):
            task = asyncio.create_task(result)
            _cb_tasks.add(task)
            task.add_done_callback(_discard_cb_task)
    except Exception:
        pass


class _FakeTunnel:
    """Minimal terminated stand-in for the reconnect loop, so a failed attempt
    doesn't allocate a full AsyncHostPortTunnel + live Queue just to be discarded.
    """

    def __init__(self, termination: "TunnelTermination") -> None:
        self.terminated = asyncio.Event()
        self._termination = termination


class AsyncHostPortTunnel:
    """Wraps the HostPortTunnel gRPC bidi stream over asyncio sockets."""

    def __init__(self, *, sandbox_port: int, sandbox_address: str, outbound: asyncio.Queue, host_addr: str) -> None:
        self.sandbox_port = sandbox_port
        self.sandbox_address = sandbox_address
        self._outbound = outbound
        self._host_addr = host_addr
        self.terminated = asyncio.Event()
        self._termination: TunnelTermination | None = None
        self._cb_fired = False
        self._callbacks: list[Callable] = []
        self._closed = False
        self._forward_task: asyncio.Task | None = None
        # Strong refs to per-substream pump tasks. Held on the instance (not a
        # local list) so they can't be GC'd mid-copy when _forward returns, and
        # so teardown can cancel them deterministically.
        self._substream_tasks: set[asyncio.Task] = set()

    def _start(self, response_iter) -> None:
        self._forward_task = asyncio.create_task(self._forward(response_iter))

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._send(pb.HostPortTunnelRequest(close=pb.HostPortTunnelClose(sub_stream_id=0, reason="sdk_closed")))
            self._outbound.put_nowait(None)
            self._finish(TunnelTermination(reason="sdk_closed"))
            if self._forward_task is not None:
                self._forward_task.cancel()

    def on_terminated(self, cb: Callable) -> None:
        if self._cb_fired:
            _invoke_cb(cb, self._termination)
        else:
            self._callbacks.append(cb)

    def _send(self, frame) -> None:
        self._outbound.put_nowait(frame)

    def _finish(self, term: TunnelTermination) -> None:
        if self._cb_fired:
            return
        self._termination = term
        callbacks = list(self._callbacks)
        self._callbacks.clear()
        self._cb_fired = True
        self.terminated.set()
        for cb in callbacks:
            _invoke_cb(cb, term)

    async def _connect_and_pump(self, sub_id, host, port, conns, pending, lock) -> None:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=10)
        except Exception as err:
            async with lock:
                pending.pop(sub_id, None)
            self._send(pb.HostPortTunnelRequest(close=pb.HostPortTunnelClose(sub_stream_id=sub_id, reason=str(err))))
            return
        async with lock:
            entry = pending.pop(sub_id, None)
            if entry is None or entry["closed"]:
                writer.close()
                return
            for payload in entry["buf"]:
                writer.write(payload)
            try:
                await writer.drain()
            except Exception:
                pass
            conns[sub_id] = writer
            half_close = entry["half_close"]
        if half_close:
            try:
                writer.write_eof()
            except Exception:
                pass
        await self._pump_host_to_sandbox(sub_id, reader, writer, conns, lock)

    def _discard_substream_task(self, task: asyncio.Task) -> None:
        self._substream_tasks.discard(task)
        # Retrieve any real (non-cancelled) failure so it doesn't linger as an
        # unretrieved task exception — matches _discard_cb_task.
        if not task.cancelled():
            task.exception()

    async def _forward(self, response_iter) -> None:
        conns: dict[int, asyncio.StreamWriter] = {}
        pending: dict[int, dict] = {}
        lock = asyncio.Lock()
        try:
            async for frame in response_iter:
                kind = frame.WhichOneof("payload")
                if kind == "accept":
                    sub_id = frame.accept.sub_stream_id
                    host, port = _resolve_host(self._host_addr)
                    async with lock:
                        pending[sub_id] = {"buf": [], "half_close": False, "closed": False}
                    task = asyncio.create_task(self._connect_and_pump(sub_id, host, port, conns, pending, lock))
                    self._substream_tasks.add(task)
                    task.add_done_callback(self._discard_substream_task)
                elif kind == "data":
                    sub_id = frame.data.sub_stream_id
                    async with lock:
                        writer = conns.get(sub_id)
                        if writer is None:
                            entry = pending.get(sub_id)
                            if entry is not None:
                                entry["buf"].append(bytes(frame.data.payload))
                    if writer is not None:
                        writer.write(bytes(frame.data.payload))
                        try:
                            await writer.drain()
                        except Exception:
                            pass
                elif kind == "half_close":
                    sub_id = frame.half_close.sub_stream_id
                    async with lock:
                        writer = conns.get(sub_id)
                        if writer is None:
                            entry = pending.get(sub_id)
                            if entry is not None:
                                entry["half_close"] = True
                    if writer is not None:
                        try:
                            writer.write_eof()
                        except Exception:
                            pass
                elif kind == "close":
                    sub_id = frame.close.sub_stream_id
                    async with lock:
                        writer = conns.pop(sub_id, None)
                        entry = pending.get(sub_id)
                        if entry is not None:
                            entry["closed"] = True
                    if writer is not None:
                        writer.close()
                elif kind == "ping":
                    self._send(pb.HostPortTunnelRequest(pong=pb.HostPortTunnelKeepalivePong(ping_id=frame.ping.ping_id)))
                elif kind == "terminated":
                    self._finish(_map_termination(frame.terminated))
                    return
        except Exception as err:
            self._finish(TunnelTermination(reason="transport_error", message=str(err)))
        finally:
            # The tunnel is ending (terminate frame, transport error, or cancel):
            # the sandbox side is gone, so stop the substream pumps and close the
            # host sockets. Tasks stay strongly referenced until cancellation
            # completes via the done-callback.
            for task in list(self._substream_tasks):
                task.cancel()
            async with lock:
                remaining = list(conns.values())
                conns.clear()
            for writer in remaining:
                try:
                    writer.close()
                except Exception:
                    pass

    async def _pump_host_to_sandbox(self, sub_id, reader, writer, conns, lock) -> None:
        try:
            while True:
                data = await reader.read(32 * 1024)
                if not data:
                    self._send(
                        pb.HostPortTunnelRequest(
                            half_close=pb.HostPortTunnelHalfClose(sub_stream_id=sub_id, client_to_server=True)
                        )
                    )
                    return
                self._send(pb.HostPortTunnelRequest(data=pb.HostPortTunnelData(sub_stream_id=sub_id, payload=data)))
        except Exception as err:
            self._send(pb.HostPortTunnelRequest(close=pb.HostPortTunnelClose(sub_stream_id=sub_id, reason=str(err))))
        finally:
            async with lock:
                conns.pop(sub_id, None)
            try:
                writer.close()
            except Exception:
                pass


async def open_host_port_tunnel(
    rpc,
    session_id: str,
    host_addr: str,
    *,
    sandbox_bind_address: str = "",
    sandbox_port: int = 0,
    host_dial_target_label: str = "",
) -> AsyncHostPortTunnel:
    outbound: asyncio.Queue = asyncio.Queue()
    outbound.put_nowait(
        pb.HostPortTunnelRequest(
            open=pb.HostPortTunnelOpen(
                session_id=session_id,
                sandbox_bind_address=sandbox_bind_address,
                sandbox_port=sandbox_port,
                host_dial_target_label=host_dial_target_label,
            )
        )
    )

    async def request_iter():
        while True:
            item = await outbound.get()
            if item is None:
                return
            yield item

    response_iter = rpc.host_port_tunnel(request_iter()).__aiter__()

    async def _abort() -> None:
        # A failed open never hands the stream to the tunnel's _forward task, so
        # nothing would otherwise drain request_iter or close the call — leaving
        # the gRPC request-iterator task pending. Feed the sentinel and aclose.
        outbound.put_nowait(None)
        aclose = getattr(response_iter, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except BaseException:  # noqa: BLE001 - cleanup must not mask the open error
                pass

    try:
        first = await response_iter.__anext__()
    except StopAsyncIteration:
        await _abort()
        raise RuntimeError("host_port_tunnel stream closed before open")
    except Exception as err:
        await _abort()
        raise map_rpc_error(err) from err

    kind = first.WhichOneof("payload")
    if kind == "opened":
        opened = first.opened
        tunnel = AsyncHostPortTunnel(
            sandbox_port=opened.sandbox_port,
            sandbox_address=opened.sandbox_address,
            outbound=outbound,
            host_addr=host_addr,
        )
        tunnel._start(response_iter)
        return tunnel

    await _abort()
    if kind == "terminated":
        term = first.terminated
        detail = term.detail or ""
        if term.reason == pb.HostPortTunnelTerminated.REASON_CAPABILITY_UNAVAILABLE:
            if detail.startswith("first_frame_timeout"):
                raise PrimitiveTimeoutError("host_port_tunnel", detail)
            raise CapabilityUnavailableError("host_port_tunnel", detail)
        raise RuntimeError(f"host_port_tunnel failed: {detail}")
    raise RuntimeError(f"host_port_tunnel: unexpected first frame: {kind}")


class AsyncResilientHostPortTunnel:
    """Auto-reconnecting wrapper around AsyncHostPortTunnel."""

    def __init__(self) -> None:
        self.sandbox_port: int = 0
        self.sandbox_address: str = ""
        self._state: str = "connected"
        self.terminated = asyncio.Event()
        self._termination: TunnelTermination | None = None
        self._cb_fired = False
        self._callbacks: list[Callable] = []
        self._state_listeners: list[Callable] = []
        self._closed = False
        self._current: AsyncHostPortTunnel | None = None
        self._reconnect_task: asyncio.Task | None = None

    @property
    def state(self) -> str:
        return self._state

    async def close(self) -> None:
        self._closed = True
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
        current = self._current
        if current is not None:
            await current.close()
        self._finish(TunnelTermination(reason="sdk_closed"))

    def on_terminated(self, cb: Callable) -> None:
        if self._cb_fired:
            _invoke_cb(cb, self._termination)
        else:
            self._callbacks.append(cb)

    def on_state_change(self, cb: Callable) -> None:
        self._state_listeners.append(cb)

    def _emit_state(self, new_state: str) -> None:
        self._state = new_state
        for cb in list(self._state_listeners):
            _invoke_cb(cb, new_state)

    def _finish(self, term: TunnelTermination) -> None:
        if self._cb_fired:
            return
        self._termination = term
        callbacks = list(self._callbacks)
        self._callbacks.clear()
        self._cb_fired = True
        self.terminated.set()
        self._emit_state("terminated")
        for cb in callbacks:
            _invoke_cb(cb, term)

    async def _reconnect_loop(self, rpc, session_id, host_addr, initial_backoff, max_backoff, tunnel_kwargs) -> None:
        attempt = 0
        current = self._current
        while True:
            if current is None:
                break
            await current.terminated.wait()
            if self._closed:
                return
            term = current._termination or TunnelTermination(reason="transport_error")

            if not _should_reconnect(term.reason):
                self._finish(term)
                return

            delay = 0.0 if term.reason == "engine_draining" else jittered_backoff(initial_backoff, max_backoff, attempt)
            self._emit_state("reconnecting")
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return
            if self._closed:
                return

            try:
                new_tunnel = await open_host_port_tunnel(rpc, session_id, host_addr, **tunnel_kwargs)
            except (
                SessionNotFoundError,
                SessionTerminatedError,
                UnauthorizedError,
                PermissionDeniedError,
                CapabilityUnavailableError,
                RuntimeError,
            ) as err:
                self._finish(TunnelTermination(reason="host_error", message=str(err)))
                return
            except Exception:
                attempt += 1
                fake = _FakeTunnel(TunnelTermination(reason="transport_error"))
                fake.terminated.set()
                current = fake
                continue

            self._current = new_tunnel
            if self._closed:
                await new_tunnel.close()
                return
            self.sandbox_port = new_tunnel.sandbox_port
            self.sandbox_address = new_tunnel.sandbox_address
            tunnel_kwargs["sandbox_port"] = new_tunnel.sandbox_port
            self._emit_state("connected")
            attempt = 0
            current = new_tunnel


async def open_resilient_host_port_tunnel(
    rpc,
    session_id: str,
    host_addr: str,
    *,
    initial_backoff: float = 0.25,
    max_backoff: float = 10.0,
    **kwargs,
) -> AsyncResilientHostPortTunnel:
    tunnel = await open_host_port_tunnel(rpc, session_id, host_addr, **kwargs)
    kwargs["sandbox_port"] = tunnel.sandbox_port
    resilient = AsyncResilientHostPortTunnel()
    resilient.sandbox_port = tunnel.sandbox_port
    resilient.sandbox_address = tunnel.sandbox_address
    resilient._current = tunnel
    resilient._reconnect_task = asyncio.create_task(
        resilient._reconnect_loop(rpc, session_id, host_addr, initial_backoff, max_backoff, kwargs)
    )
    return resilient
