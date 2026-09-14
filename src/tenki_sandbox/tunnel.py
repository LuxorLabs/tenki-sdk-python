from __future__ import annotations

import queue
import random
import socket
import threading
import time
from typing import Callable, Iterator
from urllib.parse import urlparse

from ._pb import pb
from .errors import (
    CapabilityUnavailableError,
    PermissionDeniedError,
    PrimitiveTimeoutError,
    SessionNotFoundError,
    SessionTerminatedError,
    UnauthorizedError,
    map_rpc_error,
)
from .models import TunnelTermination


def _map_termination(terminated_msg) -> TunnelTermination:
    reason_map = {
        pb.HostPortTunnelTerminated.REASON_KEEPALIVE_TIMEOUT: "timeout",
        pb.HostPortTunnelTerminated.REASON_SESSION_LOST: "engine_terminated",
        pb.HostPortTunnelTerminated.REASON_ENGINE_DRAINING: "engine_draining",
        pb.HostPortTunnelTerminated.REASON_BIND_FAILED: "host_error",
        pb.HostPortTunnelTerminated.REASON_PLATFORM_ERROR: "host_error",
        pb.HostPortTunnelTerminated.REASON_CAPABILITY_UNAVAILABLE: "host_error",
    }
    reason = reason_map.get(terminated_msg.reason, "engine_terminated")
    return TunnelTermination(reason=reason, message=terminated_msg.detail or "")


def _should_reconnect(reason: str) -> bool:
    return reason in {"transport_error", "engine_terminated", "engine_draining", "timeout"}


def jittered_backoff(initial: float, max_backoff: float, attempt: int) -> float:
    base = initial * (2 ** max(0, attempt - 1))
    base = min(base, max_backoff)
    return base * (0.75 + random.random() * 0.5)


def _resolve_host(host_addr: str) -> tuple[str, int]:
    """Parse a host dial target: bare port -> 127.0.0.1:<port>, else host:port.

    Shared by the sync and async tunnels so both route identically for the same
    input (including IPv6 handling via urlparse).
    """
    if host_addr.isdigit():
        return "127.0.0.1", int(host_addr)
    parsed = urlparse(f"tcp://{host_addr}")
    return parsed.hostname or "127.0.0.1", parsed.port or 0


class _FakeTunnel:
    """Minimal stand-in for a terminated HostPortTunnel used in the reconnect loop."""

    def __init__(self, termination: "TunnelTermination") -> None:
        self.terminated = threading.Event()
        self._termination = termination


class HostPortTunnel:
    """Wraps the HostPortTunnel gRPC bidi stream."""

    def __init__(
        self,
        *,
        sandbox_port: int,
        sandbox_address: str,
        outbound: queue.Queue,
        response_iter,
        host_addr: str,
    ) -> None:
        self.sandbox_port = sandbox_port
        self.sandbox_address = sandbox_address
        self._outbound = outbound
        self._host_addr = host_addr
        self.terminated = threading.Event()
        self._termination: TunnelTermination | None = None
        self._finish_lock = threading.Lock()
        self._cb_fired = False
        self._callbacks: list[Callable] = []
        self._closed = False

        self._forward_thread = threading.Thread(
            target=self._forward, args=(response_iter,), daemon=True
        )
        self._forward_thread.start()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._send(pb.HostPortTunnelRequest(
                close=pb.HostPortTunnelClose(sub_stream_id=0, reason="sdk_closed")
            ))
            self._outbound.put(None)
            self._finish(TunnelTermination(reason="sdk_closed"))

    def on_terminated(self, cb: Callable) -> None:
        with self._finish_lock:
            if self._cb_fired:
                term = self._termination
        # fired outside lock to avoid deadlock
            else:
                self._callbacks.append(cb)
                return
        t = threading.Thread(target=cb, args=(term,), daemon=True)
        t.start()

    def _send(self, frame) -> None:
        self._outbound.put(frame)

    def _finish(self, term: TunnelTermination) -> None:
        callbacks = []
        with self._finish_lock:
            if self._cb_fired:
                return
            self._termination = term
            callbacks = list(self._callbacks)
            self._callbacks.clear()
            self._cb_fired = True
        self.terminated.set()
        for cb in callbacks:
            t = threading.Thread(target=cb, args=(term,), daemon=True)
            t.start()

    def _connect_and_pump(
        self,
        sub_id: int,
        host: str,
        port: int,
        conns: dict,
        pending: dict,
        conns_lock: threading.Lock,
    ) -> None:
        """Connect to the host target and start pumping; runs in a background thread."""
        try:
            conn = socket.create_connection((host, port), timeout=10)
            conn.settimeout(None)
        except Exception as err:
            with conns_lock:
                pending.pop(sub_id, None)
            self._send(pb.HostPortTunnelRequest(
                close=pb.HostPortTunnelClose(sub_stream_id=sub_id, reason=str(err))
            ))
            return
        # Flush data frames that arrived while connecting, then register the
        # socket so subsequent frames go to it directly. Done under the lock so
        # no frame can slip between the flush and the registration.
        with conns_lock:
            entry = pending.pop(sub_id, None)
            if entry is None or entry["closed"]:
                try:
                    conn.close()
                except Exception:
                    pass
                return
            try:
                for payload in entry["buf"]:
                    conn.sendall(payload)
            except Exception:
                pass
            conns[sub_id] = conn
            half_close = entry["half_close"]
        if half_close:
            try:
                conn.shutdown(socket.SHUT_WR)
            except Exception:
                pass
        self._pump_host_to_sandbox(sub_id, conn, conns, conns_lock)

    def _forward(self, response_iter) -> None:
        conns: dict[int, socket.socket] = {}
        pending: dict[int, dict] = {}
        conns_lock = threading.Lock()
        try:
            for frame in response_iter:
                kind = frame.WhichOneof("payload")
                if kind == "accept":
                    sub_id = frame.accept.sub_stream_id
                    host, port = _resolve_host(self._host_addr)
                    with conns_lock:
                        pending[sub_id] = {"buf": [], "half_close": False, "closed": False}
                    t = threading.Thread(
                        target=self._connect_and_pump,
                        args=(sub_id, host, port, conns, pending, conns_lock),
                        daemon=True,
                    )
                    t.start()
                elif kind == "data":
                    sub_id = frame.data.sub_stream_id
                    with conns_lock:
                        conn = conns.get(sub_id)
                        if conn is None:
                            entry = pending.get(sub_id)
                            if entry is not None:
                                entry["buf"].append(bytes(frame.data.payload))
                            continue
                    try:
                        conn.sendall(bytes(frame.data.payload))
                    except Exception:
                        pass
                elif kind == "half_close":
                    sub_id = frame.half_close.sub_stream_id
                    with conns_lock:
                        conn = conns.get(sub_id)
                        if conn is None:
                            entry = pending.get(sub_id)
                            if entry is not None:
                                entry["half_close"] = True
                            continue
                    try:
                        conn.shutdown(socket.SHUT_WR)
                    except Exception:
                        pass
                elif kind == "close":
                    sub_id = frame.close.sub_stream_id
                    with conns_lock:
                        conn = conns.pop(sub_id, None)
                        entry = pending.get(sub_id)
                        if entry is not None:
                            entry["closed"] = True
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
                elif kind == "ping":
                    self._send(pb.HostPortTunnelRequest(
                        pong=pb.HostPortTunnelKeepalivePong(ping_id=frame.ping.ping_id)
                    ))
                elif kind == "terminated":
                    self._finish(_map_termination(frame.terminated))
                    return
        except Exception as err:
            self._finish(TunnelTermination(reason="transport_error", message=str(err)))
        finally:
            with conns_lock:
                remaining = list(conns.values())
                conns.clear()
            for conn in remaining:
                try:
                    conn.close()
                except Exception:
                    pass

    def _pump_host_to_sandbox(
        self,
        sub_id: int,
        conn: socket.socket,
        conns: dict,
        conns_lock: threading.Lock,
    ) -> None:
        try:
            while True:
                data = conn.recv(32 * 1024)
                if not data:
                    self._send(pb.HostPortTunnelRequest(
                        half_close=pb.HostPortTunnelHalfClose(
                            sub_stream_id=sub_id, client_to_server=True
                        )
                    ))
                    return
                self._send(pb.HostPortTunnelRequest(
                    data=pb.HostPortTunnelData(sub_stream_id=sub_id, payload=data)
                ))
        except Exception as err:
            self._send(pb.HostPortTunnelRequest(
                close=pb.HostPortTunnelClose(sub_stream_id=sub_id, reason=str(err))
            ))
        finally:
            with conns_lock:
                conns.pop(sub_id, None)
            try:
                conn.close()
            except Exception:
                pass


def open_host_port_tunnel(
    rpc,
    session_id: str,
    host_addr: str,
    *,
    sandbox_bind_address: str = "",
    sandbox_port: int = 0,
    host_dial_target_label: str = "",
) -> HostPortTunnel:
    outbound: queue.Queue = queue.Queue()
    outbound.put(pb.HostPortTunnelRequest(
        open=pb.HostPortTunnelOpen(
            session_id=session_id,
            sandbox_bind_address=sandbox_bind_address,
            sandbox_port=sandbox_port,
            host_dial_target_label=host_dial_target_label,
        )
    ))

    def _request_iter() -> Iterator:
        while True:
            item = outbound.get()
            if item is None:
                return
            yield item

    try:
        response_iter = rpc.host_port_tunnel(_request_iter())
        first = next(response_iter)
    except StopIteration:
        raise RuntimeError("host_port_tunnel stream closed before open")
    except Exception as err:
        raise map_rpc_error(err) from err

    kind = first.WhichOneof("payload")
    if kind == "terminated":
        term = first.terminated
        detail = term.detail or ""
        if term.reason == pb.HostPortTunnelTerminated.REASON_CAPABILITY_UNAVAILABLE:
            if detail.startswith("first_frame_timeout"):
                raise PrimitiveTimeoutError("host_port_tunnel", detail)
            raise CapabilityUnavailableError("host_port_tunnel", detail)
        raise RuntimeError(f"host_port_tunnel failed: {detail}")
    if kind != "opened":
        raise RuntimeError(f"host_port_tunnel: unexpected first frame: {kind}")

    opened = first.opened
    return HostPortTunnel(
        sandbox_port=opened.sandbox_port,
        sandbox_address=opened.sandbox_address,
        outbound=outbound,
        response_iter=response_iter,
        host_addr=host_addr,
    )


class ResilientHostPortTunnel:
    """Auto-reconnecting wrapper around HostPortTunnel."""

    def __init__(self) -> None:
        self.sandbox_port: int = 0
        self.sandbox_address: str = ""
        self._state: str = "connected"
        self._mu = threading.Lock()
        self.terminated = threading.Event()
        self._termination: TunnelTermination | None = None
        self._finish_lock = threading.Lock()
        self._cb_fired = False
        self._callbacks: list[Callable] = []
        self._state_listeners: list[Callable] = []
        self._state_mu = threading.Lock()
        self._closed = False
        self._current: HostPortTunnel | None = None

    def endpoint(self) -> tuple[str, int]:
        """Return the last established address and port from one connection."""
        with self._mu:
            current = self._current
        if current is None:
            return "", 0
        return current.sandbox_address, current.sandbox_port

    @property
    def state(self) -> str:
        with self._mu:
            return self._state

    def close(self) -> None:
        self._closed = True
        with self._mu:
            current = self._current
        if current is not None:
            current.close()
        self._finish(TunnelTermination(reason="sdk_closed"))

    def on_terminated(self, cb: Callable) -> None:
        with self._finish_lock:
            if self._cb_fired:
                term = self._termination
            else:
                self._callbacks.append(cb)
                return
        t = threading.Thread(target=cb, args=(term,), daemon=True)
        t.start()

    def on_state_change(self, cb: Callable) -> None:
        with self._state_mu:
            self._state_listeners.append(cb)

    def _emit_state(self, new_state: str, term: TunnelTermination | None = None) -> None:
        with self._mu:
            self._state = new_state
        with self._state_mu:
            listeners = list(self._state_listeners)
        for cb in listeners:
            try:
                cb(new_state)
            except Exception:
                pass

    def _finish(self, term: TunnelTermination) -> None:
        callbacks = []
        with self._finish_lock:
            if self._cb_fired:
                return
            self._termination = term
            callbacks = list(self._callbacks)
            self._callbacks.clear()
            self._cb_fired = True
        self.terminated.set()
        self._emit_state("terminated", term)
        for cb in callbacks:
            t = threading.Thread(target=cb, args=(term,), daemon=True)
            t.start()

    def _reconnect_loop(
        self,
        rpc,
        session_id: str,
        host_addr: str,
        initial_backoff: float,
        max_backoff: float,
        tunnel_kwargs: dict,
    ) -> None:
        attempt = 0
        with self._mu:
            current = self._current

        while True:
            if current is None:
                break
            current.terminated.wait()
            if self._closed:
                return
            term = current._termination or TunnelTermination(reason="transport_error")

            if not _should_reconnect(term.reason):
                self._finish(term)
                return

            delay = 0.0 if term.reason == "engine_draining" else jittered_backoff(initial_backoff, max_backoff, attempt)
            self._emit_state("reconnecting")

            # Interruptible sleep
            deadline = delay
            step = 0.05
            elapsed = 0.0
            while elapsed < deadline:
                if self._closed:
                    return
                time.sleep(min(step, deadline - elapsed))
                elapsed += step

            if self._closed:
                return

            try:
                new_tunnel = open_host_port_tunnel(rpc, session_id, host_addr, **tunnel_kwargs)
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
                current = fake  # type: ignore[assignment]
                continue

            with self._mu:
                self._current = new_tunnel
            if self._closed:
                new_tunnel.close()
                return
            self.sandbox_port = new_tunnel.sandbox_port
            self.sandbox_address = new_tunnel.sandbox_address
            tunnel_kwargs["sandbox_port"] = new_tunnel.sandbox_port
            self._emit_state("connected")
            attempt = 0
            current = new_tunnel


def open_resilient_host_port_tunnel(
    rpc,
    session_id: str,
    host_addr: str,
    *,
    initial_backoff: float = 0.25,
    max_backoff: float = 10.0,
    **kwargs,
) -> ResilientHostPortTunnel:
    tunnel = open_host_port_tunnel(rpc, session_id, host_addr, **kwargs)
    kwargs["sandbox_port"] = tunnel.sandbox_port
    resilient = ResilientHostPortTunnel()
    resilient.sandbox_port = tunnel.sandbox_port
    resilient.sandbox_address = tunnel.sandbox_address
    with resilient._mu:
        resilient._current = tunnel

    t = threading.Thread(
        target=resilient._reconnect_loop,
        args=(rpc, session_id, host_addr, initial_backoff, max_backoff, kwargs),
        daemon=True,
    )
    t.start()
    return resilient
