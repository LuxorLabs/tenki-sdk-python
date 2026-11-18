from __future__ import annotations

import asyncio

from ._pb import pb

_CLEANUP_TIMEOUT_SECONDS = 10.0


def _cleanup_kwargs(rpc):
    return {"timeout": _CLEANUP_TIMEOUT_SECONDS} if getattr(rpc, "_accepts_call_timeout", False) else {}


def cleanup_unacknowledged_session(rpc, session_id: str) -> None:
    rpc.terminate_session(pb.TerminateSessionRequest(session_id=session_id), **_cleanup_kwargs(rpc))


async def cleanup_unacknowledged_session_async(rpc, session_id: str) -> None:
    task = asyncio.create_task(asyncio.wait_for(
        rpc.terminate_session(pb.TerminateSessionRequest(session_id=session_id), **_cleanup_kwargs(rpc)),
        _CLEANUP_TIMEOUT_SECONDS,
    ))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    try:
        task.result()
    except Exception as error:
        if cancelled:
            raise asyncio.CancelledError from error
        raise
    if cancelled:
        raise asyncio.CancelledError
