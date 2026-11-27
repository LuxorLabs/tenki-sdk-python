from __future__ import annotations

import asyncio
from pathlib import Path
from typing import AsyncIterable, AsyncIterator, Iterable

from .._convert import file_info
from .._pb import pb
from ..fs import MAX_UNARY_FILE_BYTES, _overwrite_mode, _write_chunks
from ..errors import FileNotFoundError, FileTooLargeError, SandboxError, StreamClosedError, ensure_full_write, map_rpc_error, map_write_error
from ..models import FileInfo


async def _aiter_chunks(chunks: Iterable[bytes] | AsyncIterable[bytes]) -> AsyncIterator[bytes]:
    if hasattr(chunks, "__aiter__"):
        async for chunk in chunks:  # type: ignore[union-attr]
            yield chunk
    else:
        for chunk in chunks:  # type: ignore[union-attr]
            yield chunk


class AsyncSandboxFS:
    def __init__(self, resolve_rpc, session_id: str) -> None:
        # resolve_rpc is an async callable returning the data-plane rpc; it is
        # resolved per call so fs picks up a data plane bootstrapped after init.
        self._resolve_rpc = resolve_rpc
        self._session_id = session_id

    async def write_bytes(self, path: str, data: bytes) -> None:
        """Write content; above 32 MiB it goes over ``write_stream``."""
        if len(data) > MAX_UNARY_FILE_BYTES:
            try:
                info = await self.stat(path)
            except FileNotFoundError:
                info = None
            await self.write_stream(path, _write_chunks(data), mode=_overwrite_mode(info))
            return
        try:
            rpc = await self._resolve_rpc()
            resp = await rpc.write_file(pb.WriteFileRequest(session_id=self._session_id, path=path, content=data))
        except Exception as err:
            raise map_write_error(err) from err
        ensure_full_write(path, len(data), resp.size_bytes)

    async def write_text(self, path: str, text: str, *, encoding: str = "utf-8") -> None:
        await self.write_bytes(path, text.encode(encoding))

    async def read_bytes(self, path: str) -> bytes:
        """Read a file; above 32 MiB it is read over ``read_stream``."""
        try:
            rpc = await self._resolve_rpc()
            resp = await rpc.read_file(pb.ReadFileRequest(session_id=self._session_id, path=path))
            return bytes(resp.content)
        except Exception as err:
            mapped = map_rpc_error(err)
            if not isinstance(mapped, FileTooLargeError):
                raise mapped from err
        return b"".join([chunk async for chunk in self.read_stream(path)])

    async def read_text(self, path: str, *, encoding: str = "utf-8") -> str:
        return (await self.read_bytes(path)).decode(encoding, errors="replace")

    async def read_stream(
        self, path: str, *, offset: int = 0, length: int = 0, chunk_bytes: int = 0
    ) -> AsyncIterator[bytes]:
        req = pb.ReadFileStreamRequest(
            session_id=self._session_id,
            path=path,
            offset=offset,
            length=length,
            chunk_bytes=chunk_bytes,
        )
        try:
            rpc = await self._resolve_rpc()
            async for frame in rpc.read_file_stream(req):
                if frame.data:
                    yield bytes(frame.data)
                if frame.eof:
                    return
        except Exception as err:
            raise map_rpc_error(err) from err

    async def write_stream(
        self,
        path: str,
        chunks: Iterable[bytes] | AsyncIterable[bytes],
        *,
        mode: int = 0o644,
        truncate: bool = True,
        sync: bool = False,
        privileged: bool = False,
    ) -> None:
        """Streams chunks to path; ``privileged`` writes outside the workdir as root; setuid, setgid and sticky bits are never applied."""
        sent = 0
        sent_all = False
        source_errors: list[Exception] = []

        async def requests():
            nonlocal sent, sent_all
            yield pb.WriteFileStreamRequest(
                start=pb.WriteFileStreamStart(
                    session_id=self._session_id,
                    path=path,
                    mode=mode,
                    truncate=truncate,
                    sync=sync,
                    privileged=privileged,
                )
            )
            try:
                async for chunk in _aiter_chunks(chunks):
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    sent += len(chunk)
                    yield pb.WriteFileStreamRequest(data=chunk)
            except Exception as err:
                source_errors.append(err)  # grpc.aio surfaces it as CancelledError
                raise
            sent_all = True

        try:
            rpc = await self._resolve_rpc()
            resp = await rpc.write_file_stream(requests())
        except (Exception, asyncio.CancelledError) as err:
            if source_errors:
                source = source_errors[0]
                if isinstance(source, SandboxError):
                    raise source
                raise SandboxError(str(source)) from source
            if isinstance(err, asyncio.CancelledError):
                raise
            raise map_write_error(err) from err
        # An early OK would otherwise truncate silently; this only catches frames not yet handed to the transport.
        if not sent_all:
            raise StreamClosedError("server ended the write stream before all data was sent")
        ensure_full_write(path, sent, resp.bytes_written)

    async def upload(self, local_path: str | Path, remote_path: str, *, chunk_bytes: int = 1024 * 1024) -> None:
        path = Path(local_path)

        async def chunks():
            handle = await asyncio.to_thread(path.open, "rb")
            try:
                while True:
                    data = await asyncio.to_thread(handle.read, chunk_bytes)
                    if not data:
                        return
                    yield data
            finally:
                await asyncio.to_thread(handle.close)

        await self.write_stream(remote_path, chunks())

    async def download(self, remote_path: str, local_path: str | Path) -> None:
        path = Path(local_path)
        handle = await asyncio.to_thread(path.open, "wb")
        try:
            async for chunk in self.read_stream(remote_path):
                await asyncio.to_thread(handle.write, chunk)
        finally:
            await asyncio.to_thread(handle.close)

    async def stat(self, path: str) -> FileInfo:
        try:
            rpc = await self._resolve_rpc()
            resp = await rpc.stat(pb.StatRequest(session_id=self._session_id, path=path))
        except Exception as err:
            raise map_rpc_error(err) from err
        if not resp.exists:
            raise FileNotFoundError(f"no such file or directory: {path}")
        return file_info(path, resp)

    async def mkdir(self, path: str, *, recursive: bool = True, mode: int = 0o755) -> None:
        try:
            rpc = await self._resolve_rpc()
            await rpc.mkdir(pb.MkdirRequest(session_id=self._session_id, path=path, recursive=recursive, mode=mode))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def remove(self, path: str, *, recursive: bool = True) -> None:
        try:
            rpc = await self._resolve_rpc()
            await rpc.remove(pb.RemoveRequest(session_id=self._session_id, path=path, recursive=recursive))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(self, path: str, *, include_hidden: bool = False) -> list[FileInfo]:
        try:
            rpc = await self._resolve_rpc()
            resp = await rpc.list(
                pb.ListRequest(session_id=self._session_id, path=path, include_hidden=include_hidden)
            )
            return [
                FileInfo(
                    path=entry.name,
                    size=int(entry.size),
                    mode=entry.mode,
                    is_dir=entry.is_dir,
                    modified_unix_ns=int(entry.modified_unix_ns),
                )
                for entry in resp.entries
            ]
        except Exception as err:
            raise map_rpc_error(err) from err
