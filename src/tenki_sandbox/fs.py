from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ._convert import file_info
from ._pb import pb
from .errors import FileNotFoundError, FileTooLargeError, SandboxError, StreamClosedError, ensure_full_write, map_rpc_error, map_write_error
from .models import FileInfo

# The data plane refuses unary ReadFile/WriteFile above this, so larger content goes over the stream RPCs.
MAX_UNARY_FILE_BYTES = 32 << 20
_WRITE_STREAM_CHUNK_BYTES = 1 << 20


def _overwrite_mode(info: FileInfo | None) -> int:
    """Keeps an existing file's permission bits, which unary WriteFile preserves but the stream's temp-and-rename replaces."""
    if info is None or info.is_dir:
        return 0o644
    return info.mode & 0o777


def _write_chunks(data: bytes) -> Iterable[bytes]:
    view = memoryview(data)
    for offset in range(0, len(view), _WRITE_STREAM_CHUNK_BYTES):
        yield bytes(view[offset : offset + _WRITE_STREAM_CHUNK_BYTES])


class SandboxFS:
    def __init__(self, rpc, session_id: str) -> None:
        self._rpc = rpc
        self._session_id = session_id

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write content; above 32 MiB it goes over ``write_stream``."""
        if len(data) > MAX_UNARY_FILE_BYTES:
            try:
                info = self.stat(path)
            except FileNotFoundError:
                info = None
            self.write_stream(path, _write_chunks(data), mode=_overwrite_mode(info))
            return
        try:
            resp = self._rpc.write_file(pb.WriteFileRequest(session_id=self._session_id, path=path, content=data))
        except Exception as err:
            raise map_write_error(err) from err
        ensure_full_write(path, len(data), resp.size_bytes)

    def write_text(self, path: str, text: str, *, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_bytes(self, path: str) -> bytes:
        """Read a file; above 32 MiB it is read over ``read_stream``."""
        try:
            resp = self._rpc.read_file(pb.ReadFileRequest(session_id=self._session_id, path=path))
            return bytes(resp.content)
        except Exception as err:
            mapped = map_rpc_error(err)
            if not isinstance(mapped, FileTooLargeError):
                raise mapped from err
        return b"".join(self.read_stream(path))

    def read_text(self, path: str, *, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding, errors="replace")

    def read_stream(self, path: str, *, offset: int = 0, length: int = 0, chunk_bytes: int = 0) -> Iterable[bytes]:
        req = pb.ReadFileStreamRequest(
            session_id=self._session_id,
            path=path,
            offset=offset,
            length=length,
            chunk_bytes=chunk_bytes,
        )
        try:
            for frame in self._rpc.read_file_stream(req):
                if frame.data:
                    yield bytes(frame.data)
                if frame.eof:
                    return
        except Exception as err:
            raise map_rpc_error(err) from err

    def write_stream(
        self,
        path: str,
        chunks: Iterable[bytes],
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

        def requests():
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
                for chunk in chunks:
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    sent += len(chunk)
                    yield pb.WriteFileStreamRequest(data=chunk)
            except Exception as err:
                source_errors.append(err)  # grpc reports only "Exception iterating requests!"
                raise
            sent_all = True

        try:
            resp = self._rpc.write_file_stream(requests())
        except Exception as err:
            if source_errors:
                source = source_errors[0]
                if isinstance(source, SandboxError):
                    raise source
                raise SandboxError(str(source)) from source
            raise map_write_error(err) from err
        # An early OK would otherwise truncate silently; this only catches frames not yet handed to the transport.
        if not sent_all:
            raise StreamClosedError("server ended the write stream before all data was sent")
        ensure_full_write(path, sent, resp.bytes_written)

    def upload(self, local_path: str | Path, remote_path: str, *, chunk_bytes: int = 1024 * 1024) -> None:
        path = Path(local_path)

        def chunks():
            with path.open("rb") as handle:
                while True:
                    data = handle.read(chunk_bytes)
                    if not data:
                        return
                    yield data

        self.write_stream(remote_path, chunks())

    def download(self, remote_path: str, local_path: str | Path) -> None:
        path = Path(local_path)
        with path.open("wb") as handle:
            for chunk in self.read_stream(remote_path):
                handle.write(chunk)

    def stat(self, path: str) -> FileInfo:
        try:
            resp = self._rpc.stat(pb.StatRequest(session_id=self._session_id, path=path))
        except Exception as err:
            raise map_rpc_error(err) from err
        if not resp.exists:
            raise FileNotFoundError(f"no such file or directory: {path}")
        return file_info(path, resp)

    def mkdir(self, path: str, *, recursive: bool = True, mode: int = 0o755) -> None:
        try:
            self._rpc.mkdir(pb.MkdirRequest(session_id=self._session_id, path=path, recursive=recursive, mode=mode))
        except Exception as err:
            raise map_rpc_error(err) from err

    def remove(self, path: str, *, recursive: bool = True) -> None:
        try:
            self._rpc.remove(pb.RemoveRequest(session_id=self._session_id, path=path, recursive=recursive))
        except Exception as err:
            raise map_rpc_error(err) from err

    def list(self, path: str, *, include_hidden: bool = False) -> list[FileInfo]:
        try:
            resp = self._rpc.list(
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
