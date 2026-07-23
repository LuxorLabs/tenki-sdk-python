from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ._convert import file_info
from ._pb import pb
from .errors import FileNotFoundError, map_rpc_error
from .models import FileInfo


class SandboxFS:
    def __init__(self, rpc, session_id: str) -> None:
        self._rpc = rpc
        self._session_id = session_id

    def write_bytes(self, path: str, data: bytes) -> None:
        try:
            self._rpc.write_file(pb.WriteFileRequest(session_id=self._session_id, path=path, content=data))
        except Exception as err:
            raise map_rpc_error(err) from err

    def write_text(self, path: str, text: str, *, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_bytes(self, path: str) -> bytes:
        try:
            resp = self._rpc.read_file(pb.ReadFileRequest(session_id=self._session_id, path=path))
            return bytes(resp.content)
        except Exception as err:
            raise map_rpc_error(err) from err

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
    ) -> None:
        def requests():
            yield pb.WriteFileStreamRequest(
                start=pb.WriteFileStreamStart(
                    session_id=self._session_id,
                    path=path,
                    mode=mode,
                    truncate=truncate,
                    sync=sync,
                )
            )
            for chunk in chunks:
                if isinstance(chunk, str):
                    chunk = chunk.encode()
                yield pb.WriteFileStreamRequest(data=chunk)

        try:
            self._rpc.write_file_stream(requests())
        except Exception as err:
            raise map_rpc_error(err) from err

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
