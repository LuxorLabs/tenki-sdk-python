"""Raw credentials and format-independent secret source files."""
from typing import TypedDict


class _SecretName(TypedDict):
    name: str


class RuntimeSecretReference(_SecretName, total=False):
    placeholder: bool


class _SecretPath(TypedDict):
    path: str


class RuntimeSecretFile(_SecretPath, total=False):
    source: str
    content: str
    raw: RuntimeSecretReference


def valid_secret_files(files, count: int) -> bool:
    import re

    if len(files) > 32:
        return False
    for index, file in enumerate(files):
        target = file.path
        if len(target.encode()) > 4096 or re.search(r"[\x00-\x1f]", target) or not re.match(r"^/(home/tenki|workspace|app)/.+", target):
            return False
        if any(part in ("", ".", "..") for part in target.split("/")[1:]):
            return False
        if any(index != j and (target == other.path or target.startswith(other.path + "/")) for j, other in enumerate(files)):
            return False
        refs = []
        kind = file.WhichOneof("format")
        if kind == "source":
            from ._internal.proto.tenki.sandbox.v1 import secret_file_pb2 as pb
            if not valid_secret_files([pb.RuntimeSecretFile(path=file.source, raw=pb.RuntimeSecretReference(name="SOURCE"))], 0):
                return False
        elif kind == "content":
            names = secret_file_names(file.content)
            if names is None:
                return False
            count += len(names)
        elif kind == "raw":
            refs.append(file.raw)
        else:
            return False
        count += len(refs)
        if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,127}", ref.name) for ref in refs):
            return False
    return count <= 64


def secret_file_names(content: str) -> list[str] | None:
    import re
    if len(content.encode()) > 262144 or re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", content):
        return None
    names = []
    offset = 0
    while True:
        index = content.find("secrets://", offset)
        if index < 0:
            break
        offset = index + len("secrets://")
        if index > 0 and content[index-1] == "\\":
            continue
        name = re.match(r"[A-Za-z0-9_-]*", content[offset:]).group()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,127}", name):
            return None
        if name not in names:
            names.append(name)
        if len(names) > 64:
            return None
        offset += len(name)
    return names
