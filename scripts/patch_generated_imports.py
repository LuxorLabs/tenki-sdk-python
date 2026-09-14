import re
from pathlib import Path

# Buf's Python generator emits absolute imports from the proto package names.
# The SDK ships generated files under tenki_sandbox._internal.proto so callers
# do not import or collide with top-level tenki/buf packages.
base = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "tenki_sandbox"
    / "_internal"
    / "proto"
)
patterns = (
    (
        re.compile(
            r"from tenki\.cloud\.workspace\.v1beta1 import ([a-z0-9_]+_pb2) as ([A-Za-z0-9_]+)"
        ),
        r"from tenki_sandbox._internal.proto.tenki.cloud.workspace.v1beta1 import \1 as \2",
    ),
    (
        re.compile(r"from buf\.validate import validate_pb2 as ([A-Za-z0-9_]+)"),
        r"from tenki_sandbox._internal.proto.buf.validate import validate_pb2 as \1",
    ),
    (
        re.compile(
            r"from tenki\.sandbox\.v1 import ([a-z0-9_]+_pb2) as ([A-Za-z0-9_]+)"
        ),
        r"from tenki_sandbox._internal.proto.tenki.sandbox.v1 import \1 as \2",
    ),
)

for path in base.rglob("*.py"):
    text = path.read_text()
    patched = text
    for pattern, replacement in patterns:
        patched = pattern.sub(replacement, patched)
    if patched:
        patched = patched.rstrip() + "\n"
    if patched != text:
        path.write_text(patched)
