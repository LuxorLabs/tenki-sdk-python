from __future__ import annotations

import importlib.metadata
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

PYTHON_ROOT = Path(__file__).resolve().parents[1]
PROTO_ROOT = PYTHON_ROOT / "src" / "tenki_sandbox" / "_internal" / "proto"
GENCODE_VERSION = "5.29.5"


def test_generated_bindings_keep_compatible_protobuf_guard():
    generated_files = sorted(PROTO_ROOT.rglob("*_pb2.py"))
    assert generated_files

    expected_guard = re.compile(
        r"ValidateProtobufRuntimeVersion\(\s*"
        r"_runtime_version\.Domain\.PUBLIC,\s*"
        r"5,\s*29,\s*5,"
    )
    incompatible = []
    for path in generated_files:
        source = path.read_text()
        if (
            f"# Protobuf Python Version: {GENCODE_VERSION}" not in source
            or not expected_guard.search(source)
        ):
            incompatible.append(path.relative_to(PYTHON_ROOT).as_posix())

    assert incompatible == [], (
        f"bindings must be regenerated with protobuf {GENCODE_VERSION}: {incompatible}"
    )


def test_python_generator_remains_on_compatibility_bridge():
    # The mirrored SDK repository does not contain the parent Buf config, while
    # the tenki.app monorepo does. Protect the pin wherever it is available.
    buf_config = PYTHON_ROOT.parent / "buf.gen.yaml"
    if not buf_config.exists():
        pytest.skip("Buf generator config is owned by the source monorepo")

    assert "buf.build/protocolbuffers/python:v29.5" in buf_config.read_text()


def test_package_metadata_keeps_floor_without_ceiling():
    project = tomllib.loads((PYTHON_ROOT / "pyproject.toml").read_text())["project"]
    protobuf_requirements = [
        dependency
        for dependency in project["dependencies"]
        if dependency.startswith("protobuf")
    ]
    assert protobuf_requirements == ["protobuf>=5.29.5"]


def test_protobuf_631_warning_is_expected_and_scoped():
    if importlib.metadata.version("protobuf") != "6.31.1":
        pytest.skip("regression applies only to protobuf 6.31.1")

    result = subprocess.run(
        [sys.executable, "-W", "default", "-c", "import tenki_sandbox"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (
        "Protobuf gencode version 5.29.5 is exactly one major version older than the runtime version 6.31.1"
        in result.stderr
    )
