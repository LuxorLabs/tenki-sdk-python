#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_TAG="$(python3 -c 'import sys; print(f"py{sys.version_info.major}{sys.version_info.minor}")')"
RESOLUTION_REPORT="$(mktemp)"
trap 'rm -f -- "${RESOLUTION_REPORT}"' EXIT

python3 -m pip install --disable-pip-version-check --dry-run --ignore-installed \
  --report "${RESOLUTION_REPORT}" "${ROOT}[dev]" >/dev/null

DEPS_HASH="$(python3 - "${RESOLUTION_REPORT}" <<'PY'
from hashlib import sha256
import json
import sys

report = json.load(open(sys.argv[1]))
packages = {
    item["metadata"]["name"].lower().replace("_", "-"): item["metadata"]["version"]
    for item in report["install"]
}
encoded = json.dumps(packages, sort_keys=True, separators=(",", ":")).encode()
print(sha256(encoded).hexdigest()[:16])
PY
)"
DEPS_DIR="${TENKI_SANDBOX_PYTEST_DEPS:-${XDG_CACHE_HOME:-${HOME}/.cache}/tenki-sandbox-python-test-deps-${PYTHON_TAG}-${DEPS_HASH}}"

if ! PYTHONPATH="${DEPS_DIR}" python3 - "${DEPS_DIR}" "${RESOLUTION_REPORT}" <<'PY' >/dev/null 2>&1
import importlib
from importlib import metadata
import json
from pathlib import Path
import sys

deps_dir = Path(sys.argv[1]).resolve()
report = json.load(open(sys.argv[2]))
for module_name in ("pytest", "pytest_asyncio", "grpc", "websockets", "google.protobuf"):
    module_path = Path(importlib.import_module(module_name).__file__).resolve()
    if deps_dir not in module_path.parents:
        raise RuntimeError(f"{module_name} is not installed in {deps_dir}")

for item in report["install"]:
    package = item["metadata"]
    distribution = metadata.distribution(package["name"])
    distribution_path = Path(distribution.locate_file("")).resolve()
    if distribution_path != deps_dir and deps_dir not in distribution_path.parents:
        raise RuntimeError(f"{package['name']} is not installed in {deps_dir}")
    if distribution.version != package["version"]:
        raise RuntimeError(f"{package['name']} has an unexpected version")
PY
then
  python3 -m pip install --disable-pip-version-check --upgrade --target "${DEPS_DIR}" "${ROOT}[dev]"
fi

if [ "$#" -eq 0 ]; then
  set -- "${ROOT}/tests"
fi

PYTHONPATH="${ROOT}/src:${DEPS_DIR}${PYTHONPATH:+:${PYTHONPATH}}" python3 -m pytest "$@"
