from __future__ import annotations

import platform
from importlib.metadata import PackageNotFoundError, version

CLIENT_GENERATION = "1"


def _sdk_version() -> str:
    try:
        return version("tenki")
    except PackageNotFoundError:
        return "unknown"


CLIENT_METADATA: tuple[tuple[str, str], ...] = (
    ("x-tenki-client-family", "python"),
    ("x-tenki-client-version", _sdk_version()),
    ("x-tenki-client-generation", CLIENT_GENERATION),
    (
        "x-tenki-client-runtime",
        f"{platform.python_implementation().lower()}/{platform.python_version()}",
    ),
)
