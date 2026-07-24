"""Tenki Cloud Python SDK.

Canonical import namespace for the Tenki sandbox SDK. Everything is
re-exported from the implementation package :mod:`tenki_sandbox`, so
``from tenki import Sandbox`` and ``from tenki_sandbox import Sandbox`` are
equivalent. New code should import from ``tenki``.
"""

from tenki_sandbox import *  # noqa: F401,F403
from tenki_sandbox import __all__ as __all__
