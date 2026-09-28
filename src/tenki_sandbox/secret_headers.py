"""Workspace secret references substituted outside the guest."""

from typing import TypedDict


class SecretHeaderBinding(TypedDict):
    name: str
    secret_name: str
    origin: str
    methods: list[str]
    path_prefix: str
    header: str
