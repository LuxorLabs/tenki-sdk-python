"""Workspace secret references substituted outside the guest."""

from typing import TypedDict


class _OptionalSecretRequestBinding(TypedDict, total=False):
    header: str
    query_parameter: str
    json_pointer: str


class SecretRequestBinding(_OptionalSecretRequestBinding):
    name: str
    secret_name: str
    origin: str
    methods: list[str]
    path_prefix: str
