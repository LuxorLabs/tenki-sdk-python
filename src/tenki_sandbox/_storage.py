"""Storage connection and mount conversions shared by the sync and async clients."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from ._pb import pb
from ._time import datetime_from_proto
from .models import (
    StorageConnection,
    StorageConnectionCredentials,
    StorageConnectionList,
    StorageConnectionSecrets,
    StorageConnectionTest,
    StorageDefaultMount,
    StorageDefaults,
    StorageMount,
    StorageMountSpec,
    StorageSecret,
)

_ACCESS = ("READ_ONLY", "READ_WRITE")


def _enum_name(enum, value: int, prefix: str) -> str:
    name = enum.Name(value).removeprefix(prefix)
    return "" if name == "UNSPECIFIED" else name


def access_value(access: str) -> int:
    if not access:
        return pb.STORAGE_ACCESS_UNSPECIFIED
    if access not in _ACCESS:
        raise ValueError(f"access must be one of {_ACCESS}, got {access!r}")
    return pb.StorageAccess.Value(f"STORAGE_ACCESS_{access}")


def provider_value(provider: str) -> int:
    return pb.StorageProvider.Value(f"STORAGE_PROVIDER_{provider}")


def auth_method_value(auth_method: str) -> int:
    return pb.StorageAuthMethod.Value(f"STORAGE_AUTH_METHOD_{auth_method}")


def _fill_secret(target, secret: StorageSecret | None) -> None:
    if secret is None:
        return
    name = (secret.secret_name or "").strip()
    if bool(name) == bool(secret.value):
        raise ValueError("a storage secret needs exactly one of secret_name or value")
    if name:
        target.secret_name = name
    else:
        target.value = secret.value


def fill_credentials(target, credentials: StorageConnectionCredentials | None) -> None:
    if credentials is None:
        return
    for field in ("access_key_id", "secret_access_key", "session_token", "r2_parent_token"):
        secret = getattr(credentials, field)
        if secret is not None:
            _fill_secret(getattr(target, field), secret)
    target.role_arn = credentials.role_arn.strip()


def mount_spec(mount: StorageMountSpec | Mapping) -> pb.StorageMountSpec:
    if isinstance(mount, Mapping):
        mount = StorageMountSpec(**mount)
    spec = pb.StorageMountSpec(mount_path=mount.mount_path.strip(), default_mount_id=mount.default_mount_id.strip())
    if mount.prefix is not None:
        spec.prefix = mount.prefix.strip()
    if mount.access is not None:
        spec.access = access_value(mount.access)
    if mount.connection_id.strip():
        spec.connection_id = mount.connection_id.strip()
    elif mount.connection_name.strip():
        spec.connection_name = mount.connection_name.strip()
    return spec


def default_mounts(mounts: Iterable[StorageDefaultMount]) -> list[pb.StorageDefaultMount]:
    return [
        pb.StorageDefaultMount(
            id=mount.id,
            connection_id=mount.connection_id.strip(),
            prefix=mount.prefix.strip(),
            mount_path=mount.mount_path.strip(),
            access=access_value(mount.access),
        )
        for mount in mounts
    ]


def connection_from_proto(proto) -> StorageConnection:
    return StorageConnection(
        id=proto.id,
        workspace_id=proto.workspace_id,
        name=proto.name,
        provider=_enum_name(pb.StorageProvider, proto.provider, "STORAGE_PROVIDER_"),
        endpoint=proto.endpoint,
        bucket=proto.bucket,
        region=proto.region,
        auth_method=_enum_name(pb.StorageAuthMethod, proto.auth_method, "STORAGE_AUTH_METHOD_"),
        role_arn=proto.role_arn,
        secrets=StorageConnectionSecrets(
            access_key_id=proto.secrets.access_key_id,
            secret_access_key=proto.secrets.secret_access_key,
            session_token=proto.secrets.session_token,
            r2_parent_token=proto.secrets.r2_parent_token,
        ),
        default_access=_enum_name(pb.StorageAccess, proto.default_access, "STORAGE_ACCESS_"),
        revision=int(proto.revision),
        oidc_subject_pattern=proto.oidc_subject_pattern,
        active_mount_count=proto.active_mount_count,
        default_mount_count=proto.default_mount_count,
        last_tested_at=datetime_from_proto(proto.last_tested_at) if proto.HasField("last_tested_at") else None,
        last_test_reason=_enum_name(pb.StorageTestReason, proto.last_test_reason, "STORAGE_TEST_REASON_"),
        disabled=proto.disabled,
        disabled_at=datetime_from_proto(proto.disabled_at) if proto.HasField("disabled_at") else None,
    )


def connection_list_from_proto(proto) -> StorageConnectionList:
    return StorageConnectionList(
        connections=tuple(connection_from_proto(connection) for connection in proto.connections),
        oidc_issuer=proto.oidc_issuer,
        oidc_audience=proto.oidc_audience,
    )


def connection_test_from_proto(proto) -> StorageConnectionTest:
    return StorageConnectionTest(
        reachable=proto.reachable,
        reason=_enum_name(pb.StorageTestReason, proto.reason, "STORAGE_TEST_REASON_"),
        message=proto.error_message,
    )


def defaults_from_proto(proto) -> StorageDefaults:
    return StorageDefaults(
        workspace_id=proto.workspace_id,
        revision=int(proto.revision),
        mounts=tuple(
            StorageDefaultMount(
                id=mount.id,
                connection_id=mount.connection_id,
                prefix=mount.prefix,
                mount_path=mount.mount_path,
                access=_enum_name(pb.StorageAccess, mount.access, "STORAGE_ACCESS_"),
            )
            for mount in proto.mounts
        ),
    )


def mount_from_proto(proto) -> StorageMount:
    spec = proto.spec
    return StorageMount(
        id=proto.id,
        session_id=proto.session_id,
        connection_name=proto.connection_name,
        spec=StorageMountSpec(
            connection_id=spec.connection_id,
            connection_name=spec.connection_name,
            prefix=spec.prefix if spec.HasField("prefix") else None,
            mount_path=spec.mount_path,
            access=_enum_name(pb.StorageAccess, spec.access, "STORAGE_ACCESS_") if spec.HasField("access") else None,
            default_mount_id=spec.default_mount_id,
        ),
        state=proto.state,
        last_error=proto.last_error,
    )


def create_connection_request(
    *,
    workspace_id: str,
    name: str,
    provider: str,
    bucket: str,
    auth_method: str,
    default_access: str,
    credentials: StorageConnectionCredentials,
    endpoint: str = "",
    region: str = "",
) -> pb.CreateStorageConnectionRequest:
    if not default_access:
        raise ValueError("default_access is required: READ_ONLY or READ_WRITE")
    req = pb.CreateStorageConnectionRequest(
        workspace_id=workspace_id.strip(),
        name=name.strip(),
        provider=provider_value(provider),
        endpoint=endpoint.strip(),
        bucket=bucket.strip(),
        region=region.strip(),
        auth_method=auth_method_value(auth_method),
        default_access=access_value(default_access),
    )
    req.credentials.SetInParent()
    fill_credentials(req.credentials, credentials)
    return req


def update_connection_request(
    connection_id: str,
    *,
    expected_revision: int,
    name: str | None = None,
    endpoint: str | None = None,
    region: str | None = None,
    default_access: str | None = None,
    credentials: StorageConnectionCredentials | None = None,
) -> pb.UpdateStorageConnectionRequest:
    req = pb.UpdateStorageConnectionRequest(connection_id=connection_id, expected_revision=expected_revision)
    if name is not None:
        req.name = name.strip()
    if endpoint is not None:
        req.endpoint = endpoint.strip()
    if region is not None:
        req.region = region.strip()
    if default_access is not None:
        req.default_access = access_value(default_access)
    if credentials is not None:
        req.credentials.SetInParent()
        fill_credentials(req.credentials, credentials)
    return req


def save_defaults_request(defaults: StorageDefaults, expected_revision: int) -> pb.SaveStorageDefaultsRequest:
    return pb.SaveStorageDefaultsRequest(
        defaults=pb.StorageDefaults(workspace_id=defaults.workspace_id.strip(), mounts=default_mounts(defaults.mounts)),
        expected_revision=expected_revision,
    )
