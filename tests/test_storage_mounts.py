import pytest

from tenki_sandbox import StorageConnectionCredentials, StorageDefaultMount, StorageDefaults, StorageMountSpec, StorageSecret
from tenki_sandbox import _storage
from tenki_sandbox._pb import pb
from tenki_sandbox.client import build_create_session_request


def test_mount_spec_prefers_connection_id_and_leaves_access_to_connection_default():
    spec = _storage.mount_spec(StorageMountSpec(connection_id=" c1 ", connection_name="datasets", mount_path="/mnt/data"))
    assert spec.WhichOneof("connection") == "connection_id"
    assert spec.connection_id == "c1"
    assert not spec.HasField("access")
    assert not spec.HasField("prefix")

    by_name = _storage.mount_spec({"connection_name": "datasets", "mount_path": "/mnt/data", "access": "READ_ONLY"})
    assert by_name.connection_name == "datasets"
    assert by_name.access == pb.STORAGE_ACCESS_READ_ONLY

    with pytest.raises(ValueError):
        _storage.mount_spec(StorageMountSpec(connection_name="datasets", mount_path="/x", access="WRITE"))


def test_override_can_clear_prefix_and_reset_access_to_connection_default():
    spec = _storage.mount_spec(StorageMountSpec(default_mount_id="d1", prefix="", access=""))
    wire = pb.StorageMountSpec.FromString(spec.SerializeToString())
    assert wire.HasField("prefix") and wire.prefix == ""
    assert wire.HasField("access") and wire.access == pb.STORAGE_ACCESS_UNSPECIFIED

    kept = pb.StorageMountSpec.FromString(
        _storage.mount_spec(StorageMountSpec(default_mount_id="d1", mount_path="/x")).SerializeToString()
    )
    assert not kept.HasField("prefix")
    assert not kept.HasField("access")


def test_mount_reports_resolved_prefix_and_access():
    proto = pb.StorageMount(
        id="m1", spec=pb.StorageMountSpec(connection_id="c1", prefix="", mount_path="/mnt/data", access=pb.STORAGE_ACCESS_READ_WRITE)
    )
    mount = _storage.mount_from_proto(proto)
    assert mount.spec.prefix == ""
    assert mount.spec.access == "READ_WRITE"


def test_credentials_send_secret_names_or_values():
    req = _storage.create_connection_request(
        workspace_id="w",
        name="Datasets",
        provider="R2",
        bucket="data",
        auth_method="ACCESS_KEY",
        default_access="READ_WRITE",
        credentials=StorageConnectionCredentials(
            access_key_id=StorageSecret.named("R2_KEY_ID"), secret_access_key=StorageSecret.from_value("secret-value")
        ),
    )
    assert req.credentials.access_key_id.WhichOneof("source") == "secret_name"
    assert req.credentials.secret_access_key.value == "secret-value"
    assert not req.credentials.HasField("session_token")
    for secret in (StorageSecret(), StorageSecret(secret_name="  ")):
        with pytest.raises(ValueError):
            _storage.fill_credentials(pb.StorageConnectionCredentials(), StorageConnectionCredentials(access_key_id=secret))


def test_create_request_carries_defaults_controls():
    req = build_create_session_request(
        storage_mounts=[StorageMountSpec(default_mount_id="d1", mount_path="/workspace/data")],
        disable_storage_defaults=False,
        excluded_default_mount_ids=[" d2 "],
    )
    assert req.storage_mounts[0].default_mount_id == "d1"
    assert list(req.excluded_default_mount_ids) == ["d2"]
    assert not req.disable_storage_defaults


def test_save_defaults_request():
    req = _storage.save_defaults_request(
        StorageDefaults(workspace_id="w", mounts=(StorageDefaultMount(connection_id="c1", mount_path="/mnt/data", access="READ_ONLY"),)),
        expected_revision=3,
    )
    assert req.expected_revision == 3
    assert req.defaults.mounts[0].access == pb.STORAGE_ACCESS_READ_ONLY
