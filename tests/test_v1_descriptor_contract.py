from __future__ import annotations

import importlib
import pkgutil

from google.protobuf import descriptor_pb2
from tenki_sandbox._internal.proto.tenki.sandbox import v1

_REMOVED_FIELDS = {
    "tenki/sandbox/v1/sandbox.proto": {
        "Volume": {"project_id": 8},
        "SandboxSession": {"project_id": 19},
        "Snapshot": {"project_id": 12},
        "PreviewUrl": {"project_id": 2},
        "CreateSessionRequest": {"project_id": 20},
        "CreateVolumeRequest": {"project_id": 4},
        "CreatePreviewUrlRequest": {"project_id": 1},
        "ListPreviewUrlsRequest": {"project_id": 1},
        "GetWorkspaceSandboxUsageRequest": {"project_id": 2},
        "WhoAmIWorkspace": {"projects": 3},
    },
    "tenki/sandbox/v1/template.proto": {
        "Template": {"project_id": 14},
        "CreateTemplateRequest": {"project_id": 8},
    },
}


def _generated_v1_descriptors():
    descriptors = {}
    for module_info in pkgutil.iter_modules(v1.__path__):
        if not module_info.name.endswith("_pb2"):
            continue
        module = importlib.import_module(f"{v1.__name__}.{module_info.name}")
        descriptors[module.DESCRIPTOR.name] = (
            descriptor_pb2.FileDescriptorProto.FromString(
                module.DESCRIPTOR.serialized_pb
            )
        )
    return descriptors


def test_removed_field_tags_remain_reserved():
    descriptors = _generated_v1_descriptors()

    for file_name, message_contracts in _REMOVED_FIELDS.items():
        file_descriptor = descriptors[file_name]
        messages = {message.name: message for message in file_descriptor.message_type}

        for message_name, removed_fields in message_contracts.items():
            message = messages[message_name]
            used_tags = {field.number for field in message.field}
            used_names = {field.name for field in message.field}

            for field_name, tag in removed_fields.items():
                assert tag not in used_tags
                assert field_name not in used_names
                assert field_name in message.reserved_name
                assert any(
                    reserved.start <= tag < reserved.end
                    for reserved in message.reserved_range
                )
