from __future__ import annotations

import pytest

from tenki_sandbox import AsyncClient, Client
from tenki_sandbox._pb import registry_pb
from tenki_sandbox.errors import PermissionDeniedError, RegistryImageNotFoundError, map_rpc_error
from tenki_sandbox.models import (
    RegistryImage,
    RegistryImageDetail,
    RegistryImageSummary,
    RegistryListResult,
    RegistryShareGrant,
    RegistryShareResult,
    RegistryVersionDeleteResult,
    ResolvedRegistryRef,
)


# ---------------------------------------------------------------------------
# Proto helpers
# ---------------------------------------------------------------------------

_ART_UUID = "11111111-1111-1111-1111-111111111111"
_WS_UUID = "22222222-2222-2222-2222-222222222222"
_GRANT_UUID = "44444444-4444-4444-4444-444444444444"
_SNAP_UUID = "55555555-5555-5555-5555-555555555555"
_TGT_WS_UUID = "66666666-6666-6666-6666-666666666666"


def _proto_art(**kwargs) -> registry_pb.RegistryImage:
    defaults = dict(
        id=_ART_UUID,
        workspace_id=_WS_UUID,
        workspace_slug="acme",
        name="my-art",
        kind=registry_pb.REGISTRY_IMAGE_KIND_SNAPSHOT,
        visibility=registry_pb.REGISTRY_VISIBILITY_PRIVATE,
    )
    defaults.update(kwargs)
    return registry_pb.RegistryImage(**defaults)


def _proto_image(**kwargs) -> registry_pb.RegistryImage:
    defaults = dict(
        id=_ART_UUID,
        workspace_id=_WS_UUID,
        workspace_slug="acme",
        name="my-art",
        kind=registry_pb.REGISTRY_IMAGE_KIND_SNAPSHOT,
        visibility=registry_pb.REGISTRY_VISIBILITY_SHARED,
    )
    defaults.update(kwargs)
    return registry_pb.RegistryImage(**defaults)


def _proto_summary(**kwargs) -> registry_pb.RegistryImageSummary:
    defaults = dict(
        id=_ART_UUID,
        workspace_id=_WS_UUID,
        workspace_slug="acme",
        name="my-art",
        kind=registry_pb.REGISTRY_IMAGE_KIND_SNAPSHOT,
        visibility=registry_pb.REGISTRY_VISIBILITY_PRIVATE,
    )
    defaults.update(kwargs)
    return registry_pb.RegistryImageSummary(**defaults)


def _proto_detail(**kwargs) -> registry_pb.RegistryImageDetail:
    art = _proto_art()
    return registry_pb.RegistryImageDetail(image=art, **kwargs)


def _proto_grant(**kwargs) -> registry_pb.RegistryShareGrant:
    defaults = dict(
        id=_GRANT_UUID,
        image_id=_ART_UUID,
        owner_workspace_id=_WS_UUID,
        target_workspace_id=_TGT_WS_UUID,
        current_snapshot_id=_SNAP_UUID,
    )
    defaults.update(kwargs)
    return registry_pb.RegistryShareGrant(**defaults)


# ---------------------------------------------------------------------------
# Fake RPC
# ---------------------------------------------------------------------------

class FakeRPC:
    def __init__(self):
        self.called = {}
        self.last_req = {}

    def _record(self, name, req):
        self.called[name] = True
        self.last_req[name] = req

    def list_registry_images(self, req):
        self._record("list_registry_images", req)
        return registry_pb.ListRegistryImagesResponse(images=[_proto_summary()])

    def get_registry_image(self, req):
        self._record("get_registry_image", req)
        return registry_pb.GetRegistryImageResponse(detail=_proto_detail())

    def resolve_registry_ref(self, req):
        self._record("resolve_registry_ref", req)
        return registry_pb.ResolveRegistryRefResponse(
            resolved=registry_pb.ResolvedRegistryRef(
                image_id=_ART_UUID,
                owning_workspace_id=_WS_UUID,
                owning_workspace_slug="acme",
                image_name="my-art",
                kind=registry_pb.REGISTRY_IMAGE_KIND_SNAPSHOT,
                visibility=registry_pb.REGISTRY_VISIBILITY_PRIVATE,
            )
        )

    def publish_registry_image(self, req):
        self._record("publish_registry_image", req)
        return registry_pb.PublishRegistryImageResponse(image=_proto_art(), snapshot_id=_SNAP_UUID)

    def set_registry_image_visibility(self, req):
        self._record("set_registry_image_visibility", req)
        return registry_pb.SetRegistryImageVisibilityResponse(image=_proto_art())

    def delete_registry_image(self, req):
        self._record("delete_registry_image", req)
        return registry_pb.DeleteRegistryImageResponse(image=_proto_art())

    def delete_registry_image_version(self, req):
        self._record("delete_registry_image_version", req)
        return registry_pb.DeleteRegistryImageVersionResponse(
            image_id=_ART_UUID,
            snapshot_id=_SNAP_UUID,
        )

    def share_image(self, req):
        self._record("share_image", req)
        return registry_pb.ShareImageResponse(
            image=_proto_image(),
            current_snapshot_id=_SNAP_UUID,
            digest_ref="acme/my-art@" + _SNAP_UUID,
            grant=_proto_grant(),
        )

    def revoke_registry_share_grant(self, req):
        self._record("revoke_registry_share_grant", req)
        return registry_pb.RevokeRegistryShareGrantResponse(grant=_proto_grant())

    def list_registry_share_grants(self, req):
        self._record("list_registry_share_grants", req)
        return registry_pb.ListRegistryShareGrantsResponse(grants=[_proto_grant()])

    def unshare_registry_image(self, req):
        self._record("unshare_registry_image", req)
        return registry_pb.UnshareRegistryImageResponse(image=_proto_art())


class AsyncFakeRPC(FakeRPC):
    async def delete_registry_image_version(self, req):
        self._record("delete_registry_image_version", req)
        return registry_pb.DeleteRegistryImageVersionResponse(
            image_id=_ART_UUID,
            snapshot_id=_SNAP_UUID,
        )


class FakeCode:
    def __init__(self, name):
        self.name = name


class FakeRpcError(Exception):
    def __init__(self, code, details):
        super().__init__(details)
        self._code = FakeCode(code)
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

def test_list_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.list("acme")
    assert rpc.called.get("list_registry_images")
    assert isinstance(result, RegistryListResult)


def test_list_passes_workspace_slug():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.list("my-workspace")
    assert rpc.last_req["list_registry_images"].workspace_slug == "my-workspace"


def test_list_returns_summaries_tuple():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.list("acme")
    assert isinstance(result.images, tuple)
    assert len(result.images) == 1
    assert isinstance(result.images[0], RegistryImageSummary)
    assert result.images[0].id == _ART_UUID


def test_list_kind_filter():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.list("acme", kind="SNAPSHOT")
    req = rpc.last_req["list_registry_images"]
    assert req.kind == registry_pb.REGISTRY_IMAGE_KIND_SNAPSHOT


def test_list_kind_stripped():
    """kind enum prefix is stripped in summary.kind"""
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.list("acme")
    assert result.images[0].kind == "SNAPSHOT"
    assert result.images[0].visibility == "PRIVATE"


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------

def test_get_by_ref():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.get("acme/my-art", workspace_id=_WS_UUID)
    assert rpc.called.get("get_registry_image")
    req = rpc.last_req["get_registry_image"]
    assert req.ref == "acme/my-art"
    assert req.workspace_id == _WS_UUID
    assert isinstance(result, RegistryImageDetail)


def test_get_by_uuid():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.get(_ART_UUID)
    req = rpc.last_req["get_registry_image"]
    assert req.image_id == _ART_UUID
    assert not req.HasField("ref")


def test_get_returns_detail_with_image():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.get("acme/my-art")
    assert isinstance(result.image, RegistryImage)
    assert result.image.name == "my-art"
    assert result.image.kind == "SNAPSHOT"


# ---------------------------------------------------------------------------
# resolve
# ---------------------------------------------------------------------------

def test_resolve_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.resolve("acme/my-art:latest")
    assert rpc.called.get("resolve_registry_ref")
    assert isinstance(result, ResolvedRegistryRef)


def test_resolve_passes_ref():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.resolve("  acme/my-art  ", workspace_id=_WS_UUID)
    req = rpc.last_req["resolve_registry_ref"]
    assert req.ref == "acme/my-art"
    assert req.workspace_id == _WS_UUID


def test_resolve_strips_enums():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.resolve("acme/my-art")
    assert result.kind == "SNAPSHOT"
    assert result.visibility == "PRIVATE"
    assert result.image_id == _ART_UUID
    assert result.owning_workspace_slug == "acme"


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------

def test_publish_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.publish(workspace_id=_WS_UUID, name="my-art", kind="SNAPSHOT")
    assert rpc.called.get("publish_registry_image")
    assert isinstance(result, RegistryImage)


def test_publish_sets_kind_and_name():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.publish(workspace_id=_WS_UUID, name="the-art", kind="template")
    req = rpc.last_req["publish_registry_image"]
    assert req.name == "the-art"
    assert req.kind == registry_pb.REGISTRY_IMAGE_KIND_TEMPLATE
    assert req.workspace_id == _WS_UUID


def test_publish_optional_fields():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.publish(
        workspace_id=_WS_UUID,
        name="art",
        kind="SNAPSHOT",
        tag="v1",
        visibility="PUBLIC",
        labels=["stable"],
        title="My Art",
        description="Desc",
    )
    req = rpc.last_req["publish_registry_image"]
    assert req.tag == "v1"
    assert req.visibility == registry_pb.REGISTRY_VISIBILITY_PUBLIC
    assert list(req.labels) == ["stable"]
    assert req.title == "My Art"
    assert req.description == "Desc"


# ---------------------------------------------------------------------------
# unpublish
# ---------------------------------------------------------------------------

def test_unpublish_sends_private():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.unpublish("acme/my-art")
    req = rpc.last_req["set_registry_image_visibility"]
    assert req.visibility == registry_pb.REGISTRY_VISIBILITY_PRIVATE
    assert isinstance(result, RegistryImage)


def test_unpublish_by_uuid_sets_image_id():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.unpublish(_ART_UUID)
    req = rpc.last_req["set_registry_image_visibility"]
    assert req.image_id == _ART_UUID


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

def test_delete_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.delete("acme/my-art")
    assert rpc.called.get("delete_registry_image")
    assert isinstance(result, RegistryImage)


def test_delete_with_reason():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.delete("acme/my-art", reason="removing old image")
    req = rpc.last_req["delete_registry_image"]
    assert req.reason == "removing old image"
    assert req.ref == "acme/my-art"


# ---------------------------------------------------------------------------
# delete version
# ---------------------------------------------------------------------------

def test_delete_version_trims_ids_and_maps_response():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.delete_version(f"  {_ART_UUID}  ", f"  {_SNAP_UUID}  ")

    req = rpc.last_req["delete_registry_image_version"]
    assert req.image_id == _ART_UUID
    assert req.snapshot_id == _SNAP_UUID
    assert result == RegistryVersionDeleteResult(image_id=_ART_UUID, snapshot_id=_SNAP_UUID)


def test_delete_version_maps_permission_error():
    rpc = FakeRPC()

    def deny(_req):
        raise FakeRpcError("PERMISSION_DENIED", "version is shared")

    rpc.delete_registry_image_version = deny
    with pytest.raises(PermissionDeniedError, match="version is shared"):
        Client(rpc=rpc).registry.delete_version(_ART_UUID, _SNAP_UUID)


async def test_async_delete_version_trims_ids_and_maps_response():
    rpc = AsyncFakeRPC()
    result = await AsyncClient(rpc=rpc).registry.delete_version(f"  {_ART_UUID}  ", f"  {_SNAP_UUID}  ")

    req = rpc.last_req["delete_registry_image_version"]
    assert req.image_id == _ART_UUID
    assert req.snapshot_id == _SNAP_UUID
    assert result == RegistryVersionDeleteResult(image_id=_ART_UUID, snapshot_id=_SNAP_UUID)


async def test_async_delete_version_maps_permission_error():
    rpc = AsyncFakeRPC()

    async def deny(_req):
        raise FakeRpcError("PERMISSION_DENIED", "version is shared")

    rpc.delete_registry_image_version = deny
    with pytest.raises(PermissionDeniedError, match="version is shared"):
        await AsyncClient(rpc=rpc).registry.delete_version(_ART_UUID, _SNAP_UUID)


# ---------------------------------------------------------------------------
# share
# ---------------------------------------------------------------------------

def test_share_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.share("acme/my-art", target_workspace_id=_TGT_WS_UUID)
    assert rpc.called.get("share_image")
    assert isinstance(result, RegistryShareResult)
    assert isinstance(result.grant, RegistryShareGrant)


def test_share_sets_image_ref_and_target():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.share("acme/my-art", target_workspace_id=_TGT_WS_UUID)
    req = rpc.last_req["share_image"]
    assert req.image_ref == "acme/my-art"
    assert req.target_workspace_id == _TGT_WS_UUID


def test_share_sets_image_id():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.share(_ART_UUID, target_workspace_id=_TGT_WS_UUID)
    req = rpc.last_req["share_image"]
    assert req.image_id == _ART_UUID


def test_share_sets_tag():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.share("acme/my-art", target_workspace_id=_TGT_WS_UUID, tag="stable")
    req = rpc.last_req["share_image"]
    assert req.tag == "stable"


def test_share_sets_snapshot_id():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.share("acme/my-art", target_workspace_id=_TGT_WS_UUID, snapshot_id=_SNAP_UUID)
    req = rpc.last_req["share_image"]
    assert req.snapshot_id == _SNAP_UUID


# ---------------------------------------------------------------------------
# revoke_share_grant / list_share_grants
# ---------------------------------------------------------------------------

def test_revoke_share_grant_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.revoke_share_grant(_GRANT_UUID)
    assert rpc.called.get("revoke_registry_share_grant")
    assert isinstance(result, RegistryShareGrant)


def test_revoke_share_grant_with_reason():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.revoke_share_grant(_GRANT_UUID, reason="partner left")
    req = rpc.last_req["revoke_registry_share_grant"]
    assert req.reason == "partner left"
    assert req.grant_id == _GRANT_UUID


def test_list_share_grants_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.list_share_grants("acme/my-art")
    assert rpc.called.get("list_registry_share_grants")
    assert isinstance(result, list)
    assert isinstance(result[0], RegistryShareGrant)
    assert result[0].id == _GRANT_UUID


# unshare

def test_unshare_calls_rpc():
    rpc = FakeRPC()
    result = Client(rpc=rpc).registry.unshare("acme/my-art")
    assert rpc.called.get("unshare_registry_image")
    assert isinstance(result, RegistryImage)


def test_unshare_with_reason():
    rpc = FakeRPC()
    Client(rpc=rpc).registry.unshare("acme/my-art", reason="retiring")
    req = rpc.last_req["unshare_registry_image"]
    assert req.reason == "retiring"


# Error mapping

def test_not_found_registry_maps_to_registry_error():
    err = map_rpc_error(FakeRpcError("NOT_FOUND", "registry image not found"))
    assert isinstance(err, RegistryImageNotFoundError)


def test_not_found_image_maps_to_registry_error():
    err = map_rpc_error(FakeRpcError("NOT_FOUND", "registry image does not exist"))
    assert isinstance(err, RegistryImageNotFoundError)


def test_not_found_volume_still_maps_to_volume_error():
    from tenki_sandbox.errors import VolumeNotFoundError
    err = map_rpc_error(FakeRpcError("NOT_FOUND", "volume not found"))
    assert isinstance(err, VolumeNotFoundError)
