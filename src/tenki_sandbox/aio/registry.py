from __future__ import annotations

from .._convert import (
    registry_image,
    registry_image_detail,
    registry_list_result,
    registry_share_grant,
    registry_share_result,
    registry_version_delete_result,
    resolved_registry_ref,
)
from .._pb import registry_pb
from ..errors import map_rpc_error
from ..models import (
    RegistryImage,
    RegistryImageDetail,
    RegistryListResult,
    RegistryShareGrant,
    RegistryShareResult,
    RegistryVersionDeleteResult,
    ResolvedRegistryRef,
)
from ..registry import _id_or_image_ref, _id_or_ref


class AsyncRegistry:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def list(
        self,
        workspace_slug: str,
        *,
        labels: list[str] | None = None,
        kind: str | None = None,
        name: str | None = None,
        page_size: int = 50,
        cursor: str | None = None,
    ) -> RegistryListResult:
        req = registry_pb.ListRegistryImagesRequest(workspace_slug=workspace_slug, page_size=page_size)
        if labels:
            req.labels.extend(labels)
        if kind:
            req.kind = registry_pb.RegistryImageKind.Value(f"REGISTRY_IMAGE_KIND_{kind.upper()}")
        if name:
            req.name_substring = name
        if cursor:
            req.cursor = cursor
        try:
            return registry_list_result(await self._rpc.list_registry_images(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get(self, image_or_id: str, *, workspace_id: str | None = None) -> RegistryImageDetail:
        try:
            req = registry_pb.GetRegistryImageRequest(**_id_or_ref(image_or_id))
            if workspace_id:
                req.workspace_id = workspace_id.strip()
            resp = await self._rpc.get_registry_image(req)
            return registry_image_detail(resp.detail)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def resolve(self, image: str, *, workspace_id: str | None = None) -> ResolvedRegistryRef:
        try:
            req = registry_pb.ResolveRegistryRefRequest(ref=image.strip())
            if workspace_id:
                req.workspace_id = workspace_id.strip()
            resp = await self._rpc.resolve_registry_ref(req)
            return resolved_registry_ref(resp.resolved)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def publish(
        self,
        *,
        workspace_id: str,
        name: str,
        kind: str,
        source_template_id: str | None = None,
        snapshot_id: str | None = None,
        tag: str | None = None,
        visibility: str | None = None,
        labels: list[str] | None = None,
        title: str | None = None,
        description: str | None = None,
        image_id: str | None = None,
        ref: str | None = None,
    ) -> RegistryImage:
        req = registry_pb.PublishRegistryImageRequest(
            workspace_id=workspace_id,
            name=name,
            kind=registry_pb.RegistryImageKind.Value(f"REGISTRY_IMAGE_KIND_{kind.upper()}"),
        )
        if image_id:
            req.image_id = image_id
        if ref:
            req.ref = ref
        if source_template_id:
            req.source_template_id = source_template_id
        if snapshot_id:
            req.snapshot_id = snapshot_id
        if tag:
            req.tag = tag
        if visibility:
            req.visibility = registry_pb.RegistryVisibility.Value(f"REGISTRY_VISIBILITY_{visibility.upper()}")
        if labels:
            req.labels.extend(labels)
        if title:
            req.title = title
        if description:
            req.description = description
        try:
            return registry_image((await self._rpc.publish_registry_image(req)).image)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def unpublish(self, image_or_id: str) -> RegistryImage:
        try:
            resp = await self._rpc.set_registry_image_visibility(
                registry_pb.SetRegistryImageVisibilityRequest(
                    visibility=registry_pb.RegistryVisibility.Value("REGISTRY_VISIBILITY_PRIVATE"),
                    **_id_or_ref(image_or_id),
                )
            )
            return registry_image(resp.image)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def delete(self, image_or_id: str, *, reason: str | None = None) -> RegistryImage:
        req = registry_pb.DeleteRegistryImageRequest(**_id_or_ref(image_or_id))
        if reason:
            req.reason = reason
        try:
            return registry_image((await self._rpc.delete_registry_image(req)).image)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def delete_version(self, image_id: str, snapshot_id: str) -> RegistryVersionDeleteResult:
        req = registry_pb.DeleteRegistryImageVersionRequest(
            image_id=image_id.strip(),
            snapshot_id=snapshot_id.strip(),
        )
        try:
            return registry_version_delete_result(await self._rpc.delete_registry_image_version(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def share(
        self,
        image_or_id: str,
        *,
        target_workspace_id: str,
        tag: str | None = None,
        snapshot_id: str | None = None,
    ) -> RegistryShareResult:
        req = registry_pb.ShareImageRequest(
            target_workspace_id=target_workspace_id.strip(),
            **_id_or_image_ref(image_or_id),
        )
        if tag:
            req.tag = tag
        if snapshot_id:
            req.snapshot_id = snapshot_id
        try:
            return registry_share_result(await self._rpc.share_image(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def revoke_share_grant(self, grant_id: str, *, reason: str | None = None) -> RegistryShareGrant:
        req = registry_pb.RevokeRegistryShareGrantRequest(grant_id=grant_id.strip())
        if reason:
            req.reason = reason
        try:
            return registry_share_grant((await self._rpc.revoke_registry_share_grant(req)).grant)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list_share_grants(self, image_or_id: str) -> list[RegistryShareGrant]:
        try:
            resp = await self._rpc.list_registry_share_grants(registry_pb.ListRegistryShareGrantsRequest(**_id_or_ref(image_or_id)))
            return [registry_share_grant(g) for g in resp.grants]
        except Exception as err:
            raise map_rpc_error(err) from err

    async def unshare(self, image_or_id: str, *, reason: str | None = None) -> RegistryImage:
        req = registry_pb.UnshareRegistryImageRequest(**_id_or_ref(image_or_id))
        if reason:
            req.reason = reason
        try:
            return registry_image((await self._rpc.unshare_registry_image(req)).image)
        except Exception as err:
            raise map_rpc_error(err) from err
