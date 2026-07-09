from __future__ import annotations

import re

from ._convert import (
    registry_image,
    registry_image_detail,
    registry_list_result,
    registry_share_grant,
    registry_share_result,
    resolved_registry_ref,
)
from ._pb import pb
from .errors import map_rpc_error
from .models import (
    RegistryImage,
    RegistryImageDetail,
    RegistryListResult,
    RegistryShareGrant,
    RegistryShareResult,
    ResolvedRegistryRef,
)

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def _id_or_ref(image_or_id: str) -> dict:
    s = image_or_id.strip()
    if _UUID_RE.match(s):
        return {"artifact_id": s}
    return {"ref": s}


def _id_or_image_ref(image_or_id: str) -> dict:
    s = image_or_id.strip()
    if _UUID_RE.match(s):
        return {"image_id": s}
    return {"image_ref": s}


class Registry:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    def list(
        self,
        workspace_slug: str,
        *,
        labels: list[str] | None = None,
        kind: str | None = None,
        name: str | None = None,
        page_size: int = 50,
        cursor: str | None = None,
    ) -> RegistryListResult:
        req = pb.ListRegistryImagesRequest(workspace_slug=workspace_slug, page_size=page_size)
        if labels:
            req.labels.extend(labels)
        if kind:
            req.kind = pb.RegistryImageKind.Value(f"REGISTRY_IMAGE_KIND_{kind.upper()}")
        if name:
            req.name_substring = name
        if cursor:
            req.cursor = cursor
        try:
            return registry_list_result(self._rpc.list_registry_images(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    def get(self, image_or_id: str) -> RegistryImageDetail:
        try:
            resp = self._rpc.get_registry_image(pb.GetRegistryImageRequest(**_id_or_ref(image_or_id)))
            return registry_image_detail(resp.detail)
        except Exception as err:
            raise map_rpc_error(err) from err

    def resolve(self, image: str) -> ResolvedRegistryRef:
        try:
            resp = self._rpc.resolve_registry_ref(pb.ResolveRegistryRefRequest(ref=image.strip()))
            return resolved_registry_ref(resp.resolved)
        except Exception as err:
            raise map_rpc_error(err) from err

    def publish(
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
        artifact_id: str | None = None,
        ref: str | None = None,
    ) -> RegistryImage:
        req = pb.PublishRegistryImageRequest(
            workspace_id=workspace_id,
            name=name,
            kind=pb.RegistryImageKind.Value(f"REGISTRY_IMAGE_KIND_{kind.upper()}"),
        )
        if artifact_id:
            req.artifact_id = artifact_id
        if ref:
            req.ref = ref
        if source_template_id:
            req.source_template_id = source_template_id
        if snapshot_id:
            req.snapshot_id = snapshot_id
        if tag:
            req.tag = tag
        if visibility:
            req.visibility = pb.RegistryVisibility.Value(f"REGISTRY_VISIBILITY_{visibility.upper()}")
        if labels:
            req.labels.extend(labels)
        if title:
            req.title = title
        if description:
            req.description = description
        try:
            return registry_image(self._rpc.publish_registry_image(req).artifact)
        except Exception as err:
            raise map_rpc_error(err) from err

    def unpublish(self, image_or_id: str) -> RegistryImage:
        try:
            resp = self._rpc.set_registry_image_visibility(
                pb.SetRegistryImageVisibilityRequest(
                    visibility=pb.RegistryVisibility.Value("REGISTRY_VISIBILITY_PRIVATE"),
                    **_id_or_ref(image_or_id),
                )
            )
            return registry_image(resp.artifact)
        except Exception as err:
            raise map_rpc_error(err) from err

    def delete(self, image_or_id: str, *, reason: str | None = None) -> RegistryImage:
        req = pb.DeleteRegistryImageRequest(**_id_or_ref(image_or_id))
        if reason:
            req.reason = reason
        try:
            return registry_image(self._rpc.delete_registry_image(req).artifact)
        except Exception as err:
            raise map_rpc_error(err) from err

    def share(
        self,
        image_or_id: str,
        *,
        target_workspace_id: str,
        tag: str | None = None,
        snapshot_id: str | None = None,
    ) -> RegistryShareResult:
        req = pb.ShareImageRequest(
            target_workspace_id=target_workspace_id.strip(),
            **_id_or_image_ref(image_or_id),
        )
        if tag:
            req.tag = tag
        if snapshot_id:
            req.snapshot_id = snapshot_id
        try:
            return registry_share_result(self._rpc.share_image(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    def revoke_share_grant(self, grant_id: str, *, reason: str | None = None) -> RegistryShareGrant:
        req = pb.RevokeRegistryShareGrantRequest(grant_id=grant_id.strip())
        if reason:
            req.reason = reason
        try:
            return registry_share_grant(self._rpc.revoke_registry_share_grant(req).grant)
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_share_grants(self, image_or_id: str) -> list[RegistryShareGrant]:
        try:
            resp = self._rpc.list_registry_share_grants(
                pb.ListRegistryShareGrantsRequest(**_id_or_ref(image_or_id))
            )
            return [registry_share_grant(g) for g in resp.grants]
        except Exception as err:
            raise map_rpc_error(err) from err

    def unshare(self, image_or_id: str, *, reason: str | None = None) -> RegistryImage:
        req = pb.UnshareRegistryImageRequest(**_id_or_ref(image_or_id))
        if reason:
            req.reason = reason
        try:
            return registry_image(self._rpc.unshare_registry_image(req).artifact)
        except Exception as err:
            raise map_rpc_error(err) from err
