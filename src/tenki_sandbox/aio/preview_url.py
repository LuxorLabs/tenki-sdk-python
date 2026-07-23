from __future__ import annotations

from .._convert import preview_url
from .._pb import pb
from .._pagination import advance_page_token
from ..errors import map_rpc_error
from ..models import PreviewUrl


class AsyncPreviewUrls:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def create(
        self,
        slug: str,
        *,
        workspace_id: str | None = None,
        session_id: str | None = None,
        port: int | None = None,
    ) -> PreviewUrl:
        req = pb.CreatePreviewUrlRequest(slug=slug)
        if workspace_id and workspace_id.strip():
            req.workspace_id = workspace_id.strip()
        if session_id:
            req.session_id = session_id
        if port:
            req.port = port
        try:
            resp = await self._rpc.create_preview_url(req)
            return preview_url(resp.preview_url)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def delete(self, preview_url_id: str) -> None:
        try:
            await self._rpc.delete_preview_url(pb.DeletePreviewUrlRequest(preview_url_id=preview_url_id))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def bind(self, preview_url_id: str, session_id: str, port: int) -> PreviewUrl:
        try:
            resp = await self._rpc.bind_preview_url(
                pb.BindPreviewUrlRequest(preview_url_id=preview_url_id, session_id=session_id, port=port)
            )
            return preview_url(resp.preview_url)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def unbind(self, preview_url_id: str) -> PreviewUrl:
        try:
            resp = await self._rpc.unbind_preview_url(pb.UnbindPreviewUrlRequest(preview_url_id=preview_url_id))
            return preview_url(resp.preview_url)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(
        self,
        *,
        workspace_id: str | None = None,
    ) -> list[PreviewUrl]:
        try:
            preview_urls: list[PreviewUrl] = []
            page_token = ""
            while True:
                req = pb.ListPreviewUrlsRequest(
                    page_size=100,
                    page_token=page_token,
                )
                if workspace_id and workspace_id.strip():
                    req.workspace_id = workspace_id.strip()
                resp = await self._rpc.list_preview_urls(req)
                preview_urls.extend(preview_url(item) for item in resp.preview_urls)
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return preview_urls
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get(self, preview_url_id: str) -> PreviewUrl:
        try:
            resp = await self._rpc.get_preview_url(pb.GetPreviewUrlRequest(preview_url_id=preview_url_id))
            return preview_url(resp.preview_url)
        except Exception as err:
            raise map_rpc_error(err) from err
