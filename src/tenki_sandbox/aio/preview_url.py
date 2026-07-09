from __future__ import annotations

from .._convert import preview_url
from .._pb import pb
from ..errors import map_rpc_error
from ..models import PreviewUrl


class AsyncPreviewUrls:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def create(
        self, project_id: str, slug: str, *, session_id: str | None = None, port: int | None = None
    ) -> PreviewUrl:
        req = pb.CreatePreviewUrlRequest(project_id=project_id, slug=slug)
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
        self, project_id: str, *, page_size: int = 20, page_token: str = ""
    ) -> tuple[list[PreviewUrl], str]:
        try:
            resp = await self._rpc.list_preview_urls(
                pb.ListPreviewUrlsRequest(project_id=project_id, page_size=page_size, page_token=page_token)
            )
            return [preview_url(item) for item in resp.preview_urls], resp.next_page_token
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get(self, preview_url_id: str) -> PreviewUrl:
        try:
            resp = await self._rpc.get_preview_url(pb.GetPreviewUrlRequest(preview_url_id=preview_url_id))
            return preview_url(resp.preview_url)
        except Exception as err:
            raise map_rpc_error(err) from err
