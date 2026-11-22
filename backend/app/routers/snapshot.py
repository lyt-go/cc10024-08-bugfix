"""页面快照采集来源与运行历史回查接口。

通用资源路由只覆盖列表/登记/动作；采集来源需要按来源身份和正文指纹回查，
因此单独提供几个只读入口，并补一个采集入口给脚本化采集使用。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import service_for

router = APIRouter(prefix="/api", tags=["页面快照来源"])


class CapturePayload(BaseModel):
    page_url: str
    dom_hash: str = ""
    raw_dom: str = ""
    run_id: int | None = None
    snapshot_id: str | None = None


def _snapshot_service():
    service = service_for("page_snapshot")
    if service is None:
        raise HTTPException(status_code=404, detail="页面快照模块未启用")
    return service


@router.post("/page_snapshot/capture")
def capture_snapshot(payload: CapturePayload) -> dict[str, Any]:
    """采集一次页面快照；同一 URL + 同一 DOM摘要 视为同一来源的重放。"""
    service = _snapshot_service()
    record = service.capture_snapshot(
        payload.page_url,
        payload.dom_hash,
        payload.raw_dom,
        run_id=payload.run_id,
        snapshot_id=payload.snapshot_id,
    )
    record.pop("content", None)
    return {"ok": True, "message": "页面快照已采集", "source": record}


@router.get("/page_snapshot/sources")
def list_snapshot_sources(
    include_recycled: bool = True,
    fingerprint: str | None = None,
    page_url: str | None = None,
    run_id: int | None = None,
) -> dict[str, Any]:
    service = _snapshot_service()
    items = service.list_snapshot_sources(
        include_recycled=include_recycled,
        fingerprint=fingerprint,
        page_url=page_url,
        run_id=run_id,
    )
    return {"items": items, "total": len(items)}


@router.get("/page_snapshot/sources/{source_id}")
def get_snapshot_source(source_id: str) -> dict[str, Any]:
    service = _snapshot_service()
    source = service.get_snapshot_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail=f"采集来源 {source_id} 不存在")
    return source


@router.post("/page_snapshot/sources/{source_id}/recycle")
def recycle_snapshot_source(source_id: str) -> dict[str, Any]:
    service = _snapshot_service()
    record, message = service.recycle_snapshot_source(source_id)
    return {"ok": record is not None, "message": message, "source": record}


@router.get("/page_snapshot/fingerprints/{fingerprint}")
def get_content(fingerprint: str) -> dict[str, Any]:
    """按正文指纹回查归档内容及其全部来源。"""
    service = _snapshot_service()
    content = service.get_content(fingerprint)
    if content is None:
        raise HTTPException(status_code=404, detail=f"指纹 {fingerprint} 没有归档内容")
    return content


@router.get("/run_history/{entry_id}/sources")
def run_snapshot_sources(entry_id: int) -> dict[str, Any]:
    """运行历史的回查入口：该运行实际采到的页面及其原始 URL、摘要。"""
    service = service_for("run_history")
    if service is None:
        raise HTTPException(status_code=404, detail="运行历史模块未启用")
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {entry_id} 不存在")
    items = service.get_captured_pages(entry_id)
    return {"items": items, "total": len(items)}


@router.get("/run_history/{entry_id}/sources/{source_id}")
def run_snapshot_source(entry_id: int, source_id: str) -> dict[str, Any]:
    service = service_for("run_history")
    if service is None:
        raise HTTPException(status_code=404, detail="运行历史模块未启用")
    page = service.get_capture_source(entry_id, source_id)
    if page is None:
        raise HTTPException(status_code=404, detail=f"运行 {entry_id} 未关联来源 {source_id}")
    return page
