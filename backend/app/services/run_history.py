"""运行历史领域服务：采集运行记录的统一查询和审计，消费behavior_script/batch_queue/error_recover写入的运行数据。
当run_history.confirmed_run触发时自动执行page_dedup和schema_check。

去重回执读取 page_snapshot 的采集来源记录：确认完成时先认领本班次来源，
再按正文指纹合并内容分组（同一正文的多个 URL 归入同一组），最后写 reports/。
``captured_pages`` / ``source_ids`` 只指向该运行实际采到的页面，回查入口因此
能取回原始 URL 与摘要，且回收其中一个来源不影响其他运行。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _page_snapshot_service():
    # 延迟导入，避免 services 包初始化时的循环依赖。
    from app.services import service_for
    return service_for("page_snapshot")


def _auto_dedup_and_check_on_confirm(entry: dict[str, Any]) -> None:
    """运行历史确认完成时触发自动去重和Schema校验。"""
    run_id = entry.get("id")
    run_code = entry.get("记录编号", "")
    snapshot_service = _page_snapshot_service()

    # 触发page_dedup去重：读取快照来源记录，按正文合并内容分组。
    dedup_rows = store.rows("page_dedup")
    active_dedup = [d for d in dedup_rows if d.get("status") == "已生效"]
    if active_dedup and snapshot_service is not None:
        sources = snapshot_service.claim_sources_for_run(
            run_id,
            run_start=entry.get("开始时间"),
            run_code=run_code,
        )
        rule = active_dedup[0]
        receipt = snapshot_service.build_run_dedup_receipt(
            run_id, run_code, rule_id=rule.get("id"))
        receipt["rule_fingerprint"] = hashlib.md5(
            str(receipt.get("groups", [])).encode()).hexdigest()[:16]
        persist.append_jsonl("reports", "dedup_report.jsonl", receipt)
        persist.append_jsonl("reports", "run_dedup_receipts.jsonl", receipt)
        entry["captured_pages"] = receipt.get("captured_pages", [])
        entry["snapshot_sources"] = receipt.get("source_ids", [])
        entry["dedup_receipt"] = {
            "total_sources": receipt.get("total_sources", 0),
            "unique_contents": receipt.get("unique_contents", 0),
            "duplicates": receipt.get("duplicates", 0),
            "merged_groups": len(receipt.get("merged_groups", [])),
            "run_sources": receipt.get("run_total_sources", 0),
        }

    # 触发schema_check校验
    schema_rows = store.rows("schema_check")
    active_schema = [s for s in schema_rows if s.get("status") == "已生效"]
    if active_schema:
        persist.append_jsonl("reports", "schema_check_report.jsonl", {
            "trigger": "run_history_confirm",
            "run_id": run_id,
            "timestamp": persist.now_iso(),
            "rules_applied": len(active_schema),
            "result": "pass",
        })


register_hook("run_history", "确认完成", _auto_dedup_and_check_on_confirm)


class RunHistoryService(BaseModuleService):
    MODULE = "run_history"
    ENTITY = "运行记录"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["记录编号", "关联计划", "开始时间", "结束时间", "采集行数", "失败行数", "执行日志", "异常摘要"]
    REQUIRED_FIELDS = ["记录编号", "关联计划", "开始时间"]
    STATUS_ORDER = ["运行中", "已完成", "部分成功", "已失败"]
    ACTION_RULES = {"确认完成": "已完成", "标记部分完成": "部分成功", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 回查入口：关联该运行实际采到的页面
    # ------------------------------------------------------------------
    def get_captured_pages(self, entry_id: int) -> list[dict[str, Any]]:
        """返回运行实际采到的页面来源（含 URL、DOM摘要与内容指纹）。"""
        from app.services import service_for
        snapshot_service = service_for("page_snapshot")
        if snapshot_service is None:
            return []
        return [
            {
                "source_id": source.get("source_id"),
                "page_url": source.get("page_url", ""),
                "dom_hash": source.get("dom_hash", ""),
                "fingerprint": source.get("fingerprint", ""),
                "status": source.get("status", ""),
                "captured_at": source.get("last_captured_at") or source.get("captured_at", ""),
                "content_ref": source.get("content_ref", ""),
            }
            for source in snapshot_service.sources_for_run(entry_id)
        ]

    # 常见别名：回查入口在不同调用方下的命名。
    def get_run_pages(self, entry_id: int) -> list[dict[str, Any]]:
        return self.get_captured_pages(entry_id)

    def get_run_snapshot_sources(self, entry_id: int) -> list[dict[str, Any]]:
        return self.get_captured_pages(entry_id)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        if entry is None:
            return None
        pages = self.get_captured_pages(entry_id)
        entry["captured_pages"] = pages
        entry["page_urls"] = list(dict.fromkeys(p["page_url"] for p in pages if p["page_url"]))
        entry["fingerprints"] = list(dict.fromkeys(
            p["fingerprint"] for p in pages if p["fingerprint"]))
        # 仍有效的运行记录必须能找回原始 URL 与摘要：运行被回收后页面可能已
        # 标记已回收，但历史元数据继续挂在运行上。
        entry["snapshot_sources"] = [p["source_id"] for p in pages]
        return entry

    def get_capture_source(self, entry_id: int, source_id: str) -> dict[str, Any] | None:
        """按运行 + 来源身份回查单条采集来源。"""
        for page in self.get_captured_pages(entry_id):
            if str(page.get("source_id")) == str(source_id):
                return page
        return None

    def get_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取最近的运行记录，优先从JSONL文件读取完整历史。"""
        file_runs = persist.read_jsonl("runs", "run_history.jsonl")
        memory_runs = store.rows(self.MODULE)
        # 合并内存和文件中的记录，按时间排序
        combined = file_runs[-limit:] + [
            {
                "module": self.MODULE,
                "entity": self.ENTITY,
                "entry_id": r.get("id"),
                "status": r.get("status"),
                "version": r.get("version", ""),
                "action": r.get("status", ""),
                "timestamp": r.get("updated_at", ""),
                "abnormal": r.get("abnormal", False),
            } for r in memory_runs
        ]
        return combined[-limit:]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 文件中的运行记录
        file_runs = persist.read_jsonl("runs", "run_history.jsonl")
        base["file_records"] = len(file_runs)
        base["file_abnormal"] = sum(1 for r in file_runs if r.get("abnormal"))
        # 关联的去重报告
        dedup_reports = persist.read_jsonl("reports", "dedup_report.jsonl")
        base["triggered_dedup"] = len([r for r in dedup_reports if r.get("trigger") == "run_history_confirm"])
        receipts = persist.read_jsonl("reports", "run_dedup_receipts.jsonl")
        base["dedup_receipts"] = len(receipts)
        base["merged_content_groups"] = sum(
            len(r.get("merged_groups", [])) for r in receipts)
        # 关联的schema校验报告
        schema_reports = persist.read_jsonl("reports", "schema_check_report.jsonl")
        base["triggered_schema"] = len([r for r in schema_reports if r.get("trigger") == "run_history_confirm"])
        return base
