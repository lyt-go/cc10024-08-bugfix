"""页面去重领域服务：页面内容相似度检测与去重，消费page_snapshot快照数据，为export_channel提供去重结果。

与 page_snapshot 的采集来源记录配合：同一份正文被多个 URL 采集时，去重按
正文指纹把它们归入同一个内容分组，而不是把后写入的快照当成覆盖。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.store import store
from app.services.base import BaseModuleService
from app import persistence as persist


class PageDedupService(BaseModuleService):
    MODULE = "page_dedup"
    ENTITY = "去重规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "去重维度", "特征字段", "相似算法", "指纹长度", "时间窗口", "冲突策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "去重维度", "特征字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def compute_fingerprint(self, content: str, rule_id: int) -> str:
        """根据去重规则计算内容的指纹。"""
        rule = store.find(self.MODULE, rule_id)
        algorithm = rule.get("相似算法", "md5") if rule else "md5"
        if algorithm == "sha256":
            return hashlib.sha256(content.encode()).hexdigest()[:32]
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def deduplicate_pages(self, pages: list[dict[str, Any]], rule_id: int) -> dict[str, Any]:
        """对页面列表执行去重，返回去重结果和统计。"""
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []
        for page in pages:
            fp = self.compute_fingerprint(str(page), rule_id)
            if fp in seen:
                duplicates.append({**page, "fingerprint": fp})
            else:
                seen.add(fp)
                unique.append({**page, "fingerprint": fp})
        result = {
            "rule_id": rule_id,
            "total": len(pages),
            "unique": len(unique),
            "duplicates": len(duplicates),
            "timestamp": persist.now_iso(),
        }
        persist.append_jsonl("reports", "dedup_report.jsonl", result)
        return result

    def snapshot_sources(self, *, include_recycled: bool = False) -> list[dict[str, Any]]:
        """读取 page_snapshot 的采集来源记录（默认不含已回收来源）。"""
        from app.services import service_for
        snapshot_service = service_for("page_snapshot")
        if snapshot_service is None:
            return []
        return snapshot_service.list_snapshot_sources(include_recycled=include_recycled)

    def build_content_groups(
        self, sources: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """按正文指纹合并内容分组；同一正文的多个 URL 归入同一分组。"""
        from app.services import service_for
        snapshot_service = service_for("page_snapshot")
        if snapshot_service is None:
            return []
        items = sources if sources is not None else self.snapshot_sources()
        groups = snapshot_service.group_sources_by_content(items)
        total = sum(group["count"] for group in groups)
        for group in groups:
            # 去重省下的重复条数：分组内来源数 - 1 份正文。
            group["duplicates"] = group["count"] - 1
            group["share"] = round(group["count"] / total, 4) if total else 0.0
        return groups

    def deduplicate_snapshot_sources(self, rule_id: int | None = None) -> dict[str, Any]:
        """基于采集来源记录执行去重，并把内容分组写进去重报告。"""
        sources = self.snapshot_sources()
        groups = self.build_content_groups(sources)
        total = len(sources)
        unique = len(groups)
        result = {
            "rule_id": rule_id,
            "source": "page_snapshot.sources",
            "total": total,
            "unique": unique,
            "duplicates": total - unique,
            "groups": groups,
            "content_groups": groups,
            "timestamp": persist.now_iso(),
        }
        persist.append_jsonl("reports", "dedup_report.jsonl", result)
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 去重报告消费
        reports = persist.read_jsonl("reports", "dedup_report.jsonl")
        base["dedup_reports"] = len(reports)
        # 关联的采集来源与内容分组
        groups = self.build_content_groups()
        base["snapshot_sources"] = sum(group["count"] for group in groups)
        base["content_groups"] = len(groups)
        base["merged_groups"] = sum(1 for group in groups if group["merged"])
        # 关联的导出配置
        channels = store.rows("export_channel")
        base["export_channels"] = len(channels)
        return base
