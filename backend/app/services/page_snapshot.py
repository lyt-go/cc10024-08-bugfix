"""页面快照领域服务：页面DOM快照的采集与存储，消费login_mgr凭证和canvas_config配置。

存储模型（每次抓取都可回溯）：

* ``data/snapshots/page_sources.jsonl``：采集来源事件流。每次抓取、重放到同一
  来源、来源被回收都会追加一条事件，读侧按 ``source_id`` 归并出当前状态。
* ``data/snapshots/page_src_<source_id>.json``：单个来源的最新快照，来源身份、
  URL、DOM摘要、内容指纹一一对应，刷新进程后仍可独立回查。
* ``data/snapshots/page_<fingerprint>.json``：按正文指纹归档的内容索引。同一份
  正文被多个 URL 采集时只保留一份内容，同时记录全部来源，避免后写覆盖先写。

来源身份 ``source_id`` 由 ``(page_url, dom_hash)`` 稳定派生：
同一 URL 重放同一份内容只算一个来源（只增加抓取次数），不同 URL 即使 DOM
摘要完全相同也各自拥有独立的来源记录。
"""
from __future__ import annotations

import hashlib
import re
import threading
from typing import Any, Iterable

from app.store import store
from app.services.base import BaseModuleService, _version_id, register_hook
from app import persistence as persist


SOURCE_LOG = "page_sources.jsonl"
# 进程内串行化来源事件写入，防止“确认运行/回收来源”交错的写覆盖。
SOURCE_LOCK = threading.RLock()
SOURCE_PREFIX = "page_src_"
CONTENT_PREFIX = "page_"
RECYCLED = "已回收"
ACTIVE = "已存档"


def _now() -> str:
    return persist.now_iso()


def _safe_token(value: str) -> str:
    """把指纹转成可安全落盘的文件名片段。"""
    token = re.sub(r"[^A-Za-z0-9_.-]", "_", str(value or "")).strip("._")
    return token[:96] or hashlib.sha256(str(value).encode()).hexdigest()[:32]


def _source_id(page_url: str, dom_hash: str) -> str:
    raw = f"{page_url}\n{dom_hash}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:16]


def _normalise_fingerprint(dom_hash: str, raw_dom: str) -> str:
    """DOM摘要就是正文指纹；缺省时从原始正文派生。"""
    fingerprint = str(dom_hash or "").strip()
    if fingerprint:
        return fingerprint
    return hashlib.sha256(str(raw_dom).encode("utf-8")).hexdigest()


class PageSnapshotService(BaseModuleService):
    MODULE = "page_snapshot"
    ENTITY = "页面快照"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["快照编号", "采集来源", "页面版本", "DOM摘要", "快照时间", "快照类型", "数据量", "回收时间"]
    REQUIRED_FIELDS = ["快照编号", "采集来源", "页面版本"]
    STATUS_ORDER = ["草稿", "已存档", "已清洗", "已回收"]
    ACTION_RULES = {"标记存档": "已存档", "确认清洗": "已清洗", "启动回收": "已回收"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 采集来源的持久化
    # ------------------------------------------------------------------
    def _source_states(self) -> dict[str, dict[str, Any]]:
        """归并出 ``source_id -> 当前来源记录``。

        先读每个来源的回查文件，再重放事件流：事件流是权威顺序，文件是刷新后
        仍可独立回查的兜底（事件流被截断时来源也不会凭空消失）。
        """
        states: dict[str, dict[str, Any]] = {}
        file_counts: dict[str, int] = {}
        for name in persist.list_files("snapshots", ".json"):
            if not name.startswith(SOURCE_PREFIX):
                continue
            stored = persist.read_json("snapshots", name)
            if not stored:
                continue
            sid = str(stored.get("source_id") or "")
            if not sid:
                continue
            states[sid] = dict(stored)
            file_counts[sid] = int(stored.get("capture_count", 1) or 1)

        event_counts: dict[str, int] = {}
        for event in persist.read_jsonl("snapshots", SOURCE_LOG):
            sid = str(event.get("source_id") or "")
            if not sid:
                continue
            kind = event.get("event")
            if kind == "capture":
                event_counts[sid] = event_counts.get(sid, 0) + 1
                previous = states.get(sid, {})
                record = dict(event)
                if not record.get("run_id") and previous.get("run_id"):
                    # 重放采集不改变来源归属的运行，避免同一次运行被重复计数。
                    record["run_id"] = previous.get("run_id")
                    record["run_code"] = previous.get("run_code", "")
                record["first_captured_at"] = (
                    previous.get("first_captured_at") or event.get("captured_at") or "")
                record["last_captured_at"] = event.get("captured_at") or ""
                record["capture_count"] = max(event_counts[sid], file_counts.get(sid, 0))
                record["replayed"] = event_counts[sid] > 1
                record["status"] = ACTIVE
                record["recycled_at"] = ""
                states[sid] = record
            elif kind == "recycle":
                record = dict(states.get(sid, {}))
                record.update(event)
                record["status"] = RECYCLED
                states[sid] = record
            else:
                record = dict(states.get(sid, {}))
                record.update(event)
                states[sid] = record
        return states

    def _append_source_event(self, record: dict[str, Any], event: str) -> None:
        persist.append_jsonl("snapshots", SOURCE_LOG, {**record, "event": event})

    def _source_file(self, source_id: str) -> str:
        return f"{SOURCE_PREFIX}{_safe_token(source_id)}.json"

    def _write_source_file(self, record: dict[str, Any]) -> None:
        persist.write_json("snapshots", self._source_file(record["source_id"]), record)

    def _persist_source(self, record: dict[str, Any], event: str) -> None:
        """同一来源的事件流与回查文件在同一把锁内完成，避免交错写入。"""
        with SOURCE_LOCK:
            self._append_source_event(record, event)
            self._write_source_file(record)

    def _content_file(self, fingerprint: str) -> str:
        return f"{CONTENT_PREFIX}{_safe_token(fingerprint)}.json"

    def _refresh_content_index(self, fingerprint: str) -> dict[str, Any] | None:
        """按指纹重建内容索引，保留所有关联来源（含已回收来源）。"""
        sources = [
            source for source in self._source_states().values()
            if source.get("fingerprint") == fingerprint or source.get("dom_hash") == fingerprint
        ]
        if not sources:
            return None
        previous = persist.read_json("snapshots", self._content_file(fingerprint)) or {}
        # 事件流之外的历史来源也保留下来，保证索引是并集而不是末次覆盖。
        merged: dict[str, dict[str, Any]] = {
            str(item.get("source_id")): dict(item)
            for item in previous.get("sources", [])
            if item.get("source_id")
        }
        for source in sources:
            merged[source["source_id"]] = {
                "source_id": source["source_id"],
                "page_url": source.get("page_url", ""),
                "status": source.get("status", ""),
                "captured_at": source.get("first_captured_at") or source.get("captured_at", ""),
            }
        ordered = sorted(merged.values(), key=lambda item: (item.get("captured_at", ""), item["source_id"]))
        latest = max(sources, key=lambda item: item.get("last_captured_at") or item.get("captured_at") or "")
        content = {
            # 兼容旧版 page_<dom_hash>.json 的字段形状：
            # 同一份正文可能来自多个 URL，单值字段取最近一次抓取。
            "page_url": latest.get("page_url", ""),
            "dom_hash": fingerprint,
            "fingerprint": fingerprint,
            "content_hash": latest.get("content_hash", ""),
            "raw_dom_length": latest.get("raw_dom_length", 0),
            "dom_preview": latest.get("dom_preview", ""),
            "timestamp": latest.get("last_captured_at") or latest.get("captured_at", ""),
            "first_captured_at": min(
                (s.get("first_captured_at") or s.get("captured_at") or "" for s in sources), default=""),
            "last_captured_at": latest.get("last_captured_at") or latest.get("captured_at", ""),
            "sources": ordered,
            "source_ids": [item["source_id"] for item in ordered],
            "page_urls": list(dict.fromkeys(item["page_url"] for item in ordered if item.get("page_url"))),
            "active_source_ids": [
                item["source_id"] for item in ordered if item.get("status") != RECYCLED],
        }
        persist.write_json("snapshots", self._content_file(fingerprint), content)
        return content

    def get_snapshot_source(self, source_id: str) -> dict[str, Any] | None:
        """刷新后按来源身份回查快照记录（不依赖内存缓存）。"""
        source = self._source_states().get(str(source_id))
        if source is None:
            return None
        # 以落盘文件为准做一次一致性校验；文件缺失时补写。
        stored = persist.read_json("snapshots", self._source_file(source_id))
        return stored or source

    # 常见别名，便于调用方按“来源/快照”命名回查。
    get_source = get_snapshot_source
    lookup_source = get_snapshot_source
    get_capture_source = get_snapshot_source

    def list_snapshot_sources(
        self,
        *,
        include_recycled: bool = True,
        fingerprint: str | None = None,
        page_url: str | None = None,
        run_id: int | str | None = None,
    ) -> list[dict[str, Any]]:
        """列出来源记录；默认包含已回收来源，便于审计与回溯。"""
        sources = list(self._source_states().values())
        if not include_recycled:
            sources = [s for s in sources if s.get("status") != RECYCLED]
        if fingerprint:
            sources = [
                s for s in sources
                if fingerprint in (str(s.get("fingerprint") or ""), str(s.get("dom_hash") or ""))
            ]
        if page_url:
            sources = [s for s in sources if s.get("page_url") == page_url]
        if run_id is not None:
            sources = [s for s in sources if str(s.get("run_id") or "") == str(run_id)]
        return sorted(sources, key=lambda s: (s.get("first_captured_at", ""), s.get("source_id", "")))

    list_sources = list_snapshot_sources
    get_snapshot_sources = list_snapshot_sources

    def sources_for_fingerprint(
        self, fingerprint: str, *, include_recycled: bool = True,
    ) -> list[dict[str, Any]]:
        return self.list_snapshot_sources(
            include_recycled=include_recycled, fingerprint=fingerprint)

    lookup_fingerprint = sources_for_fingerprint
    query_fingerprint = sources_for_fingerprint

    def get_content(self, fingerprint: str) -> dict[str, Any] | None:
        """按指纹读取归档正文索引，多个来源共享同一份正文时不会互相覆盖。"""
        return persist.read_json("snapshots", self._content_file(fingerprint))

    def _sync_entry_status(self, source_id: str, *, status: str, recycled_at: str = "") -> None:
        """让页面快照列表行与来源记录保持一致。"""
        for row in store.rows(self.MODULE):
            if str(row.get("source_id") or "") != str(source_id):
                continue
            row["status"] = status
            row["pending"] = status != self.STATUS_ORDER[-1]
            if status == RECYCLED:
                row["回收时间"] = recycled_at or _now()
            row["version"] = _version_id(row)
            row["updated_at"] = _now()
            row["last_action"] = "启动回收" if status == RECYCLED else row.get("last_action", "")

    def _upsert_entry(self, record: dict[str, Any]) -> dict[str, Any]:
        """每个来源对应一条页面快照列表记录，重放时复用而不是新增。"""
        rows = store.rows(self.MODULE)
        existing = next(
            (row for row in rows if str(row.get("source_id") or "") == record["source_id"]), None)
        if existing is None:
            existing = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "source_id": record["source_id"],
            }
            rows.append(existing)
        existing.update({
            "快照编号": record.get("snapshot_id")
            or f"PAGE-{record['source_id'][:8].upper()}",
            "采集来源": record["page_url"],
            "页面版本": record["fingerprint"],
            "DOM摘要": record["fingerprint"],
            "快照时间": record.get("last_captured_at") or record["captured_at"],
            "快照类型": "采集来源快照",
            "数据量": str(record.get("raw_dom_length", 0)),
            "回收时间": "",
            "status": ACTIVE,
            "pending": True,
            "abnormal": False,
            "content_ref": record.get("content_ref", ""),
            "fingerprint": record["fingerprint"],
            "page_url": record["page_url"],
            "run_id": record.get("run_id"),
            "updated_at": _now(),
        })
        existing["version"] = _version_id(existing)
        return existing

    def capture_snapshot(
        self,
        page_url: str,
        dom_hash: str,
        raw_dom: str,
        run_id: int | str | None = None,
        *,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        """采集一次页面快照，按来源身份幂等持久化。

        同一 URL 重放同一 DOM 摘要时复用同一个来源（``replayed=True``、抓取
        次数加一），不同 URL 即使正文相同也各自写入来源记录。
        """
        with SOURCE_LOCK:
            return self._capture_snapshot_locked(page_url, dom_hash, raw_dom, run_id, snapshot_id)

    def _capture_snapshot_locked(
        self,
        page_url: str,
        dom_hash: str,
        raw_dom: str,
        run_id: int | str | None,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        page_url = str(page_url)
        fingerprint = _normalise_fingerprint(str(dom_hash or ""), raw_dom)
        source_id = _source_id(page_url, fingerprint)
        captured_at = _now()
        content_hash = hashlib.sha256(str(raw_dom).encode("utf-8")).hexdigest()

        previous = self._source_states().get(source_id)
        stable_snapshot_id = (
            (previous or {}).get("snapshot_id")
            or snapshot_id
            or f"PAGE-{source_id[:8].upper()}"
        )
        record = {
            "source_id": source_id,
            "page_url": page_url,
            "dom_hash": str(dom_hash or ""),
            "fingerprint": fingerprint,
            "content_hash": content_hash,
            "raw_dom_length": len(str(raw_dom)),
            "dom_preview": str(raw_dom)[:500],
            "captured_at": captured_at,
            # 兼容旧版 page_<dom_hash>.json 的字段命名。
            "timestamp": captured_at,
            "content_ref": self._content_file(fingerprint),
            "status": ACTIVE,
            # 绑定的运行由运行历史认领（claim/register_capture）写入；
            # 采集本身不重置归属，重放同一来源不会另计一次运行。
            "run_id": run_id,
            "run_code": (previous or {}).get("run_code", ""),
            # 来源身份稳定：同一来源重放时沿用首次登记的编号。
            "snapshot_id": stable_snapshot_id,
            "replayed": previous is not None,
            "first_in_batch": previous is None,
        }
        if previous and run_id is None:
            # 重放沿用已认领的运行归属。
            record["run_id"] = previous.get("run_id")
        if previous:
            record["first_captured_at"] = previous.get("first_captured_at") or captured_at
            record["capture_count"] = int(previous.get("capture_count", 0) or 0) + 1
        else:
            record["first_captured_at"] = captured_at
            record["capture_count"] = 1
        record["last_captured_at"] = captured_at
        record["recycled_at"] = ""

        self._persist_source(record, "capture")
        content = self._refresh_content_index(fingerprint)
        if content is not None:
            record["content_ref"] = self._content_file(fingerprint)
        self._upsert_entry(record)
        record["content"] = content
        return record

    capture = capture_snapshot

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记页面快照时同样落一条可回溯的来源记录。

        只要给出采集来源（URL）与 DOM摘要，就按来源身份幂等登记：同一来源重复
        登记复用原记录，不会因为列表操作而丢掉指针对应的快照内容。
        """
        page_url = str(values.get("采集来源") or "").strip()
        dom_hash = str(values.get("DOM摘要") or values.get("页面版本") or "").strip()
        if page_url and dom_hash:
            record = self.capture_snapshot(
                page_url,
                dom_hash,
                dom_hash,
                snapshot_id=str(values.get("快照编号") or "").strip() or None,
            )
            rows = store.rows(self.MODULE)
            row = next(
                (item for item in rows
                 if str(item.get("source_id") or "") == record["source_id"]),
                None,
            )
            if row is None:
                return None, ["采集来源"]
            for field in self.LIST_FIELDS:
                value = values.get(field)
                if value not in (None, ""):
                    row[field] = value
            row["version"] = _version_id(row)
            row["updated_at"] = _now()
            self._persist_to_jsonl(row, action="create")
            return row, []
        return super().create_entry(values)

    def recycle_snapshot_source(self, source_id: str) -> tuple[dict[str, Any] | None, str]:
        """回收一个来源；其他来源引用的同一份正文保持可回查。"""
        with SOURCE_LOCK:
            return self._recycle_snapshot_source_locked(source_id)

    def _recycle_snapshot_source_locked(
        self, source_id: str,
    ) -> tuple[dict[str, Any] | None, str]:
        states = self._source_states()
        source = states.get(str(source_id))
        if source is None:
            return None, f"采集来源 {source_id} 不存在"
        if source.get("status") == RECYCLED:
            return source, "采集来源已回收"
        recycled_at = _now()
        record = {**source, "status": RECYCLED, "recycled_at": recycled_at}
        self._persist_source(record, "recycle")
        # 只更新内容索引中的来源状态，正文仍保留给其他有效来源。
        self._refresh_content_index(record.get("fingerprint") or record.get("dom_hash") or "")
        self._sync_entry_status(str(source_id), status=RECYCLED, recycled_at=recycled_at)
        return record, "采集来源已回收"

    recycle_source = recycle_snapshot_source
    recycle_capture = recycle_snapshot_source

    def recycle_by_url(self, page_url: str) -> list[dict[str, Any]]:
        """按 URL 回收该地址下仍有效的来源。"""
        recycled = []
        for source in self.list_snapshot_sources(include_recycled=False, page_url=page_url):
            record, _ = self.recycle_snapshot_source(source["source_id"])
            if record:
                recycled.append(record)
        return recycled

    # ------------------------------------------------------------------
    # 与运行历史的衔接
    # ------------------------------------------------------------------
    def claim_sources_for_run(
        self,
        run_id: int | str,
        run_start: str | None = None,
        run_code: str | None = None,
    ) -> list[dict[str, Any]]:
        """取出某次运行实际消费的采集来源并打上运行标记。

        优先使用显式绑定到该运行的来源；否则取运行时间窗内尚未被其他运行
        认领的来源；都取不到时沿用当前有效来源作为共享视图。已回收来源不会
        被重新计入本班次去重，但它的历史元数据仍可用于运行回查。
        """
        with SOURCE_LOCK:
            return self._claim_sources_for_run_locked(run_id, run_start, run_code)

    def _claim_sources_for_run_locked(
        self,
        run_id: int | str,
        run_start: str | None,
        run_code: str | None,
    ) -> list[dict[str, Any]]:
        states = self._source_states()
        explicit = [
            source for source in states.values()
            if str(source.get("run_id") or "") == str(run_id)
        ]
        unclaimed = [
            source for source in states.values()
            if not str(source.get("run_id") or "")
            and source.get("status") != RECYCLED
        ]
        windowed = [
            source for source in unclaimed
            if not run_start
            or str(source.get("first_captured_at") or source.get("captured_at") or "") >= str(run_start)
        ]
        if explicit:
            # 已经显式绑定到本次运行的来源，就是本次运行实际采到的页面。
            # 其中已回收的来源仍保留在回查视图里，但不参与本班次去重。
            chosen = explicit
        elif windowed:
            chosen = windowed
        else:
            chosen = unclaimed

        claimed_at = _now()
        claimed_ids: set[str] = set()
        for source in chosen:
            if source["source_id"] in claimed_ids:
                continue
            claimed_ids.add(source["source_id"])
            if source.get("run_id") and str(source.get("run_id")) == str(run_id):
                continue
            if source.get("status") == RECYCLED:
                continue
            updated = {
                **source,
                "run_id": run_id,
                "run_code": run_code or source.get("run_code", ""),
                "claimed_at": claimed_at,
            }
            self._persist_source(updated, "claim")
            source.update(updated)
        return sorted(chosen, key=lambda s: (s.get("first_captured_at", ""), s["source_id"]))

    claim_for_run = claim_sources_for_run

    def sources_for_run(self, run_id: int | str) -> list[dict[str, Any]]:
        return self.list_snapshot_sources(include_recycled=True, run_id=run_id)

    def register_capture(
        self,
        page_url: str,
        dom_hash: str,
        raw_dom: str,
        *,
        run_id: int | str | None = None,
        run_code: str | None = None,
    ) -> dict[str, Any]:
        """采集并把来源直接绑定到某次运行，供运行确认时精确认领。"""
        record = self.capture_snapshot(page_url, dom_hash, raw_dom, run_id=run_id)
        if run_id is not None:
            with SOURCE_LOCK:
                source = self._source_states().get(record["source_id"], record)
                updated = {**source, "run_id": run_id, "run_code": run_code or source.get("run_code", "")}
                self._persist_source(updated, "claim")
                record = updated
        return record

    record_capture_for_run = register_capture

    def group_sources_by_content(
        self, sources: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """把正文相同（指纹一致）的来源合并成一个内容分组。"""
        groups: dict[str, list[dict[str, Any]]] = {}
        for source in sources:
            fingerprint = str(source.get("fingerprint") or source.get("dom_hash") or "")
            if not fingerprint:
                continue
            groups.setdefault(fingerprint, []).append(source)
        result = []
        for fingerprint, items in groups.items():
            urls = sorted({
                str(item.get("page_url") or "") for item in items if item.get("page_url")})
            result.append({
                "fingerprint": fingerprint,
                "dom_hash": fingerprint,
                "count": len(items),
                "source_count": len(items),
                "page_urls": urls,
                "source_ids": [str(item.get("source_id")) for item in items],
                "content_ref": self._content_file(fingerprint),
                "merged": len(items) > 1,
            })
        return sorted(result, key=lambda item: (item["fingerprint"], item["source_ids"]))

    def build_run_dedup_receipt(
        self,
        run_id: int | str,
        run_code: str | None = None,
        *,
        rule_id: int | None = None,
    ) -> dict[str, Any]:
        """运行确认时读取来源记录，生成可回查的去重回执。

        去重按“本班次”生效：内容分组覆盖当前仍有效的全部采集来源，这样同一
        份正文分别从两个 URL 采到时能合并成一个分组；``captured_pages`` 和
        ``source_ids`` 仍只回查本次运行实际采到的页面。
        """
        batch = self.list_snapshot_sources(include_recycled=False)
        run_sources = self.sources_for_run(run_id)
        run_active = [s for s in run_sources if s.get("status") != RECYCLED]
        groups = self.group_sources_by_content(batch)
        merged = [group for group in groups if group["merged"]]
        receipt = {
            "trigger": "run_history_confirm",
            "run_id": run_id,
            "run_code": run_code or "",
            "rule_id": rule_id,
            "timestamp": _now(),
            "snapshots_checked": len(batch),
            "total_sources": len(batch),
            "unique": len(groups),
            "unique_contents": len(groups),
            "duplicates": len(batch) - len(groups),
            "groups": groups,
            "content_groups": groups,
            "merged_groups": merged,
            "merged_content_groups": len(merged),
            "run_total_sources": len(run_active),
            "run_source_ids": [s["source_id"] for s in run_active],
            "source_ids": [s["source_id"] for s in run_active],
            "captured_pages": [
                {
                    "source_id": s["source_id"],
                    "page_url": s.get("page_url", ""),
                    "dom_hash": s.get("dom_hash", ""),
                    "fingerprint": s.get("fingerprint", ""),
                    "status": s.get("status", ""),
                    "content_ref": s.get("content_ref", ""),
                } for s in run_sources
            ],
        }
        return receipt

    # ------------------------------------------------------------------
    # 动作与统计
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        source_id = str(entry.get("source_id") or "")
        if not source_id:
            return entry, message
        if action == "启动回收":
            # 列表行与来源记录指向同一来源：回收来源本身，保证两者状态一致。
            record, _ = self.recycle_snapshot_source(source_id)
            if record is not None:
                entry.update({
                    "status": RECYCLED,
                    "pending": False,
                    "回收时间": record.get("recycled_at", ""),
                })
        elif action in ("标记存档", "确认清洗"):
            # 列表状态与来源状态保持一致：非回收态不会让来源失去有效身份。
            states = self._source_states()
            source = states.get(source_id)
            if source and source.get("status") == RECYCLED:
                restored = {**source, "status": ACTIVE, "recycled_at": ""}
                self._persist_source(restored, "restore")
                self._refresh_content_index(restored.get("fingerprint", ""))
                entry["回收时间"] = ""
        return entry, message

    def last_activity(self) -> str:
        """来源事件流最近一次变更时间，供刷新前判断是否需要重读。"""
        events = persist.read_jsonl("snapshots", SOURCE_LOG)
        return str(events[-1].get("captured_at") or events[-1].get("recycled_at") or "") if events else ""

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        sources = self.list_snapshot_sources(include_recycled=True)
        active = [s for s in sources if s.get("status") != RECYCLED]
        contents = {str(s.get("fingerprint") or s.get("dom_hash")) for s in active}
        snap_files = persist.list_files("snapshots", ".json")
        base["snapshot_files"] = len([f for f in snap_files if f.startswith(SOURCE_PREFIX)])
        base["content_files"] = len([
            f for f in snap_files if f.startswith(CONTENT_PREFIX) and not f.startswith(SOURCE_PREFIX)
        ])
        base["source_records"] = len(sources)
        base["active_sources"] = len(active)
        base["recycled_sources"] = len(sources) - len(active)
        base["unique_contents"] = len(contents)
        base["merged_content_groups"] = sum(
            1 for group in self.group_sources_by_content(active) if group["merged"])
        # 关联的登录凭证
        credentials = store.rows("login_mgr")
        snaps = store.rows(self.MODULE)
        sites = {r.get("采集来源") for r in snaps}
        base["sites_with_credentials"] = sum(1 for c in credentials if c.get("目标站点") in sites)
        return base
