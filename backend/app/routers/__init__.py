"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, snapshot

# 固定路径（如 /page_snapshot/sources）必须排在通用 /{module}/{entry_id} 之前，
# 否则会被动态路径抢先匹配成 entry_id 校验失败。
ROUTERS = [snapshot, resource]
