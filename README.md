# 网页行为采集与内容清洗平台

面向网页行为录制、脚本化采集、内容清洗与结构化输出的数据采集与加工平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 行为脚本 | `behavior_script` | 行为脚本 | 脚本编号、目标站点、录制步骤 |
| 元素定位 | `element_selector` | 元素锚点 | 锚点编号、关联脚本、选择器类型 |
| 数据抽取 | `data_extract` | 抽取模板 | 模板编号、关联脚本、抽取字段 |
| 清洗管道 | `clean_pipeline` | 清洗工序 | 工序编号、输入源、清洗规则 |
| 转换规则 | `transform_rule` | 转换规则 | 规则编号、关联工序、源字段 |
| 质量防护 | `quality_guard` | 质量规则 | 规则编号、检核对象、检核维度 |
| 脚本调度 | `script_schedule` | 调度计划 | 计划编号、关联脚本、触发条件 |
| 登录凭证 | `login_mgr` | 登录凭证 | 凭证编号、目标站点、认证方式 |
| 页面快照 | `page_snapshot` | 页面快照 | 快照编号、采集来源、页面版本 |
| 错误恢复 | `error_recover` | 恢复任务 | 任务编号、关联计划、失败步骤 |
| 页面去重 | `page_dedup` | 去重规则 | 规则编号、去重维度、特征字段 |
| 字段归一化 | `field_normalize` | 归一规则 | 规则编号、关联字段、归一类型 |
| 批次队列 | `batch_queue` | 采集批次 | 批次编号、关联计划、任务数量 |
| 画布配置 | `canvas_config` | 画布配置 | 配置编号、关联站点、分辨率 |
| 导出通道 | `export_channel` | 导出配置 | 通道编号、输出格式、目标路径 |
| 运行历史 | `run_history` | 运行记录 | 记录编号、关联计划、开始时间 |
| Schema校验 | `schema_check` | Schema规则 | 规则编号、目标数据、字段名 |
| 内容缓存 | `content_cache` | 缓存配置 | 配置编号、缓存类型、过期策略 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 页面快照采集来源

页面快照不再用 `page_<DOM摘要>.json` 覆盖式存档：两个 URL 采到同一份正文时，
后写入的不会再抹掉先写入的来源。落盘结构：

| 文件 | 内容 |
| --- | --- |
| `data/snapshots/page_sources.jsonl` | 采集来源事件流（capture / claim / recycle / restore） |
| `data/snapshots/page_src_<来源>.json` | 单个来源的最新快照，来源身份、URL、DOM摘要、内容指纹一一对应 |
| `data/snapshots/page_<指纹>.json` | 按正文指纹归档的内容索引，记录全部关联来源与仍有效的来源 |

来源身份由 `(URL, DOM摘要)` 稳定派生：同一 URL 重放同一采集只增加抓取次数、
不新增来源；不同 URL 即使正文相同也各自保留来源记录。相关入口：

```text
POST /api/page_snapshot/capture                     采集一次页面快照
GET  /api/page_snapshot/sources                     列出来源（可含已回收）
GET  /api/page_snapshot/sources/{source_id}         按来源身份回查
POST /api/page_snapshot/sources/{source_id}/recycle 回收一个来源
GET  /api/page_snapshot/fingerprints/{fingerprint}  按正文指纹回查内容与来源
GET  /api/run_history/{entry_id}                    运行记录（含 captured_pages）
GET  /api/run_history/{entry_id}/sources            该运行实际采到的页面
```

运行历史确认完成时会认领本次运行实际采到的来源，按正文指纹合并内容分组并写
`data/reports/run_dedup_receipts.jsonl`；回收一个来源不会影响其他引用同一正文
的来源，也不会让仍有效的运行记录丢失原始 URL 与摘要。

领域：数据工程
子方向：数据采集、爬虫与RPA
