# 实时数仓CDC变更捕获中心

面向实时数仓建设场景，提供Binlog解析、变更事件捕获、DDL追踪、SCD处理和下游分发同步的CDC治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/views/BackfillPanel.vue      回填批次回执 / 完成明细 / 恢复清单面板
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/backfill_ledger.py   回填批次台账（按任务+批号去重）
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
| Binlog解析 | `binlog_parser` | 解析配置 | 配置名称、源数据库、解析模式 |
| 变更事件 | `change_event` | 变更记录 | 事件ID、来源表、操作类型 |
| DDL追踪 | `ddl_tracker` | DDL记录 | 记录ID、变更数据库、变更表名 |
| Schema同步 | `schema_sync` | 同步任务 | 任务名称、源表、目标表 |
| 采集通道 | `cdc_channel` | 通道实例 | 通道名称、Binlog源、输出目标 |
| 检查点管理 | `checkpoint_manager` | 检查点 | 检查点ID、所属通道、Binlog文件 |
| 位点存储 | `position_store` | 位点记录 | 记录ID、数据库实例、Binlog名称 |
| 格式适配 | `format_adapter` | 适配规则 | 规则名称、输入格式、输出格式 |
| 缓慢变化维 | `scd_handler` | SCD配置 | 配置名称、维度表、SCD类型 |
| 去重窗口 | `dedup_window` | 窗口配置 | 配置名称、去重键、窗口大小 |
| 路由分发 | `route_dispatcher` | 路由规则 | 规则名称、匹配条件、目标通道 |
| 回填位点 | `backfill_position` | 回填任务 | 任务名称、源位点、目标位点 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标时间、所属通道、延迟秒数 |
| 快照比对 | `snapshot_compare` | 比对报告 | 报告名称、源表名、目标表名 |
| 事件过滤 | `event_filter` | 过滤规则 | 规则名称、过滤类型、匹配表达式 |
| 分区同步 | `partition_sync` | 同步配置 | 配置名称、源分区策略、目标分区策略 |
| 审计轨迹 | `audit_trail` | 审计日志 | 审计时间、操作类型、目标对象 |
| 故障切换 | `failover_group` | 切换组 | 组名、主通道、备用通道 |

## 回填进度与审计恢复清单

回填进度不再是全局计数器，而是按任务独立汇总已确认批次：

- 批次回执按 **（任务，批号）** 去重，重复提交只记 `duplicate`，不计入有效行数；
  行数或跨度不一致的重复回执记为 `duplicate_conflict`，仍不改动已发布进度。
- `预估记录数` 只作为进度分母，只带预估值的回执会被拒绝
  （`estimated_rows_not_actual`），不会写成已回填值。
- 每条已确认回执带 Binlog 文件与起止位点；中断时保留“上次确认的日志跨度”，
  中断后从未确认区间算出实际续做范围。
- 中断轮与晚到回执同时出现时由 `absorb_late` 取舍：吸收则推进续做范围；
  不吸收则回执记为 `held_for_review`，挂起待复核，已发布进度保持不变。
- `回填完成明细数据集` 一行一个已确认批次；`审计轨迹` 消费该数据集后回写
  `审计恢复清单`，两份清单按已确认批号严格对齐，不一致会出现在 `reconcile` 中。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/backfill_position/receipts` | 同一批次执行入口：单条、批量或 `groups` 整组提交 |
| POST | `/api/backfill_position/groups` | 多选作业组整组提交 |
| POST | `/api/backfill_position/batches` | 执行单批（任务 + 批号 + 实际行数 + 可选跨度） |
| POST | `/api/backfill_position/interrupts` | 中断回填，记录 `absorb_late` 取舍与上次确认跨度 |
| GET | `/api/backfill_position-progress` | 按任务进度（有效行数、确认批号、续做范围） |
| GET | `/api/backfill_position-tasks` | 回填任务名清单 |
| GET | `/api/backfill-position/summary` | 有效行数、悬而未决 Binlog 区间、实际续做范围 |
| GET | `/api/backfill-position/completion-detail` | 回填完成明细数据集 |
| GET | `/api/backfill-position/batch-log` | 回执处理日志与冲突明细 |
| GET | `/api/audit-trail/recovery-checklist` | 审计恢复清单 + 与完成明细的对账结果 |

## 测试

```bash
cd backend && python -m unittest discover -s tests -v
```

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
