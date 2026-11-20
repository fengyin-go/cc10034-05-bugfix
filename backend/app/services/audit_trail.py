"""审计轨迹领域服务：记录CDC管道配置变更和事件处理的全链审计。

上游：所有服务模块都会写入审计日志到 pipeline_bus
消费：审计查询消费端通过本服务查询历史轨迹；
     回填完成明细数据集 → 本服务入账并回写恢复清单。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.backfill_ledger import ledger
from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class AuditTrailService(BaseModuleService):
    MODULE = "audit_trail"
    ENTITY = "审计日志"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["审计时间", "操作类型", "目标对象", "变更摘要", "操作来源", "结果状态", "审计备注"]
    REQUIRED_FIELDS = ["审计时间", "操作类型", "目标对象"]
    STATUS_ORDER = ["已记录", "已复核", "异常标记"]
    ACTION_RULES = {"复核记录": "已记录", "标记异常": "已记录"}
    NEGATIVE_ACTIONS = []

    def query_audit_logs(self, from_time: str = "", to_time: str = "",
                         operation_type: str = "", source_module: str = "",
                         limit: int = 100) -> list[dict[str, Any]]:
        """查询审计日志。”

        支持按时间范围、操作类型、来源模块筛选。
        """
        result: list[dict[str, Any]] = []
        for entry in reversed(pipeline_bus.audit_log):
            entry_time = entry.get("审计时间", "")
            if from_time and entry_time < from_time:
                continue
            if to_time and entry_time > to_time:
                continue
            if operation_type and operation_type not in entry.get("操作类型", ""):
                continue
            if source_module and source_module != entry.get("操作来源", ""):
                continue
            result.append(dict(entry))
            if len(result) >= limit:
                break
        return result

    # ------------------------------------------------------------------
    # 回填完成明细消费与恢复清单回写
    # ------------------------------------------------------------------
    def consume_backfill_completion(self, detail: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """消费回填完成明细数据集，按 (任务, 批号) 幂等入账。

        同一批号重复到达只保留一条恢复清单行；已入账的批号不会因为
        晚到或重放回执而重复累加有效行数。
        """
        if detail is None:
            detail = ledger.completion_detail()
        pipeline_bus.backfill_completion_detail = [dict(item) for item in detail]
        accepted = 0
        duplicates = 0
        for item in detail:
            task = str(item.get("任务名称") or item.get("task") or "").strip()
            batch_no = int(item.get("批号") or item.get("batch_no") or 0)
            if not task or batch_no < 1:
                continue
            key = (task.strip().casefold(), batch_no)
            if key in pipeline_bus.backfill_recovery_checklist:
                # 幂等：同一 (任务, 批号) 重复消费只保留一条清单行
                duplicates += 1
                continue
            audit_entry = pipeline_bus.write_audit(
                "回填完成入账", task,
                (
                    f"批号 {batch_no} 确认 {int(item.get('有效行数') or 0)} 行，"
                    f"日志跨度 {item.get('日志跨度', '')}"
                ),
                "audit_trail", "已记录",
                remark=f"receipt={item.get('receipt_id', '')}",
            )
            pipeline_bus.backfill_recovery_checklist[key] = {
                "任务名称": task,
                "已确认批号": batch_no,
                "有效行数": int(item.get("有效行数") or 0),
                "日志跨度": str(item.get("日志跨度") or ""),
                "确认时间": str(item.get("confirmed_at") or ""),
                "审计记录ID": audit_entry["id"],
                "审计时间": audit_entry["审计时间"],
                "状态": "已入账",
                "重复入账次数": 0,
            }
            accepted += 1
        return {
            "ok": True,
            "accepted": accepted,
            "duplicates": duplicates,
            "checklist_size": len(pipeline_bus.backfill_recovery_checklist),
            "message": f"回填完成明细已入账 {accepted} 行，重复 {duplicates} 行未重复计数",
        }

    def recovery_checklist(self) -> list[dict[str, Any]]:
        """审计恢复清单：按已确认批号与回填完成明细逐条对齐。"""
        self.consume_backfill_completion()
        rows = sorted(
            pipeline_bus.backfill_recovery_checklist.values(),
            key=lambda item: (item["任务名称"], item["已确认批号"]),
        )
        result = []
        for item in rows:
            row = dict(item)
            row["审计状态"] = row.get("状态", "待入账")
            result.append(row)
        return result

    def reconcile_backfill_checklists(self) -> dict[str, Any]:
        """核对回填完成清单与审计恢复清单是否按已确认批号一致。"""
        self.consume_backfill_completion()
        completion = {
            (item["任务名称"].strip().casefold(), int(item["批号"])): item
            for item in pipeline_bus.backfill_completion_detail
        }
        audit = set(pipeline_bus.backfill_recovery_checklist)
        missing_in_audit = sorted(f"{task}#{batch}" for task, batch in completion.keys() - audit)
        missing_in_completion = sorted(f"{task}#{batch}" for task, batch in audit - completion.keys())
        mismatched = []
        for key in completion.keys() & audit:
            expected = int(completion[key].get("有效行数") or 0)
            actual = int(pipeline_bus.backfill_recovery_checklist[key].get("有效行数") or 0)
            if expected != actual:
                mismatched.append({
                    "task": completion[key]["任务名称"],
                    "batch_no": key[1],
                    "completion_rows": expected,
                    "audit_rows": actual,
                })
        consistent = not missing_in_audit and not missing_in_completion and not mismatched
        return {
            "consistent": consistent,
            "completion_rows": len(completion),
            "audit_rows": len(audit),
            "missing_in_audit": missing_in_audit,
            "missing_in_completion": missing_in_completion,
            "row_count_mismatch": mismatched,
            "confirmed_batches": sorted(
                [{"task": task, "batch_no": batch} for task, batch in completion],
                key=lambda item: (item["task"], item["batch_no"]),
            ),
            "checked_at": datetime.now().isoformat(),
        }

    def unresolved_coordinates(self) -> dict[str, Any]:
        """给值班者定位尚未纳入恢复清单的日志坐标。"""
        checklist_keys = set(pipeline_bus.backfill_recovery_checklist)
        pending: list[dict[str, Any]] = []
        for item in pipeline_bus.backfill_completion_detail:
            key = (item["任务名称"].strip().casefold(), int(item["批号"]))
            if key not in checklist_keys:
                pending.append({
                    "任务名称": item["任务名称"],
                    "批号": item["批号"],
                    "日志跨度": item["日志跨度"],
                })
        summary = ledger.summary()
        return {
            "pending_completion_rows": pending,
            "unresolved_intervals": summary["unresolved_intervals"],
            "resume_scopes": summary["resume_scopes"],
            "message": (
                "全部已确认批次均已纳入恢复清单"
                if not pending else f"仍有 {len(pending)} 条确认批次未纳入恢复清单"
            ),
        }

    def audit_summary(self) -> dict[str, Any]:
        """审计日志汇总统计。"""
        logs = pipeline_bus.audit_log
        by_module: dict[str, int] = {}
        by_operation: dict[str, int] = {}
        by_status: dict[str, int] = {}
        for entry in logs:
            module = entry.get("操作来源", "unknown")
            op = entry.get("操作类型", "unknown")
            status = entry.get("结果状态", "unknown")
            by_module[module] = by_module.get(module, 0) + 1
            by_operation[op] = by_operation.get(op, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1
        return {
            "total_entries": len(logs),
            "by_module": by_module,
            "by_operation": by_operation,
            "by_status": by_status,
            "last_entry_time": logs[-1]["审计时间"] if logs else "",
            "generated_at": datetime.now().isoformat(),
        }

    def find_anomalies(self, window_count: int = 500) -> list[dict[str, Any]]:
        """从审计日志中检测异常模式。"""
        recent = pipeline_bus.audit_log[-window_count:]
        anomalies = []
        error_keywords = ["失败", "异常", "错误", "failed", "error"]
        for entry in recent:
            summary = entry.get("变更摘要", "")
            status = entry.get("结果状态", "")
            if any(kw in summary for kw in error_keywords) or status == "异常标记":
                anomalies.append({
                    "entry": entry,
                    "risk": "high" if "失败" in summary or "严重" in summary else "medium",
                    "detected_at": datetime.now().isoformat(),
                })
        return anomalies

    def compliance_check(self) -> dict[str, Any]:
        """合规性检查。"""
        logs = pipeline_bus.audit_log
        total = len(logs)
        required_ops = ["DDL分析", "Schema同步", "通道启动", "路由分发"]
        op_coverage: dict[str, bool] = {}
        for op in required_ops:
            op_coverage[op] = any(op in entry.get("操作类型", "") for entry in logs)
        all_covered = all(op_coverage.values())
        return {
            "total_audit_entries": total,
            "required_operation_coverage": op_coverage,
            "all_required_covered": all_covered,
            "compliance_status": "合规" if all_covered else "部分覆盖",
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "复核记录":
            pipeline_bus.write_audit(
                "审计复核", entry.get("目标对象", ""),
                f"审计记录 {entry_id} 已复核",
                "audit_trail", "已复核"
            )
        return entry, msg