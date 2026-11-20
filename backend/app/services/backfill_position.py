"""回填位点领域服务：基于位点范围执行历史数据回填。

上游：position_store 提供位点范围 → backfill_position 执行回填
消费：pipeline_bus 回填行数统计供 lag_monitor 消费；
      audit_trail 消费回填完成明细并回写恢复清单。

关键约定：
- 进度按任务独立计算，批次回执按 (任务, 批号) 去重；
- 预估记录数只做进度分母，不写进已回填行数；
- 中断后以“上次确认的日志跨度”为续做起点；
- 中断轮与晚到回执相遇时由 ``absorb_late`` 取舍：吸收则推进续做范围，
  不吸收则挂起待复核、不改已发布进度。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.backfill_ledger import ledger
from app.pipeline import pipeline_bus
from app.services.base import BaseModuleService


def _optional_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "吸收", "是"}


def _settled(item: dict[str, Any]) -> bool:
    return bool(item.get("accepted") or item.get("duplicate") or item.get("held"))


class BackfillPositionService(BaseModuleService):
    MODULE = "backfill_position"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务名称", "源位点", "目标位点", "扫描范围", "批次大小", "并发度", "预估记录数"]
    REQUIRED_FIELDS = ["任务名称", "源位点", "目标位点"]
    STATUS_ORDER = ["待回填", "回填中", "已完成", "已中断"]
    ACTION_RULES = {"开始回填": "回填中", "中断回填": "已中断", "完成回填": "已完成"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 列表 / 登记
    # ------------------------------------------------------------------
    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        rows, total = super().list_entries(**kwargs)
        for row in rows:
            self._decorate(row)
        return rows, total

    def known_tasks(self) -> list[str]:
        """已知回填任务名：工作台任务行 + 已登记台账任务。"""
        names: list[str] = []
        for row in self.rows():
            name = str(row.get("任务名称", "")).strip()
            if name and name not in names:
                names.append(name)
        for state in ledger.tasks.values():
            if state["task"] not in names:
                names.append(state["task"])
        return names

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = super().get_entry(entry_id)
        if row is not None:
            self._decorate(row)
        return row

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is not None:
            ledger.register_from_row(entry)
            self._decorate(entry)
            pipeline_bus.write_audit(
                "回填任务创建", entry.get("任务名称", ""),
                "回填任务已登记，进度从 0 起算，未写入任何已回填行数",
                "backfill_position", "已记录",
            )
        return entry, missing

    def create_backfill_task(self, config: dict[str, Any]) -> dict[str, Any]:
        """创建回填任务并计算预估指标（预估不落到已回填值）。"""
        task_name = str(config.get("任务名称") or config.get("task") or "").strip()
        source_pos = self._parse_pos(str(config.get("源位点") or config.get("source_pos") or "0"))
        target_pos = self._parse_pos(str(config.get("目标位点") or config.get("target_pos") or "0"))
        batch_size = self._parse_int(config.get("批次大小") or config.get("batch_size"), 1000)
        parallelism = self._parse_int(config.get("并发度") or config.get("parallelism"), 2)
        avg_row_bytes = 256

        estimate = pipeline_bus.compute_backfill_estimate(
            source_pos, target_pos, batch_size, avg_row_bytes
        )
        state = ledger.register_task(
            task_name,
            source_pos=source_pos,
            target_pos=target_pos,
            binlog_file=str(config.get("binlog_file") or config.get("Binlog文件") or ""),
            batch_size=batch_size,
            parallelism=parallelism,
            estimated_rows=estimate["estimated_rows"],
            scan_range=str(config.get("扫描范围") or ""),
            status="回填中",
        )
        task = {
            "task_name": task_name,
            "source_pos": source_pos,
            "target_pos": target_pos,
            "batch_size": batch_size,
            "parallelism": parallelism,
            "estimated_rows": estimate["estimated_rows"],
            "estimated_batches": estimate["estimated_batches"],
            "position_diff_bytes": estimate["position_diff_bytes"],
            "created_at": datetime.now().isoformat(),
        }
        pipeline_bus.write_audit(
            "回填任务创建", task_name,
            f"回填任务创建，预估 {estimate['estimated_rows_str']} 条记录（预估不计入已回填）",
            "backfill_position", "已记录",
        )
        return {**task, "state": state}

    # ------------------------------------------------------------------
    # 批次执行入口
    # ------------------------------------------------------------------
    def execute_batch(self, task: dict[str, Any], batch_no: int, **kwargs: Any) -> dict[str, Any]:
        """执行单批回填并登记回执。

        这是“同一批次执行入口”：无论任务来自工作台勾选还是逐条重放，
        都走这里，进度只累加到当前任务名下。
        """
        task_name = str(task.get("任务名称") or task.get("task") or task.get("task_name") or "").strip()
        if not task_name:
            raise ValueError("回填任务缺少任务名称")
        payload = dict(kwargs.pop("payload", {}) or {})
        payload.setdefault("task", task_name)
        payload.setdefault("batch_no", int(batch_no))
        if "rows" not in payload and "实际行数" not in payload:
            if task.get("rows_actual") is not None:
                payload["rows"] = task["rows_actual"]
            elif task.get("rows") is not None:
                payload["rows"] = task["rows"]
            # 没有实际行数时不拿批次大小/预估记录数兜底，交给台账拒绝
        if task.get("binlog_file") and "binlog_file" not in payload:
            payload["binlog_file"] = task["binlog_file"]
        if task.get("start_pos") is not None and "start_pos" not in payload:
            payload["start_pos"] = task["start_pos"]
        if task.get("end_pos") is not None and "end_pos" not in payload:
            payload["end_pos"] = task["end_pos"]
        payload.setdefault("arrival", kwargs.get("arrival", "normal"))
        payload.setdefault("group_id", kwargs.get("group_id"))
        result = ledger.submit_receipt(
            payload,
            arrival=kwargs.get("arrival"),
            group_id=kwargs.get("group_id"),
            absorb_late=kwargs.get("absorb_late"),
        )
        return {
            "batch_no": int(batch_no),
            "rows_processed": (result.get("receipt") or {}).get("rows_actual", 0),
            "task": task_name,
            "processed_at": datetime.now().isoformat(),
            "cumulative": (result.get("progress") or {}).get("actual_rows", 0),
            **result,
        }

    def submit_batch_receipt(self, payload: dict[str, Any]) -> dict[str, Any]:
        """批次回执入口：支持单条、批量、整组三种载荷。"""
        if not isinstance(payload, dict):
            return {"ok": False, "message": "回执载荷必须是对象", "results": []}

        if payload.get("groups"):
            return self.submit_groups(payload["groups"], default_group_id=payload.get("group_id"))

        receipts = None
        for key in ("receipts", "batch_receipts", "items", "deliveries", "delivery"):
            if isinstance(payload.get(key), list):
                receipts = payload[key]
                break
        if receipts is not None:
            results = []
            group_id = payload.get("group_id")
            for receipt in receipts:
                merged = {**payload, **receipt} if isinstance(receipt, dict) else {}
                merged.pop("receipts", None)
                merged.pop("batch_receipts", None)
                merged.pop("items", None)
                merged.pop("deliveries", None)
                results.append(ledger.submit_receipt(
                    merged,
                    group_id=group_id,
                    absorb_late=_optional_bool(merged.get("absorb_late")),
                ))
            return {
                "ok": all(_settled(item) for item in results),
                "message": f"已处理 {len(results)} 条批次回执",
                "results": results,
                "group_id": group_id,
                "progress": self.progress(),
            }

        result = ledger.submit_receipt(
            payload,
            absorb_late=_optional_bool(payload.get("absorb_late")),
        )
        result["progress"] = self.progress(payload.get("task") or payload.get("任务名称"))
        return result

    def submit_groups(self, groups: list[Any], default_group_id: str | None = None) -> dict[str, Any]:
        """多选作业组后整组提交；逐条回执仍按 (任务, 批号) 去重。"""
        group_results: list[dict[str, Any]] = []
        for index, group in enumerate(groups or [], start=1):
            if not isinstance(group, dict):
                group_results.append({
                    "group_id": f"group-{index}",
                    "ok": False,
                    "message": "作业组必须是对象",
                    "results": [],
                })
                continue
            group_id = str(
                group.get("group_id")
                or group.get("group")
                or group.get("组名")
                or default_group_id
                or f"group-{index}"
            )
            receipts = (
                group.get("receipts")
                or group.get("batch_receipts")
                or group.get("items")
                or group.get("deliveries")
                or []
            )
            task = group.get("task") or group.get("任务名称")
            results = []
            for receipt in receipts:
                if not isinstance(receipt, dict):
                    continue
                merged = {**group, **receipt}
                for drop in ("receipts", "batch_receipts", "items", "deliveries", "groups"):
                    merged.pop(drop, None)
                if task and not (merged.get("task") or merged.get("任务名称")):
                    merged["task"] = task
                results.append(ledger.submit_receipt(
                    merged,
                    group_id=group_id,
                    absorb_late=_optional_bool(merged.get("absorb_late")),
                ))
            group_results.append({
                "group_id": group_id,
                "ok": bool(results) and all(_settled(item) for item in results),
                "message": f"作业组 {group_id} 提交 {len(results)} 条回执",
                "task": task,
                "results": results,
            })
        return {
            "ok": all(item["ok"] for item in group_results) if group_results else False,
            "message": f"已提交 {len(group_results)} 个作业组",
            "groups": group_results,
            "progress": self.progress(),
            "summary": self.summary(),
        }

    def interrupt(
        self,
        task: str,
        batch_no: int | None = None,
        reason: str = "",
        absorb_late: bool = True,
    ) -> dict[str, Any]:
        """中断回填：记录中断轮次，保留上次确认跨度用于续做。

        ``absorb_late`` 决定这一轮是否吸收晚到回执，会改变续做范围与已发布进度。
        """
        record = ledger.interrupt(task, batch_no, reason, absorb_late=absorb_late)
        row = self._find_row(task)
        if row is not None:
            row["status"] = "已中断"
            row["pending"] = False
        return record

    def complete(self, task: str) -> dict[str, Any]:
        """完成回填：仅以已确认批次为准，预估行数不参与。"""
        progress = ledger.task_progress(task)
        row = self._find_row(task)
        if row is not None:
            row["status"] = "已完成"
            row["pending"] = False
        state = ledger.ensure_task(task)
        state["status"] = "已完成"
        pipeline_bus.write_audit(
            "回填完成", progress["task"],
            (
                f"回填任务完成，已确认批号 {progress['confirmed_batches'] or '无'}，"
                f"有效行数 {progress['actual_rows']}，预估 {progress['estimated_rows']} 行仅作分母"
            ),
            "backfill_position", "已完成",
        )
        return {"ok": True, "message": "回填任务已完成", "progress": progress}

    # ------------------------------------------------------------------
    # 只读视图
    # ------------------------------------------------------------------
    def progress(self, task: str | None = None) -> Any:
        if task:
            return ledger.task_progress(task)
        return [ledger.task_progress(state["task"]) for state in ledger.tasks.values()]

    def completion_detail(self, task: str | None = None) -> list[dict[str, Any]]:
        detail = ledger.completion_detail(task)
        pipeline_bus.backfill_completion_detail = detail
        return detail

    def recovery_checklist(self) -> list[dict[str, Any]]:
        self.completion_detail()
        return ledger.recovery_checklist()

    def summary(self) -> dict[str, Any]:
        self.completion_detail()
        return ledger.summary()

    def batch_log(self, task: str | None = None) -> list[dict[str, Any]]:
        logs = ledger.receipt_log
        if task:
            key = str(task).strip().casefold()
            logs = [log for log in logs if str(log.get("task", "")).strip().casefold() == key]
        return [dict(log) for log in logs]

    def conflicts(self) -> list[dict[str, Any]]:
        return [dict(item) for item in ledger.conflicts]

    def estimate_progress(self, task: dict[str, Any], completed_batches: int) -> dict[str, Any]:
        """估算回填进度：只用于计划，不写回实际行数。"""
        task_name = str(task.get("任务名称") or task.get("task") or "")
        progress = ledger.task_progress(task_name)
        total_batches = progress.get("estimated_batches") or task.get("estimated_batches", 1) or 1
        pct = min(round(completed_batches / max(total_batches, 1) * 100, 1), 100.0)
        parallelism = task.get("parallelism", 1)
        eta_batches = max(total_batches - completed_batches, 0) / max(parallelism, 1)
        return {
            "task": task_name,
            "progress_pct": pct,
            "completed_batches": completed_batches,
            "total_batches_estimate": total_batches,
            "eta_remaining_batches": int(eta_batches),
            "actual_rows": progress.get("actual_rows", 0),
            "estimated_rows": progress.get("estimated_rows", 0),
        }

    def validate_position_range(self, source_pos: int, target_pos: int) -> dict[str, Any]:
        """校验位点范围合法性。"""
        if source_pos < 0 or target_pos < 0:
            return {"valid": False, "reason": "位点不能为负"}
        if source_pos > target_pos:
            return {"valid": False, "reason": "源位点不能大于目标位点"}
        return {"valid": True, "range_size_bytes": target_pos - source_pos}

    # ------------------------------------------------------------------
    # 动作
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        task_name = str(entry.get("任务名称", ""))
        if action == "开始回填":
            entry["status"] = "回填中"
            entry["pending"] = True
            entry["abnormal"] = False
            state = ledger.register_from_row(entry)
            state["status"] = "回填中"
            self.create_backfill_task(entry)
            pipeline_bus.write_audit(
                "回填开始", task_name,
                f"回填任务 {entry_id} 已开始，进度按任务独立计数",
                "backfill_position", "回填中",
            )
            return self.get_entry(entry_id), "回填任务已开始回填"
        if action == "中断回填":
            if not ledger.tasks.get(task_name.strip().casefold()):
                ledger.register_from_row(entry)
            record = self.interrupt(task_name)
            return self.get_entry(entry_id), (
                f"回填任务已中断，上次确认跨度 {record['last_confirmed_span'] or '无'}"
            )
        if action == "完成回填":
            self.complete(task_name)
            return self.get_entry(entry_id), "回填任务已完成"
        return entry, f"{self.ENTITY}已{action}"

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------
    def _decorate(self, row: dict[str, Any]) -> None:
        name = str(row.get("任务名称", ""))
        progress = ledger.task_progress(name)
        row["已回填行数"] = progress["actual_rows"]
        row["有效行数"] = progress["actual_rows"]
        row["已确认批号"] = progress["confirmed_batches"]
        row["已确认批次数"] = progress["confirmed_batch_count"]
        row["进度百分比"] = progress["progress_pct"]
        row["上次确认跨度"] = progress["last_confirmed_span"]
        row["续做范围"] = progress["resume_scope"]["text"]
        row["悬而未决区间"] = progress["unresolved_intervals"]
        row["中断待恢复"] = progress["interrupted"]

    def _find_row(self, task: str) -> dict[str, Any] | None:
        target = str(task or "").strip().casefold()
        for row in self.rows():
            if str(row.get("任务名称", "")).strip().casefold() == target:
                return row
        return None

    def rows(self) -> list[dict[str, Any]]:
        from app.store import store
        return store.rows(self.MODULE)

    def _parse_pos(self, pos_str: str) -> int:
        try:
            pos_str = str(pos_str).replace(",", "").replace("_", "")
            if ":" in pos_str:
                pos_str = pos_str.rsplit(":", 1)[-1]
            if "-" in pos_str:
                pos_str = pos_str.split("-", 1)[0]
            return int(pos_str)
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _parse_int(value: Any, default: int) -> int:
        try:
            parsed = int(str(value).replace(",", "").replace("_", "").strip())
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default
