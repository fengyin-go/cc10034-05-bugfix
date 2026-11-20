"""回填批次台账：以 (任务, 批号) 为唯一键登记实际回填行与日志跨度。

设计要点：
- 进度只按任务汇总已确认批次的 ``rows_actual``，不共享全局计数器；
- 同一 (任务, 批号) 重复送达只记一次，回执差异单独记为冲突；
- 预估记录数只作为进度分母，绝不写成已回填值；
- 每个回执携带 Binlog 文件与起止位点，中断后可以从已确认跨度继续；
- 中断轮与晚到回执相遇时，采用“已确认交付优先”策略：晚到回执
  只吸收一次并推进续做范围，未确认的批次仍然落在悬而未决区间。
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from app.pipeline import pipeline_bus

POSITION_RE = re.compile(
    r"^(?:(?P<file>[A-Za-z0-9._\-/]+):)?(?P<start>\d+)(?:\s*[-~至]\s*(?P<end>\d+))?$"
)


def _first(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None and payload[key] != "":
            return payload[key]
    return default


def _strict_int(value: Any, default: int) -> int:
    """只接受纯数字文本；种子数据里的占位文本按默认值处理。"""
    if isinstance(value, bool) or value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().replace(",", "").replace("_", "")
    return int(text) if text.lstrip("-").isdigit() else default


def _strict_int_or_none(value: Any) -> int | None:
    """严格解析整数；占位文本返回 None，避免把样例名称当成批号/行数。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().replace(",", "").replace("_", "")
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_position(value: Any) -> dict[str, Any]:
    """解析 ``mysql-bin.000001:4096-8192`` / ``4096`` / ``:4096`` 形式位点。"""
    text = str(value or "").strip()
    match = POSITION_RE.match(text)
    if not match:
        return {"binlog_file": "", "start_pos": 0, "end_pos": 0, "valid": False}
    start = int(match.group("start"))
    end = int(match.group("end") or start)
    return {
        "binlog_file": match.group("file") or "",
        "start_pos": start,
        "end_pos": end,
        "valid": True,
    }


def _task_key(name: str) -> str:
    return str(name or "").strip().casefold()


def _now() -> str:
    return datetime.now().isoformat()


class BackfillLedger:
    """内存回填台账；和 store 一样由进程内单例持有。"""

    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.receipts: dict[tuple[str, int], dict[str, Any]] = {}
        self.receipt_log: list[dict[str, Any]] = []
        self.interrupts: list[dict[str, Any]] = []
        self.held_receipts: list[dict[str, Any]] = []
        self.conflicts: list[dict[str, Any]] = []
        self.groups: dict[str, dict[str, Any]] = {}
        self._receipt_seq = 0

    # ------------------------------------------------------------------
    # 任务登记
    # ------------------------------------------------------------------
    def register_task(
        self,
        task: str,
        *,
        source_pos: int | None = None,
        target_pos: int | None = None,
        binlog_file: str = "",
        batch_size: int | None = None,
        parallelism: int | None = None,
        estimated_rows: int | None = None,
        scan_range: str = "",
        status: str = "回填中",
    ) -> dict[str, Any]:
        """登记或补齐任务元数据，不覆盖已经确认的进度。"""
        key = _task_key(task)
        display = str(task or "").strip()
        if not key:
            raise ValueError("任务名称不能为空")
        state = self.tasks.get(key)
        if state is None:
            state = {
                "task": display,
                "key": key,
                "source_pos": 0,
                "source_binlog": "",
                "target_pos": 0,
                "target_binlog": "",
                "binlog_file": binlog_file or "",
                "batch_size": 1000,
                "parallelism": 1,
                "estimated_rows": 0,
                "estimated_batches": 0,
                "scan_range": scan_range,
                "status": status,
                "interrupted": False,
                "interrupt_batch_no": None,
                "interrupt_at": None,
                "interrupt_resolved_by_receipt": False,
                "created_at": _now(),
            }
            self.tasks[key] = state

        if source_pos is not None:
            state["source_pos"] = int(source_pos)
        if target_pos is not None:
            state["target_pos"] = int(target_pos)
        if binlog_file:
            state["binlog_file"] = binlog_file
        if batch_size is not None and int(batch_size) > 0:
            state["batch_size"] = int(batch_size)
        if parallelism is not None and int(parallelism) > 0:
            state["parallelism"] = int(parallelism)
        if estimated_rows is not None:
            state["estimated_rows"] = max(int(estimated_rows), 0)
        if scan_range:
            state["scan_range"] = scan_range
        self._recompute_estimates(state)
        return state

    def register_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """从回填任务列表行（中文列名）登记任务。"""
        name = str(_first(row, "任务名称", "task", "task_name", default="") or "")
        source = parse_position(_first(row, "源位点", "source_pos", default=""))
        target = parse_position(_first(row, "目标位点", "target_pos", default=""))
        scan = str(_first(row, "扫描范围", "scan_range", default="") or "")
        if not source["valid"] and scan:
            source = parse_position(scan)
        if not target["valid"] and scan:
            parts = scan.split("-")
            if len(parts) >= 2:
                tail = parse_position(parts[-1])
                target = {
                    "binlog_file": tail["binlog_file"] or source["binlog_file"],
                    "start_pos": tail["start_pos"],
                    "end_pos": tail["end_pos"],
                    "valid": True,
                }
        estimated = _strict_int(_first(row, "预估记录数", "estimated_rows"), 0)
        file = source["binlog_file"] or target["binlog_file"]
        state = self.register_task(
            name,
            source_pos=source["start_pos"] if source["valid"] else None,
            target_pos=target["end_pos"] if target["valid"] else None,
            binlog_file=file,
            batch_size=_strict_int_or_none(_first(row, "批次大小", "batch_size")),
            parallelism=_strict_int_or_none(_first(row, "并发度", "parallelism")),
            estimated_rows=estimated,
            scan_range=scan,
            status=str(row.get("status") or "回填中"),
        )
        state["source_binlog"] = source["binlog_file"] or file
        state["target_binlog"] = target["binlog_file"] or file
        if state["target_pos"] < state["source_pos"]:
            state["target_pos"] = state["source_pos"]
        self._recompute_estimates(state)
        return state

    def ensure_task(self, task: str) -> dict[str, Any]:
        key = _task_key(task)
        state = self.tasks.get(key)
        if state is None:
            state = self.register_task(task)
        return state

    @staticmethod
    def _recompute_estimates(state: dict[str, Any]) -> None:
        source, target = state["source_pos"], state["target_pos"]
        if state.get("estimated_rows", 0) <= 0 and target > source:
            state["estimated_rows"] = max((target - source) // 256, 0)
        batch_size = max(int(state.get("batch_size") or 1000), 1)
        estimated_rows = max(int(state.get("estimated_rows") or 0), 0)
        state["estimated_batches"] = (
            (estimated_rows + batch_size - 1) // batch_size if estimated_rows else 0
        )

    # ------------------------------------------------------------------
    # 回执登记
    # ------------------------------------------------------------------
    def submit_receipt(
        self,
        payload: dict[str, Any],
        *,
        arrival: str | None = None,
        group_id: str | None = None,
        absorb_late: bool | None = None,
    ) -> dict[str, Any]:
        """登记一份回填批次回执，按 (任务, 批号) 幂等去重。

        返回 ``outcome``：``accepted`` / ``duplicate`` / ``duplicate_conflict`` /
        ``held_for_review`` / ``rejected``。只有 ``accepted`` 会增加有效行数。

        中断轮与晚到回执同时出现时 ``absorb_late`` 决定取舍：
        - ``True``（默认）：已确认交付优先，晚到回执吸收一次并推进续做范围；
        - ``False``：挂起待复核，不计入进度、不推进续做范围；
        - ``None``：采用任务上登记的 ``late_policy``（默认吸收）。
        """
        task_name = str(_first(payload, "task", "任务名称", "task_name", "任务", default="") or "").strip()
        batch_raw = _first(payload, "batch_no", "批号", "batch", "批次号")
        batch_no = _strict_int_or_none(batch_raw)
        arrival = self._normalize_arrival(
            arrival
            or str(_first(payload, "arrival", "到达方式", "回执类型", default="normal") or "normal")
        )

        if not task_name:
            return self._reject("", batch_no, "缺少任务名称，无法确定回执归属", payload, arrival, group_id)
        if batch_no is None or batch_no < 1:
            return self._reject(task_name, batch_no, "缺少有效批号（批号从 1 开始）", payload, arrival, group_id)

        key = (_task_key(task_name), batch_no)
        existing = self.receipts.get(key)

        rows_raw = _first(
            payload,
            "rows",
            "rows_actual",
            "row_count",
            "实际行数",
            "回填行数",
            "有效行数",
            "完成行数",
        )
        if rows_raw is None and existing is not None:
            # 只带任务与批号的重放：按已确认回执比对，不当作缺行数的新回执
            rows_raw = existing["rows_actual"]
        if rows_raw is None:
            if _first(payload, "预估记录数", "estimated_rows") is not None:
                return self._reject(
                    task_name,
                    batch_no,
                    "回执只有预估记录数，预估行数不能写成已回填值",
                    payload,
                    arrival,
                    group_id,
                    reason_code="estimated_rows_not_actual",
                )
            return self._reject(
                task_name, batch_no, "缺少实际回填行数", payload, arrival, group_id,
                reason_code="missing_actual_rows",
            )
        rows = _strict_int_or_none(rows_raw)
        if rows is None or rows < 0:
            return self._reject(task_name, batch_no, "实际回填行数必须是非负整数", payload, arrival, group_id)

        state = (
            self.register_from_row({**payload, "任务名称": task_name})
            if _first(payload, "源位点", "目标位点")
            else self.ensure_task(task_name)
        )
        span = self._resolve_span(state, payload, batch_no)
        if span["end_pos"] < span["start_pos"]:
            return self._reject(task_name, batch_no, "日志跨度结束位点不能小于起始位点", payload, arrival, group_id)
        if span["located"] and span["binlog_file"] and not state.get("binlog_file"):
            state["binlog_file"] = span["binlog_file"]
            if not state.get("source_binlog"):
                state["source_binlog"] = span["binlog_file"]
            if not state.get("target_binlog"):
                state["target_binlog"] = span["binlog_file"]

        if existing is not None:
            self._receipt_seq += 1
            if not span["located"] and existing.get("located", True):
                # 重放/重复回执常常只带任务与批号，沿用已确认跨度比对
                span = {
                    "binlog_file": existing["binlog_file"],
                    "start_pos": existing["start_pos"],
                    "end_pos": existing["end_pos"],
                    "located": True,
                    "text": existing["span"],
                }
            same_rows = int(existing["rows_actual"]) == rows
            same_span = (
                existing["start_pos"] == span["start_pos"]
                and existing["end_pos"] == span["end_pos"]
                and existing["binlog_file"] == span["binlog_file"]
            )
            outcome = "duplicate" if same_rows and same_span else "duplicate_conflict"
            log_entry = {
                "seq": self._receipt_seq,
                "task": state["task"],
                "batch_no": batch_no,
                "rows_actual": rows,
                "outcome": outcome,
                "reason": "同一任务同一批号已确认，重复回执不计入有效行数",
                "arrival": arrival,
                "group_id": group_id,
                "received_at": _now(),
                "existing_receipt_id": existing["receipt_id"],
            }
            self.receipt_log.append(log_entry)
            if outcome == "duplicate_conflict":
                self.conflicts.append({
                    **log_entry,
                    "existing_rows": existing["rows_actual"],
                    "incoming_rows": rows,
                    "existing_span": existing["span"],
                    "incoming_span": span["text"],
                })
            pipeline_bus.write_audit(
                "回填回执去重",
                state["task"],
                (
                    f"批号 {batch_no} 重复回执，有效行数保持 {existing['rows_actual']}，"
                    f"本次 {rows} 行不计入"
                ),
                "backfill_position",
                "已忽略",
                remark=f"receipt={existing['receipt_id']}",
            )
            return {
                "ok": True,
                "accepted": False,
                "outcome": outcome,
                "duplicate": True,
                "conflict": outcome == "duplicate_conflict",
                "receipt": dict(existing),
                "message": "重复的回填批次回执已按任务和批号去重，未重复计数",
                "progress": self.task_progress(state["task"]),
                "effective_rows": existing["rows_actual"],
            }

        self._receipt_seq += 1
        conflict_with_interrupt = bool(
            state.get("interrupted")
            and state.get("interrupt_batch_no") == batch_no
        )
        if conflict_with_interrupt:
            policy = absorb_late
            if policy is None:
                raw_policy = _first(payload, "absorb_late", "absorb", "是否吸收", "late_policy")
                if raw_policy is not None:
                    policy = str(raw_policy).strip().lower() in {"1", "true", "yes", "吸收", "是"}
                else:
                    policy = bool(state.get("late_policy_absorb", True))
            if not policy:
                return self._hold_for_review(
                    state, payload, batch_no, rows, span, arrival, group_id,
                )
            absorbed_after_interrupt = True
        else:
            absorbed_after_interrupt = False
        receipt = {
            "receipt_id": f"bfr_{self._receipt_seq:06d}",
            "task": state["task"],
            "task_key": state["key"],
            "batch_no": batch_no,
            "rows_actual": rows,
            "binlog_file": span["binlog_file"] or state.get("binlog_file", ""),
            "start_pos": span["start_pos"],
            "end_pos": span["end_pos"],
            "span": span["text"],
            "located": span["located"],
            "arrival": arrival,
            "group_id": group_id,
            "status": "已确认",
            "source": str(_first(payload, "source", "来源", default="batch_entry") or "batch_entry"),
            "absorbed_after_interrupt": absorbed_after_interrupt,
            "confirmed_at": _now(),
            "submitted_payload_keys": sorted(payload),
        }
        self.receipts[key] = receipt
        if absorbed_after_interrupt:
            state["interrupted"] = False
            state["interrupt_resolved_by_receipt"] = True
            state["interrupt_resolved_at"] = receipt["confirmed_at"]

        log_entry = {
            "seq": self._receipt_seq,
            "task": state["task"],
            "batch_no": batch_no,
            "rows_actual": rows,
            "outcome": "accepted",
            "reason": "新增确认批次",
            "arrival": arrival,
            "group_id": group_id,
            "received_at": receipt["confirmed_at"],
            "receipt_id": receipt["receipt_id"],
            "absorbed_after_interrupt": absorbed_after_interrupt,
        }
        self.receipt_log.append(log_entry)

        pipeline_bus.pipeline_metrics["backfill_rows"] = self.total_actual_rows()
        pipeline_bus.pipeline_metrics.setdefault("backfill_rows_by_task", {})
        pipeline_bus.pipeline_metrics["backfill_rows_by_task"][state["task"]] = (
            self.task_progress(state["task"])["actual_rows"]
        )

        progress = self.task_progress(state["task"])
        summary = (
            f"{state['task']} 批号 {batch_no} 确认 {rows} 行，"
            f"日志跨度 {receipt['span']}，任务累计有效 {progress['actual_rows']} 行"
        )
        if absorbed_after_interrupt:
            summary += "；中断轮吸收晚到回执，续做范围随之推进"
        pipeline_bus.write_audit(
            "回填批次确认",
            state["task"],
            summary,
            "backfill_position",
            "已确认",
            remark=f"receipt={receipt['receipt_id']}",
        )
        return {
            "ok": True,
            "accepted": True,
            "outcome": "accepted",
            "duplicate": False,
            "conflict": False,
            "late_absorbed": absorbed_after_interrupt,
            "receipt": dict(receipt),
            "message": "回填批次回执已按任务和批号登记",
            "progress": progress,
        }

    def _hold_for_review(
        self,
        state: dict[str, Any],
        payload: dict[str, Any],
        batch_no: int,
        rows: int,
        span: dict[str, Any],
        arrival: str,
        group_id: str | None,
    ) -> dict[str, Any]:
        """中断轮不吸收晚到回执：挂起待复核，不改变已发布进度。"""
        log_entry = {
            "seq": self._receipt_seq,
            "task": state["task"],
            "batch_no": batch_no,
            "rows_actual": rows,
            "outcome": "held_for_review",
            "reason": "中断轮选择不吸收晚到回执，等待值班者复核续做范围",
            "arrival": arrival,
            "group_id": group_id,
            "received_at": _now(),
            "span": span["text"],
            "held_rows": rows,
        }
        self.receipt_log.append(log_entry)
        self.held_receipts.append({**log_entry, "payload": dict(payload)})
        pipeline_bus.write_audit(
            "回填回执挂起",
            state["task"],
            (
                f"批号 {batch_no} 晚到回执 {rows} 行在中断轮被挂起，未计入进度，"
                f"续做范围保持不动"
            ),
            "backfill_position",
            "待复核",
            remark=f"span={span['text']}",
        )
        progress = self.task_progress(state["task"])
        return {
            "ok": False,
            "accepted": False,
            "outcome": "held_for_review",
            "duplicate": False,
            "conflict": False,
            "late_absorbed": False,
            "held": True,
            "receipt": None,
            "message": "中断轮选择不吸收该晚到回执，已挂起待复核，已发布进度保持不变",
            "progress": progress,
        }

    def _reject(
        self,
        task: str,
        batch_no: int | None,
        reason: str,
        payload: dict[str, Any],
        arrival: str,
        group_id: str | None,
        *,
        reason_code: str = "invalid_receipt",
    ) -> dict[str, Any]:
        self._receipt_seq += 1
        display = str(task or "").strip()
        log_entry = {
            "seq": self._receipt_seq,
            "task": display,
            "batch_no": batch_no,
            "rows_actual": None,
            "outcome": "rejected",
            "reason": reason,
            "reason_code": reason_code,
            "arrival": arrival,
            "group_id": group_id,
            "received_at": _now(),
            "payload_keys": sorted(payload),
        }
        self.receipt_log.append(log_entry)
        pipeline_bus.write_audit(
            "回填回执拒绝",
            display or "未命名任务",
            f"批号 {batch_no if batch_no is not None else '—'} 回执被拒绝：{reason}",
            "backfill_position",
            "异常标记" if reason_code == "estimated_rows_not_actual" else "已拒绝",
        )
        return {
            "ok": False,
            "accepted": False,
            "outcome": "rejected",
            "duplicate": False,
            "conflict": False,
            "message": reason,
            "reason_code": reason_code,
            "receipt": None,
            "progress": self.task_progress(display) if display else None,
        }

    @staticmethod
    def _normalize_arrival(value: str) -> str:
        text = str(value or "normal").strip().lower()
        if text in {"late", "补达", "晚到", "停机补达", "延迟", "delayed"}:
            return "late"
        if text in {"replay", "重放", "逐条重放", "resend"}:
            return "replay"
        if text in {"group", "整组", "批量"}:
            return "group"
        return "normal"

    def _resolve_span(self, state: dict[str, Any], payload: dict[str, Any], batch_no: int) -> dict[str, Any]:
        """解析回执日志跨度；缺少坐标时标记未定位，不虚构位点。"""
        file = str(_first(payload, "binlog_file", "Binlog文件", "日志文件", "file", default="") or "")
        start_raw = _first(payload, "start_pos", "起始位点", "from_pos", "start")
        end_raw = _first(payload, "end_pos", "结束位点", "to_pos", "end")
        span_raw = _first(payload, "span", "日志跨度", "binlog_span")

        start = _strict_int_or_none(start_raw)
        end = _strict_int_or_none(end_raw)
        if span_raw:
            parsed = parse_position(span_raw)
            if parsed["valid"]:
                file = file or parsed["binlog_file"]
                start = start if start is not None else parsed["start_pos"]
                end = end if end is not None else parsed["end_pos"]
        has_explicit = start is not None and end is not None

        if not has_explicit and int(state.get("target_pos") or 0) > int(state.get("source_pos") or 0):
            derived = self.batch_span(state, batch_no)
            start = derived["start_pos"] if start is None else start
            end = derived["end_pos"] if end is None else end
            file = file or derived["binlog_file"]
        elif not has_explicit and (start is None or end is None):
            # 任务没有位点范围：不推导，标记为未定位
            return {
                "binlog_file": file,
                "start_pos": 0,
                "end_pos": 0,
                "located": False,
                "text": "",
            }

        file = file or state.get("binlog_file") or ""
        located = bool(file) and int(end) >= int(start) and (int(start) or int(end))
        return {
            "binlog_file": file,
            "start_pos": int(start),
            "end_pos": int(end),
            "located": located,
            "text": f"{file}:{int(start)}-{int(end)}" if located else "",
        }

    def batch_span(self, state: dict[str, Any], batch_no: int) -> dict[str, Any]:
        """为没有显式回执跨度的任务推导批号对应区间（仅用于定位续做范围）。"""
        source, target = int(state["source_pos"]), int(state["target_pos"])
        batches = max(int(state.get("estimated_batches") or 0), 1)
        if target > source:
            span_size = max((target - source) // batches, 1)
            start = source + (batch_no - 1) * span_size
            if start >= target:
                start = max(target - span_size, source)
            end = min(start + span_size, target)
        else:
            span_size = max(int(state.get("batch_size") or 1000) * 256, 1)
            start = source + (batch_no - 1) * span_size
            end = start + span_size
        return {
            "binlog_file": state.get("binlog_file") or "mysql-bin.000001",
            "start_pos": start,
            "end_pos": end,
            "text": f"{state.get('binlog_file') or 'mysql-bin.000001'}:{start}-{end}",
        }

    # ------------------------------------------------------------------
    # 中断与恢复
    # ------------------------------------------------------------------
    def interrupt(
        self,
        task: str,
        batch_no: int | None = None,
        reason: str = "",
        absorb_late: bool = True,
    ) -> dict[str, Any]:
        state = self.ensure_task(task)
        state["late_policy_absorb"] = bool(absorb_late)
        if batch_no is None:
            confirmed = [
                r["batch_no"] for r in self.receipts.values()
                if r["task_key"] == state["key"]
            ]
            batch_no = (max(confirmed) + 1) if confirmed else 1
        record = {
            "task": state["task"],
            "batch_no": int(batch_no),
            "reason": reason or "收到中断请求",
            "interrupted_at": _now(),
            "late_policy_absorb": bool(absorb_late),
            "last_confirmed_span": self._last_confirmed_span(state),
            "resume_scope": self.task_progress(state["task"])["resume_scope"],
        }
        state["interrupted"] = True
        state["interrupt_batch_no"] = int(batch_no)
        state["interrupt_at"] = record["interrupted_at"]
        state["interrupt_resolved_by_receipt"] = False
        state["status"] = "已中断"
        self.interrupts.append(record)
        pipeline_bus.write_audit(
            "回填中断",
            state["task"],
            (
                f"批号 {batch_no} 中断，上次确认跨度 {record['last_confirmed_span'] or '无'}，"
                f"续做范围 {record['resume_scope']['text']}"
            ),
            "backfill_position",
            "已中断",
        )
        return {**record, "progress": self.task_progress(state["task"])}

    def task_progress(self, task: str) -> dict[str, Any]:
        key = _task_key(task)
        state = self.tasks.get(key)
        if state is None:
            return {
                "task": str(task or "").strip(),
                "status": "",
                "confirmed_batches": [],
                "confirmed_batch_count": 0,
                "actual_rows": 0,
                "valid_rows": 0,
                "estimated_rows": 0,
                "estimated_batches": 0,
                "progress_pct": 0.0,
                "last_confirmed_span": "",
                "last_confirmed_receipt": None,
                "resume_scope": {"binlog_file": "", "from_pos": 0, "to_pos": 0, "text": ""},
                "unresolved_intervals": [],
                "interrupted": False,
                "interrupt_batch_no": None,
                "interrupt_resolved_by_receipt": False,
            }
        confirmed = sorted(
            (r for r in self.receipts.values() if r["task_key"] == key),
            key=lambda r: (r["batch_no"], r["start_pos"]),
        )
        actual = sum(int(r["rows_actual"]) for r in confirmed)
        estimated = int(state.get("estimated_rows") or 0)
        pct = round(min(actual / estimated * 100, 100.0), 1) if estimated > 0 else 0.0
        intervals = self._merged_intervals(state, confirmed)
        unresolved = self._unresolved_intervals(state, intervals)
        located = [r for r in confirmed if r.get("located", True)]
        file = (located[-1]["binlog_file"] if located else "") or state.get("binlog_file") or ""
        target = int(state.get("target_pos") or 0)
        if unresolved:
            resume = unresolved[0]
        elif confirmed:
            last_end = max(
                (int(r["end_pos"]) for r in located if r["binlog_file"] in {"", file}),
                default=int(state.get("source_pos") or 0),
            )
            from_pos = last_end + 1
            to_pos = target if target > from_pos else from_pos
            resume = {
                "binlog_file": file,
                "from_pos": from_pos,
                "to_pos": to_pos,
            }
        else:
            resume = {
                "binlog_file": file,
                "from_pos": int(state.get("source_pos") or 0),
                "to_pos": target,
            }
        return {
            "task": state["task"],
            "status": state.get("status", ""),
            "confirmed_batches": [r["batch_no"] for r in confirmed],
            "confirmed_batch_count": len(confirmed),
            "actual_rows": actual,
            "valid_rows": actual,
            "estimated_rows": estimated,
            "estimated_batches": state.get("estimated_batches", 0),
            "progress_pct": pct,
            "last_confirmed_span": self._last_confirmed_span(state),
            "last_confirmed_receipt": (
                {k: confirmed[-1][k] for k in ("receipt_id", "batch_no", "rows_actual", "span", "confirmed_at")}
                if confirmed else None
            ),
            "resume_scope": {
                "binlog_file": resume.get("binlog_file", ""),
                "from_pos": resume.get("from_pos", 0),
                "to_pos": resume.get("to_pos", 0),
                "text": self._scope_text(resume),
            },
            "unresolved_intervals": [
                {**interval, "text": interval["span"]} for interval in unresolved
            ],
            "interrupted": bool(state.get("interrupted")),
            "interrupt_batch_no": state.get("interrupt_batch_no"),
            "interrupt_resolved_by_receipt": bool(state.get("interrupt_resolved_by_receipt")),
        }

    def _last_confirmed_span(self, state: dict[str, Any]) -> str:
        confirmed = [
            r for r in self.receipts.values() if r["task_key"] == state["key"]
        ]
        located = [r for r in confirmed if r.get("located", True)]
        if not located:
            return ""
        latest = max(located, key=lambda r: (r["batch_no"], r["end_pos"]))
        return latest["span"]

    def _merged_intervals(self, state: dict[str, Any], confirmed: list[dict[str, Any]]) -> list[dict[str, Any]]:
        intervals = sorted(
            (
                {
                    "binlog_file": r["binlog_file"] or state.get("binlog_file", ""),
                    "start_pos": int(r["start_pos"]),
                    "end_pos": int(r["end_pos"]),
                }
                for r in confirmed
                if r.get("located", True)
            ),
            key=lambda item: (item["binlog_file"], item["start_pos"]),
        )
        merged: list[dict[str, Any]] = []
        for item in intervals:
            if (
                merged
                and merged[-1]["binlog_file"] == item["binlog_file"]
                and item["start_pos"] <= merged[-1]["end_pos"] + 1
            ):
                merged[-1]["end_pos"] = max(merged[-1]["end_pos"], item["end_pos"])
            else:
                merged.append(dict(item))
        return merged

    def _unresolved_intervals(self, state: dict[str, Any], intervals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        source, target = int(state["source_pos"]), int(state["target_pos"])
        file = state.get("binlog_file") or ""
        if target <= source:
            return []
        cursor = source
        result: list[dict[str, Any]] = []
        for item in intervals:
            if item["binlog_file"] not in {"", file}:
                continue
            if item["start_pos"] > cursor:
                result.append(self._interval(file, cursor, item["start_pos"] - 1, "未确认"))
            cursor = max(cursor, item["end_pos"] + 1)
        if cursor < target:
            result.append(self._interval(file, cursor, target, "未确认"))
        return result

    @staticmethod
    def _scope_text(scope: dict[str, Any]) -> str:
        file = scope.get("binlog_file") or ""
        start = int(scope.get("from_pos") or 0)
        end = int(scope.get("to_pos") or 0)
        if not file and not start and not end:
            return ""
        if not file:
            return f"{start}-{end}"
        return f"{file}:{start}-{end}"

    @staticmethod
    def _interval(file: str, start: int, end: int, reason: str) -> dict[str, Any]:
        return {
            "binlog_file": file,
            "from_pos": start,
            "to_pos": end,
            "span": f"{file}:{start}-{end}" if file else f"{start}-{end}",
            "reason": reason,
        }

    # ------------------------------------------------------------------
    # 数据集 / 汇总
    # ------------------------------------------------------------------
    def total_actual_rows(self) -> int:
        return sum(int(r["rows_actual"]) for r in self.receipts.values())

    def completion_detail(self, task: str | None = None) -> list[dict[str, Any]]:
        """回填完成明细数据集：一行代表一个已确认的 (任务, 批号)。"""
        key = _task_key(task) if task else None
        rows = sorted(
            (
                r for r in self.receipts.values()
                if key is None or r["task_key"] == key
            ),
            key=lambda r: (r["task"], r["batch_no"]),
        )
        detail = []
        for index, receipt in enumerate(rows, start=1):
            detail.append({
                "明细行号": index,
                "task": receipt["task"],
                "任务名称": receipt["task"],
                "batch_no": receipt["batch_no"],
                "批号": receipt["batch_no"],
                "rows_actual": receipt["rows_actual"],
                "有效行数": receipt["rows_actual"],
                "回填行数": receipt["rows_actual"],
                "binlog_file": receipt["binlog_file"],
                "start_pos": receipt["start_pos"],
                "end_pos": receipt["end_pos"],
                "日志跨度": receipt["span"],
                "坐标已定位": bool(receipt.get("located", True)),
                "status": receipt["status"],
                "结果状态": receipt["status"],
                "arrival": receipt["arrival"],
                "group_id": receipt["group_id"],
                "absorbed_after_interrupt": receipt["absorbed_after_interrupt"],
                "confirmed_at": receipt["confirmed_at"],
                "receipt_id": receipt["receipt_id"],
            })
        return detail

    def completion_keys(self) -> list[dict[str, Any]]:
        return [
            {"task": r["task"], "batch_no": r["batch_no"]}
            for r in self.completion_detail()
        ]

    def recovery_checklist(self) -> list[dict[str, Any]]:
        """恢复清单按已确认批号一行一条，与审计回填清单共用同一键。"""
        audit_entries = pipeline_bus.backfill_recovery_checklist
        rows: list[dict[str, Any]] = []
        for detail in self.completion_detail():
            key = (_task_key(detail["task"]), int(detail["batch_no"]))
            audited = audit_entries.get(key, {})
            state = self.tasks.get(_task_key(detail["task"]), {})
            rows.append({
                "任务名称": detail["task"],
                "已确认批号": detail["batch_no"],
                "有效行数": detail["有效行数"],
                "日志跨度": detail["日志跨度"],
                "坐标已定位": detail["坐标已定位"],
                "确认时间": detail["confirmed_at"],
                "审计记录ID": audited.get("审计记录ID"),
                "审计状态": audited.get("状态", "待入账"),
                "续做起点": detail["end_pos"],
                "扫描目标": state.get("target_pos", 0),
                "结果状态": "已确认",
            })
        return rows

    def summary(self) -> dict[str, Any]:
        progress = [self.task_progress(state["task"]) for state in self.tasks.values()]
        unresolved: list[dict[str, Any]] = []
        for item in progress:
            for interval in item["unresolved_intervals"]:
                unresolved.append({"task": item["task"], **interval})
        resume_scopes = [
            {"task": item["task"], **item["resume_scope"]}
            for item in progress
        ]
        duplicates = [log for log in self.receipt_log if log["outcome"].startswith("duplicate")]
        rejected = [log for log in self.receipt_log if log["outcome"] == "rejected"]
        held = [log for log in self.receipt_log if log["outcome"] == "held_for_review"]
        detail = self.completion_detail()
        valid_rows = sum(item["rows_actual"] for item in detail)
        by_task: dict[str, int] = {}
        for item in detail:
            by_task[item["task"]] = by_task.get(item["task"], 0) + item["rows_actual"]
        return {
            "valid_rows": valid_rows,
            "有效行数": valid_rows,
            "valid_rows_by_task": by_task,
            "completion_row_count": len(detail),
            "confirmed_batches_by_task": {
                item["task"]: item["confirmed_batches"] for item in progress
            },
            "unresolved_intervals": unresolved,
            "悬而未决的Binlog区间": unresolved,
            "resume_scopes": resume_scopes,
            "实际续做范围": resume_scopes,
            "last_confirmed_spans": {
                item["task"]: item["last_confirmed_span"] for item in progress
            },
            "duplicate_submissions": len(duplicates),
            "duplicate_rows_ignored": sum(
                int(log.get("rows_actual") or 0) for log in duplicates
            ),
            "conflicts": len(self.conflicts),
            "late_receipts_absorbed": sum(
                1 for r in self.receipts.values() if r["absorbed_after_interrupt"]
            ),
            "late_receipts_held": len(held),
            "held_rows": sum(int(log.get("held_rows") or 0) for log in held),
            "interrupt_rounds": len(self.interrupts),
            "interrupt_decisions": [
                {
                    "task": item["task"],
                    "batch_no": item["batch_no"],
                    "absorb_late": item["late_policy_absorb"],
                    "last_confirmed_span": item["last_confirmed_span"],
                    "resume_scope_at_interrupt": item["resume_scope"]["text"],
                }
                for item in self.interrupts
            ],
            "rejected_receipts": len(rejected),
            "estimated_rows_not_counted": sum(
                int(log.get("rows_actual") or 0) for log in rejected
                if log.get("reason_code") == "estimated_rows_not_actual"
            ),
            "late_receipt_policy": "中断轮按 absorb_late 取舍：吸收则推进续做范围，不吸收则挂起待复核且不改已发布进度",
            "progress": progress,
            "generated_at": _now(),
        }

    def reset(self) -> None:
        """测试用：清空台账。"""
        self.__init__()


ledger = BackfillLedger()
