"""回填与审计的领域接口：批次回执、进度、完成明细、恢复清单。

资源路由保持通用；这里只暴露跨模块消费链路需要的固定端点，
全部放在 ``/api`` 下且在动态 ``/{module}`` 路由之前注册。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.services import service_for
from app.services.audit_trail import AuditTrailService
from app.services.backfill_position import BackfillPositionService

router = APIRouter(prefix="/api", tags=["回填与审计"])


def _backfill() -> BackfillPositionService:
    service = service_for("backfill_position")
    assert isinstance(service, BackfillPositionService)
    return service


def _audit() -> AuditTrailService:
    service = service_for("audit_trail")
    assert isinstance(service, AuditTrailService)
    return service


@router.post("/backfill_position/receipts")
def submit_backfill_receipts(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """同一批次执行入口：单条回执、批量回执或多作业组整组提交。"""
    return _backfill().submit_batch_receipt(payload)


@router.post("/backfill_position/groups")
def submit_backfill_groups(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """多选作业组后整组提交。"""
    groups = payload.get("groups")
    if not isinstance(groups, list):
        raise HTTPException(status_code=400, detail="groups 必须是数组")
    return _backfill().submit_groups(groups, default_group_id=payload.get("group_id"))


@router.post("/backfill_position/batches")
def execute_backfill_batch(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """执行单批：任务 + 批号 + 实际行数（可选日志跨度）。"""
    task = payload.get("task") or payload.get("任务名称")
    batch_no = payload.get("batch_no") or payload.get("批号")
    if not task or batch_no is None:
        raise HTTPException(status_code=400, detail="需要 task 与 batch_no")
    return _backfill().execute_batch(
        dict(payload), int(batch_no),
        arrival=str(payload.get("arrival") or "normal"),
        group_id=payload.get("group_id"),
        payload=payload,
    )


@router.post("/backfill_position/interrupts")
def interrupt_backfill(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """中断请求：记录中断轮次和上次确认跨度。"""
    task = payload.get("task") or payload.get("任务名称")
    if not task:
        raise HTTPException(status_code=400, detail="需要 task")
    batch_no = payload.get("batch_no") or payload.get("批号")
    absorb_raw = payload.get("absorb_late")
    absorb_late = True
    if absorb_raw is not None:
        absorb_late = (
            absorb_raw if isinstance(absorb_raw, bool)
            else str(absorb_raw).strip().lower() in {"1", "true", "yes", "吸收", "是"}
        )
    return _backfill().interrupt(
        str(task),
        int(batch_no) if batch_no is not None else None,
        str(payload.get("reason") or ""),
        absorb_late=absorb_late,
    )


@router.get("/backfill_position-tasks")
def backfill_known_tasks() -> dict[str, Any]:
    """回填任务名清单，供工作台勾选作业组使用。"""
    tasks = _backfill().known_tasks()
    return {"items": tasks, "total": len(tasks)}


@router.get("/backfill_position-progress")
def backfill_progress(task: str | None = Query(default=None)) -> Any:
    """按任务查询进度；不带任务返回全部任务，互不累加。"""
    return _backfill().progress(task)


@router.get("/backfill-position/summary")
def backfill_summary() -> dict[str, Any]:
    """有效行数、悬而未决区间、实际续做范围。"""
    return _backfill().summary()


@router.get("/backfill-position/completion-detail")
def backfill_completion_detail(task: str | None = Query(default=None)) -> dict[str, Any]:
    """回填完成明细数据集：一行一个已确认批次。"""
    detail = _backfill().completion_detail(task)
    return {"items": detail, "total": len(detail)}


@router.get("/backfill-position/batch-log")
def backfill_batch_log(task: str | None = Query(default=None)) -> dict[str, Any]:
    """回执处理日志：accepted / duplicate / duplicate_conflict / rejected。"""
    items = _backfill().batch_log(task)
    return {"items": items, "total": len(items), "conflicts": _backfill().conflicts()}


@router.get("/audit-trail/recovery-checklist")
def audit_recovery_checklist() -> dict[str, Any]:
    """审计恢复清单：消费回填完成明细后回写。"""
    rows = _audit().recovery_checklist()
    return {
        "items": rows,
        "total": len(rows),
        "reconcile": _audit().reconcile_backfill_checklists(),
        "unresolved": _audit().unresolved_coordinates(),
    }


@router.post("/audit-trail/recovery-checklist/consume")
def audit_consume_completion(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """显式触发审计消费回填完成明细。"""
    detail = payload.get("detail")
    if detail is not None and not isinstance(detail, list):
        raise HTTPException(status_code=400, detail="detail 必须是数组")
    return _audit().consume_backfill_completion(detail)
