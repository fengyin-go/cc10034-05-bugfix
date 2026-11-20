"""回填进度与审计恢复清单的领域回归测试（标准库 unittest，无额外依赖）。

运行：cd backend && python -m unittest discover -s tests -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.backfill_ledger import ledger  # noqa: E402
from app.pipeline import pipeline_bus  # noqa: E402
from app.services import service_for  # noqa: E402
from app.services.audit_trail import AuditTrailService  # noqa: E402
from app.services.backfill_position import BackfillPositionService  # noqa: E402

FILE = "mysql-bin.000100"


class BackfillProgressTest(unittest.TestCase):
    def setUp(self) -> None:
        ledger.reset()
        pipeline_bus.backfill_recovery_checklist.clear()
        pipeline_bus.backfill_completion_detail.clear()
        pipeline_bus.pipeline_metrics["backfill_rows"] = 0
        pipeline_bus.pipeline_metrics["backfill_rows_by_task"] = {}
        self.bf: BackfillPositionService = service_for("backfill_position")  # type: ignore[assignment]
        self.au: AuditTrailService = service_for("audit_trail")  # type: ignore[assignment]

    def receipt(self, task: str, batch_no: int, rows: int,
                start: int | None = None, end: int | None = None, **extra):
        payload = {
            "task": task,
            "batch_no": batch_no,
            "rows": rows,
            "binlog_file": FILE,
        }
        if start is not None and end is not None:
            payload["start_pos"] = start
            payload["end_pos"] = end
        payload.update(extra)
        return self.bf.submit_batch_receipt(payload)

    def test_progress_is_per_task_not_shared(self) -> None:
        for batch in (1, 2, 3):
            self.receipt("orders_repair", batch, 1, batch * 100, batch * 100 + 99)
        for batch in (1, 2, 3, 4):
            result = self.receipt("items_repair", batch, 1, batch * 100, batch * 100 + 99)
        self.assertEqual(self.bf.progress("orders_repair")["valid_rows"], 3)
        self.assertEqual(result["progress"]["valid_rows"], 4)
        self.assertEqual(pipeline_bus.pipeline_metrics["backfill_rows_by_task"]["items_repair"], 4)

    def test_resend_batch_does_not_inflate_rows(self) -> None:
        first = self.receipt("orders_repair", 1, 4, 100, 199)
        again = self.receipt("orders_repair", 1, 4)
        third = self.receipt("orders_repair", 1, 4)
        self.assertEqual(first["outcome"], "accepted")
        self.assertEqual(again["outcome"], "duplicate")
        self.assertEqual(third["outcome"], "duplicate")
        self.assertEqual(third["progress"]["valid_rows"], 4)
        self.assertEqual(len(self.bf.completion_detail()), 1)

    def test_conflicting_duplicate_is_flagged_not_counted(self) -> None:
        self.receipt("orders_repair", 1, 4, 100, 199)
        result = self.receipt("orders_repair", 1, 9, 100, 199)
        self.assertEqual(result["outcome"], "duplicate_conflict")
        self.assertTrue(result["conflict"])
        self.assertEqual(result["progress"]["valid_rows"], 4)
        self.assertEqual(len(self.bf.conflicts()), 1)

    def test_estimated_rows_are_rejected(self) -> None:
        result = self.bf.submit_batch_receipt({"task": "orders_repair", "batch_no": 1, "预估记录数": 9000})
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason_code"], "estimated_rows_not_actual")
        self.assertEqual(self.bf.progress("orders_repair")["valid_rows"], 0)

    def test_interrupt_keeps_last_confirmed_span(self) -> None:
        self.receipt("orders_repair", 1, 4, 100, 199)
        self.receipt("orders_repair", 2, 4, 200, 299)
        record = self.bf.interrupt("orders_repair", 4)
        self.assertEqual(record["last_confirmed_span"], f"{FILE}:200-299")
        self.assertEqual(record["resume_scope"]["from_pos"], 300)

    def test_absorb_late_receipt_advances_scope(self) -> None:
        self.receipt("orders_repair", 1, 4, 100, 199)
        self.bf.interrupt("orders_repair", 2, absorb_late=True)
        result = self.receipt("orders_repair", 2, 6, 200, 299, arrival="late")
        self.assertEqual(result["outcome"], "accepted")
        self.assertTrue(result["late_absorbed"])
        self.assertEqual(result["progress"]["valid_rows"], 10)
        self.assertFalse(result["progress"]["interrupted"])
        self.assertEqual(result["progress"]["resume_scope"]["from_pos"], 300)

    def test_hold_late_receipt_keeps_published_progress(self) -> None:
        self.receipt("orders_repair", 1, 4, 100, 199)
        self.bf.interrupt("orders_repair", 2, absorb_late=False)
        held = self.receipt("orders_repair", 2, 6, 200, 299, arrival="late")
        self.assertEqual(held["outcome"], "held_for_review")
        self.assertTrue(held["held"])
        self.assertEqual(held["progress"]["valid_rows"], 4)
        self.assertTrue(held["progress"]["interrupted"])
        self.assertEqual(held["progress"]["resume_scope"]["from_pos"], 200)
        self.assertEqual(self.bf.summary()["late_receipts_held"], 1)

    def test_group_submit_and_replay(self) -> None:
        result = self.bf.submit_batch_receipt({
            "groups": [
                {"group_id": "g1", "task": "orders_repair", "receipts": [
                    {"batch_no": 1, "rows": 4, "start_pos": 100, "end_pos": 199, "binlog_file": FILE},
                    {"batch_no": 2, "rows": 4, "start_pos": 200, "end_pos": 299, "binlog_file": FILE},
                ]},
                {"group_id": "g2", "task": "items_repair", "receipts": [
                    {"batch_no": 1, "rows": 2, "start_pos": 100, "end_pos": 199, "binlog_file": FILE},
                ]},
            ],
        })
        self.assertTrue(result["ok"])
        replay = self.bf.submit_batch_receipt({"receipts": [
            {"task": "orders_repair", "batch_no": 1, "rows": 4},
            {"task": "items_repair", "batch_no": 1, "rows": 2},
        ]})
        self.assertEqual([item["outcome"] for item in replay["results"]], ["duplicate", "duplicate"])
        self.assertEqual(self.bf.progress("orders_repair")["valid_rows"], 8)
        self.assertEqual(self.bf.progress("items_repair")["valid_rows"], 2)

    def test_completion_detail_reconciles_with_audit_checklist(self) -> None:
        self.receipt("orders_repair", 1, 4, 100, 199)
        self.receipt("items_repair", 1, 2, 100, 199)
        detail = self.bf.completion_detail()
        self.assertEqual(len(detail), 2)
        first = self.au.consume_backfill_completion()
        second = self.au.consume_backfill_completion()
        self.assertEqual(first["accepted"], 2)
        self.assertEqual(second["accepted"], 0)
        self.assertEqual(second["duplicates"], 2)
        checklist = self.au.recovery_checklist()
        self.assertEqual(len(checklist), len(detail))
        reconcile = self.au.reconcile_backfill_checklists()
        self.assertTrue(reconcile["consistent"])
        self.assertEqual(reconcile["completion_rows"], reconcile["audit_rows"])
        keys = {(row["任务名称"], row["已确认批号"]) for row in checklist}
        self.assertEqual(keys, {(row["任务名称"], row["批号"]) for row in detail})

    def test_unresolved_intervals_point_to_resume_scope(self) -> None:
        row, missing = self.bf.create_entry({
            "任务名称": "orders_repair",
            "源位点": f"{FILE}:1000",
            "目标位点": f"{FILE}:5000",
            "扫描范围": f"{FILE}:1000-5000",
            "批次大小": "1000",
            "并发度": "2",
            "预估记录数": "4000",
        })
        self.assertEqual(missing, [])
        self.receipt("orders_repair", 1, 1000, 1000, 1999)
        self.receipt("orders_repair", 2, 1000, 3000, 3999)
        summary = self.bf.summary()
        spans = [item["span"] for item in summary["unresolved_intervals"]]
        self.assertIn(f"{FILE}:2000-2999", spans)
        self.assertIn(f"{FILE}:4000-5000", spans)
        self.assertEqual(summary["resume_scopes"][0]["text"], f"{FILE}:2000-2999")
        decorated = self.bf.get_entry(row["id"])
        assert decorated is not None
        self.assertEqual(decorated["已回填行数"], 2000)
        self.assertEqual(decorated["进度百分比"], 50.0)


if __name__ == "__main__":
    unittest.main()
