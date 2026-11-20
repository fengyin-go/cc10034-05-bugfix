import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  BackfillInvocation,
  CompletionDetail,
  EntryRow,
  OverviewResult,
  PageResult,
  ReceiptOutcome,
  RecoveryChecklistRow,
  TaskProgress,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 回填批次回执与进度：所有入口共用同一批次执行入口，按 (任务, 批号) 去重
// ---------------------------------------------------------------------------
export async function submitBatchReceipt(payload: {
  task: string
  batch_no: number
  rows: string | number
  binlog_file?: string
  start_pos?: string | number
  end_pos?: string | number
  arrival?: 'normal' | 'late'
  absorb_late?: boolean
  group_id?: string
}): Promise<ReceiptOutcome> {
  const response = await request('/api/backfill_position/receipts', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function submitBatchGroups(
  groups: { group_id: string; task: string; receipts: BackfillInvocation[] }[],
): Promise<{ ok: boolean; message: string; groups: { group_id: string; ok: boolean; results: ReceiptOutcome[] }[] }> {
  const response = await request('/api/backfill_position/groups', {
    method: 'POST',
    body: JSON.stringify({ groups }),
  })
  if (!response.ok) throw new Error('作业组提交失败')
  return await response.json()
}

export async function submitBackfillInvocations(
  invocations: BackfillInvocation[],
): Promise<{ ok: boolean; message: string; results: ReceiptOutcome[] }> {
  const response = await request('/api/backfill_position/receipts', {
    method: 'POST',
    body: JSON.stringify({ receipts: invocations }),
  })
  if (!response.ok) throw new Error('批次回执提交失败')
  return await response.json()
}

export async function loadKnownTasks(): Promise<{ items: string[]; total: number }> {
  const response = await request('/api/backfill_position-tasks')
  if (!response.ok) throw new Error('回填任务清单读取失败')
  return await response.json()
}

export async function loadProgress(task?: string): Promise<TaskProgress | TaskProgress[]> {
  const query = task ? `?task=${encodeURIComponent(task)}` : ''
  const response = await request(`/api/backfill_position-progress${query}`)
  if (!response.ok) throw new Error('回填进度读取失败')
  return await response.json()
}

export async function loadCompletionDetail(task?: string): Promise<{ items: CompletionDetail[]; total: number }> {
  const query = task ? `?task=${encodeURIComponent(task)}` : ''
  const response = await request(`/api/backfill-position/completion-detail${query}`)
  if (!response.ok) throw new Error('回填完成明细读取失败')
  return await response.json()
}

export async function loadRecoveryChecklist(): Promise<{
  items: RecoveryChecklistRow[]
  total: number
  reconcile: { consistent: boolean; completion_rows: number; audit_rows: number }
  unresolved: { pending_completion_rows: unknown[]; unresolved_intervals: unknown[]; message: string }
}> {
  const response = await request('/api/audit-trail/recovery-checklist')
  if (!response.ok) throw new Error('审计恢复清单读取失败')
  return await response.json()
}

export async function loadBackfillSummary(): Promise<Record<string, unknown>> {
  const response = await request('/api/backfill-position/summary')
  if (!response.ok) throw new Error('回填汇总读取失败')
  return await response.json()
}

export async function interruptBackfill(
  task: string,
  batch_no?: number,
  absorb_late = true,
): Promise<{
  task: string
  batch_no: number
  last_confirmed_span: string
  resume_scope: { text: string }
  late_policy_absorb: boolean
  progress: TaskProgress
}> {
  const response = await request('/api/backfill_position/interrupts', {
    method: 'POST',
    body: JSON.stringify({ task, batch_no, absorb_late }),
  })
  if (!response.ok) throw new Error('中断请求失败')
  return await response.json()
}
