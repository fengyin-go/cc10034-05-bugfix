/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

export type BackfillInvocation = {
  task?: string
  batch_no: number
  rows: string | number
  span?: string
  binlog_file?: string
  start_pos?: string | number
  end_pos?: string | number
  arrival?: 'normal' | 'late'
}

export type SpanCoordinate = {
  binlog_file: string
  from_pos: number
  to_pos: number
  text: string
  reason?: string
}

export type TaskProgress = {
  task: string
  status: string
  confirmed_batches: number[]
  confirmed_batch_count: number
  actual_rows: number
  valid_rows: number
  estimated_rows: number
  estimated_batches: number
  progress_pct: number
  last_confirmed_span: string
  resume_scope: SpanCoordinate
  unresolved_intervals: (SpanCoordinate & { span: string; text: string })[]
  interrupted: boolean
  interrupt_batch_no: number | null
  interrupt_resolved_by_receipt: boolean
}

export type CompletionDetail = {
  明细行号: number
  任务名称: string
  批号: number
  有效行数: number
  日志跨度: string
  坐标已定位: boolean
  结果状态: string
  receipt_id: string
  confirmed_at: string
}

export type RecoveryChecklistRow = {
  任务名称: string
  已确认批号: number
  有效行数: number
  日志跨度: string
  确认时间: string
  审计记录ID: number | null
  审计状态: string
  续做起点: number
  扫描目标: number
}

export type ReceiptOutcome = {
  ok: boolean
  accepted: boolean
  outcome: 'accepted' | 'duplicate' | 'duplicate_conflict' | 'held_for_review' | 'rejected'
  duplicate: boolean
  conflict: boolean
  late_absorbed?: boolean
  held?: boolean
  message: string
  receipt: { batch_no: number; rows_actual: number; span: string; receipt_id: string } | null
  progress: TaskProgress
}
