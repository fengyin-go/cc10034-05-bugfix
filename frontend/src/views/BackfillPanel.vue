<template>
  <section class="backfill-panel" data-testid="backfill-panel">
    <header class="panel-head">
      <div>
        <h3>回填批次回执与恢复清单</h3>
        <p class="page-desc">
          进度按任务独立计数；回执按（任务，批号）去重，重复提交与预估行数都不会抬高已回填值。
        </p>
      </div>
      <span class="policy-tag">{{ policy }}</span>
    </header>

    <div class="analysis-summary">
      <article class="analysis-card">
        <span class="stat-label">有效行数</span>
        <strong class="stat-value" data-testid="valid-rows">{{ summary.valid_rows ?? 0 }}</strong>
        <small v-for="(rows, task) in summary.valid_rows_by_task" :key="task" class="breakdown">
          {{ task }}：{{ rows }}
        </small>
      </article>
      <article class="analysis-card">
        <span class="stat-label">重复提交（未计数）</span>
        <strong class="stat-value" data-testid="duplicate-count">{{ summary.duplicate_submissions ?? 0 }}</strong>
        <small class="breakdown">冲突 {{ summary.conflicts ?? 0 }} 条</small>
      </article>
      <article class="analysis-card">
        <span class="stat-label">晚到回执已吸收</span>
        <strong class="stat-value" data-testid="late-absorbed">{{ summary.late_receipts_absorbed ?? 0 }}</strong>
        <small class="breakdown">中断轮次 {{ summary.interrupt_rounds ?? 0 }}</small>
      </article>
      <article class="analysis-card">
        <span class="stat-label">悬而未决区间</span>
        <strong class="stat-value" data-testid="unresolved-count">{{ unresolved.length }}</strong>
      </article>
    </div>

    <form class="analysis-form" @submit.prevent="submitSingle">
      <label>
        <span>任务</span>
        <input
          v-model="form.task"
          data-testid="receipt-task"
          list="backfill-task-options"
          placeholder="orders_repair / items_repair"
        />
        <datalist id="backfill-task-options">
          <option v-for="task in knownTasks" :key="task" :value="task" />
        </datalist>
      </label>
      <label>
        <span>批号</span>
        <input v-model="form.batchNo" data-testid="receipt-batch" inputmode="numeric" placeholder="1" />
      </label>
      <label>
        <span>实际回填行数</span>
        <input v-model="form.rows" data-testid="receipt-rows" inputmode="numeric" placeholder="不要填预估值" />
      </label>
      <label>
        <span>Binlog 文件</span>
        <input v-model="form.file" data-testid="receipt-file" placeholder="mysql-bin.000001" />
      </label>
      <label>
        <span>起始位点</span>
        <input v-model="form.start" data-testid="receipt-start" inputmode="numeric" />
      </label>
      <label>
        <span>结束位点</span>
        <input v-model="form.end" data-testid="receipt-end" inputmode="numeric" />
      </label>
      <label>
        <span>中断轮取舍</span>
        <select v-model="demoState.absorbLate" data-testid="absorb-late">
          <option :value="true">吸收晚到回执</option>
          <option :value="false">挂起待复核</option>
        </select>
      </label>
      <button class="btn primary" data-testid="submit-receipt" type="submit" :disabled="busy">
        提交批次回执
      </button>
      <button class="btn" data-testid="interrupt-task" type="button" :disabled="busy" @click="interrupt">
        中断当前任务
      </button>
    </form>

    <div class="panel-actions">
      <button class="btn" data-testid="replay-batch-one" type="button" :disabled="busy" @click="replayBatchOne">
        逐条重放批号 1
      </button>
      <button class="btn" data-testid="submit-group" type="button" :disabled="busy" @click="submitGroup">
        勾选两组整组提交
      </button>
      <button class="btn" data-testid="submit-late" type="button" :disabled="busy" @click="submitLate">
        模拟停机后补达凭证
      </button>
      <button class="btn ghost" data-testid="refresh-panel" type="button" :disabled="busy" @click="refresh">
        刷新
      </button>
      <span v-if="notice" class="notice-text" data-testid="panel-notice">{{ notice }}</span>
      <span v-if="errorMessage" class="error-text" data-testid="panel-error">{{ errorMessage }}</span>
    </div>

    <h4 class="section-title">任务进度</h4>
    <table class="data-table" data-testid="progress-table">
      <thead>
        <tr>
          <th>任务</th>
          <th>已确认批号</th>
          <th>有效行数</th>
          <th>预估行数（仅分母）</th>
          <th>上次确认跨度</th>
          <th>实际续做范围</th>
          <th>中断状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in progressList" :key="item.task" :data-testid="`progress-${item.task}`">
          <td>{{ item.task }}</td>
          <td>{{ item.confirmed_batches.length ? item.confirmed_batches.join('、') : '—' }}</td>
          <td data-testid="task-valid-rows">{{ item.valid_rows }}</td>
          <td>{{ item.estimated_rows }}</td>
          <td>{{ item.last_confirmed_span || '—' }}</td>
          <td>{{ item.resume_scope.text || '—' }}</td>
          <td>
            <span v-if="item.interrupted" class="warn-tag">中断待恢复</span>
            <span v-else-if="item.interrupt_resolved_by_receipt" class="ok-tag">晚到回执已吸收</span>
            <span v-else>—</span>
          </td>
        </tr>
        <tr v-if="!progressList.length">
          <td colspan="7" class="empty-state">暂无回填进度，先提交一条批次回执</td>
        </tr>
      </tbody>
    </table>

    <h4 class="section-title">悬而未决的 Binlog 区间</h4>
    <table class="data-table" data-testid="unresolved-table">
      <thead>
        <tr><th>任务</th><th>区间</th><th>原因</th></tr>
      </thead>
      <tbody>
        <tr v-for="(item, index) in unresolved" :key="`${item.task}-${index}`">
          <td>{{ item.task }}</td>
          <td>{{ item.span || item.text }}</td>
          <td>{{ item.reason }}</td>
        </tr>
        <tr v-if="!unresolved.length">
          <td colspan="3" class="empty-state">没有悬而未决的区间</td>
        </tr>
      </tbody>
    </table>

    <div class="panel-split">
      <div>
        <h4 class="section-title">回填完成明细数据集</h4>
        <table class="data-table" data-testid="completion-table">
          <thead>
            <tr><th>任务</th><th>批号</th><th>有效行数</th><th>日志跨度</th><th>状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in completion" :key="row.receipt_id" :data-testid="`completion-${row.任务名称}-${row.批号}`">
              <td>{{ row.任务名称 }}</td>
              <td>{{ row.批号 }}</td>
              <td>{{ row.有效行数 }}</td>
              <td>{{ row.坐标已定位 ? row.日志跨度 : '未定位' }}</td>
              <td>{{ row.结果状态 }}</td>
            </tr>
            <tr v-if="!completion.length">
              <td colspan="5" class="empty-state">暂无已确认批次</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div>
        <h4 class="section-title">
          审计恢复清单
          <span v-if="reconcile" :class="reconcile.consistent ? 'ok-tag' : 'warn-tag'">
            {{ reconcile.consistent ? '已按批号对齐' : '清单不一致' }}
          </span>
        </h4>
        <table class="data-table" data-testid="recovery-table">
          <thead>
            <tr><th>任务</th><th>已确认批号</th><th>有效行数</th><th>日志跨度</th><th>审计状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in recovery" :key="`${row.任务名称}-${row.已确认批号}`">
              <td>{{ row.任务名称 }}</td>
              <td>{{ row.已确认批号 }}</td>
              <td>{{ row.有效行数 }}</td>
              <td>{{ row.日志跨度 || '未定位' }}</td>
              <td>{{ row.审计状态 }}</td>
            </tr>
            <tr v-if="!recovery.length">
              <td colspan="5" class="empty-state">审计尚未消费完成明细</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  interruptBackfill,
  loadBackfillSummary,
  loadCompletionDetail,
  loadKnownTasks,
  loadProgress,
  loadRecoveryChecklist,
  submitBackfillInvocations,
  submitBatchGroups,
  submitBatchReceipt,
} from '@/api/backend-service'
import type { CompletionDetail, RecoveryChecklistRow, TaskProgress } from '@/data/types'

const props = defineProps<{ tasks: string[] }>()

const form = reactive({
  task: props.tasks[0] ?? 'orders_repair',
  batchNo: '1',
  rows: '',
  file: 'mysql-bin.000001',
  start: '',
  end: '',
})
const progressList = ref<TaskProgress[]>([])
const completion = ref<CompletionDetail[]>([])
const recovery = ref<RecoveryChecklistRow[]>([])
const summary = ref<Record<string, unknown>>({})
const reconcile = ref<{ consistent: boolean; completion_rows: number; audit_rows: number } | null>(null)
const notice = ref('')
const errorMessage = ref('')
const busy = ref(false)
const demoState = reactive({ replayDone: false, lateIndex: 1, absorbLate: true })

const policy = computed(() => String(summary.value.late_receipt_policy ?? '已确认交付优先'))
const ledgerTasks = ref<string[]>([])
const knownTasks = computed(() => {
  const values = new Set([...props.tasks, ...ledgerTasks.value])
  for (const item of progressList.value) values.add(item.task)
  return [...values].filter(Boolean)
})
const unresolved = computed(() => {
  const rows = (summary.value.unresolved_intervals ?? []) as { task: string; span?: string; text?: string; reason?: string }[]
  return rows
})

function currentInvocation(overrides: Record<string, unknown> = {}) {
  return {
    task: form.task,
    batch_no: Number(form.batchNo || 0),
    rows: form.rows,
    binlog_file: form.file,
    start_pos: form.start,
    end_pos: form.end,
    ...overrides,
  }
}

async function refresh() {
  errorMessage.value = ''
  try {
    const [progress, detail, audit, summaryPayload, tasksPayload] = await Promise.all([
      loadProgress(),
      loadCompletionDetail(),
      loadRecoveryChecklist(),
      loadBackfillSummary(),
      loadKnownTasks(),
    ])
    ledgerTasks.value = tasksPayload.items
    if (!props.tasks.includes(form.task) && !tasksPayload.items.includes(form.task)) {
      form.task = tasksPayload.items[0] ?? form.task
    }
    progressList.value = Array.isArray(progress) ? progress : [progress]
    completion.value = detail.items
    recovery.value = audit.items
    reconcile.value = audit.reconcile
    summary.value = summaryPayload
    if (!progressList.value.length && props.tasks.length) {
      progressList.value = props.tasks.map((task) => emptyProgress(task))
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回填面板读取失败'
  }
}

function emptyProgress(task: string): TaskProgress {
  return {
    task,
    status: '',
    confirmed_batches: [],
    confirmed_batch_count: 0,
    actual_rows: 0,
    valid_rows: 0,
    estimated_rows: 0,
    estimated_batches: 0,
    progress_pct: 0,
    last_confirmed_span: '',
    resume_scope: { binlog_file: '', from_pos: 0, to_pos: 0, text: '' },
    unresolved_intervals: [],
    interrupted: false,
    interrupt_batch_no: null,
    interrupt_resolved_by_receipt: false,
  }
}

async function run(label: string, action: () => Promise<string>) {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    notice.value = `${label}：${await action()}`
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : `${label}失败`
  } finally {
    busy.value = false
  }
}

async function submitSingle() {
  await run('提交回执', async () => {
    const result = await submitBatchReceipt(currentInvocation())
    return `${result.outcome}｜${result.message}｜任务有效行数 ${result.progress?.valid_rows ?? 0}`
  })
}

async function replayBatchOne() {
  await run('重放批号 1', async () => {
    const confirmed = completion.value.filter((row) => Number(row.批号) === 1)
    if (!confirmed.length) {
      return '没有已确认的批号 1，先提交一次批次回执再重放'
    }
    const results = await submitBackfillInvocations(
      confirmed.map((row) => ({
        task: row.任务名称,
        batch_no: 1,
        rows: row.有效行数,
        arrival: 'late' as const,
      })),
    )
    demoState.replayDone = true
    const outcomes = results.results.map((item) => `${item.progress?.task ?? ''}:${item.outcome}`)
    return `逐条重放批号 1 完成（${outcomes.join('，')}），重复批号未计数`
  })
}

async function submitGroup() {
  await run('整组提交', async () => {
    const tasks = knownTasks.value.slice(0, 2)
    const groups = tasks.map((task, index) => ({
      group_id: `group-${index + 1}-${task}`,
      task,
      receipts: [1, 2].map((batchNo) => ({
        batch_no: batchNo,
        rows: batchNo === 1 ? (form.rows || '0') : String(Number(form.rows || 0) + batchNo),
        binlog_file: form.file,
        start_pos: String(100 * batchNo),
        end_pos: String(100 * batchNo + 99),
      })),
    }))
    const result = await submitBatchGroups(groups)
    return `${result.message}（${groups.map((g) => g.group_id).join('、')}）`
  })
}

async function submitLate() {
  await run('补达回执', async () => {
    const task = form.task || knownTasks.value[0]
    const batchNo = Number(form.batchNo || 1)
    const result = await submitBatchReceipt({
      ...currentInvocation({ task, batch_no: batchNo }),
      arrival: 'late',
      absorb_late: demoState.absorbLate,
    })
    demoState.lateIndex += 1
    if (result.held) {
      return '中断轮选择不吸收，回执已挂起待复核，已发布进度保持不变'
    }
    return `${result.outcome}｜晚到回执${result.late_absorbed ? '已吸收并推进续做范围' : '未改变已发布进度'}`
  })
}

async function interrupt() {
  await run('中断任务', async () => {
    const record = await interruptBackfill(form.task, Number(form.batchNo || 0) || undefined, demoState.absorbLate)
    return `上次确认跨度 ${record.last_confirmed_span || '无'}，续做范围 ${record.resume_scope?.text || '—'}，本轮${demoState.absorbLate ? '吸收' : '不吸收'}晚到回执`
  })
}

onMounted(refresh)
</script>

<style scoped>
.backfill-panel {
  margin: 16px 0 24px;
  padding: 14px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 10px;
}
.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}
.panel-head h3 { margin: 0; font-size: 15px; }
.policy-tag {
  font-size: 12px;
  color: #1f6feb;
  background: #eaf2ff;
  border-radius: 999px;
  padding: 2px 10px;
  white-space: nowrap;
}
.panel-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  margin: 12px 0;
}
.notice-text { font-size: 12px; color: #1f6feb; }
.section-title { margin: 16px 0 6px; font-size: 13px; }
.panel-split { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.breakdown { display: block; color: var(--muted); font-size: 11px; }
.ok-tag { color: #15803d; font-size: 12px; margin-left: 6px; }
.warn-tag { color: #b45309; font-size: 12px; }
@media (max-width: 1100px) {
  .panel-split { grid-template-columns: 1fr; }
}
</style>
