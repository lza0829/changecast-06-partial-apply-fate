<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const batches = ref([])
const settings = ref({})
const streamId = ref(null)
const note = ref('')
const policy = ref('rollback')
const failAtSeq = ref(null)
const failEventId = ref('')
const skipSeq = ref(null)
const last = ref(null)
const detail = ref(null)
const busy = ref(false)
const err = ref('')
const msg = ref('')

async function refresh() {
  streams.value = await getJSON('/api/streams')
  batches.value = await getJSON('/api/batches?limit=20')
  settings.value = await getJSON('/api/settings')
  if (streamId.value == null && streams.value.length) streamId.value = streams.value[0].id
}

async function loadDetail(id) {
  detail.value = await getJSON('/api/batches/' + id)
}

async function run(payload) {
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    last.value = await postJSON('/api/project', payload)
    await loadDetail(last.value.id)
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function injection() {
  return {
    policy: policy.value,
    fail_at_seq: failAtSeq.value === '' || failAtSeq.value === null ? null : Number(failAtSeq.value),
    fail_event_id: failEventId.value || null,
    skip_seq: skipSeq.value === '' || skipSeq.value === null ? null : Number(skipSeq.value),
  }
}

function applyRun() {
  return run({ stream_id: Number(streamId.value), note: note.value || null, ...injection() })
}

async function resetStream() {
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    const r = await postJSON(`/api/streams/${Number(streamId.value)}/reset`)
    last.value = null
    detail.value = null
    await refresh()
    msg.value = `已复位投影流：清空 ${r.cleared_batches} 个批次，水位归零。`
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function demoSuccess() {
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    await postJSON(`/api/streams/${Number(streamId.value)}/reset`)
    last.value = await postJSON('/api/project', {
      stream_id: Number(streamId.value),
      note: '演示：全成功',
      policy: policy.value,
    })
    await loadDetail(last.value.id)
    await refresh()
    msg.value = `演示完成：${policy.value === 'rollback' ? '整批成功提交' : '连续前缀全部提交'}。`
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function demoFailure() {
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    await postJSON(`/api/streams/${Number(streamId.value)}/reset`)
    const seq = 3
    last.value = await postJSON('/api/project', {
      stream_id: Number(streamId.value),
      note: `演示：中途失败 @ seq ${seq}`,
      policy: policy.value,
      fail_at_seq: seq,
    })
    await loadDetail(last.value.id)
    await refresh()
    msg.value =
      policy.value === 'rollback'
        ? '演示完成：中途失败整批回滚，投影与开跑前一致，水位未前进。'
        : `演示完成：保留连续成功前缀（seq 1–2），水位停在 2，未跨过失败 seq ${seq}。`
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  try {
    await refresh()
    policy.value = settings.value.partial_failure_policy || 'rollback'
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>

<template>
  <div class="page">
    <h1>投影台</h1>
    <p class="muted">
      读取 CDC 夹具并按 seq 增量应用到投影表。部分失败结局二选一：<b>整批回滚</b>（本趟投影与留痕一并撤销，水位不前进）
      或 <b>保留连续前缀</b>（仅连续成功 seq 可保留，水位推进到前缀末，失败及其后不得标成功）。
      可在指定 seq / 事件注入中途失败，也可制造 seq 缺口验证“不得越过”。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="msg" class="tag ok">{{ msg }}</p>

    <div class="panel">
      <h3 style="margin-top:0">发起投影</h3>
      <div style="display:flex;flex-wrap:wrap;gap:0.9rem;align-items:flex-end">
        <label>
          投影流
          <select v-model="streamId" style="display:block;min-width:16rem;margin-top:0.25rem">
            <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.title }}</option>
          </select>
        </label>
        <label>
          部分失败结局
          <select v-model="policy" style="display:block;min-width:13rem;margin-top:0.25rem">
            <option value="rollback">整批回滚</option>
            <option value="keep_prefix">保留连续前缀</option>
          </select>
        </label>
        <label>
          注入失败 seq（可选）
          <input v-model.number="failAtSeq" type="number" min="1" style="display:block;width:7rem;margin-top:0.25rem" placeholder="如 3" />
        </label>
        <label>
          注入失败 event_id（可选）
          <input v-model="failEventId" style="display:block;width:9rem;margin-top:0.25rem" placeholder="如 mp-003" />
        </label>
        <label>
          制造缺口 seq（可选）
          <input v-model.number="skipSeq" type="number" min="1" style="display:block;width:7rem;margin-top:0.25rem" placeholder="如 4" />
        </label>
        <label>
          备注（可选）
          <input v-model="note" style="display:block;width:12rem;margin-top:0.25rem" placeholder="本趟说明" />
        </label>
        <button :disabled="busy || !streamId" @click="applyRun">{{ busy ? '投影中…' : '应用变更流' }}</button>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:0.6rem;margin-top:0.9rem">
        <button :disabled="busy || !streamId" @click="demoSuccess">演示：全成功</button>
        <button :disabled="busy || !streamId" @click="demoFailure">演示：中途失败（当前策略）</button>
        <button :disabled="busy || !streamId" class="secondary" @click="resetStream">复位该投影流</button>
      </div>
      <p class="muted" style="margin-bottom:0">
        演示按钮会先把该流复位到水位 0、清空投影与留痕，再跑一趟，便于两种结局对照。
      </p>
    </div>

    <div class="panel" v-if="detail">
      <h3 style="margin-top:0">最近一趟 · {{ detail.batch_no }}</h3>
      <p>
        {{ detail.stream_title }} ·
        <span class="tag" :class="detail.status === 'completed' ? 'ok' : 'danger'">{{ detail.status }}</span>
        · 结局策略 <span class="tag">{{ detail.outcome_policy }}</span>
        <span v-if="detail.rolled_back" class="tag danger" style="margin-left:0.3rem">已回滚</span>
      </p>
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ detail.success_count }}</div><div class="stat-label">成功（留痕）</div></div>
        <div class="stat"><div class="stat-num">{{ detail.failure_count }}</div><div class="stat-label">失败</div></div>
        <div class="stat"><div class="stat-num">{{ detail.blocked_count }}</div><div class="stat-label">截停未尝试</div></div>
        <div class="stat"><div class="stat-num">{{ detail.discarded_count }}</div><div class="stat-label">回滚撤销</div></div>
        <div class="stat"><div class="stat-num">{{ detail.watermark_before }} → {{ detail.watermark_after }}</div><div class="stat-label">水位（开跑→收束）</div></div>
        <div class="stat"><div class="stat-num">{{ detail.watermark_now }}</div><div class="stat-label">水位（当前）</div></div>
        <div class="stat"><div class="stat-num">{{ detail.applied_upsert }}</div><div class="stat-label">写入</div></div>
        <div class="stat"><div class="stat-num">{{ detail.applied_delete }}</div><div class="stat-label">删除</div></div>
      </div>

      <p style="margin:0.9rem 0 0.35rem">
        投影摘要（开跑前）<span class="mono">{{ detail.projection_before_digest }}</span>
        →（收束）<span class="mono">{{ detail.projection_after_digest }}</span>
        <span v-if="detail.projection_before_digest === detail.projection_after_digest" class="tag ok" style="margin-left:0.4rem">与开跑前一致</span>
        <span v-else class="tag warn" style="margin-left:0.4rem">已变化（前缀写入）</span>
      </p>
      <p v-if="detail.fail_seq != null" style="margin:0.3rem 0">
        失败点 seq <span class="mono">{{ detail.fail_seq }}</span>
        <span v-if="detail.fail_event_id" class="mono"> · {{ detail.fail_event_id }}</span>
        <span style="margin-left:0.5rem">{{ detail.error_message }}</span>
      </p>

      <h3 style="margin-top:1.1rem">互证检查</h3>
      <table>
        <thead><tr><th>检查</th><th>期望</th><th>实际</th><th>结论</th></tr></thead>
        <tbody>
          <tr v-for="c in detail.checks" :key="c.name">
            <td>{{ c.name }}<div class="muted" style="font-size:0.78rem">{{ c.note }}</div></td>
            <td class="mono">{{ c.expected }}</td>
            <td class="mono">{{ c.actual }}</td>
            <td><span class="tag" :class="c.ok ? 'ok' : 'danger'">{{ c.ok ? '通过' : '不符' }}</span></td>
          </tr>
        </tbody>
      </table>

      <h3 style="margin-top:1.1rem">本趟留痕</h3>
      <table>
        <thead><tr><th>seq</th><th>event_id</th><th>实体键</th><th>op</th><th>结果</th></tr></thead>
        <tbody>
          <tr v-for="e in detail.events" :key="e.id">
            <td class="mono">{{ e.seq }}</td>
            <td class="mono">{{ e.event_id }}</td>
            <td class="mono">{{ e.entity_key }}</td>
            <td class="mono">{{ e.op }}</td>
            <td>
              <span class="tag" :class="e.result === 'applied_delete' ? 'danger' : (e.result === 'skipped_duplicate' ? 'warn' : 'ok')">{{ e.result }}</span>
            </td>
          </tr>
          <tr v-if="!detail.events.length"><td colspan="5" class="muted">
            本趟无成功留痕（{{ detail.rolled_back ? '回滚策略已撤销本趟写入' : '扫描止于失败/缺口之前' }}）。
          </td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">历史批次</h3>
      <table>
        <thead>
          <tr>
            <th>批次</th><th>投影流</th><th>状态</th><th>策略</th>
            <th>成功</th><th>失败</th><th>水位</th><th>互证</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id" style="cursor:pointer" @click="loadDetail(b.id)">
            <td class="mono">{{ b.batch_no }}</td>
            <td>{{ b.stream_title }}</td>
            <td><span class="tag" :class="b.status === 'completed' ? 'ok' : 'danger'">{{ b.status }}</span></td>
            <td><span class="tag">{{ b.outcome_policy }}</span></td>
            <td>{{ b.success_count }}</td>
            <td>{{ b.failure_count }}</td>
            <td class="mono">{{ b.watermark_before }} → {{ b.watermark_after }}</td>
            <td><span class="tag" :class="b.checks_ok ? 'ok' : 'danger'">{{ b.checks_ok ? '全通过' : '有不符' }}</span></td>
          </tr>
          <tr v-if="!batches.length"><td colspan="8" class="muted">还没有批次，先跑一趟。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
