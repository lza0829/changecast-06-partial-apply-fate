<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const rows = ref([])
const err = ref('')
onMounted(async () => {
  try { rows.value = await getJSON('/api/batches?limit=100') } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>应用批次</h1>
    <p class="muted">
      每趟投影一批次：成功数、失败数、水位与投影/留痕互证。整批回滚的批次成功数为 0、水位不前进；
      保留前缀的批次成功数等于留痕行数，水位停在连续成功前缀末，且不得越过失败 seq。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>批次</th><th>投影流</th><th>状态</th><th>策略</th>
            <th>成功</th><th>失败</th><th>截停</th><th>回滚撤销</th>
            <th>写入</th><th>删除</th><th>跳过</th>
            <th>水位 开跑→收束→当前</th><th>失败点</th><th>互证</th><th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in rows" :key="b.id">
            <td class="mono">{{ b.batch_no }}</td>
            <td>{{ b.stream_title }}</td>
            <td><span class="tag" :class="b.status === 'completed' ? 'ok' : 'danger'">{{ b.status }}</span></td>
            <td><span class="tag">{{ b.outcome_policy }}</span></td>
            <td>{{ b.success_count }}</td>
            <td>{{ b.failure_count }}</td>
            <td>{{ b.blocked_count }}</td>
            <td>{{ b.discarded_count }}</td>
            <td>{{ b.applied_upsert }}</td>
            <td>{{ b.applied_delete }}</td>
            <td>{{ b.skipped_duplicate }}</td>
            <td class="mono">{{ b.watermark_before }} → {{ b.watermark_after }} → {{ b.watermark_now }}</td>
            <td class="mono">{{ b.fail_seq == null ? '—' : b.fail_seq }}{{ b.fail_event_id ? ' · ' + b.fail_event_id : '' }}</td>
            <td>
              <span class="tag" :class="b.checks_ok ? 'ok' : 'danger'">{{ b.checks_ok ? '全通过' : '有不符' }}</span>
            </td>
            <td class="mono">{{ b.created_at }}</td>
          </tr>
          <tr v-if="!rows.length"><td colspan="15" class="muted">尚无批次。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
