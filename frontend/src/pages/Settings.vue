<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'
const settings = ref({})
const policy = ref('rollback')
const err = ref('')
const msg = ref('')
const busy = ref(false)

async function load() {
  settings.value = await getJSON('/api/settings')
  policy.value = settings.value.partial_failure_policy || 'rollback'
}

async function save(value) {
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    settings.value = await postJSON('/api/settings', { partial_failure_policy: value }, 'PUT')
    policy.value = settings.value.partial_failure_policy
    msg.value = '已保存。下一趟投影按新策略收束。'
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  try { await load() } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>设置</h1>
    <p class="muted">
      部分失败结局互斥二选一，决定一趟投影在首个失败事件处如何收束。切换只影响之后的批次。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="msg" class="tag ok">{{ msg }}</p>

    <div class="panel">
      <h3 style="margin-top:0">部分失败结局</h3>
      <div class="policy-grid">
        <label class="policy-card" :class="policy === 'rollback' ? 'active' : ''">
          <input type="radio" value="rollback" v-model="policy" @change="save('rollback')" :disabled="busy" />
          <div>
            <div class="policy-title">整批回滚</div>
            <div class="muted">
              失败则回滚本趟已写入的投影变更与本趟留痕，批次整批失败，水位不前进。
              失败后投影与开跑前逐行一致。
            </div>
          </div>
        </label>
        <label class="policy-card" :class="policy === 'keep_prefix' ? 'active' : ''">
          <input type="radio" value="keep_prefix" v-model="policy" @change="save('keep_prefix')" :disabled="busy" />
          <div>
            <div class="policy-title">保留连续成功前缀</div>
            <div class="muted">
              仅连续成功的 seq 前缀可保留，水位推进到该前缀末；失败及其后事件不得标成功。
              缺口后即使夹具里有更高 seq，也不得在本趟越过失败点写入。
            </div>
          </div>
        </label>
      </div>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">只读底座</h3>
      <table>
        <thead><tr><th>键</th><th>值</th></tr></thead>
        <tbody>
          <tr v-for="(v, k) in settings" :key="k">
            <td class="mono">{{ k }}</td>
            <td><span class="tag">{{ v }}</span></td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
