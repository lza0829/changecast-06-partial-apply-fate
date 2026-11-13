# 04-changecast（变投影）

Changecast — 变更日志投影：源变更流 → 按序投影 → 乱序/晚到策略钩子 → 快照可查互证。

与 Feedstitch（多源 Feed 归并）、Batchgate（作业依赖编排）、Syncledger（双端对账）主链路不同。夹具全本地，不连活库。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5400 |
| API | http://localhost:10400 |

## 主链

配置投影流 → 读取 CDC 夹具并按 seq 增量应用到投影表 → 批次计数、水位与快照互证

每趟只处理 `seq > 水位` 的事件，按连续 seq 推进。首个失败或 seq 缺口处停下，之后的事件本趟不再尝试（夹具里更高的 seq 也不会越过失败点写入）。

## 部分失败结局（互斥）

`partial_failure_policy` 二选一，可在「设置」页切换，只影响之后的批次：

| 策略 | 失败时 | 水位 | 投影与留痕 |
| --- | --- | --- | --- |
| `rollback` 整批回滚 | 本趟已写入的投影变更与本趟留痕一并撤销，批次整批失败 | 不前进 | 与开跑前逐行一致（批次记录开跑/收束摘要，二者相同） |
| `keep_prefix` 保留连续前缀 | 只保留从 `水位+1` 起的连续成功前缀，失败及其后不得标成功 | 推进到该前缀末，且 `< fail_seq` | 只含前缀写入；成功数 = 留痕行数 |

注入与演示（投影台）：

- **注入失败 seq / event_id**：在指定事件处制造中途失败。
- **制造缺口 seq**：从候选流中抽掉该 seq，验证不会越过缺口。
- **演示：全成功** / **演示：中途失败（当前策略）**：先复位该流再跑一趟，便于对照。

互证检查（批次详情内逐项展示）：成功数 = 留痕行数、写入+删除+跳过 = 留痕、水位停在连续成功前缀末且不跨失败 seq、投影最大 seq ≤ 水位、回滚后投影摘要与开跑前一致且水位不动、成功+失败+回滚撤销 = 尝试处理。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/project` | `{stream_id, policy?, fail_at_seq?, fail_event_id?, skip_seq?, note?}` 跑一趟 |
| GET | `/api/batches` | 批次列表，含互证结果 `checks_ok` |
| GET | `/api/batches/{id}` | 批次详情：互证明细、本趟留痕 |
| PUT | `/api/settings` | `{partial_failure_policy: "rollback"｜"keep_prefix"}` |
| POST | `/api/streams/{id}/reset` | 演示复位：清空该流投影/留痕/批次，水位归零 |

## 测试

```bash
python -m pytest backend/tests -q
```


## 种子投影流

- **member_profile**：会员档案变更（`member_id`，字段 `display_name` / `tier` / `region`）
- **store_archive**：门店档案变更（`store_id`，字段 `name` / `status` / `city`）
- **channel_attr**：渠道属性变更（`channel_code`，字段 `label` / `active`）

底座按 **事件 seq 升序** 投影；乱序缓冲表空挂，深逻辑留给后题。对外文案不用「工单」「CRM」。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见出题规划）

`arrival_vs_event_order` / `ooo_window` / `event_idempotency` / `tombstone_policy` / `watermark_rewind` / …
（`06-partial-apply-fate`：部分失败结局与水位前缀推进，已实现于本版本。）
