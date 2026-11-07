# 事故复盘：财务日报 Job 连续失败与 Gemini 上游封禁 (403) 修复记

> **日期**：2026-11-06 ~ 2026-11-07  
> **触发**：每日 23:50 财务日报（`Batch ETL [report | DAILY | finance]`）  
> **影响**：日报中断，Slack 未收到研报  
> **状态**：✅ 已修复并回归验证通过  
> **关联提交**：`56bd409` · **关键运行**：失败 `42246810556` / 回归成功 `42378200983`

---

## 1. 摘要 (TL;DR)

- **现象**：GitHub Actions 的 `report | DAILY` 运行连续 20 分钟"超时"失败；收尾 `kubectl describe pods` 只报 `No resources found`，表面看像"无日志超时"，极具迷惑性。
- **真因**：报告算子在调用大模型时，Higress 网关返回 `403 Antigravity API error: Forbidden` —— 底层 **Gemini 账号被集体封禁**，`gemini-3.8-flash` 路由彻底失效；同时集群 Secret 仍持有**已轮换的旧 API Key**（直连 401）。
- **修复**：把 Secret 的 `LLM_MODEL_NAME` 切到 `deepseek-v4.1-flash-1017`、`LLM_API_KEY` 换成新 v2 Virtual Key；清理代码中 `gemini → antigravity` 死路由；重建镜像。
- **附带发现**：workflow 的失败检测在 `restartPolicy: OnFailure` + `ttlSecondsAfterFinished: 60` 的组合下会**误报超时并吞掉真实日志**，需单独改进。

---

## 2. 故障现象

```
Run 42246810556  |  Batch ETL [report | DAILY | finance]
创建时间 : 2026-11-06 15:50:41 UTC (= 北京时间 23:50)
结论     : failure  (耗时 20m18s)
```

工作流日志末尾：

```text
⏳ Waiting for REPORT execution to complete on NUC...
❌ [Timeout] Job nuc-report-42246810556 timed out after 1200 seconds.
❌ Job nuc-report-42246810556 did not succeed (status: TIMEOUT). Inspecting pod status and logs immediately:
No resources found in batch-jobs namespace.
No resources found in batch-jobs namespace.
```

> 注意：`2026-11-05 23:50` 同样一份日报是**成功**的，只有 `2026-11-06` 起开始失败 —— 说明是**上游环境变化**，而非代码逻辑本身。

---

## 3. 影响范围

| 阶段 | 是否依赖 LLM | 是否受影响 |
|---|---|---|
| `ods`（Gmail IMAP 入湖） | ❌ | 不受影响 |
| `dwd`（ODS→DWD 清洗） | ❌ | 不受影响 |
| `report`（财务研报生成 + Slack 投递） | ✅ | **受影响（唯一受害阶段）** |

> ⚠️ **重要认知**：`pipeline_stage=all` 只会执行 `ods + dwd`，**并不包含 `report`**（见 `trigger-nuc-batch-runner.yml` 的 stage 分支）。因此 "all 全绿" ≠ "日报健康"。日报仅由显式 `report` 阶段触发（每日 23:50 定时）。

---

## 4. 排查过程与证据链

### 4.1 时间线

| 时间 (UTC) | 事件 |
|---|---|
| 11-06 15:50:55 | Job `nuc-report-42246810556` 创建成功 |
| 11-06 15:52:08 | Pod `nuc-report-42246810556-9m6d8` 启动（镜像拉取 69s） |
| 11-06 15:53:13 | **首次崩溃**：`JobExecutionException` caused by `OpenAiHttpException 403 Antigravity ... Forbidden` |
| 11-06 15:54 / 15:55 / 15:57 | 容器原地重启，反复失败，进入 `CrashLoopBackOff` |
| 11-06 16:00:09 | Pod 被清理（Job `activeDeadlineSeconds` / 底层回收） |
| 11-06 16:10:55 | 工作流自身 1200s 轮询超时，误报 `TIMEOUT`，此时 Job 早已消失 → `No resources found` |

### 4.2 真凶日志（VictoriaLogs 中 Pod 容器 stderr）

```text
🤖 [Model Factory] Assembling OpenAiChatModel:
   endpoint='http://100.105.130.0:31880/v1',
   model='gemini-3.8-flash', provider='null'
Caused by: dev.ai4j.openai4j.OpenAiHttpException:
   {"error":{"message":"Antigravity API error: Forbidden. Check server logs for details.",
             "type":"api_error"}}
→ org.apache.flink.runtime.JobException: Job execution failed.
   (Recovery is suppressed by NoRestartBackoffTimeStrategy)
```

> 关键点：`FinancialChatModelFactory` 对含 `gemini` 的模型会自动注入 `x-higress-llm-provider: openai-proxy-for-antigravity`，从而把请求路由到已封禁的 Antigravity 上游，直接 403。

### 4.3 对照组实测（2026-11-07）

| 模型 | 结果 |
|---|---|
| `gemini-3.8-flash` | ❌ HTTP 403 `Antigravity API error: Forbidden` |
| `deepseek-v4.1-flash-1017` | ✅ HTTP 200 |
| 旧 v1 Key（已失效） | ❌ HTTP 401 `Invalid or missing API key`（11-06 已轮换） |
| 新 v2 Key | ✅ HTTP 200 |

---

## 5. 根因分析（三层叠加）

### 5.1 上游层：Gemini 账号集体封禁
`gemini-3.8-flash` 经 Antigravity 代理路由到 Google Gemini 上游，而上游账号被封 → 恒定 403。此为**环境事实**，代码无法绕过，只能换模型。

### 5.2 配置层：集群 Secret 陈旧
`sms-flink-secret` 中：
- `LLM_MODEL_NAME=gemini-3.8-flash`（已死上游）
- `LLM_API_KEY=<旧 v1 Key>`（已轮换失效，直连 401）

且 `ConfigUtils` 的取值优先级为 **OS 环境变量 > .env > 代码默认值**，Secret 通过 `envFrom` 注入为环境变量，会**覆盖代码里的默认模型** —— 因此"只改代码默认值"对线上无效，必须改 Secret。

### 5.3 观测层：workflow 失败检测缺陷（放大器）
- Job 采用 `restartPolicy: OnFailure`，容器在 Pod 内**原地重启**，Job 的 `.status.failed` 长期为 `0`，于是 `failed >= 3` 的快速失败判定**永不触发**；
- 与此同时 CronJob 模板带 `ttlSecondsAfterFinished: 60`，Job 失败后 60s 即被删除；
- 结果：轮询循环傻等到自身 1200s 超时，随后 `describe pods` 时现场已被 TTL 清空 → 呈现"超时 + 无日志"的假象，掩盖了真实崩溃。

---

## 6. 修复措施（已实施）

### 6.1 生产 Secret 补丁（关键）

> 密钥一律通过环境注入，文档中不落任何真实值。示例（`<...>` 为占位符）：

```bash
# 1) 备份（回滚用）
kubectl -n batch-jobs get secret sms-flink-secret -o json > sms-flink-secret.backup.json

# 2) 打补丁：模型 + 新 v2 Virtual Key
kubectl -n batch-jobs patch secret sms-flink-secret --type merge -p \
  '{"data":{"LLM_MODEL_NAME":"<base64(deepseek-v4.1-flash-1017)>","LLM_API_KEY":"<base64(<HIGRESS_V2_VIRTUAL_KEY>)>"}}'
```

### 6.2 代码清理（提交 `56bd409`）

- `FinancialChatModelFactory`：`DEFAULT_MODEL_NAME` 改为 `deepseek-v4.1-flash-1017`；**删除** `gemini → openai-proxy-for-antigravity` 的自动路由分支（保留通用的显式 `HIGRESS_LLM_PROVIDER` 覆盖能力）。
- `FinancialAdvisorAgent` / `FinancialChatModelFactoryTest`：清理残留的 Gemini 文案，测试方法更名 `...WithDeepSeek`。

### 6.3 重建镜像

推送 `main` 触发 `build-sms-gmail-r2-job.yml`，重建 `ghcr.io/nvd11/sms-flink-batch-etl:latest`（将 deepseek 默认值烘焙进镜像，作为 Secret 之外的二次保险）。

---

## 7. 回归验证

| 验证项 | 结果 |
|---|---|
| 网关直连（curl） | ✅ HTTP 200 |
| 生产 `report | DAILY` 实跑（run `42378200983`） | ✅ **109 秒成功**，研报 1667 字，Slack 投递成功 |
| JUnit `FinancialChatModelFactoryTest` | ✅ Tests run: 2, Failures: 0, Errors: 0 |

回归关键日志：

```text
🤖 Model Factory: model='deepseek-v4.1-flash-1017', provider='null'
✅ Financial report generated successfully for DAILY (length: 1667 chars)
📬 Posting report ... → ✅ SlackYuiClient: Message successfully posted (U0AM8G9AARF)
🎉 Financial Reporter Flink Batch Job Completed Successfully!
```

---

## 8. 经验教训与后续改进

### 8.1 必须做
1. **严禁硬编码真实 Key**：`FinancialChatModelFactory` 里的 `DEFAULT_API_KEY` 仍硬编码着一串**已失效的旧 Key** 作为兜底，应改为空/占位，强制走环境变量。
2. **模型存活自检**：`report` 阶段启动时对目标模型发一次轻量 Ping，若不可用则**快速失败并明确告警**（而非让它 CrashLoop 20 分钟）。

### 8.2 建议做（workflow 健壮性）
- 失败判定改为读取 `containerStatuses[0].restartCount` 或 Job `conditions`（`Failed` 原因），不要只看 `.status.failed`；
- 将 `ttlSecondsAfterFinished` 调大（如 600s），或在 Job 结束前**先把日志 dump 到 `$GITHUB_STEP_SUMMARY`**，避免现场被 TTL 销毁；
- 超时分级：给 LLM 阶段单独设置较短超时 + 明确错误码透传。

### 8.3 待观察（另一独立隐患，非本次事故）
`2026-11-07` 的 `dwd` 运行（`42371223842`）在写出 `Got brand-new compressor [.gz]` 后**静默挂起约 16 分钟**，直到 `activeDeadlineSeconds` 被杀。疑似 **Cloudflare R2 / S3 写入卡死**，与 LLM 无关，建议单独排查 R2 连接与重试策略。

### 8.4 附带提示
同源 `gemini-4-argon` 还残留在若干 Agent 实例配置中，上游已随账号封禁失效，后续可统一迁移至 Higress DeepSeek。

---

## 9. 附录

### 9.1 回滚

```bash
kubectl -n batch-jobs apply -f sms-flink-secret.backup.json
```

### 9.2 相关文件

| 文件 | 说明 |
|---|---|
| `src/main/java/com/finance/etl/model/FinancialChatModelFactory.java` | 模型装配工厂（本次清理） |
| `src/main/java/com/finance/etl/agent/FinancialAdvisorAgent.java` | 研报生成 Agent |
| `src/main/java/com/finance/etl/transform/report/FinancialReportBroadcastProcessFunction.java` | 触发 LLM 的报告算子 |
| `.github/workflows/trigger-nuc-batch-runner.yml` | 批量 Job 调度与失败检测逻辑 |
| `k8s Secret: batch-jobs/sms-flink-secret` | 模型、Key、湖仓与 Slack 凭据 |

### 9.3 定位手法备忘
- Pod 已被 TTL 删除时，可到 **VictoriaLogs**（`fluent-bit` 汇聚，`100.95.20.57:9428`）按 `kubernetes.pod_name` 检索历史容器日志；
- 节点侧可用 `journalctl -u k3s-agent --utc --since ...` 观察 `StartContainer` / `CrashLoopBackOff` 事件。
