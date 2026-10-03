# 个人财务湖仓智能中台实施计划 (Implementation Plan)
## DWS 汇总模型、LLM 智能分析与 Yui Slack 图文播报工程推进方案

> **文件标识**：`docs/financial-analytics-implementation-plan.md`  
> **制定时间**：2026-10-03  
> **指导需求**：`docs/financial-analytics-dws-spec.md`  
> **执行原则**：奥卡姆剃刀、敏捷交付、两阶段验证、零冗余架构

---

## 1. 实施全景图 (Implementation Overview)

本项目旨在将已清洗完成的 **DWD 金融动账事实明细 (`dwd_financial_transactions`)**，自底向上提炼为日/周/月多维汇总指标（DWS），并通过 **LLM（大语言模型）** 赋予数据智能洞察与情绪价值，最终由 **Slack 机器人「Yui」** 以富文本卡片 + 高清图表形式向主人私密推送。

```text
[阶段 1: DWS 视图层建模] ──> [阶段 2: 财务分析 Agent] ──> [阶段 3: Yui 图文卡片引擎] ──> [阶段 4: 定时调度自动化]
  • 每日/周/月视图建表          • 结构化 Prompt 注入          • QuickChart 短链生成         • EventBridge / CronJob
  • Trino 毫秒级聚合验证        • 调用 LiteLLM / Gemini        • Slack Block Kit 组装        • 睡前/周末自动精准推送
```

---

## 2. 分阶段实施路线与任务拆解 (Milestones & Tasks)

### 📌 Milestone 1: DWS 聚合服务层建模与 Trino 验证 (已完成 ✅)
* **核心目标**：在数据湖中建立纯 SQL View 聚合模型，引入 `min_id` 与 `max_id` 物理行号聚合边界，无需额外批处理计算资源，随查随得。
* **具体任务项**：
  1. **编写 DDL 脚本 (`scripts/schema-dws.sql` / `schema-dws-dev.sql`)**：
     * `dws_financial_summary_daily`（日度流水、净开销、五大类目切片、单笔峰值、包含 `min_id`/`max_id`）；
     * `dws_financial_summary_weekly`（自然周总览、日均消费、周中 vs 周末开销对比、Top 类目、包含 `min_id`/`max_id`）；
     * `dws_financial_summary_monthly`（月度资产负债大盘、净支出、信用卡对账、外币统计、包含 `min_id`/`max_id`）；
     * `dws_merchant_spending_ranking`（商户排行 Mart，含频次、客单价与渗透率、包含 `min_id`/`max_id`）；
  2. **双环境同步与对账**：
     * 将 DWS 脚本加入 `.github/workflows/sync-iceberg-schema.yml` 自动同步触发路径；
     * 直连 Trino 执行验证：对比 9 月与 10 月真实数据，确保 `net_expense = total_expense - total_refund` 严格平账；
  3. **验收标准**：
     * 执行 `SELECT * FROM iceberg.finance.dws_financial_summary_monthly` 在 0.5 秒内极速返回。已 100% 验收通过！✅

---

### 📌 Milestone 2: 财务分析 Agent 与 LangChain4j 算子构建 (预计耗时: 1 天)
* **核心目标**：采用 **LangChain4j (0.35.0+)** 声明式 `AiServices` 驱动，直连私有 LiteLLM 网关（姿势 A），调度 `gemini-3.8-flash` 产出专业温暖的财务报告。
* **具体任务项**：
  1. **引入 Maven 依赖**：
     * `dev.langchain4j:langchain4j-open-ai:0.35.0`（轻量独立，零外部容器侵入）；
  2. **声明式 AI Service 接口定义 (`FinancialAdvisorService.java`)**：
     * 采用 `@SystemMessage` 锁定 Yui 贴身秘书 + CFA 级财务分析人设；
     * 采用 `@UserMessage` 动态注入周期标签与 DWS 聚合指标 JSON；
     * 严格防范幻觉：严禁捏造金额，严格基于输入上下文；
  3. **模型工厂与 DAO 层装配**：
     * `FinancialChatModelFactory.java` 封装直连 `https://gw.jppwl.asia/litellm/v1`，调度 `gemini-3.8-flash`；
     * `FinancialDwsDao.java` 负责从 Trino 中秒级捞取日/周/月聚合指标与商户排行切片；
     * `FinancialLakehouseTools.java` 用 `@Tool` 暴露数据查询能力；
  4. **智能体实体类 (`FinancialAdvisorAgent.java`)**：
     * 纯正智能体，内置 `fromConfig()`，持有工具箱、模型并提供高级分析方法；
  5. **验收标准**：
     * 编写 JUnit 测试，成功跑通 9 月全量数据的 Gemini 智能总结与深度点评。

---

### 📌 Milestone 3: ADS 湖仓落盘、Flink 双写与 Yui Slack 集成 (预计耗时: 1 天)
* **核心目标**：构建 ADS 持久化物理表 `ads_financial_reports`，在 Flink 算子链中实现**“分析结果落盘湖仓 ➕ Yui Slack 私信投递”双写闭环**，并以单一通用 Job 结合**方案 A（纯行号增量区间推进）**驱动日/周/月。
* **具体任务项**：
  1. **编写 ADS 事实表 DDL (`scripts/schema-ads.sql`)**：
     * 在 `iceberg.finance` 和 `iceberg.finance_dev` 创建 `ads_financial_reports`；
     * 包含指标快照、LLM 分析文本、QuickChart 图片短链、Slack 履约状态；
     * 锁定主键 `[report_id, report_date]` 支撑幂等 Equality Delete Upsert；
  2. **领域模型与 RowData 映射器**：
     * `com.finance.etl.model.FinancialReport.java`；
     * `com.finance.etl.sink.iceberg.FinancialReportToRowDataMapper.java`；
  3. **通用 Flink 批处理主作业 (`FinancialReporterJob.java`)**：
     * 遵循 DRY 架构，以单一通用 Job 承载日/周/月三套分析需求；
     * 支持命令行参数 `--period=daily/weekly/monthly`（或环境变量 `REPORT_PERIOD`）；
     * **方案 A 增量游标核算**：从 `etl_sync_offsets` 提取对应周期的 `last_offset`，精准只处理 `WHERE id > last_offset AND id <= currentMaxId` 的增量事实行；
     * **天然防重与静默机制 (Quiet Mode)**：若 `currentMaxId <= last_offset`（无新增动账），秒级安全退出，零 Token 消耗且不打扰主人；
     * Flink 双写算子编排：`Trino DWS Source ➔ FinancialAdvisorProcessFunction ➔ [IcebergR2Sink (ADS表) + SlackYuiSink (私聊)]`；
     * 成功后向前推进该周期的 `last_offset = currentMaxId`；
  4. **QuickChart 短链与 Slack Block Kit 发送器**：
     * 封装标准 `POST https://quickchart.io/chart/create` API，彻底规避裂图隐患；
     * 组装 Slack Block Kit 发送器并私聊直达主人 `U0AM8G9AARF`；
  5. **验收标准**：
     * 运行端到端测试，主人的 Slack 收到图文并茂的卡片，同时在 Trino 中 `SELECT * FROM iceberg.finance_dev.ads_financial_reports` 成功查出落盘的完整报告与图表短链！

---

### 📌 Milestone 4: 自动化调度与云原生落地 (预计耗时: 1 天)
* **核心目标**：实现无人值守、定时自动推送，打造全天候贴身伴随体验。
* **具体任务项**：
  1. **编排推送时钟策略**：
     * ☕ **每日晚报 (`report-daily`)**：每天 23:00 自动触发，带 `--period=daily`；
     * 📅 **每周复盘 (`report-weekly`)**：每周日晚 23:00 自动触发，带 `--period=weekly`；
     * 🏆 **每月大盘 (`report-monthly`)**：每月末 23:30 自动触发，带 `--period=monthly`；
  2. **调度工作流改造 (`trigger-nuc-batch-runner.yml`)**：
     * 扩展 `pipeline_stage` 支持 `report-daily`, `report-weekly`, `report-monthly`；
     * 在 K3s 上拉起 Pod 并注入对应参数与环境变量；
  3. **验收标准**：
     * 经历一次真实的定时触发，主人在设定时间准时收到 Yui 的自动私信。

---

## 3. 任务排期与优先级矩阵 (Schedule & Priority)

| 阶段 | 交付物 | 依赖项 | 优先级 | 预期周期 |
| :--- | :--- | :--- | :---: | :---: |
| **M1: 视图建模** | `scripts/schema-dws.sql` + Trino 跑通 | DWD 生产数据 | **P0 (最高)** | Day 1 |
| **M2: Agent & LLM**| 提示词模版 + 数据抽取 + LLM 接口调用 | M1 聚合数据 | **P0 (最高)** | Day 2 |
| **M3: Yui 图文整合**| QuickChart 短链模块 + Slack Block Kit | Yui Bot 凭证 | **P1 (核心)** | Day 3 |
| **M4: 自动调度** | Actions / K3s CronJob 接入 | M2, M3 | **P1 (核心)** | Day 4 |

---

## 4. 关键技术避坑指南与容错机制 (Risk Control)

1. **图表乱码与裂图防范**：
   * 严禁将中文标签直接塞在 GET 请求的 Query 参数里，**必须一律走 QuickChart `POST /chart/create` 换取标准短链**；
2. **零数据容错（Quiet Mode）**：
   * 当遇到主人全天无任何刷卡动账的“自律日”，Yui 应发出特定的温馨问候（例如：“主人今天好自律呀，钱包一分钱都没缩水哦～”），而不是粗暴报错；
3. **敏感凭证防泄漏**：
   * Yui 的 Token `xoxb-...` 严禁提交进 Git 仓库，统一通过环境变量 `SLACK_YUI_BOT_TOKEN` 安全注入；
4. **LLM 幻觉控制**：
   * 严格禁止 LLM 自行计算或胡乱捏造数字，所有金额、笔数百分之百取自 Trino DWS 的结构化统计结果，LLM 仅负责解读与点评。

---

## 5. 立即开启行动 (Next Steps)

按照敏捷交付原则，我们立即从 **Milestone 1（编写 `scripts/schema-dws.sql`）** 启动：
1. 编写包含日、周、月三大维度的 Trino View SQL 脚本；
2. 在开发环境与生产环境 Trino 上部署并实施即席数据验证；
3. 验证无误后，紧接着推进 Milestone 2 的 Python/Node 自动化脚本！
