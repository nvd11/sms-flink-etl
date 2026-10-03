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

### 📌 Milestone 1: DWS 聚合服务层建模与 Trino 验证 (预计耗时: 1 天)
* **核心目标**：在数据湖中建立纯 SQL View 聚合模型，无需额外批处理计算资源，随查随得。
* **具体任务项**：
  1. **编写 DDL 脚本 (`scripts/schema-dws.sql`)**：
     * `dws_financial_summary_daily`（日度流水、净开销、五大类目切片、单笔峰值）；
     * `dws_financial_summary_weekly`（自然周总览、日均消费、周中 vs 周末开销对比、Top 类目）；
     * `dws_financial_summary_monthly`（月度资产负债大盘、净支出、信用卡对账、外币统计）；
     * `dws_merchant_spending_ranking`（商户排行 Mart，含频次、客单价与渗透率）；
  2. **双环境同步与对账**：
     * 将 DWS 脚本加入 `.github/workflows/sync-iceberg-schema.yml` 自动同步触发路径；
     * 直连 Trino 执行验证：对比 9 月与 10 月真实数据，确保 `net_expense = total_expense - total_refund` 严格平账；
  3. **验收标准**：
     * 执行 `SELECT * FROM iceberg.finance.dws_financial_summary_monthly` 在 0.5 秒内极速返回。

---

### 📌 Milestone 2: 财务分析 Agent 与 LangChain4j 驱动 (预计耗时: 1 天)
* **核心目标**：采用 **LangChain4j (0.35.0+)** 声明式 `AiServices` 驱动，直连私有 LiteLLM 网关（姿势 A），调度 `gemini-3.8-flash` 产出专业温暖的财务报告。
* **具体任务项**：
  1. **引入 Maven 依赖**：
     * `dev.langchain4j:langchain4j-open-ai:0.35.0`（轻量独立，零外部容器侵入）；
  2. **声明式 AI Service 接口定义 (`FinancialAdvisorService.java`)**：
     * 采用 `@SystemMessage` 锁定 Yui 贴身秘书 + CFA 级财务分析人设；
     * 采用 `@UserMessage` 动态注入周期标签与 DWS 聚合指标 JSON；
     * 严格防范幻觉：严禁捏造金额，严格基于输入上下文；
  3. **模型实例装配 (姿势 A · 统一网关)**：
     * 使用 `OpenAiChatModel.builder()` 直连 `https://gw.jppwl.asia/litellm/v1`；
     * 注入 Yui 专属凭证 `sk-WhW6BWdwKN_LITjCuAmgiA`，模型锁定为 `gemini-3.8-flash`；
  4. **数据提炼与编排 (`FinancialAnalysisAgent.java`)**：
     * 编排 `TrinoDwsClient` 拉取指标 ➔ 组装结构化上下文 ➔ 驱动 LangChain4j 产出专业洞察点评；
  5. **验收标准**：
     * 编写 JUnit 测试，成功跑通 9 月全量数据的 Gemini 智能总结与深度点评。

---

### 📌 Milestone 3: Yui Slack 富文本卡片与图表集成 (预计耗时: 1 天)
* **核心目标**：将 DWS 指标、QuickChart 高清图表与 LangChain4j 点评文本组装为 Slack Block Kit 消息，私信直达主人。
* **具体任务项**：
  1. **QuickChart 短链渲染客户端 (`QuickChartClient.java`)**：
     * 封装标准 `POST https://quickchart.io/chart/create` API，彻底规避 URL 编码截断与裂图隐患；
     * 动态生成：环形消费分类饼图（`doughnut`）与核心商户横向柱状图（`horizontalBar`）；
  2. **Slack Block Kit 发送器 (`YuiSlackSender.java`)**：
     * 调用 Slack 原生 Web API `chat.postMessage`；
     * 发送至主人专属私聊频道 `U0AM8G9AARF`；
     * 支持日结小卡片、周度体检图文大卡片、月度全景白皮书大卡片；
  3. **安全凭证注入与集成**：
     * 环境变量 `SLACK_YUI_BOT_TOKEN` 安全纳管；
  4. **验收标准**：
     * 运行端到端测试，主人的 Slack 收到图文并茂、排版精美的真实 9 月度财务复盘大卡片。

---

### 📌 Milestone 4: 自动化调度与云原生落地 (预计耗时: 1 天)
* **核心目标**：实现无人值守、定时自动推送，打造全天候贴身伴随体验。
* **具体任务项**：
  1. **编排推送时钟策略**：
     * ☕ **每日晚报**：每天 22:30 自动推送当日账单；
     * 📅 **每周复盘**：每周日晚 23:00 自动推送本周体检；
     * 🏆 **每月大盘**：每月 1 日早 09:00 自动推送上月全貌；
  2. **轻量执行载体落地（二选一）**：
     * **选项 A（GitHub Actions Cron，最轻便）**：直接配置 GitHub 定时工作流唤起推送脚本；
     * **选项 B（NUC K3s CronJob，局域网直连）**：在 K3s `batch-jobs` 下部署轻量 Python/Node 镜像，直连局域网 Trino 和 Slack API；
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
