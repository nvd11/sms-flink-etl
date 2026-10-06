# Milestone 2: 财务分析智能体 (Agent) 详细实施步骤规范
## 基于 LangChain4j + LiteLLM (Gemini 3.8 Flash) 的 Tool Calling 智能分析系统

> **文档标识**：`docs/milestone-2-implementation-steps.md`  
> **制定时间**：2026-10-04  
> **所属阶段**：Milestone 2 (M2)  
> **依赖就绪**：`iceberg.finance.dws_financial_summary_*` (M1 已竣工 ✅)  
> **目标交付**：纯正 Java 声明式 Agent、湖仓 DWS 数据访问 DAO、@Tool 武器库、端到端测试套件

---

## 1. 架构目标与技术基线

在 M1 竣工后，日、周、月三大维度的财务指标已在 Trino DWS 视图中秒级就绪。  
M2 的核心使命是**为冰冷的数据注入智能的大脑与温存的人设**，使 Flink / 应用层具备自主调用工具查数、进行专业财务诊断与生成图表短链的能力。

### 技术基线选型：
1. **Agent 核心框架**：`LangChain4j` (`dev.langchain4j:langchain4j-open-ai:0.35.0`)
   * 优势：纯 POJO，零 Spring 容器绑定，支持反射声明式 `@SystemMessage` 与智能 `@Tool` 函数回调；
2. **底层大模型 (姿势 A · 统一网关)**：
   * 网关地址：`https://gw.jppwl.asia/litellm/v1` (通过 Starfive 千兆代理加速)；
   * 模型名称：**`gemini-3.8-flash`**；
   * 凭证绑定：Yui 专属虚拟 Key (`sk-WhW6BWdwKN_LITjCuAmgiA`)；
3. **数据输入层 (Flink 双流)**：统一由 Trino 网关提供宏观 DWS 视图流与微观 DWD Top 10 大额明细流；
4. **图表短链生成**：`QuickChartClient`，通过 `POST https://quickchart.io/chart/create` 交换标准短链。

---

## 2. 详细实施步骤 (Step-by-Step)

整个 M2 划分为 **8 个循序渐进的交付步骤**，步步有断言、步步有验证：

```text
[Step 2.1: 引入 Maven 依赖] 
             ⬇
[Step 2.2: 编写模型工厂 FinancialChatModelFactory] 
             ⬇
[Step 2.3: 编写图表基础设施 QuickChartClient 与 FinancialChartTools] 
             ⬇
[Step 2.4: 编写上下文模型 DwsSummaryRecord、FinancialReportContext 与 AI 契约 FinancialAdvisorService] 
             ⬇
[Step 2.5: 编写智能体本体 FinancialAdvisorAgent 与推送客户端 SlackYuiClient]
             ⬇
[Step 2.6: 编写 Flink 双流广播汇聚算子 FinancialReportBroadcastProcessFunction (P=1)]
             ⬇
[Step 2.7: 编写 Flink 流水线编排实体 FinancialReporterPipeline]
             ⬇
[Step 2.8: 编写 Flink 统一批作业 FinancialReporterJob 与全链路端到端验收]
```

---

### 步骤 2.1：引入 LangChain4j 依赖 (`pom.xml`)
在 `pom.xml` 中引入轻量级 OpenAI 兼容包（完全适配 LiteLLM 网关协议）：
```xml
<dependency>
    <groupId>dev.langchain4j</groupId>
    <artifactId>langchain4j-open-ai</artifactId>
    <version>0.35.0</version>
</dependency>
```
* **检查点**：执行 `mvn compile`，确保没有 ClassLoader 冲突，依赖树干净。

---

### 步骤 2.2：构建模型工厂门面 (`model/FinancialChatModelFactory.java`)
* **职责**：单一真理源。统一管理大模型连接参数、超时重试、温度系数与日志通道。
* **核心规范**：
  * 读取环境变量 `LLM_API_BASE` (默认: `https://gw.jppwl.asia/litellm/v1`)；
  * 读取环境变量 `LLM_API_KEY` (默认: `sk-WhW6BWdwKN_LITjCuAmgiA`)；
  * 读取环境变量 `LLM_MODEL_NAME` (默认: `gemini-3.8-flash`)；
  * 锁定 `temperature(0.3)`：消除财务虚构幻觉，保证严谨度；
  * 开启 `logRequests(true)` 和 `logResponses(true)` 便于控制台观测思维链；
* **测试验证**：编写单测验证模型连通性与简单问答。

---

### 步骤 2.3：配置 Flink 统一 Trino 双流数据源 (Trino Dual JdbcSource)
* **核心思路**：彻底弃用“算子内通过 DAO 查数”的破绽反模式，同时避免直接读取底层 Iceberg S3A 裸文件的繁重依赖，统一由 Trino 网关提供双流输入并进行排序下推：
  1. **宏观指标流 (`JdbcSource<DwsSummaryRecord>`)**：
     * 通过 Flink 原生 JDBC Connector 连接 Trino (`io.trino.jdbc.TrinoDriver`)；
     * 读取 `dws_financial_summary_*` 视图，以流式发射精准平账的周期大盘行（包含净支出、还款、理赔、分类汇总、max_id）；
  2. **微观代表性交易流 (`JdbcSource<FinancialTransaction>`) 与 Flink Batch 分级聚合策略**：
     * **日度轻播报 (Daily)**：采用 **全局 Top 3~5 策略**，直接由 Trino 下推 `WHERE date(tx_time) = :date ORDER BY amount DESC LIMIT 3`，聚焦全天核心大单；
     * **周复盘 / 月度白皮书 (Weekly / Monthly)**：采用 **按分类 Top 1 代表作策略 (Top-N per Category)**，在 Flink 算子内通过优先队列（小顶堆）或 Trino 窗口函数 `ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) <= 1` 下推获取各核心分类的最具代表性动账，确保打车、外卖、商超、医疗各维度均有鲜活案例，杜绝单一巨额支出掩盖其他维度的失语现象；
     * 毫秒级直接产出最具代表性的结构化明细，避免全量流水传输，将 Prompt Token 严格收敛在数百以内。

---

### 步骤 2.4：构建图表短链工具与全局单例广播汇聚算子
1. **图表短链生成客户端与工具 (`client/QuickChartClient.java` & `tools/FinancialChartTools.java`)**：
   * 封装 QuickChart 的官方 `POST https://quickchart.io/chart/create` 短链换取，杜绝中文 GET 编码截断与 Slack 卡片裂图；
   * 向 AI Agent 暴露 `@Tool` 方法：`createCategoryPieChart` 与 `createMerchantBarChart`；
   * *(注：因双流已全量喂入宏观与微观数据，无需再向 Agent 暴露湖仓查数 Tool，避免模型盲目查数与幻觉)*。
2. **双流广播汇聚算子 (`com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction`)**：
   * 继承 Flink `BroadcastProcessFunction`，**显式声明 `.setParallelism(1)` 全局单例执行**；
   * **广播端 (`processBroadcastElement`)**：接收 DWS 宏观大盘统计行并存入广播状态；
   * **数据流端 (`processElement`)**：流式接收微观 Top 10 真实交易案例；
   * **批结束触发器**：组装包含宏观数字与微观案例的完整 `FinancialReportContext`，投喂给 AI 智能体生成报告。

---

### 步骤 2.5：装配声明式服务与智能体本体
1. **声明式服务接口 (`service/FinancialAdvisorService.java`)**：
   * 使用 LangChain4j 的 `@SystemMessage` 固化 Yui 专属人设：
     * *“你是主人 Jason 的专属贴身财务秘书与特许金融分析师 Yui……”*
     * *“【铁律】：严禁捏造金额，必须基于输入的宏观指标与微观明细进行分析……”*
     * *“必须主动调用绘图工具生成分类饼图与商户柱状图短链……”*
   * 定义 `@UserMessage` 业务方法，自动完成 Prompt 上下文变量注入。
2. **智能体实体类 (`agent/FinancialAdvisorAgent.java`)**：
   * 内置 `fromConfig()` 静态工厂方法，自动装配 Model 与 `FinancialChartTools`；
   * 对外暴露高内聚行为：
     * `String generateReport(FinancialReportContext context)`
3. **Slack 投递客户端 (`client/SlackYuiClient.java`)**：
   * 使用 Bot Token 直连 Slack Web API，组装 Block Kit 富文本卡片推送到主人频道 (`U0AM8G9AARF`)。

---

### 步骤 2.6：编写 Flink 双流广播汇聚算子 (`FinancialReportBroadcastProcessFunction`)
* **包路径**：`com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction`
* **职责**：继承 Flink `BroadcastProcessFunction`，**显式声明 `.setParallelism(1)` 全局单例执行**；
  * **广播端 (`processBroadcastElement`)**：接收 DWS 宏观大盘统计行并存入广播状态；
  * **数据流端 (`processElement`)**：流式接收微观交易明细，维护 Top-N 案例缓冲池；
  * **批结束触发器**：组装包含宏观数字与微观案例的完整 `FinancialReportContext`，投喂给 AI 智能体生成研报。

---

### 步骤 2.7：编写 Flink 流水线编排实体 (`FinancialReporterPipeline`)
* **包路径**：`com.finance.etl.pipeline.FinancialReporterPipeline`
* **职责**：遵循整洁架构与依赖倒置原则 (DIP)，严格对标 `SmsOdsToDwdPipeline` 范式。
  * 接收外部注入的 `Source<DwsSummaryRecord, ?, ?>`（宏观流源）与 `Source<FinancialTransaction, ?, ?>`（微观流源）；
  * 编排 `microStream.connect(macroBroadcastStream).process(reportFunction).setParallelism(1)` 数据流拓扑；
  * 提供可插拔的 Sink 输出挂载能力（支持 Mock 输出与真实投递验证）。

---

### 步骤 2.8：编写 Flink 统一批作业与全链路端到端验收 (`FinancialReporterJob` & 测试套件)
1. **统一批作业启动入口 (`com.finance.etl.jobs.FinancialReporterJob`)**：
   * 负责 Flink `StreamExecutionEnvironment` 批模式配置；
   * 解析命令行运行参数（支持 `--period DAILY/WEEKLY/MONTHLY` 与日期范围）；
   * 通过 Trino JDBC 构建双流输入源，委托 `FinancialReporterPipeline` 编排提交作业。
2. **端到端集成测试与实盘报告验收 (`FinancialAdvisorAgentTest.java` & `FinancialReporterPipelineTest.java`)**：
   * **用例 1 (双流上下文驱动报告生成)**：给 Agent 传入封装好 9 月宏观指标与微观明细的 `FinancialReportContext`，验证大模型自主调用 `createCategoryPieChart` 与 `createMerchantBarChart` 生成短链；
   * **用例 2 (金额真实性与平账验证)**：断言 Agent 生成的研报中，净支出（`22443.56`）、理赔（`2790.59`）与明细案例金额完全严丝合缝；
   * **用例 3 (Slack 卡片交付验收)**：断言生成的 Block Kit 卡片可成功投递且图片短链在客户端直接渲染显示。

---

## 3. 产出类与代码结构对照表

| 类全限定名 | 职责类型 | 依赖组件 | 核心考量 |
| :--- | :--- | :--- | :--- |
| `com.finance.etl.model.FinancialChatModelFactory` | 模型工厂 | LangChain4j OpenAi | 统一注入 LiteLLM 与 Gemini 3.8 Flash |
| `com.finance.etl.model.FinancialReportContext` | 领域模型 | Lombok / POJO | 承载双流汇聚后的宏观指标与微观明细 |
| `com.finance.etl.client.QuickChartClient` | 外部服务客户端 | JDK 21 HttpClient | 负责 POST 交换图表短链 |
| `com.finance.etl.client.SlackYuiClient` | 外部服务客户端 | JDK 21 HttpClient | 负责向主人 Slack 推送 Block Kit 原生图片卡片 |
| `com.finance.etl.tools.FinancialChartTools` | Agent 武器库 | `QuickChartClient` | 暴露画图能力 (`@Tool`) |
| `com.finance.etl.service.FinancialAdvisorService` | 声明式 AI 契约 | LangChain4j 注解 | 锁定 Yui 秘书人设与零幻觉红线 |
| `com.finance.etl.agent.FinancialAdvisorAgent` | 智能体实体本体 | `AiServices` | 提供 `fromConfig()` 与一键分析接口 |
| `com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction` | Flink 算子 | Flink Streaming API | 负责双流广播汇聚与上下文打包 (P=1) |
| `com.finance.etl.pipeline.FinancialReporterPipeline` | Flink 拓扑编排 | Flink DataStream API | 规范化双流流图编排 (DIP 原则) |
| `com.finance.etl.jobs.FinancialReporterJob` | Flink 批作业 | Flink Batch Pipeline | 统一入口，挂载双流并执行端到端闭环 |

---

## 4. 立即执行计划 (Action Items)

按照本步骤指南，我们立即从 **Step 2.1** 开始依序推进至 **Step 2.8** 完成全链路工程化闭环！
