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
3. **数据访问层 (DAO)**：`FinancialDwsDao`，直连 Trino REST API 提取结构化指标；
4. **图表短链生成**：`QuickChartClient`，通过 `POST https://quickchart.io/chart/create` 交换标准短链。

---

## 2. 详细实施步骤 (Step-by-Step)

整个 M2 划分为 **5 个循序渐进的交付步骤**，步步有断言、步步有验证：

```text
[Step 2.1: 引入 Maven 依赖] 
             ⬇
[Step 2.2: 编写模型工厂 FinancialChatModelFactory] 
             ⬇
[Step 2.3: 编写数据访问 DAO FinancialDwsDao] 
             ⬇
[Step 2.4: 编写大模型专属武器库 @Tool (Lakehouse & Chart)] 
             ⬇
[Step 2.5: 编写声明式服务 FinancialAdvisorService 与智能体本体 FinancialAdvisorAgent]
             ⬇
[Step 2.6: 编写端到端单元测试与实盘生成验证]
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

### 步骤 2.3：编写 DWS 湖仓数据访问 DAO (`repository/FinancialDwsDao.java`)
* **职责**：专职负责向 Trino 发起轻量查询，从 M1 的视图中拉取结构化财务指标。
* **核心方法定义**：
  1. `queryDailySummary(LocalDate date)`: 从 `dws_financial_summary_daily` 读取单日净开销与五大类目切片；
  2. `queryWeeklySummary(int year, int week)`: 从 `dws_financial_summary_weekly` 读取周开销、周末 vs 工作日比例与 `max_id`；
  3. `queryMonthlySummary(String month)`: 从 `dws_financial_summary_monthly` 读取全月大盘、还款与理赔；
  4. `queryTopMerchants(String month, int limit)`: 从 `dws_merchant_spending_ranking` 提取消费前 N 名商户排行榜；
* **测试验证**：直连 Trino 查询真实 9 月份数据，断言返回的 `net_expense` 必须为 `￥22,679.46`。

---

### 步骤 2.4：构建 Agent 工具箱 (`tools/` · @Tool 声明)
将底层 DAO 与 QuickChart 能力，包装为大模型可自主决断调用的声明式函数：
1. **`FinancialLakehouseTools.java`**：
   * `@Tool("查询指定周期的消费大盘指标(总支出、退款、净开销、各分类金额)")`
   * `@Tool("查询当月消费最高的核心商户排行榜")`
2. **`FinancialChartTools.java`**：
   * `@Tool("根据给定的类目与金额生成环形饼图高清短链")`
   * `@Tool("根据商户排行榜生成横向柱状图高清短链")`
   * 底层封装 `QuickChartClient` 走 `POST` 短链交换，彻底防范裂图。

---

### 步骤 2.5：装配声明式服务与智能体本体
1. **声明式服务接口 (`service/FinancialAdvisorService.java`)**：
   * 使用 LangChain4j 的 `@SystemMessage` 固化 Yui 专属人设：
     * *“你是主人 Jason 的专属贴身财务秘书与特许金融分析师 Yui……”*
     * *“【铁律】：严禁捏造金额，必须基于工具返回的数据。大额还款作为资产划转对待……”*
   * 定义 `@UserMessage` 业务方法，自动完成 Prompt 变量替换。
2. **智能体实体类 (`agent/FinancialAdvisorAgent.java`)**：
   * 内置 `fromConfig()` 静态工厂方法，自动装配 Model、Tools 与 Agent 代理；
   * 对外暴露高内聚行为：
     * `String generateDailyReview(LocalDate date)`
     * `String generateWeeklyReview(int year, int week)`
     * `String generateMonthlyReview(String month)`

---

### 步骤 2.6：端到端集成测试与实盘报告验收 (`FinancialAdvisorAgentTest.java`)
* **核心用例**：
  * **用例 1 (工具自动调用与决策验证)**：给 Agent 传入指令：“帮我分析 2026 年 9 月的全月财务大盘”，断言大模型自主触发了 `queryMonthlySummary` 和 `createCategoryPieChart` 两次工具调用；
  * **用例 2 (金额真实性与无幻觉对账)**：断言 Agent 生成的文本中，必须准确包含 `22679.46`（净支出）和 `2790.59`（理赔款）；
  * **用例 3 (图表短链有效性)**：断言生成的报告中包含合法的 QuickChart 图片短链。

---

## 3. 产出类与代码结构对照表

| 类全限定名 | 职责类型 | 依赖组件 | 核心考量 |
| :--- | :--- | :--- | :--- |
| `com.finance.etl.model.FinancialChatModelFactory` | 模型工厂 | LangChain4j OpenAi | 统一注入 LiteLLM 与 Gemini 3.8 Flash |
| `com.finance.etl.repository.FinancialDwsDao` | 数据访问 DAO | Trino REST / HTTP | 负责执行 DWS 视图 SQL 查询 |
| `com.finance.etl.client.QuickChartClient` | 外部服务客户端 | JDK 21 HttpClient | 负责 POST 交换图表短链 |
| `com.finance.etl.tools.FinancialLakehouseTools` | Agent 武器库 | `FinancialDwsDao` | 暴露查数能力 (`@Tool`) |
| `com.finance.etl.tools.FinancialChartTools` | Agent 武器库 | `QuickChartClient` | 暴露画图能力 (`@Tool`) |
| `com.finance.etl.service.FinancialAdvisorService` | 声明式 AI 契约 | LangChain4j 注解 | 锁定 Yui 秘书人设与零幻觉红线 |
| `com.finance.etl.agent.FinancialAdvisorAgent` | 智能体实体本体 | `AiServices` | 提供 `fromConfig()` 与一键分析接口 |
| `com.finance.etl.agent.FinancialAdvisorAgentTest` | 自动化集成测试 | JUnit 5 | 验证全链路真实跑通与指标精确平账 |

---

## 4. 立即执行计划 (Action Items)

按照本步骤指南，我们立即从 **Step 2.1（修改 `pom.xml` 引入 LangChain4j）** 开始，依序推进至 Step 2.6 完成实盘验收！
