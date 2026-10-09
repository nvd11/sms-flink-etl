# Flink 批处理驱动的湖仓 AI 财务研报流水线：从双流广播汇聚到 Agent 智能决策与原生 Iceberg 落盘全景实战

在以数据湖仓（Lakehouse）与大语言模型（LLM）为核心的现代数据架构中，如何将底层的海量动账流水、经过会计复式平账后的宏观统计指标，与具备专业推理能力的大模型结合起来，自动产出一份兼具“宏观全局视野、微观鲜活论据、零数字幻觉、自带可视化图表”的专业财务研报，并完成“湖仓物理表持久化 ➕ 即时通讯工具原生富文本推送”的双写闭环，是一个兼具分布式计算复杂度与工程美感的挑战。

我们放弃了传统的“离线脚本调用大模型”或“Flink 算子内部穿透数据库查数”这类充满破绽的野生架构，依托 **Apache Flink 1.20**（运行于 `RuntimeExecutionMode.BATCH` 模式）、**Apache Iceberg 1.7** 列存湖仓、**Trino 471** 分布式 SQL 计算引擎、**LangChain4j 0.35** 声明式 Agent 体系、**QuickChart** 静态图表短链以及 **Slack Block Kit**，构建了一套标准的、整洁分层的批处理财务智能研报流水线。

本文将从数据源头开始，极度详尽地复盘整条流水线的数据流转全貌，覆盖双流广播、内存状态绑定、有界批发射、大模型多态调用以及湖仓原生落盘的每一个核心细节与关键技术决策。

---

## 1. 架构总览与数据血缘全景

整套系统围绕个人财务的日（Daily）、周（Weekly）、月（Monthly）三大周期运转。数据从最原始的非结构化银行短信开始，逐层提纯、聚合，并在最后的应用层（ADS）由 Flink 批作业驱动 AI 智能体完成研报生成与双写归档。

### 1.1 湖仓整体数据流向图

```text
[银行/支付机构短信邮件 (Gmail)]
              │
              ▼  (由 SmsGmailR2Job 写入)
【ODS 原始层】 iceberg.finance_dev.raw_sms_records
              │
              ▼  (由 SmsOdsToDwdJob 清洗、规则分类后写入)
【DWD 事实层】 iceberg.finance_dev.dwd_financial_transactions (物理表，Parquet 列存)
              │
              │ ⚡ (此处无物理落盘！由 Trino 引擎基于 SQL 实时动态聚合计算)
              ▼
【DWS 汇总层】 iceberg.finance_dev.dws_financial_summary_daily / monthly (Trino 动态视图 View)
              │
              │ 🔍 (由 FinancialLakehouseRepository / DataGeneratorSource 提取为有界输入源)
              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                 Flink 批处理研报流水线 (FinancialReporterJob / Pipeline)          │
│                                                                                 │
│   macroSource (1行大盘指标) ────> DataStream ────> .broadcast(DESCRIPTOR) ────┐ │
│                                                                               │ │
│   microSource (N条事实流水) ────> DataStream ─────────────────────────────────┼─┤ │
│                                                                               │ │
│                                         ┌─────────────────────────────────────┘ │
│                                         ▼                                       │
│                .connect(...) ──> FinancialReportBroadcastProcessFunction (P=1)  │
│                                         │                                       │
│                                         ▼                                       │
│                       [内存组装 FinancialReportContext]                         │
│                                         │                                       │
│                                         ▼ 唤起调用                              │
│                      [FinancialAdvisorAgent (纯计算大脑)]                       │
│                                         │                                       │
│                         ┌───────────────┴───────────────┐                       │
│                         ▼                               ▼                       │
│             [Gemini 3.8 Flash 模型推理]       [QuickChartClient 交换图表短链]     │
│                         └───────────────┬───────────────┘                       │
│                                         ▼                                       │
│                           [产出完整 Markdown 研报正文]                           │
│                                         │                                       │
│                         ┌───────────────┴───────────────┐                       │
│                         ▼                               ▼                       │
│             【分支 A: 即时通讯交互】           【分支 B: 湖仓原生持久化】             │
│             SlackYuiClient 原生图文直显         收集为 FinancialReportRecord 实体    │
│             推送到主人私聊 U0AM8G9AARF         经过 ReportRecordToRowDataMapper 映射 │
│                                                         │                       │
│                                                         ▼                       │
│                                            IcebergR2Sink 原生两阶段提交           │
│                                                         │                       │
└─────────────────────────────────────────────────────────┼───────────────────────┘
                                                          ▼
                               【ADS 应用层】 iceberg.finance_dev.ads_financial_reports (物理事实表)
```

---

## 2. 为什么要做“宏观（Macro）与微观（Micro）”双流分离？

在设计这套流水线时，我们没有采用常规思路中“把流水全查出来塞给大模型”或“在 Flink 算子里写个 JDBC 连接去查数据库”的做法。这背后有深思熟虑的领域建模与分布式性能考量：

### 2.1 财务分析方法论的必然要求
一篇具备专业特许金融分析师（CFA）水准的财务白皮书，严禁两种极端：
1. **只看流水明细（流水账陷阱）**：通篇列举今天买了哪杯奶茶、坐了几次地铁。看不到当期的宏观资产负债结构、恩格尔系数、保险防御敞口以及信用卡负债清偿，缺乏全局格局；
2. **只看汇总指标（干瘪数字陷阱）**：通篇只有干瘪的“本月餐饮支出占比 43%”、“医疗支出 ￥2,360”。大模型无法得知具体是在哪家医院看病、在大额宴请上到底光顾了哪家餐厅，缺乏鲜活的生活论据。

因此，研报必须做到**“宏微观穿透”**：既有已平账的宏观总盘约束（大模型绝不允许在总数之外胡编乱造），又有微观真实流水作为典型论据。

### 2.2 数据特性的物理不对称性
这两批数据在分布式系统中的物理形态截然不同：
* **宏观数据源 (`macroSource`)**：
  对应 Trino 中的 DWS 视图。数据量极小，一个统计周期内**往往只有 1 行记录**（包含净支出、消费毛额、冲正退款、理赔回血、信用卡划转、各大分类汇总等）。
* **微观数据源 (`microSource`)**：
  对应 DWD 事实物理表。数据量多，流水化分布（包含周期内每一笔交易的 ID、精确时间戳、商户名、金额、分类等）。

这种“一大一小”的不对称结构，天然决定了它们在流计算引擎中必须采用不同的管道组织方式。

---

## 3. 核心类职责划分与设计模式

系统严格遵循整洁架构（Clean Architecture）与面向接口编程的依赖倒置原则（DIP）。各层的角色分工非常纯粹：

| 模块分层 | 核心类全限定名 | 职责与角色定位 |
| :--- | :--- | :--- |
| **作业调度层 (Job)** | `com.finance.etl.jobs.FinancialReporterJob` | 外部生产调度的统一 Main 函数入口。解析 CLI 参数，初始化 Flink 批环境，探测湖仓数据并组装提交拓扑。 |
| **拓扑编排层 (Pipeline)** | `com.finance.etl.pipeline.FinancialReporterPipeline` | 纯 Flink DAG 流图编排器。接收抽象的 `Source` 与 `Sink` 接口，定义数据流向与连接方式，具备 100% 可测试性。 |
| **业务算子层 (Operator)** | `com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction` | 核心双流汇聚与状态中枢。在 Worker 内存中持有广播状态，管理微观明细缓冲，批结束时装配上下文驱动 Agent 并触发双写。 |
| **湖仓数据访问层 (DAO)** | `com.finance.etl.repository.FinancialLakehouseRepository` | 专职通过 Trino JDBC 查询 DWS 视图大盘与 DWD 事实流水，向外界屏蔽复杂 SQL。 |
| **智能体本体层 (Agent)** | `com.finance.etl.agent.FinancialAdvisorAgent` | 纯计算与推理大脑。不持有任何数据流状态，仅负责接收上下文模型，多态调用大模型生成纯 Markdown 研报。 |
| **AI 声明式契约层 (Service)** | `com.finance.etl.service.FinancialAdvisorService` | 基于 LangChain4j 的 `@SystemMessage` 固化 Yui 秘书人设与零幻觉红线。 |
| **图表与消息客户端层** | `com.finance.etl.client.QuickChartClient`<br>`com.finance.etl.client.SlackYuiClient` | 负责与外部 HTTP Web API 交互（QuickChart POST 交换短链，Slack 原生 Block Kit 图片卡片推送）。 |
| **湖仓列式映射层 (Mapper)** | `com.finance.etl.sink.iceberg.ReportRecordToRowDataMapper` | 纯函数、无状态。将研报业务实体映射为 Flink 底层列式内存行 `GenericRowData`。 |

---

## 4. 数据获取与输入源构建机制

整个流水线从 `FinancialReporterJob.main()` 开始启动，数据获取分为两大部分：

### 4.1 周期参数解析与湖仓探测
作业支持灵活的命令行启动参数：
* `--period DAILY --date 2026-10-05`：指定某一天，生成日度财务轻播报；
* `--period MONTHLY --month 2026-09`：指定某一月份，生成资产损益全景白皮书；
* **缺省自动探查机制**：若未传入任何参数，作业调用 `FinancialLakehouseRepository.queryLatestActiveDailySummary()`，通过 Trino SQL `ORDER BY stat_date DESC LIMIT 1` 自动锁定数据库中最新有动账支出的那个自然日。

### 4.2 为什么宏观汇总 DWS 不用物理表落盘，而是用 Trino View？
在整个架构中，`dws_financial_summary_daily` 和 `dws_financial_summary_monthly` 是定义在 Trino 上的动态 SQL 视图（定义见 `scripts/schema-dws.sql`）。
* **单一真理源**：数仓事实基石永远是 DWD 物理表 `dwd_financial_transactions`。一旦某笔交易被冲正、或通过 `DWD_OVERRIDE_OFFSET` 重新全量清洗历史数据，DWS 视图在被查询的瞬间自动计算出最新指标，不需要任何外部反向补偿任务去刷物理表。
* **极速元数据加速**：依托 Iceberg 底层针对分区和列存的 Min/Max 统计元数据，Trino 针对单日或单月的聚合查询在数十毫秒至数百毫秒内即可极速返回。

### 4.3 将数据封装为 Flink 有界批源
在 `FinancialReporterJob` 中，从 Trino 查出的 1 行宏观大盘实体 `DwsSummaryRecord` 和查出的全量明细列表 `List<FinancialTransaction>`，分别被包装为 Flink 标准的有界数据生成源：
```java
DataGeneratorSource<DwsSummaryRecord> macroSource = new DataGeneratorSource<>(
        index -> finalMacro,
        1L,
        RateLimiterStrategy.noOp(),
        TypeInformation.of(DwsSummaryRecord.class)
);

DataGeneratorSource<FinancialTransaction> microSource = new DataGeneratorSource<>(
        index -> finalMicro.get(index.intValue()),
        (long) finalMicro.size(),
        RateLimiterStrategy.noOp(),
        TypeInformation.of(FinancialTransaction.class)
);
```

---

## 5. 拓扑编排与 Flink 广播机制深度解构

在 `FinancialReporterPipeline.build(env)` 中，核心拓扑通过标准的 DataStream API 组装完成。这段代码虽然只有短短十几行，但每一行都包含了 Flink 底层的核心技术约束。

### 5.1 为什么声明 `WatermarkStrategy.noWatermarks()`？
```java
DataStream<DwsSummaryRecord> macroStream = env
        .fromSource(macroSource, WatermarkStrategy.noWatermarks(), "Macro-DwsSummary-Source");
DataStream<FinancialTransaction> microStream = env
        .fromSource(microSource, WatermarkStrategy.noWatermarks(), "Micro-DwdTransaction-Source");
```
Flink 的新 Source API 要求必须传入水位线策略。在此处显式使用 `noWatermarks()` 的原因在于：
1. **纯净的有界批处理**：作业显式运行于 `RuntimeExecutionMode.BATCH` 模式，数据读完即终止，天然拥有明确的边界（End of Input），不需要依靠水位线去推算数据是否延迟到达；
2. **算子内部未定义事件时间窗口**：下游并没有调用任何 `.window(TumblingEventTimeWindows.of(...))`；
3. **消除无效开销与多流时钟锁死**：广播流只有 1 行数据，早早就会结束；如果开启水位线推进，可能导致下游由于等待该通道的时间戳推进而陷入非预期的阻塞。声明 `noWatermarks()` 可以消除后台无意义的水位线广播事件开销。

### 5.2 `MACRO_STATE_DESCRIPTOR` 到底是什么？
在算子中声明的：
```java
public static final MapStateDescriptor<String, DwsSummaryRecord> MACRO_STATE_DESCRIPTOR =
        new MapStateDescriptor<>(
                "macro-dws-summary-state",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(DwsSummaryRecord.class)
        );
```
它是一个**状态描述符（State Descriptor）**，包含了三项核心要素：
1. **状态唯一标识名（`"macro-dws-summary-state"`）**：作为 Checkpoint / Savepoint 备份到存储系统时的命名空间；
2. **Key 的类型元数据**（`String`，即统计周期标识 `periodValue`，如 `"2026-09"`）；
3. **Value 的类型元数据**（`DwsSummaryRecord` 实体类），Flink 会据此为其自动生成高效的二进制序列化器 `PojoSerializer`。

### 5.3 调用 `.broadcast(DESCRIPTOR)` 的作用与本质
在 Pipeline 中：
```java
BroadcastStream<DwsSummaryRecord> broadcastMacroStream = macroStream
        .broadcast(FinancialReportBroadcastProcessFunction.MACRO_STATE_DESCRIPTOR);
```
这句代码运行在作业提交阶段（编译期），**它并没有真正往内存里存入数据**。它的核心作用有两点：
1. **网络传输层的物理切换**：
   普通流采用 Hash 或 Rebalance 通道，一条数据只发给下游一个 TaskExecutor；调用 `.broadcast()` 后，网络底层切换为 **`BroadcastPartitioner`**，上游发射的那 1 行大盘数据会被 **100% 全量网络克隆，下发给下游的每一个并发子任务（Subtask）**。
2. **拓扑类型的契约演进**：
   将普通 `DataStream` 提升为强类型的 `BroadcastStream`，并在算子状态仓库中开辟以该 Descriptor 命名的受托管存储槽位。

### 5.4 为什么不直接在 stream 里拿数据，非要去状态库里取？
在 Flink 中，`.connect()` 并不是 SQL 意义上那个把左右两张表拼成宽表一行的“物理 Join”。
* 流是**事件驱动且时间错开的**：广播大盘数据先流经管道，如果不把它放进一个存储容器中，大盘数据流过方法体之后立刻会被 GC 垃圾回收！后续流过来的微观刷卡流水根本找不到大盘数据。
* 双输入算子拥有两个完全独立的方法入口：`processElement`（只喂给流水事实）与 `processBroadcastElement`（只喂给大盘记录）。在处理单笔流水时，参数列表里根本没有大盘字段，只能通过 Descriptor 钥匙去算子受托管的本地状态库中查取。

### 5.5 为什么必须在算子上显式锁定 `.setParallelism(1)`？
```java
SingleOutputStreamOperator<FinancialReportRecord> reportStream = microStream
        .connect(broadcastMacroStream)
        .process(reportProcessFunction)
        .setParallelism(1);
```
* 在通常的高吞吐流计算中，主流数据会被切片（Partition）分发给多个并发 Worker 并行提速；
* **但在这个财务分析场景中，AI Agent 生成全盘研报时必须具备“上帝视角”**，需要纵观全局所有流水（比如挑选全月最大的单笔美团消费、在医疗类目中挑出陪护中心）。
* 如果允许并发切片，每个 Worker 只拿到残缺的三分之一流水，喂给 AI 的就是盲人摸象的残缺数据，会得出错误的分析结论。
* 因此，通过强制锁定 **`Parallelism = 1`**，确保下游算子在物理集群中仅有全局唯一单例，整条流水线的所有事实交易一笔不漏地完整收拢在同一个节点的缓冲区中。

---

## 6. 算子内部处理逻辑与 Agent 驱动细节

在核心算子 `FinancialReportBroadcastProcessFunction` 内部，数据的接收、持久化与触发分为三个清晰阶段：

### 6.1 阶段一：广播大盘真正的物理入库
在运行期，当 Trino DWS 视图里的那 1 行大盘数据流经管道时，Flink 自动回调：
```java
@Override
public void processBroadcastElement(DwsSummaryRecord macroRecord,
                                    Context ctx,
                                    Collector<FinancialReportRecord> out) throws Exception {
    if (macroRecord != null) {
        // 🎯 真正的物理入库动作：将 1 行 DWS 宏观平账大盘写入受 Flink Checkpoint/Savepoint 托管的内存镜像中
        ctx.getBroadcastState(MACRO_STATE_DESCRIPTOR).put(periodValue, macroRecord);
    }
}
```
* 传入的 `macroRecord` 正是从上游 `macroStream` 里读出的实体；
* 算子持有具有独占写权限的 `Context`，凭 `MACRO_STATE_DESCRIPTOR` 钥匙，以当前周期 `periodValue`（如 `"2026-10-05"`）为 Key，将其安全写入受 Flink 状态后端托管的内存存储区中。

### 6.2 阶段二：微观交易流式缓冲
主流中的刷卡交易流水逐笔到达时，Flink 回调：
```java
@Override
public void processElement(FinancialTransaction transaction,
                           ReadOnlyContext ctx,
                           Collector<FinancialReportRecord> out) throws Exception {
    if (transaction != null) {
        microTransactionsBuffer.add(transaction);
        receivedMicroCount++;
    }

    // 当预期记录数达到最后一笔时，立即在持有 Collector 的方法内触发生成并发射
    if (expectedMicroRecordCount > 0 && receivedMicroCount >= expectedMicroRecordCount) {
        FinancialReportRecord record = triggerReportGeneration();
        if (record != null) {
            out.collect(record);
        }
    }
}
```
* **权限防护**：主流端持有 `ReadOnlyContext`，在编译期被剥夺写权限，严禁修改广播状态，杜绝并发脏写。
* **数据装箱**：将流经的每笔流水存入 `microTransactionsBuffer` 列表。
* **有界批发事实测机制（Bug 根因与修复）**：
  在 Flink 算子生命周期中，`close()` 方法是没有 `Collector<T> out` 参数的！如果把调用 Agent 和发射逻辑写在 `close()` 里，数据根本无法通过 `out.collect(...)` 发射给下游 Sink，导致下游表永远是 0 条数据。
  因此，算子在构造时传入批数据总量 `expectedMicroRecordCount`，在处理到最后一笔数据时，于当前持有 `Collector` 的上下文中直接触发研报生成，并精准执行 `out.collect(record)`，将完整的研报事实发射给下游 `IcebergR2Sink`。

### 6.3 阶段三：AI Agent 纯计算大脑推理与图表生成
在 `triggerReportGeneration()` 中：
1. **组装数据胶囊**：将宏观平账指标与微观全量明细封装为 `FinancialReportContext`；
2. **结构化 Prompt 注入**：`FinancialReportPromptBuilder` 将上下文序列化为包含平账等式、类目明细、交易明细清单的 Markdown 文本；
3. **Agent 多态分流**：`FinancialAdvisorAgent.generateReport(context)` 根据周期类型（Daily / Weekly / Monthly）调用对应的专属方法，触发 LiteLLM 网关上的 `gemini-3.8-flash`；
4. **Tool Calling 动态画图**：
   * 大模型在思考链中识别到图表需求，发出工具调用请求；
   * `FinancialChartTools` 调用 `QuickChartClient`，通过 `POST https://quickchart.io/chart/create` 交换出高清图表短链（如 `https://quickchart.io/chart/render/zf-xxx`）；
   * 模型将短链内嵌至 Markdown 正文中，返回包含完整 CFA 洞察与温存点评的研报；
5. **Slack 原生图文直显**：
   * `SlackYuiClient.postReportWithBlocks(report)` 正则扫描 Markdown 中的 QuickChart 图片链接，自动将其拆分包装为 Slack Block Kit 的原生 `image` 块；
   * 正文按段落安全切片为 `section` 块（单块上限 3000 字符防护），直接推送到主人的 Slack 私信频道（`U0AM8G9AARF`）。

---

## 7. 湖仓原生持久化写端设计：`ads_financial_reports`

这是整条链路的终点——将 AI 生成的数字资产以原生 Iceberg 表的形式持久化存储在 Cloudflare R2 上。

### 7.1 DDL 表结构规格 (`scripts/schema-ads.sql`)
```sql
CREATE TABLE IF NOT EXISTS iceberg.finance_dev.ads_financial_reports (
    report_id           VARCHAR,                             -- 主键 (如 'report_daily_2026-10-05')
    period_type         VARCHAR,                             -- 'DAILY', 'WEEKLY', 'MONTHLY'
    period_value        VARCHAR,                             -- '2026-10-05', '2026-09'
    report_date         DATE,                                -- 归属日期 (隐藏分区字段)
    total_expense       DECIMAL(12, 2),                      -- 周期消费总额
    total_refund        DECIMAL(12, 2),                      -- 周期退款冲正
    net_expense         DECIMAL(12, 2),                      -- 周期净支出 (平账基准)
    total_income        DECIMAL(12, 2),                      -- 周期理赔回血
    total_transfer      DECIMAL(12, 2),                      -- 周期信用卡还款流水
    tx_count            BIGINT,                              -- 有效交易笔数
    metrics_json        VARCHAR,                             -- 结构化切片 JSON 快照
    summary_text        VARCHAR,                             -- 完整 Markdown 研报正文
    chart_url           VARCHAR,                             -- QuickChart 高清短链 URL
    slack_status        VARCHAR,                             -- 'SENT', 'FAILED', 'SKIPPED'
    created_at          TIMESTAMP(6) WITH TIME ZONE          -- 入湖时间戳
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(report_date)'],              -- Iceberg 隐藏分区
    sorted_by = ARRAY['report_date DESC']
);
```

### 7.2 实体映射：`ReportRecordToRowDataMapper`
将领域对象 `FinancialReportRecord` 映射为 Flink 列式内存行 `GenericRowData(15)`。
* 关键点：`report_date` 映射为以 epoch day（自 1970-01-01 以来的整型天数），以便底层触发 Iceberg 的 `month(report_date)` 隐藏分区路由。

### 7.3 流水线挂载写端与 Equality Delete 规则
在 `FinancialReporterPipeline.java` 中：
```java
IcebergR2Sink adsSink = IcebergR2Sink.fromConfig("ads_financial_reports", "report_id,report_date");
DataStream<RowData> rowStream = reportStream
        .map(new ReportRecordToRowDataMapper())
        .name("ReportRecord-To-RowData")
        .uid("report-to-rowdata");
adsSink.append(rowStream);
```
* **技术约束点**：在 Iceberg 分区表上开启 Upsert 模式时，Equality 字段**必须包含分区键对应的源字段**！因此这里显式声明 Equality 列为 `report_id,report_date`，使得重新运行同一天或同一个月的报表任务时，底层自动生成 Equality Delete 文件，平滑覆盖旧快照，天然具备幂等性。
* **单并发约束**：`IcebergR2Sink` 内部锁定 `writeParallelism = 1`，将研报单条记录整合成一个合法的 Parquet 数据文件，避免产生任何碎片文件，并在批处理结束时由 `IcebergFilesCommitter` 完成 Snapshot 提交。

### 7.4 零 Schema 改造的多图存储与起止日期拓展 (Multi-Chart JSON & Date Range Enrichment)
在满足“不变更既有 Iceberg 表结构”的工程约束下，系统对持久化字段进行了深度增强：
1. **多图无损归档 (`chart_url` 列)**：
   * 算子在生成报告后，自动提取 Markdown 中出现的全部 QuickChart 短链（去重处理）；
   * 将多张图表序列化为标准的 JSON 数组字符串（如 `["https://quickchart.io/...1", "https://quickchart.io/...2"]`）直接写入 `chart_url` 字段；
   * 在 Trino / DBeaver 中，可通过 `json_extract_scalar(chart_url, '$[0]')` 快速提取首图，或通过 `json_array_length(chart_url)` 统计图表总数；
   * 在 Java 领域模型 `FinancialReportRecord` 中，提供 `getChartUrlList()` 与 `getPrimaryChartUrl()` 保持双向无缝兼容。
2. **周期时间边界显式记录 (`metrics_json` 列)**：
   * 针对周度和月度报表，将 `startDate` 与 `endDate` 直接内嵌在 `metrics_json` 中（如 `"startDate":"2026-10-05","endDate":"2026-10-08"`）；
   * 业务方既可通过 JSON 属性直读起止日期，也可以通过基准锚定日 `report_date`（周一）在 SQL 中使用 `date_add('day', 6, report_date)` 衍生出自然周全区间。

### 7.5 调度决策、动态周感知与时间窗口重算机制 (Dispatching, Auto-Discovery & Idempotent Re-evaluation)
针对周度报表（Weekly Report）的触发逻辑，系统建立了清晰的决策中枢：
1. **调度双轨制策略**：
   * **显式指定模式 (`--week 2026-W40`)**：作业直接锁定指定周，用于历史周度研报的精准回补与重跑；
   * **自动感知模式 (缺省未传参)**：作业向 Trino DWS 视图发起探测查询：
     ```sql
     SELECT week_period FROM iceberg.finance_dev.dws_financial_summary_weekly
     WHERE tx_count > 0 ORDER BY week_period DESC LIMIT 1;
     ```
     以湖仓中实际存在动账流水的最高周作为目标周期（如当前探查到 `2026-W41`）。
2. **Summary 研报无流式水位设计**：
   * 传统的流式增量水位（`etl_sync_offsets`）服务于 ODS/DWD 层的数据去重与断点续传；
   * 而服务层的 Summary 研报**面向的是整个自然时间窗口的宏观平账**，属于典型的**时间窗口幂等全量核算**，不记录单调递增的流位点。
3. **同周内多次触发的自愈累加与覆盖机制**：
   * 若今天（10月9日）生成了 W41 研报，明天（10月10日周六）主人产生了新消费并再次触发作业，由于 10月10日 仍属于 2026-W41 窗口，系统依然会选定 `2026-W41`；
   * Trino DWS 视图会自动将 10月10日 的新流水纳入聚合，产出包含周六消费的更新版大盘；
   * Flink 通过 Iceberg Equality Delete（`report_id, report_date`）在写入时**自动覆盖替换旧的 W41 研报**，保证湖仓中永远只有一份最新、最完整的全周研报；
   * 只有当进入下周一（如 10月12日）且产生新周流水后，最新周探测才会自然跃迁至 `2026-W42`。

---

## 8. 端到端测试与真实落盘验证

为了确保整套复杂拓扑的严密性，测试套件分层覆盖：
1. **`FinancialReportBroadcastProcessFunctionTest`**：使用 Mock 依赖，在毫秒级内验证算子的缓冲、上下文组装与研报触发逻辑；
2. **`FinancialReporterPipelineTest`**：驱动本地 MiniCluster，验证双流连接拓扑能够正确生成 Flink 执行图（Execution Plan），证明通道连接无死锁；
3. **`FinancialReporterJobIntegrationTest`**：真实的端到端湖仓落盘集成测试。
   * 提交批作业并等待执行结束；
   * 通过官方 `IcebergGenerics.read(table)` 直接物理扫描 Cloudflare R2 上的表文件；
   * **扫描实测输出**：
     ```text
     2026-10-09 02:07:12.836 INFO  SnapshotScan - Scanning table finance_dev.ads_financial_reports snapshot 5746070051761336574
     📄 [Row 1] report_id=report_daily_2026-10-05, period=2026-10-05, date=2026-10-05, slack=SKIPPED
     ================================================================================
     📊 湖仓表 finance_dev.ads_financial_reports 当前记录总行数: 1 条
     ================================================================================
     ```
   * 证明整套从 Flink 双流广播 $\to$ 状态缓冲 $\to$ AI 生成 $\to$ 列式转换 $\to$ Iceberg 两阶段提交的完整链路 100% 真实打通闭环！

---

## 9. 总结

通过这套架构设计，我们达成了三个核心收益：
1. **架构纯粹性**：彻底消除了在计算算子中穿透查询数据库的反模式，通过 Flink 官方的广播流与状态后端解决了大流配小表的时空不对齐问题；
2. **数据严肃性**：大模型的一切推理基于湖仓已平账的权威大盘与真实动账明细，彻底杜绝了财务 AI 虚构金额的致命幻觉；
3. **工程健壮性**：实现了“Slack 移动端图文卡片原生直显”与“Iceberg V2 Parquet 物理列存落盘”的双写统一，为个人智能化财务中台奠定了工业级的技术底座。
