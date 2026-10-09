package com.finance.etl.transform.report;

import com.finance.etl.agent.FinancialAdvisorAgent;
import com.finance.etl.client.SlackYuiClient;
import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialReportContext;
import com.finance.etl.model.FinancialReportRecord;
import com.finance.etl.model.FinancialTransaction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 财务分析研报 Flink 双流广播汇聚算子 (FinancialReportBroadcastProcessFunction)
 * 职责：
 * 1. 继承 Flink 原生 BroadcastProcessFunction，通过广播流接收 Trino 产出的 DWS 宏观大盘汇总行；
 * 2. 主流流式接收对应周期内的 DWD 微观事实动账流水；
 * 3. 强制全局单例执行 (Parallelism = 1)，保证上下文汇聚完整且消除多 Task 竞争；
 * 4. 批结束/触发时组装 FinancialReportContext，唤起 FinancialAdvisorAgent 生成 Markdown 研报；
 * 5. 组装并向下游发射结构化 FinancialReportRecord 实体，支撑 Flink 原生 IcebergR2Sink 落盘到 ads_financial_reports 事实表；
 * 6. 联动 SlackYuiClient 进行 Slack Block Kit 原生富媒体图片卡片投递。
 */
public class FinancialReportBroadcastProcessFunction
        extends BroadcastProcessFunction<FinancialTransaction, DwsSummaryRecord, FinancialReportRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FinancialReportBroadcastProcessFunction.class);

    public static final MapStateDescriptor<String, DwsSummaryRecord> MACRO_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "macro-dws-summary-state",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(DwsSummaryRecord.class)
            );

    // 周期类型: "DAILY" (日度), "WEEKLY" (周度), "MONTHLY" (月度)
    private final String periodType;

    /**
     * 🎯 核心注解: periodValue 的含义与来源生命周期：
     * 1) 【具体含义】: 表示当前财务研报对应的具体时间业务窗口取值。
     *    - 日度场景 (DAILY): 自然日 ISO 字符串，如 "2026-10-05" (YYYY-MM-DD)；
     *    - 周度场景 (WEEKLY): 周序号字符串，如 "2026-W40" (YYYY-Www)；
     *    - 月度场景 (MONTHLY): 自然月字符串，如 "2026-09" (YYYY-MM)。
     * 2) 【数据来源】: 由作业入口层 (FinancialReporterJob) 解析注入：
     *    - 方式 A: 外部 CLI 参数显式指定 (--date 2026-10-05 或 --month 2026-09)；
     *    - 方式 B: 定时任务缺省调度时，自动调用 FinancialLakehouseRepository 动态探测湖仓 View 中最新有动账流水的自然日。
     * 3) 【核心作用】: 
     *    - 作为广播状态 MapState 的唯一主键 (Key): ctx.getBroadcastState(DESCRIPTOR).put(periodValue, macroRecord)；
     *    - 注入 FinancialReportContext，作为时间事实基准告知 AI Agent，防止生成研报标题与日期时发生时序幻觉。
     */
    private final String periodValue;
    private final boolean postToSlack;
    private final int expectedMicroRecordCount; // 预期接收的主流微观记录总数，用于有界批处理精准触发发射

    private transient FinancialAdvisorAgent agent;
    private transient SlackYuiClient slackClient;
    private transient List<FinancialTransaction> microTransactionsBuffer;
    private transient int receivedMicroCount;
    private transient DwsSummaryRecord latestMacroSummary;
    private transient boolean reportGenerated;

    public FinancialReportBroadcastProcessFunction(String periodType, String periodValue) {
        this(periodType, periodValue, true, 0);
    }

    public FinancialReportBroadcastProcessFunction(String periodType, String periodValue, boolean postToSlack) {
        this(periodType, periodValue, postToSlack, 0);
    }

    public FinancialReportBroadcastProcessFunction(String periodType, String periodValue, boolean postToSlack, int expectedMicroRecordCount) {
        this.periodType = Objects.requireNonNull(periodType, "periodType must not be null");
        this.periodValue = Objects.requireNonNull(periodValue, "periodValue must not be null");
        this.postToSlack = postToSlack;
        this.expectedMicroRecordCount = expectedMicroRecordCount;
        this.microTransactionsBuffer = new ArrayList<>();
        this.receivedMicroCount = 0;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("⚡ [Report Broadcast Function] Initializing operator (period={}:{}, postSlack={}, expectedCount={})...",
                periodType, periodValue, postToSlack, expectedMicroRecordCount);
        this.agent = FinancialAdvisorAgent.fromConfig();
        this.slackClient = SlackYuiClient.fromConfig();
        this.microTransactionsBuffer = new ArrayList<>();
        this.receivedMicroCount = 0;
        this.latestMacroSummary = null;
        this.reportGenerated = false;
    }

    /**
     * 1. 主流事实交易处理入口 (processElement)
     * 🎯 核心注解:
     * - 数据来源链路：
     *   1) 来源绑定：由 FinancialReporterPipeline 中 microStream.connect(broadcastMacroStream) 显式声明；
     *      作为调用者的 microStream 走主管通道，其携带的每一笔 DWD 交易流水都会被 Flink 运行时自动分派至本方法；
     *   2) 装箱缓冲：内部通过 microTransactionsBuffer.add(transaction) 逐笔收拢入队，将整条流的数据完整收集；
     * - 结果发射时机 (Bug 根因与修复)：
     *   在 Flink 生命周期中，close() 方法没有 Collector 参数，因此不能在 close() 中发射数据；
     *   本算子在接收到最后一笔预期数据 (receivedMicroCount >= expectedMicroRecordCount) 时，
     *   在持有 Collector<FinancialReportRecord> out 的本方法内立即触发研报生成，并通过 out.collect(record)
     *   正式发射给下游挂载的 IcebergR2Sink，完成落盘与 Snapshot 提交！
     */
    private static final Pattern QUICKCHART_URL_PATTERN =
            Pattern.compile("(https?://quickchart\\.io/chart/render/[^\\s\\)\"]+)");

    @Override
    public void processElement(FinancialTransaction transaction,
                                ReadOnlyContext ctx,
                                Collector<FinancialReportRecord> out) throws Exception {
        if (transaction != null) {
            // 🎯 核心存入点：将 microStream 管道中流经的每一笔真实刷卡事实明细一笔不漏地收集至内存缓冲列表
            microTransactionsBuffer.add(transaction);
            receivedMicroCount++;
        }

        // 防御性同步：若当前尚未拿到宏观指标，尝试从只读广播状态中获取
        if (this.latestMacroSummary == null) {
            try {
                this.latestMacroSummary = ctx.getBroadcastState(MACRO_STATE_DESCRIPTOR).get(periodValue);
            } catch (Exception ignored) {}
        }

        // 🎯 关键修复：当预期记录数达到或为最后一笔时，立即在 processElement 内部唤起研报生成并正式 collect 发射给下游 Sink
        if (expectedMicroRecordCount > 0 && receivedMicroCount >= expectedMicroRecordCount) {
            LOG.info("🏁 [Report Broadcast Function] Reached expected micro count ({}/{}). Triggering report generation and collecting to downstream sink...",
                    receivedMicroCount, expectedMicroRecordCount);
            FinancialReportRecord record = triggerReportGeneration();
            if (record != null) {
                out.collect(record);
            }
        }
    }

    /**
     * 2. 广播流宏观大盘处理入口 (processBroadcastElement)
     * 🎯 核心注解:
     * - 入参 macroRecord 来源与生命周期：
     *   1) 它 100% 正是从上游广播流 (macroStream) 管道中流出来的实体对象！由 Flink Worker 线程从广播输入队列 (InputGate) 逐条拉取传入；
     *   2) 上游若发射 1 行 DWS 大盘，本方法在作业生命周期内即精准执行 1 次；若发射多行，则每来一条触发一次；
     * - 状态入库机制：
     *   Pipeline 中的 macroStream.broadcast(DESCRIPTOR) 仅仅是在【编译期/拓扑声明时】为这块状态划分了内存命名空间并建立广播连接通道；
     *   而真正将广播流送达的 macroRecord 实体【物理写入】Flink 状态仓库的动作，正是发生在这里！
     * - 权限安全：
     *   持有独占写权限的 Context，通过 ctx.getBroadcastState(DESCRIPTOR).put(periodValue, macroRecord) 
     *   将大盘平账真理源稳固持久化，供集群内本节点随时安全访问与故障自愈。
     */
    @Override
    public void processBroadcastElement(DwsSummaryRecord macroRecord,
                                        Context ctx,
                                        Collector<FinancialReportRecord> out) throws Exception {
        if (macroRecord != null) {
            LOG.info("📢 [Report Broadcast Function] Received macro DWS summary: statDate={}, netExpense={}, count={}",
                    macroRecord.getStatDate(), macroRecord.getNetExpense(), macroRecord.getTxCount());
            this.latestMacroSummary = macroRecord;
            // 🎯 真正的物理入库动作：将 1 行 DWS 宏观平账大盘写入受 Flink Checkpoint/Savepoint 托管的全局只读内存镜像中
            ctx.getBroadcastState(MACRO_STATE_DESCRIPTOR).put(periodValue, macroRecord);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("🏁 [Report Broadcast Function] Closing batch stream...");
        if (agent != null && microTransactionsBuffer != null && !reportGenerated) {
            // 批处理结束时，若尚未在 processElement 中发射则执行保底生成
            triggerReportGeneration();
        }
        super.close();
    }

    /**
     * 手动或结束时触发生成研报与发送，并输出结构化落盘实体
     */
    public FinancialReportRecord triggerReportGeneration() {
        if (reportGenerated) {
            LOG.info("ℹ️ [Report Broadcast Function] Report already generated and emitted, skipping duplicate call.");
            return null;
        }
        reportGenerated = true;

        LOG.info("🚀 [Report Broadcast Function] Assembling FinancialReportContext for period={}:{} with {} raw transactions",
                periodType, periodValue, microTransactionsBuffer.size());

        // 🎯 核心修复：优先取广播流送达的真实宏观 DWS 指标；若极端情况下为空，自适应从微观交易明细合成保底大盘
        DwsSummaryRecord macroSummary = this.latestMacroSummary;
        if (macroSummary == null) {
            LOG.warn("⚠️ [Report Broadcast Function] No macro summary received from broadcast stream, synthesizing fallback summary from micro buffer.");
            macroSummary = synthesizeFallbackSummary();
        }

        FinancialReportContext context = new FinancialReportContext(
                periodType,
                periodValue,
                macroSummary,
                microTransactionsBuffer
        );

        String report = agent.generateReport(context);
        LOG.info("✅ [Report Broadcast Function] Financial report generated successfully (length: {} chars)",
                report != null ? report.length() : 0);

        String slackStatus = "SKIPPED";
        if (postToSlack && slackClient != null && report != null) {
            if (!slackClient.getBotToken().isEmpty() && !slackClient.getBotToken().startsWith("mock-")) {
                LOG.info("📬 [Report Broadcast Function] Posting report with native image blocks to Slack...");
                boolean ok = slackClient.postReportWithBlocks(report);
                slackStatus = ok ? "SENT" : "FAILED";
            }
        }

        // 提取生成的全部图表短链并序列化为 JSON 数组字符串 (零 Schema 变更方案，完整持久化所有图表)
        String chartUrl = null;
        if (report != null) {
            Matcher m = QUICKCHART_URL_PATTERN.matcher(report);
            List<String> urls = new ArrayList<>();
            while (m.find()) {
                String u = m.group(1);
                if (!urls.contains(u)) {
                    urls.add(u);
                }
            }
            if (!urls.isEmpty()) {
                StringBuilder jsonBuilder = new StringBuilder("[");
                for (int i = 0; i < urls.size(); i++) {
                    jsonBuilder.append("\"").append(urls.get(i)).append("\"");
                    if (i < urls.size() - 1) {
                        jsonBuilder.append(",");
                    }
                }
                jsonBuilder.append("]");
                chartUrl = jsonBuilder.toString();
            }
        }

        // 组装落盘到 ads_financial_reports 的实体
        String reportId = String.format("report_%s_%s", periodType.toLowerCase(), periodValue);
        LocalDate reportDate = LocalDate.now();
        if ("DAILY".equalsIgnoreCase(periodType)) {
            try {
                reportDate = LocalDate.parse(periodValue);
            } catch (Exception ignored) {}
        } else if ("MONTHLY".equalsIgnoreCase(periodType)) {
            try {
                reportDate = LocalDate.parse(periodValue + "-01");
            } catch (Exception ignored) {}
        } else if ("WEEKLY".equalsIgnoreCase(periodType)) {
            try {
                java.time.format.DateTimeFormatter dtf = java.time.format.DateTimeFormatter.ISO_WEEK_DATE;
                reportDate = LocalDate.parse(periodValue + "-1", dtf);
            } catch (Exception ignored) {}
        }

        FinancialReportRecord record = new FinancialReportRecord();
        record.setReportId(reportId);
        record.setPeriodType(periodType);
        record.setPeriodValue(periodValue);
        record.setReportDate(reportDate);
        record.setTotalExpense(macroSummary.getTotalExpense());
        record.setTotalRefund(macroSummary.getTotalRefund());
        record.setNetExpense(macroSummary.getNetExpense());
        record.setTotalIncome(macroSummary.getTotalIncome());
        record.setTotalTransfer(macroSummary.getTotalTransfer());
        record.setTxCount(macroSummary.getTxCount() != null ? macroSummary.getTxCount() : (long) microTransactionsBuffer.size());
        record.setMetricsJson(macroSummary.toJson());
        record.setSummaryText(report);
        record.setChartUrl(chartUrl);
        record.setSlackStatus(slackStatus);
        record.setCreatedAt(Instant.now());

        return record;
    }

    /**
     * 当广播宏观流因极端时序未达时，自适应从微观交易流明细合成宏观大盘，彻底杜绝指标为 0 幻觉
     */
    private DwsSummaryRecord synthesizeFallbackSummary() {
        BigDecimal totalExpense = BigDecimal.ZERO;
        BigDecimal totalRefund = BigDecimal.ZERO;
        BigDecimal totalIncome = BigDecimal.ZERO;
        BigDecimal totalTransfer = BigDecimal.ZERO;
        BigDecimal foodExpense = BigDecimal.ZERO;
        BigDecimal transportExpense = BigDecimal.ZERO;
        BigDecimal onlineShoppingExpense = BigDecimal.ZERO;
        BigDecimal offlineShoppingExpense = BigDecimal.ZERO;
        BigDecimal medicalExpense = BigDecimal.ZERO;
        BigDecimal communicationExpense = BigDecimal.ZERO;
        BigDecimal insuranceExpense = BigDecimal.ZERO;
        BigDecimal propertyExpense = BigDecimal.ZERO;
        BigDecimal travelExpense = BigDecimal.ZERO;
        BigDecimal personalTransferExpense = BigDecimal.ZERO;
        BigDecimal otherExpense = BigDecimal.ZERO;

        for (FinancialTransaction tx : microTransactionsBuffer) {
            BigDecimal amt = tx.getAmount() != null ? tx.getAmount() : BigDecimal.ZERO;
            String type = tx.getTxType() != null ? tx.getTxType().toUpperCase() : "EXPENSE";
            String cat = tx.getCategory() != null ? tx.getCategory().toUpperCase() : "OTHER";

            switch (type) {
                case "EXPENSE" -> {
                    totalExpense = totalExpense.add(amt);
                    switch (cat) {
                        case "FOOD" -> foodExpense = foodExpense.add(amt);
                        case "TRANSPORT" -> transportExpense = transportExpense.add(amt);
                        case "ONLINE_SHOPPING" -> onlineShoppingExpense = onlineShoppingExpense.add(amt);
                        case "OFFLINE_SHOPPING" -> offlineShoppingExpense = offlineShoppingExpense.add(amt);
                        case "MEDICAL" -> medicalExpense = medicalExpense.add(amt);
                        case "COMMUNICATION" -> communicationExpense = communicationExpense.add(amt);
                        case "INSURANCE" -> insuranceExpense = insuranceExpense.add(amt);
                        case "PROPERTY_MANAGEMENT" -> propertyExpense = propertyExpense.add(amt);
                        case "TRAVEL" -> travelExpense = travelExpense.add(amt);
                        case "PERSONAL_TRANSFER" -> personalTransferExpense = personalTransferExpense.add(amt);
                        default -> otherExpense = otherExpense.add(amt);
                    }
                }
                case "REFUND" -> totalRefund = totalRefund.add(amt);
                case "INCOME" -> totalIncome = totalIncome.add(amt);
                case "TRANSFER" -> totalTransfer = totalTransfer.add(amt);
                default -> totalExpense = totalExpense.add(amt);
            }
        }

        BigDecimal netExpense = totalExpense.subtract(totalRefund);

        LocalDate statDate = null;
        if ("DAILY".equalsIgnoreCase(periodType)) {
            try {
                statDate = LocalDate.parse(periodValue);
            } catch (Exception ignored) {}
        }

        return new DwsSummaryRecord(
                periodType,
                periodValue,
                statDate,
                null,
                null,
                (long) microTransactionsBuffer.size(),
                totalExpense,
                totalRefund,
                netExpense,
                totalIncome,
                totalTransfer,
                foodExpense,
                transportExpense,
                onlineShoppingExpense,
                offlineShoppingExpense,
                medicalExpense,
                communicationExpense,
                insuranceExpense,
                propertyExpense,
                travelExpense,
                personalTransferExpense,
                otherExpense,
                null,
                null
        );
    }

    public void setLatestMacroSummary(DwsSummaryRecord latestMacroSummary) {
        this.latestMacroSummary = latestMacroSummary;
    }

    public List<FinancialTransaction> getMicroTransactionsBuffer() {
        return microTransactionsBuffer;
    }

    public void setAgent(FinancialAdvisorAgent agent) {
        this.agent = agent;
    }

    public void setSlackClient(SlackYuiClient slackClient) {
        this.slackClient = slackClient;
    }
}
