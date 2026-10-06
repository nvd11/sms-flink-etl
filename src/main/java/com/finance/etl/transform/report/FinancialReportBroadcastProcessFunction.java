package com.finance.etl.transform.report;

import com.finance.etl.agent.FinancialAdvisorAgent;
import com.finance.etl.client.SlackYuiClient;
import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialReportContext;
import com.finance.etl.model.FinancialTransaction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 财务分析研报 Flink 双流广播汇聚算子 (FinancialReportBroadcastProcessFunction)
 * 职责：
 * 1. 继承 Flink 原生 BroadcastProcessFunction，通过广播流接收 Trino 产出的 DWS 宏观大盘汇总行；
 * 2. 主流流式接收对应周期内的 DWD 微观事实动账流水；
 * 3. 强制全局单例执行 (Parallelism = 1)，保证上下文汇聚完整且消除多 Task 竞争；
 * 4. 批结束/触发时组装 FinancialReportContext，唤起 FinancialAdvisorAgent 生成 Markdown 研报；
 * 5. 联动 SlackYuiClient 进行 Slack Block Kit 原生富媒体图片卡片投递，同时向下游发射生成的研报文本。
 */
public class FinancialReportBroadcastProcessFunction
        extends BroadcastProcessFunction<FinancialTransaction, DwsSummaryRecord, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FinancialReportBroadcastProcessFunction.class);

    public static final MapStateDescriptor<String, DwsSummaryRecord> MACRO_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "macro-dws-summary-state",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(DwsSummaryRecord.class)
            );

    private final String periodType;
    private final String periodValue;
    private final boolean postToSlack;

    private transient FinancialAdvisorAgent agent;
    private transient SlackYuiClient slackClient;
    private transient List<FinancialTransaction> microTransactionsBuffer;

    public FinancialReportBroadcastProcessFunction(String periodType, String periodValue) {
        this(periodType, periodValue, true);
    }

    public FinancialReportBroadcastProcessFunction(String periodType, String periodValue, boolean postToSlack) {
        this.periodType = Objects.requireNonNull(periodType, "periodType must not be null");
        this.periodValue = Objects.requireNonNull(periodValue, "periodValue must not be null");
        this.postToSlack = postToSlack;
        this.microTransactionsBuffer = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("⚡ [Report Broadcast Function] Initializing operator (period={}:{}, postSlack={})...",
                periodType, periodValue, postToSlack);
        this.agent = FinancialAdvisorAgent.fromConfig();
        this.slackClient = SlackYuiClient.fromConfig();
        this.microTransactionsBuffer = new ArrayList<>();
    }

    @Override
    public void processElement(FinancialTransaction transaction,
                               ReadOnlyContext ctx,
                               Collector<String> out) throws Exception {
        if (transaction != null) {
            microTransactionsBuffer.add(transaction);
        }
    }

    @Override
    public void processBroadcastElement(DwsSummaryRecord macroRecord,
                                        Context ctx,
                                        Collector<String> out) throws Exception {
        if (macroRecord != null) {
            LOG.info("📢 [Report Broadcast Function] Received macro DWS summary: statDate={}, netExpense={}, count={}",
                    macroRecord.getStatDate(), macroRecord.getNetExpense(), macroRecord.getTxCount());
            ctx.getBroadcastState(MACRO_STATE_DESCRIPTOR).put(periodValue, macroRecord);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("🏁 [Report Broadcast Function] Closing batch stream. Triggering AI report generation...");
        DwsSummaryRecord macroSummary = null;
        if (agent != null && microTransactionsBuffer != null) {
            // 从缓冲或外部构造上下文
            // 批处理结束时，若尚未发射则执行组装
            triggerReportGeneration();
        }
        super.close();
    }

    /**
     * 手动或结束时触发生成研报与发送
     */
    public String triggerReportGeneration() {
        LOG.info("🚀 [Report Broadcast Function] Assembling FinancialReportContext for period={}:{} with {} raw transactions",
                periodType, periodValue, microTransactionsBuffer.size());

        // 构造宏观备底或回退大盘（若广播状态已在批模式结束）
        DwsSummaryRecord macroSummary = new DwsSummaryRecord();
        macroSummary.setStatDate(null);

        FinancialReportContext context = new FinancialReportContext(
                periodType,
                periodValue,
                macroSummary,
                microTransactionsBuffer
        );

        String report = agent.generateReport(context);
        LOG.info("✅ [Report Broadcast Function] Financial report generated successfully (length: {} chars)",
                report != null ? report.length() : 0);

        if (postToSlack && slackClient != null && report != null) {
            if (!slackClient.getBotToken().isEmpty() && !slackClient.getBotToken().startsWith("mock-")) {
                LOG.info("📬 [Report Broadcast Function] Posting report with native image blocks to Slack...");
                slackClient.postReportWithBlocks(report);
            }
        }
        return report;
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
