package com.finance.etl.pipeline;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 财务研报生成流水线编排执行实体 (FinancialReporterPipeline)
 * 职责：
 * 严格遵循整洁架构与依赖倒置原则 (DIP)：
 * - Source<DwsSummaryRecord, ?, ?>: 宏观 DWS 聚合平账输入源
 * - Source<FinancialTransaction, ?, ?>: 微观 DWD 动账事实明细输入源
 * - FinancialReportBroadcastProcessFunction: 双流广播汇聚核心算子
 * - Sink<String>: (可选) 研报输出写端 (如文件输出、Iceberg 报表审计表落盘等)
 * 负责通过标准 Flink DataStream API 组装完整端到端 双流 -> 广播汇聚 (P=1) -> 投递/落盘拓扑图。
 */
public class FinancialReporterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialReporterPipeline.class);

    private final Source<DwsSummaryRecord, ?, ?> macroSource;
    private final Source<FinancialTransaction, ?, ?> microSource;
    private final FinancialReportBroadcastProcessFunction reportProcessFunction;
    private final Sink<String> sink;

    public FinancialReporterPipeline(Source<DwsSummaryRecord, ?, ?> macroSource,
                                    Source<FinancialTransaction, ?, ?> microSource,
                                    FinancialReportBroadcastProcessFunction reportProcessFunction) {
        this(macroSource, microSource, reportProcessFunction, null);
    }

    public FinancialReporterPipeline(Source<DwsSummaryRecord, ?, ?> macroSource,
                                    Source<FinancialTransaction, ?, ?> microSource,
                                    FinancialReportBroadcastProcessFunction reportProcessFunction,
                                    Sink<String> sink) {
        this.macroSource = Objects.requireNonNull(macroSource, "macroSource must not be null");
        this.microSource = Objects.requireNonNull(microSource, "microSource must not be null");
        this.reportProcessFunction = Objects.requireNonNull(reportProcessFunction, "reportProcessFunction must not be null");
        this.sink = sink;
    }

    /**
     * 编排构建 Flink 双流广播拓扑
     *
     * @param env Flink 流/批执行环境
     * @return 汇聚后的研报文本输出流
     */
    public SingleOutputStreamOperator<String> build(StreamExecutionEnvironment env) {
        Objects.requireNonNull(env, "StreamExecutionEnvironment must not be null");
        LOG.info("⚡ [FinancialReporterPipeline] Building Dual-Stream Flink Topology...");

        // 1. 读取宏观大盘流 (macroSource: Trino DWS 平账大盘指标，仅1行数据，作为全局对照真理源)
        // 🎯 核心注解: 使用 WatermarkStrategy.noWatermarks() 的原因：
        // 1) 批处理模式下数据为有界数据集 (Bounded Source)，读取结束即自然终止，无需水位线推算乱序延迟；
        // 2) 广播双流汇聚算子基于数据到达与内存状态缓存触发，未定义任何事件时间窗口 (Event-Time Window)；
        // 3) 关闭水位线生成可彻底消除后台无意义的水位线广播事件开销与多流时钟对齐等待，获得最高流水线传输性能。
        DataStream<DwsSummaryRecord> macroStream = env
                .fromSource(macroSource, WatermarkStrategy.noWatermarks(), "Macro-DwsSummary-Source")
                .uid("macro-dws-source");

        BroadcastStream<DwsSummaryRecord> broadcastMacroStream = macroStream
                .broadcast(FinancialReportBroadcastProcessFunction.MACRO_STATE_DESCRIPTOR);

        // 2. 读取微观明细流 (microSource: DWD 真实动账事实交易流水明细，为 AI 报告提供具体消费场景论据)
        // 同样声明 noWatermarks()，纯净消费有界批事实数据
        DataStream<FinancialTransaction> microStream = env
                .fromSource(microSource, WatermarkStrategy.noWatermarks(), "Micro-DwdTransaction-Source")
                .uid("micro-dwd-source");

        // 3. 双流连接汇聚并强制单例执行 (Parallelism = 1)
        SingleOutputStreamOperator<String> reportStream = microStream
                .connect(broadcastMacroStream)
                .process(reportProcessFunction)
                .name("FinancialReport-Broadcast-ProcessFunction")
                .uid("financial-report-process")
                .setParallelism(1);

        // 4. 若配置了持久化写端，则挂载输出 Sink
        if (sink != null) {
            LOG.info("💾 [FinancialReporterPipeline] Attaching sink connector to report stream...");
            reportStream.sinkTo(sink).name("FinancialReport-Sink").uid("financial-report-sink");
        }

        return reportStream;
    }

    public Source<DwsSummaryRecord, ?, ?> getMacroSource() {
        return macroSource;
    }

    public Source<FinancialTransaction, ?, ?> getMicroSource() {
        return microSource;
    }

    public FinancialReportBroadcastProcessFunction getReportProcessFunction() {
        return reportProcessFunction;
    }

    public Sink<String> getSink() {
        return sink;
    }
}
