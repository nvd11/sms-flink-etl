package com.finance.etl.pipeline;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialReportRecord;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.sink.iceberg.IcebergR2Sink;
import com.finance.etl.sink.iceberg.ReportRecordToRowDataMapper;
import com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
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
 * - Sink<FinancialReportRecord>: (可选) 研报输出写端，若未显式指定，默认自动装配挂载 Flink 原生 IcebergR2Sink 直写 Cloudflare R2 ads_financial_reports 事实表
 * 负责通过标准 Flink DataStream API 组装完整端到端 双流 -> 广播汇聚 (P=1) -> 投递/落盘拓扑图。
 */
public class FinancialReporterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialReporterPipeline.class);

    private final Source<DwsSummaryRecord, ?, ?> macroSource;
    private final Source<FinancialTransaction, ?, ?> microSource;
    private final FinancialReportBroadcastProcessFunction reportProcessFunction;
    private final Sink<FinancialReportRecord> sink;

    public FinancialReporterPipeline(Source<DwsSummaryRecord, ?, ?> macroSource,
                                    Source<FinancialTransaction, ?, ?> microSource,
                                    FinancialReportBroadcastProcessFunction reportProcessFunction) {
        this(macroSource, microSource, reportProcessFunction, null);
    }

    public FinancialReporterPipeline(Source<DwsSummaryRecord, ?, ?> macroSource,
                                    Source<FinancialTransaction, ?, ?> microSource,
                                    FinancialReportBroadcastProcessFunction reportProcessFunction,
                                    Sink<FinancialReportRecord> sink) {
        this.macroSource = Objects.requireNonNull(macroSource, "macroSource must not be null");
        this.microSource = Objects.requireNonNull(microSource, "microSource must not be null");
        this.reportProcessFunction = Objects.requireNonNull(reportProcessFunction, "reportProcessFunction must not be null");
        this.sink = sink;
    }

    /**
     * 编排构建 Flink 双流广播拓扑
     *
     * @param env Flink 流/批执行环境
     * @return 汇聚后的研报结构化实体输出流
     */
    public SingleOutputStreamOperator<FinancialReportRecord> build(StreamExecutionEnvironment env) {
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

        // 🎯 核心注解: 调用 .broadcast(DESCRIPTOR) 与直接 connect 普通 macroStream 的根本区别：
        // 1) 【网络传输模式】: 
        //    - 若仅 connect 普通流，底层采用单播/哈希通道，1 行大盘数据会被网络随机丢给某一个 TaskSlot，其余并发节点将永远拿不到大盘；
        //    - 调用 .broadcast() 后，底层网络路由切换为 BroadcastPartitioner，数据被 100% 全量网络克隆，
        //      下游不论有 1 个、10 个还是 100 个并发 Subtask，每个节点的本地 JVM 堆内存中都能独立、完整持有一份权威大盘镜像！
        // 2) 【流类型与算子支持】: 
        //    - 普通流连接仅返回 ConnectedStreams (仅支持挂载 CoProcessFunction)；
        //    - 广播流连接返回专属 BroadcastConnectedStream，下游得以规范挂载 BroadcastProcessFunction，
        //      受 Flink 状态后端托管管理，并在编译期强制对主流执行 ReadOnlyContext 只读隔离，彻底杜绝并发脏写。
        BroadcastStream<DwsSummaryRecord> broadcastMacroStream = macroStream
                .broadcast(FinancialReportBroadcastProcessFunction.MACRO_STATE_DESCRIPTOR);

        // 2. 读取微观明细流 (microSource: DWD 真实动账事实交易流水明细，为 AI 报告提供具体消费场景论据)
        // 同样声明 noWatermarks()，纯净消费有界批事实数据
        DataStream<FinancialTransaction> microStream = env
                .fromSource(microSource, WatermarkStrategy.noWatermarks(), "Micro-DwdTransaction-Source")
                .uid("micro-dwd-source");

        // 3. 双流连接汇聚并强制单例执行 (Parallelism = 1)
        // 🎯 核心注解: 为什么即使使用了广播流，这里仍然必须显式锁定 .setParallelism(1)？
        // - 普通流计算中，microStream 往往被切片并发分发给不同 Subtask 处理；
        // - 但在本报表生成业务中，AI Agent 生成全景研报必须整吞周期内的全局明细 (挑选全局最高大额大单与各分类代表作)；
        // - 若切片分散到多个 Worker，各节点拿到的将是残缺流水，造成 AI 盲人摸象；
        // - 因此通过 .setParallelism(1) 强制单例汇聚，保证全量事实流水一笔不漏地完整收拢在唯一节点的内存缓冲区中投喂给 AI！
        SingleOutputStreamOperator<FinancialReportRecord> reportStream = microStream
                .connect(broadcastMacroStream)
                .process(reportProcessFunction)
                .name("FinancialReport-Broadcast-ProcessFunction")
                .uid("financial-report-process")
                .setParallelism(1);

        // 4. 挂载持久化写端
        // 优先使用外部注入的自定义 Sink；若未提供，默认装配 Flink 原生 IcebergR2Sink 直写 ads_financial_reports 物理表
        if (sink != null) {
            LOG.info("💾 [FinancialReporterPipeline] Attaching custom sink connector to report stream...");
            reportStream.sinkTo(sink).name("FinancialReport-Custom-Sink").uid("financial-report-custom-sink");
        } else {
            LOG.info("🧊 [FinancialReporterPipeline] Auto-mounting native IcebergR2Sink to ads_financial_reports table...");
            // 🎯 在 Iceberg 分区表上开启 Upsert 模式时，Equality 字段必须包含分区键源字段 report_date
            IcebergR2Sink adsSink = IcebergR2Sink.fromConfig("ads_financial_reports", "report_id,report_date");
            DataStream<RowData> rowStream = reportStream
                    .map(new ReportRecordToRowDataMapper())
                    .name("ReportRecord-To-RowData")
                    .uid("report-to-rowdata");
            adsSink.append(rowStream);
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

    public Sink<FinancialReportRecord> getSink() {
        return sink;
    }
}
