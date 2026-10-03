package com.finance.etl.pipeline;

import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.sink.iceberg.FinancialTransactionToRowDataMapper;
import com.finance.etl.sink.iceberg.IcebergR2Sink;
import com.finance.etl.transform.dwd.SmsRecordToDwdTransactionMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * DWD 动账事实流水线编排执行实体 (SmsOdsToDwdPipeline)
 * 职责：
 * 严格遵循依赖倒置原则 (DIP) 与整洁架构规范：
 * - Source<SmsRecord, ?, ?>：抽象数据输入源 (支持 IcebergR2Source 读 ODS 表、Kafka 或测试 Mock 源)
 * - MapFunction<SmsRecord, FinancialTransaction>：领域模型规整与多提取器编排算子
 * - IcebergR2Sink：湖仓写端实体门面 (支持 R2 落盘写入 dwd_financial_transactions，具备单写漏斗与 Upsert 能力)
 * 负责通过标准 Flink DataStream API 组装完整端到端 ODS -> DWD 数据流拓扑图。
 */
public class SmsOdsToDwdPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SmsOdsToDwdPipeline.class);

    private final Source<SmsRecord, ?, ?> source;
    private final MapFunction<SmsRecord, FinancialTransaction> dwdMapper;
    private final IcebergR2Sink sink;

    public SmsOdsToDwdPipeline(Source<SmsRecord, ?, ?> source) {
        this(source, new SmsRecordToDwdTransactionMapper(), null);
    }

    public SmsOdsToDwdPipeline(Source<SmsRecord, ?, ?> source,
                              MapFunction<SmsRecord, FinancialTransaction> dwdMapper) {
        this(source, dwdMapper, null);
    }

    public SmsOdsToDwdPipeline(Source<SmsRecord, ?, ?> source,
                              MapFunction<SmsRecord, FinancialTransaction> dwdMapper,
                              IcebergR2Sink sink) {
        this.source = Objects.requireNonNull(source, "Source connector must not be null");
        this.dwdMapper = Objects.requireNonNull(dwdMapper, "DWD mapper function must not be null");
        this.sink = sink;
    }

    /**
     * 官方正统 FLIP-27 数据流拓扑编排 (构建 FinancialTransaction 领域流)
     */
    public DataStream<FinancialTransaction> buildStream(StreamExecutionEnvironment env) {
        LOG.info("🌊 [DwdPipeline] Assembling ODS -> DWD DataStream topology with abstract Source & DWD Mapper...");

        // 1. 接入 Flink 官方标准 FLIP-27 抽象数据源 (从 ODS raw_sms_records 读取)
        DataStream<SmsRecord> odsStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Iceberg-ODS-Source"
        );

        // 2. 挂载 DWD 业务规整算子 (编排所有 Extractors 提取维度)
        return odsStream
                .map(dwdMapper)
                .name("SmsRecord-To-DwdTransaction-Mapper");
    }

    /**
     * 组装完整端到端流图并挂载 Iceberg DWD 事实表 Sink
     */
    public DataStreamSink<?> assembleAndAttachSink(StreamExecutionEnvironment env) {
        DataStream<FinancialTransaction> dwdStream = buildStream(env);

        if (sink == null) {
            LOG.warn("⚠️ [DwdPipeline] No Iceberg Sink configured. Falling back to stdout print sink.");
            return dwdStream.print().name("DwdTransaction-Print-Sink");
        }

        // 3. 转换为列式 RowData 并挂载 IcebergSink (writeParallelism=1, 写入 dwd_financial_transactions)
        DataStream<RowData> rowStream = dwdStream
                .map(new FinancialTransactionToRowDataMapper())
                .name("FinancialTransaction-To-RowData-Mapper");

        return sink.append(rowStream);
    }

    public Source<SmsRecord, ?, ?> getSource() {
        return source;
    }

    public MapFunction<SmsRecord, FinancialTransaction> getDwdMapper() {
        return dwdMapper;
    }

    public IcebergR2Sink getSink() {
        return sink;
    }
}
