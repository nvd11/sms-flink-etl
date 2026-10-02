package com.finance.etl.pipeline;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.sink.iceberg.IcebergR2Sink;
import com.finance.etl.sink.iceberg.SmsRecordToRowDataMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 流水线执行实体对象 (SmsGmailR2Pipeline)
 * 职责：严格遵循依赖倒置原则 (DIP)，全面面向 Flink 官方顶层抽象接口与实体门面编程：
 * - Source<RawEmail, ?, ?>：抽象数据输入源 (支持 IMAP、Kafka、文件系统或测试 Mock 数据源)
 * - FlatMapFunction<RawEmail, SmsRecord>：抽象业务转换规整算子
 * - IcebergR2Sink：湖仓写端实体门面 (支持 R2 落盘写入与 writeParallelism=1 漏斗控制)
 * 负责通过标准 API 组装完整端到端有向无环图 (Source -> FlatMap -> RowDataMapper -> IcebergSink)。
 */
public class SmsGmailR2Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2Pipeline.class);

    private final Source<RawEmail, ?, ?> source;
    private final FlatMapFunction<RawEmail, SmsRecord> parser;
    private final IcebergR2Sink sink;

    public SmsGmailR2Pipeline(Source<RawEmail, ?, ?> source, FlatMapFunction<RawEmail, SmsRecord> parser) {
        this(source, parser, null);
    }

    public SmsGmailR2Pipeline(Source<RawEmail, ?, ?> source,
                             FlatMapFunction<RawEmail, SmsRecord> parser,
                             IcebergR2Sink sink) {
        this.source = Objects.requireNonNull(source, "Source connector must not be null");
        this.parser = Objects.requireNonNull(parser, "Record parser function must not be null");
        this.sink = sink;
    }

    /**
     * 官方正统 FLIP-27 数据流拓扑编排 (构建 SmsRecord 流)
     */
    public DataStream<SmsRecord> buildStream(StreamExecutionEnvironment env) {
        LOG.info("🌊 [Pipeline] Assembling FLIP-27 DataStream topology with abstract Source & FlatMapFunction...");

        // 1. 接入 Flink 官方标准 FLIP-27 抽象数据源
        DataStream<RawEmail> emailStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Generic-Email-Source"
        );

        // 2. 挂载抽象业务清洗算子 (由 Parser 内部负责规整与实体日志记录)
        return emailStream
                .flatMap(parser)
                .name("SmsRecordParser-FlatMap");
    }

    /**
     * 组装完整端到端流图并挂载 Iceberg 湖仓 Sink
     */
    public DataStreamSink<?> assembleAndAttachSink(StreamExecutionEnvironment env) {
        DataStream<SmsRecord> smsStream = buildStream(env);

        if (sink == null) {
            LOG.warn("⚠️ [Pipeline] No Iceberg Sink configured. Falling back to stdout print sink.");
            return smsStream.print().name("SmsRecord-Print-Sink");
        }

        // 转换为列式 RowData 并挂载 IcebergSink (writeParallelism=1)
        DataStream<RowData> rowStream = smsStream
                .map(new SmsRecordToRowDataMapper())
                .name("SmsRecord-To-RowData-Mapper");

        return sink.append(rowStream);
    }

    public Source<RawEmail, ?, ?> getSource() {
        return source;
    }

    public FlatMapFunction<RawEmail, SmsRecord> getParser() {
        return parser;
    }

    public IcebergR2Sink getSink() {
        return sink;
    }
}
