package com.finance.etl.pipeline;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 流水线执行实体对象 (SmsGmailR2Pipeline)
 * 职责：严格遵循依赖倒置原则 (DIP)，全面面向 Flink 官方顶层抽象接口编程：
 * - Source<RawEmail, ?, ?>：抽象数据输入源 (支持 IMAP、Kafka、文件系统或测试 Mock 数据源)
 * - FlatMapFunction<RawEmail, SmsRecord>：抽象业务转换规整算子
 * 负责通过标准 API 组装有向无环图 (Source -> FlatMap -> Log/Sink)。
 */
public class SmsGmailR2Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2Pipeline.class);

    private final Source<RawEmail, ?, ?> source;
    private final FlatMapFunction<RawEmail, SmsRecord> parser;

    public SmsGmailR2Pipeline(Source<RawEmail, ?, ?> source, FlatMapFunction<RawEmail, SmsRecord> parser) {
        this.source = Objects.requireNonNull(source, "Source connector must not be null");
        this.parser = Objects.requireNonNull(parser, "Record parser function must not be null");
    }

    /**
     * 官方正统 FLIP-27 数据流拓扑编排 (基于抽象接口装配)
     */
    public DataStream<SmsRecord> buildStream(StreamExecutionEnvironment env) {
        LOG.info("🌊 [Pipeline] Assembling FLIP-27 DataStream topology with abstract Source & FlatMapFunction...");

        // 1. 接入 Flink 官方标准 FLIP-27 抽象数据源
        DataStream<RawEmail> emailStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Generic-Email-Source"
        );

        // 2. 挂载抽象业务清洗算子
        DataStream<SmsRecord> smsStream = emailStream
                .flatMap(parser)
                .name("SmsRecordParser-FlatMap");

        // 3. 挂载工人处理日志记录
        return smsStream.map(record -> {
            String logMsg = String.format("[Nova-Worker-Slot] [sms-gmail-r2] Ingesting: [Sender: %s, UID: %s, Body: %s]",
                    record.getSender(),
                    record.getMsgUid() != null ? record.getMsgUid().substring(0, Math.min(8, record.getMsgUid().length())) : "N/A",
                    record.getRawBody());
            LOG.info(logMsg);
            return record;
        });
    }

    public Source<RawEmail, ?, ?> getSource() {
        return source;
    }

    public FlatMapFunction<RawEmail, SmsRecord> getParser() {
        return parser;
    }
}
