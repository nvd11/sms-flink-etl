package com.finance.etl.pipeline;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.source.imap.ImapSource;
import com.finance.etl.transform.SmsRecordParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 流水线执行实体对象 (SmsGmailR2Pipeline)
 * 职责：持有 ImapSource (FLIP-27 连接器) 与 SmsRecordParser 算子实例，
 * 负责通过 Flink 官方正统 API 组装 DataStream 算子拓扑 (Source -> FlatMap -> Log/Sink)。
 */
public class SmsGmailR2Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2Pipeline.class);

    private final ImapSource source;
    private final SmsRecordParser parser;

    public SmsGmailR2Pipeline(ImapSource source, SmsRecordParser parser) {
        this.source = source;
        this.parser = parser;
    }

    /**
     * 官方正统 FLIP-27 数据流拓扑编排
     */
    public DataStream<SmsRecord> buildStream(StreamExecutionEnvironment env) {
        LOG.info("🌊 [Pipeline] Assembling FLIP-27 DataStream topology (ImapSource -> SmsRecordParser)...");

        // 1. 接入 Flink 官方标准 FLIP-27 数据源
        DataStream<RawEmail> emailStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Gmail-IMAP-FLIP27-Source"
        );

        // 2. 挂载业务清洗算子 (FlatMapFunction 转换)
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

    public ImapSource getSource() {
        return source;
    }

    public SmsRecordParser getParser() {
        return parser;
    }
}
