package com.finance.etl.pipeline;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.reader.GmailImapReader;
import com.finance.etl.transform.SmsRecordParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * 流水线执行实体对象 (SmsGmailR2Pipeline)
 * 职责：持有 GmailImapReader 与 SmsRecordParser 实例，负责协调邮件提取、短信规整并组装 Flink DataStream 算子拓扑。
 */
public class SmsGmailR2Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2Pipeline.class);

    private final GmailImapReader reader;
    private final SmsRecordParser parser;

    public SmsGmailR2Pipeline(GmailImapReader reader, SmsRecordParser parser) {
        this.reader = reader;
        this.parser = parser;
    }

    /**
     * 协调获取所有短信记录 (邮件提取 -> 业务脱壳)
     */
    public List<SmsRecord> extractAllSmsRecords() {
        List<RawEmail> emails = reader.fetchEmails();
        List<SmsRecord> smsRecords = new ArrayList<>();

        for (RawEmail email : emails) {
            List<SmsRecord> extracted = parser.parse(email);
            if (extracted != null && !extracted.isEmpty()) {
                smsRecords.addAll(extracted);
            }
        }
        return smsRecords;
    }

    /**
     * 组装 Flink 数据流拓扑
     */
    public DataStream<SmsRecord> buildStream(StreamExecutionEnvironment env) {
        List<SmsRecord> allSmsRecords = extractAllSmsRecords();
        LOG.info("📦 Total extracted SMS records ready for ingestion: {}", allSmsRecords.size());

        if (allSmsRecords.isEmpty()) {
            LOG.info("📭 No new SMS records to process. Emitting empty heartbeat record.");
            allSmsRecords = Collections.singletonList(createHeartbeatRecord());
        }

        DataStream<SmsRecord> smsStream = env.fromData(allSmsRecords);
        return smsStream.map(record -> {
            String logMsg = String.format("[Nova-Worker-Slot] [sms-gmail-r2] Ingesting: [Sender: %s, UID: %s, Body: %s]",
                    record.getSender(), record.getMsgUid().substring(0, Math.min(8, record.getMsgUid().length())), record.getRawBody());
            LOG.info(logMsg);
            return record;
        });
    }

    /**
     * 辅助工具：空数据时的兜底心跳记录
     */
    public SmsRecord createHeartbeatRecord() {
        SmsRecord r = new SmsRecord();
        r.setId(System.nanoTime());
        r.setMsgUid("HEARTBEAT_" + UUID.randomUUID());
        r.setChannel("EMAIL_IMAP");
        r.setSender("SYSTEM_HEARTBEAT");
        r.setReceiverPhone("NONE");
        r.setReceivedAt(Instant.now());
        r.setRawBody("[HEARTBEAT] No incoming SMS in Gmail. Pulse tick ok.");
        r.setCreatedAt(Instant.now());
        return r;
    }

    public GmailImapReader getReader() {
        return reader;
    }

    public SmsRecordParser getParser() {
        return parser;
    }
}
