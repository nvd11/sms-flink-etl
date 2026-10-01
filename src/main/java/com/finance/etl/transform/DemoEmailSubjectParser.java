package com.finance.etl.transform;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.transform.extractor.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 管道化动账报文解析算子 (DemoEmailSubjectParser)
 * 职责：纯函数管道总调度器。
 * 遍历装配好的 SmsFieldExtractor 提取器族，将返回的 Map 字段字典自动聚合规整为 SmsRecord 实体。
 */
public class DemoEmailSubjectParser implements FlatMapFunction<RawEmail, SmsRecord>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DemoEmailSubjectParser.class);

    private final List<SmsFieldExtractor> extractors;

    public DemoEmailSubjectParser() {
        this.extractors = List.of(
                new RawBodyExtractor(),
                new SenderExtractor(),
                new SimSlotExtractor(),
                new FingerprintExtractor()
        );
    }

    public DemoEmailSubjectParser(List<SmsFieldExtractor> extractors) {
        this.extractors = extractors != null ? extractors : List.of();
    }

    @Override
    public void flatMap(RawEmail email, Collector<SmsRecord> out) throws Exception {
        if (email == null) {
            return;
        }

        // 1. 初始化标准基础物理属性
        SmsRecord record = new SmsRecord();
        record.setId(email.getImapUid() != null ? email.getImapUid() : System.nanoTime());
        record.setChannel("EMAIL_IMAP");
        record.setReceivedAt(email.getReceivedAt() != null ? email.getReceivedAt() : Instant.now());
        record.setCreatedAt(Instant.now());

        // 2. 纯函数遍历抽取并聚合所有提取器贡献的键值字典
        Map<String, Object> enrichedFields = new HashMap<>();
        for (SmsFieldExtractor extractor : extractors) {
            Map<String, Object> fields = extractor.extract(email);
            if (fields != null && !fields.isEmpty()) {
                enrichedFields.putAll(fields);
            }
        }

        // 3. 动态属性规整分发至 SmsRecord
        applyFields(record, enrichedFields);

        // 4. 清晰结构化的生产级实体提取日志输出
        String bodyPreview = record.getRawBody() != null ? record.getRawBody().replace("\n", " ").trim() : "";
        if (bodyPreview.length() > 60) {
            bodyPreview = bodyPreview.substring(0, 60) + "...";
        }
        LOG.info("📱 [Sms Extracted] [ID: {}, Sender: {}, Slot: {}, Time: {}, Fingerprint: {}, Body: '{}']",
                record.getId(),
                record.getSender(),
                record.getReceiverPhone(),
                record.getReceivedAt(),
                record.getMsgUid() != null ? record.getMsgUid().substring(0, Math.min(8, record.getMsgUid().length())) : "N/A",
                bodyPreview);

        // 5. 发射给下游管道
        out.collect(record);
    }

    private void applyFields(SmsRecord record, Map<String, Object> fields) {
        if (fields.containsKey("rawBody")) {
            record.setRawBody((String) fields.get("rawBody"));
        }
        if (fields.containsKey("sender")) {
            record.setSender((String) fields.get("sender"));
        }
        if (fields.containsKey("receiverPhone")) {
            record.setReceiverPhone((String) fields.get("receiverPhone"));
        }
        if (fields.containsKey("msgUid")) {
            record.setMsgUid((String) fields.get("msgUid"));
        }
    }

    public List<SmsFieldExtractor> getExtractors() {
        return extractors;
    }
}
