package com.finance.etl.transform;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SmsEmailParser 管道总调度单元测试")
class SmsEmailParserTest {

    @Test
    @DisplayName("应该成功驱动四大提取器并组装出完整 SmsRecord 实体")
    void shouldExtractAllFieldsAndEmitEnrichedRecord() throws Exception {
        SmsEmailParser parser = new SmsEmailParser();

        RawEmail email = new RawEmail();
        email.setImapUid(999L);
        email.setMessageId("<test-msg-999@gmail.com>");
        email.setSubject("106980095508");
        email.setBody("106980095508【广发银行】您尾号3342信用卡消费21.24元。SubId：22026-09-27 18:54:07");
        email.setReceivedAt(Instant.parse("2026-09-29T10:00:00Z"));

        List<SmsRecord> collected = new ArrayList<>();
        Collector<SmsRecord> testCollector = new Collector<>() {
            @Override
            public void collect(SmsRecord record) {
                collected.add(record);
            }

            @Override
            public void close() {}
        };

        parser.flatMap(email, testCollector);

        assertEquals(1, collected.size());
        SmsRecord emitted = collected.get(0);
        assertEquals(999L, emitted.getId());
        assertEquals("EMAIL_IMAP", emitted.getChannel());
        assertEquals("CGB", emitted.getSender());
        assertEquals("SIM_SLOT_2", emitted.getReceiverPhone());
        assertEquals("106980095508【广发银行】您尾号3342信用卡消费21.24元。SubId：22026-09-27 18:54:07", emitted.getRawBody());
        assertNotNull(emitted.getMsgUid());
        assertEquals(64, emitted.getMsgUid().length());
        assertEquals(email.getReceivedAt(), emitted.getReceivedAt());
    }

    @Test
    @DisplayName("空邮件输入时应安全跳过且不产生记录")
    void shouldHandleNullEmailSafely() throws Exception {
        SmsEmailParser parser = new SmsEmailParser();
        List<SmsRecord> collected = new ArrayList<>();
        Collector<SmsRecord> testCollector = new Collector<>() {
            @Override
            public void collect(SmsRecord record) {
                collected.add(record);
            }

            @Override
            public void close() {}
        };

        parser.flatMap(null, testCollector);
        assertTrue(collected.isEmpty());
    }
}
