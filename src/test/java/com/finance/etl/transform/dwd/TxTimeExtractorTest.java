package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TxTimeExtractor 交易时间提取器单元测试")
class TxTimeExtractorTest {

    private TxTimeExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new TxTimeExtractor();
    }

    @Test
    @DisplayName("测试从 SubId 精确提取时间戳")
    void testExtractFromSubId() {
        String raw = "【广发银行】消费10元。SubId：12026-09-29 19:43:16";
        SmsRecord record = new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1",
                Instant.now(), raw, Instant.now());

        Map<String, Object> result = extractor.extract(record);
        Instant txTime = (Instant) result.get("tx_time");

        assertNotNull(txTime);
        ZonedDateTime zdt = txTime.atZone(ZoneId.of("Asia/Shanghai"));
        assertEquals(2026, zdt.getYear());
        assertEquals(9, zdt.getMonthValue());
        assertEquals(29, zdt.getDayOfMonth());
        assertEquals(19, zdt.getHour());
        assertEquals(43, zdt.getMinute());
        assertEquals(16, zdt.getSecond());
    }

    @Test
    @DisplayName("测试无正文时间时降级为 receivedAt 物理时间")
    void testFallbackToReceivedAt() {
        Instant fallbackTime = Instant.parse("2026-10-01T12:00:00Z");
        SmsRecord record = new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1",
                fallbackTime, "【广发银行】消费10元。", Instant.now());

        Map<String, Object> result = extractor.extract(record);
        assertEquals(fallbackTime, result.get("tx_time"));
    }
}
