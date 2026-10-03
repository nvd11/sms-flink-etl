package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
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

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量审计交易发生时间解析")
    void testAuditTxTimeAcrossAllDevRecords() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        int parsedCount = 0;
        int subIdCount = 0;
        int dayMinCount = 0;
        int fallbackCount = 0;
        List<String> auditLogs = new ArrayList<>();

        System.out.println("================================================================================");
        System.out.println("⏰ [Dev Lakehouse TxTime Audit] Auditing tx_time across all ODS records...");
        System.out.println("================================================================================");

        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> records =
                     org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
            for (org.apache.iceberg.data.Record r : records) {
                totalCount++;
                Long id = r.get(0, Long.class);
                String msgUid = r.get(1, String.class);
                String channel = r.get(2, String.class);
                String sender = r.get(3, String.class);
                String receiverPhone = r.get(4, String.class);
                Instant receivedAt = r.get(5, java.time.OffsetDateTime.class).toInstant();
                String rawBody = r.get(6, String.class);

                SmsRecord sms = new SmsRecord(id, msgUid, channel, sender, receiverPhone, receivedAt, rawBody, null);
                Map<String, Object> result = extractor.extract(sms);

                Instant txTime = (Instant) result.get("tx_time");
                assertNotNull(txTime, "tx_time 不应为 null，至少有 receivedAt 保底");
                parsedCount++;

                String source = "FALLBACK";
                if (rawBody != null && rawBody.matches("(?s).*SubId：\\d{1,2}(\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}).*")) {
                    source = "SUBID";
                    subIdCount++;
                } else if (rawBody != null && rawBody.matches("(?s).*(\\d{1,2})日(\\d{2}:\\d{2}).*")) {
                    source = "DAY_TIME";
                    dayMinCount++;
                } else {
                    fallbackCount++;
                }

                auditLogs.add(String.format("[TIME #%3d | ID:%3d | %-8s] => txTime: %s | %s",
                        totalCount, id, source, txTime,
                        rawBody != null && rawBody.length() > 50 ? rawBody.substring(0, 50).replace("\n", " ") + "..." : rawBody));
            }
        }

        java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/opencode/time_audit_all.txt"), auditLogs);

        System.out.println("================================================================================");
        System.out.println("📈 [TxTime Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS)       : %d 封\n", totalCount);
        System.out.printf("  • 成功解析出时间 (tx_time)       : %d 条 (100%%)\n", parsedCount);
        System.out.printf("  • 来源于 SubId 高精度时间        : %d 笔 (占 %.1f%%)\n", subIdCount, (double) subIdCount * 100 / totalCount);
        System.out.printf("  • 来源于正文日时分               : %d 笔 (占 %.1f%%)\n", dayMinCount, (double) dayMinCount * 100 / totalCount);
        System.out.printf("  • 降级使用 received_at 保底时间  : %d 笔 (占 %.1f%%)\n", fallbackCount, (double) fallbackCount * 100 / totalCount);
        System.out.println("================================================================================");

        assertTrue(totalCount > 0);
        assertTrue(subIdCount > 0);
    }
}
