package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AccountExtractor 账户类型与卡号尾号提取器单元测试")
class AccountExtractorTest {

    private AccountExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new AccountExtractor();
    }

    private SmsRecord createRecord(String rawBody) {
        return new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), rawBody, Instant.now());
    }

    @ParameterizedTest(name = "[{index}] 账户识别: {0} => tail: {1}, type: {2}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡03日10:58消费14.89人民币', 3342, CREDIT_CARD",
            "'106910095366【汇丰银行中国】您尾号为0025的美元信用卡当月账单', 0025, CREDIT_CARD",
            "'106980095508【广发银行】您尾号8888信用卡消费人民币128.50元', 8888, CREDIT_CARD",
            "'【中国银联】尾号3342的银行卡向中国平安财产保险付款6646.00元', 3342, DEBIT_CARD",
            "'【微信支付】微信零钱已向某某便利店成功付款8.00元。SubId：1', 'NULL', WALLET",
            "'【支付宝】花呗自动扣款通知：扣款成功299.00元。SubId：2', 'NULL', WALLET",
            "'95566您在我行住房贷款（本金76.00万元）的供款账户余额不足', 'NULL', LOAN"
    })
    void testAccountExtraction(String rawBody, String expectedTail, String expectedType) {
        Map<String, Object> result = extractor.extract(createRecord(rawBody));

        if ("NULL".equals(expectedTail)) {
            assertNull(result.get("card_tail"), "无尾号时应为 null");
        } else {
            assertEquals(expectedTail, result.get("card_tail"), "卡号尾号必须匹配");
        }
        assertEquals(expectedType, result.get("account_type"), "账户类型必须匹配");
    }

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量审计账户类型与卡尾号提取")
    void testAuditAccountAcrossAllDevRecords() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        int extractedTailCount = 0;
        int extractedTypeCount = 0;
        Map<String, Integer> accountTypeCounts = new HashMap<>();
        Map<String, Integer> cardTailCounts = new HashMap<>();
        List<String> auditLogs = new ArrayList<>();

        System.out.println("================================================================================");
        System.out.println("💳 [Dev Lakehouse Account Audit] Auditing account_type and card_tail across all ODS records...");
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
                String rawBody = r.get(6, String.class);

                SmsRecord sms = new SmsRecord(id, msgUid, channel, sender, receiverPhone, null, rawBody, null);
                Map<String, Object> result = extractor.extract(sms);

                String cardTail = (String) result.get("card_tail");
                String accountType = (String) result.get("account_type");

                if (cardTail != null) {
                    extractedTailCount++;
                    cardTailCounts.put(cardTail, cardTailCounts.getOrDefault(cardTail, 0) + 1);
                }

                if (accountType != null) {
                    extractedTypeCount++;
                    accountTypeCounts.put(accountType, accountTypeCounts.getOrDefault(accountType, 0) + 1);
                }

                if (cardTail != null || accountType != null) {
                    auditLogs.add(String.format("[ACCT #%3d | ID:%3d | %-4s] => tail: %-4s | type: %-11s | %s",
                            auditLogs.size() + 1, id, sender != null ? sender : "N/A",
                            cardTail != null ? cardTail : "null",
                            accountType != null ? accountType : "null",
                            rawBody));
                }
            }
        }

        java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/opencode/account_audit_all.txt"), auditLogs);

        System.out.println("================================================================================");
        System.out.println("📈 [Account Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS)       : %d 封\n", totalCount);
        System.out.printf("  • 识别出账户特征记录数           : %d 条\n", auditLogs.size());
        System.out.printf("  • 成功识别卡号尾号 (card_tail)   : %d 笔\n", extractedTailCount);
        System.out.printf("  • 成功分类账户类型 (account_type): %d 笔\n", extractedTypeCount);
        System.out.println("  • 卡尾分布 (Tail Distribution)   : " + cardTailCounts);
        System.out.println("  • 账户类型分布 (Type Counts)     : " + accountTypeCounts);
        System.out.println("================================================================================");

        assertTrue(totalCount > 0);
        assertTrue(extractedTailCount > 0);
    }
}
