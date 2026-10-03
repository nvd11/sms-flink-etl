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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CategoryExtractor 交易消费类目智能归类单元测试")
class CategoryExtractorTest {

    private CategoryExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new CategoryExtractor();
    }

    private SmsRecord createRecord(String rawBody) {
        return new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), rawBody, Instant.now());
    }

    @ParameterizedTest(name = "[{index}] 消费分类: {0} => category: {1}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-高德打车。', TRANSPORT",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-广州地铁集团有限公司。', TRANSPORT",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-知味园自选快餐。', FOOD",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-煲珠公收款。', FOOD",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-广州市阿赛小面餐饮管理有限公司。', FOOD",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-拼多多平台商户。', SHOPPING",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-浙江天猫供应链管理有限公司。', SHOPPING",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-盒马。', SHOPPING",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-广州市番禺区何贤纪念医院。', MEDICAL",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-福源堂大药房。', MEDICAL",
            "'10690661440018【中意人寿】尊敬的潘瑞成：赔款金额279.95元将于0-5个工作日到账', OTHER",
            "'10692576032【平安产险】尊敬的潘文林，您已付6646.00元的保单已承保', OTHER"
    })
    void testCategoryClassification(String rawBody, String expectedCategory) {
        Map<String, Object> result = extractor.extract(createRecord(rawBody));
        assertEquals(expectedCategory, result.get("category"));
    }

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量审计交易大类分类分布")
    void testAuditCategoryAcrossAllDevRecords() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        Map<String, Integer> categoryCounts = new HashMap<>();
        List<String> auditLogs = new ArrayList<>();

        System.out.println("================================================================================");
        System.out.println("🏷️ [Dev Lakehouse Category Audit] Auditing category classification across all ODS records...");
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

                String category = (String) result.get("category");
                categoryCounts.put(category, categoryCounts.getOrDefault(category, 0) + 1);

                auditLogs.add(String.format("[CAT #%3d | ID:%3d | %-4s] => category: %-10s | %s",
                        totalCount, id, sender != null ? sender : "N/A",
                        category,
                        rawBody != null && rawBody.length() > 60 ? rawBody.substring(0, 60).replace("\n", " ") + "..." : rawBody));
            }
        }

        java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/opencode/category_audit_all.txt"), auditLogs);

        for (int i = 0; i < Math.min(25, auditLogs.size()); i++) {
            System.out.println(auditLogs.get(i));
        }

        System.out.println("================================================================================");
        System.out.println("📈 [Category Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS)       : %d 封\n", totalCount);
        System.out.println("  • 五大消费类目分布 (Category Distribution):");
        final int finalTotalCount = totalCount;
        categoryCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach(e -> System.out.printf("      - %-12s : %3d 笔 (占 %.1f%%)\n",
                        e.getKey(), e.getValue(), (double) e.getValue() * 100 / finalTotalCount));
        System.out.println("================================================================================");

        assertTrue(totalCount > 0);
        assertTrue(categoryCounts.containsKey("FOOD"));
        assertTrue(categoryCounts.containsKey("TRANSPORT"));
        assertTrue(categoryCounts.containsKey("SHOPPING"));
        assertTrue(categoryCounts.containsKey("MEDICAL"));
    }
}
