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

@DisplayName("MerchantExtractor 商户与纯净品牌名提炼单元测试")
class MerchantExtractorTest {

    private MerchantExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new MerchantExtractor();
    }

    private SmsRecord createRecord(String rawBody) {
        return new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), rawBody, Instant.now());
    }

    @ParameterizedTest(name = "[{index}] 商户提取: {0} => counterparty: {1}, cleaned: {2}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-高德打车。', 支付宝-高德打车, 高德打车",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-知味园自选快餐。', 财付通-知味园自选快餐, 知味园自选快餐",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-煲珠公收款。', 财付通-煲珠公收款, 煲珠公",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-拼多多平台商户。', 支付宝-拼多多平台商户, 拼多多",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-广州盒马鲜生网络科技有限公司。', 支付宝-广州盒马鲜生网络科技有限公司, 盒马鲜生",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-北京三快在线科技有限公司。', 支付宝-北京三快在线科技有限公司, 美团",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-上海拉扎斯信息科技有限公司。', 支付宝-上海拉扎斯信息科技有限公司, 饿了么",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-广州地铁集团有限公司。', 支付宝-广州地铁集团有限公司, 广州地铁",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-广州市阿赛小面餐饮管理有限公司。', 财付通-广州市阿赛小面餐饮管理有限公司, 广州市阿赛小面",
            "'10690661440018【中意人寿】尊敬的潘瑞成：赔款金额279.95元将于0-5个工作日到账', 中意人寿, 中意人寿",
            "'10692576032【平安产险】尊敬的潘文林，您已付6646.00元的保单已承保', 中国平安, 中国平安"
    })
    void testMerchantExtraction(String rawBody, String expectedCounterparty, String expectedCleaned) {
        Map<String, Object> result = extractor.extract(createRecord(rawBody));
        assertEquals(expectedCounterparty, result.get("counterparty"));
        assertEquals(expectedCleaned, result.get("cleaned_merchant"));
    }

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量审计商户对手方与清洗结果")
    void testAuditMerchantAcrossAllDevRecords() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        int extractedCount = 0;
        Map<String, Integer> cleanedMerchantCounts = new HashMap<>();
        List<String> auditLogs = new ArrayList<>();

        System.out.println("================================================================================");
        System.out.println("🏪 [Dev Lakehouse Merchant Audit] Auditing merchant extraction across all ODS records...");
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

                String counterparty = (String) result.get("counterparty");
                String cleaned = (String) result.get("cleaned_merchant");

                if (counterparty != null) {
                    extractedCount++;
                    cleanedMerchantCounts.put(cleaned, cleanedMerchantCounts.getOrDefault(cleaned, 0) + 1);
                    auditLogs.add(String.format("[MERCHANT #%3d | ID:%3d | %-4s] => cleaned: %-16s | counterparty: %-32s | raw: %s",
                            extractedCount, id, sender != null ? sender : "N/A",
                            cleaned, counterparty,
                            rawBody != null && rawBody.length() > 60 ? rawBody.substring(0, 60).replace("\n", " ") + "..." : rawBody));
                }
            }
        }

        java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/opencode/merchant_audit_all.txt"), auditLogs);

        for (int i = 0; i < Math.min(30, auditLogs.size()); i++) {
            System.out.println(auditLogs.get(i));
        }

        System.out.println("================================================================================");
        System.out.println("📈 [Merchant Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS)         : %d 封\n", totalCount);
        System.out.printf("  • 识别出交易商户 (Extracted Count) : %d 笔\n", extractedCount);
        System.out.printf("  • 纯净品牌商户种类 (Unique Brands) : %d 种\n", cleanedMerchantCounts.size());
        System.out.println("  • 高频商户 Top 10 (Merchant Top 10):");
        cleanedMerchantCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(e -> System.out.printf("      - %-20s : %d 笔\n", e.getKey(), e.getValue()));
        System.out.println("================================================================================");

        assertTrue(totalCount > 0);
        assertTrue(extractedCount > 0);
    }
}
