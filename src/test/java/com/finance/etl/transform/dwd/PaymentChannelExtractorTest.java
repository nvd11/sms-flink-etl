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

@DisplayName("PaymentChannelExtractor 支付通路中介提取器单元测试")
class PaymentChannelExtractorTest {

    private PaymentChannelExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new PaymentChannelExtractor();
    }

    private SmsRecord createRecord(String rawBody) {
        return new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), rawBody, Instant.now());
    }

    @ParameterizedTest(name = "[{index}] 支付通路判定: {0} => channel: {1}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:支付宝-高德打车。', ALIPAY",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:财付通-知味园自选快餐。', WECHAT_PAY",
            "'【微信支付】微信零钱已向某某便利店成功付款8.00元。', WECHAT_PAY",
            "'【支付宝】花呗自动扣款通知：扣款成功299.00元。', ALIPAY",
            "'【中国银联】付款验证码527521，尾号3342的银行卡向中国平安付款', UNIONPAY",
            "'106980095508【广发银行】您尾号3342信用卡消费，交易商户:广东永旺番禺广场店。', DIRECT"
    })
    void testPaymentChannel(String rawBody, String expectedChannel) {
        Map<String, Object> result = extractor.extract(createRecord(rawBody));
        assertEquals(expectedChannel, result.get("payment_channel"));
    }

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量审计支付渠道提取")
    void testAuditPaymentChannelAcrossAllDevRecords() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        int recognizedCount = 0;
        Map<String, Integer> channelCounts = new HashMap<>();
        List<String> auditLogs = new ArrayList<>();

        System.out.println("================================================================================");
        System.out.println("🌐 [Dev Lakehouse PaymentChannel Audit] Auditing payment_channel across all ODS records...");
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

                String payChannel = (String) result.get("payment_channel");
                if (payChannel != null) {
                    recognizedCount++;
                    channelCounts.put(payChannel, channelCounts.getOrDefault(payChannel, 0) + 1);
                    auditLogs.add(String.format("[CHANNEL #%3d | ID:%3d | %-4s] => channel: %-10s | %s",
                            recognizedCount, id, sender != null ? sender : "N/A", payChannel, rawBody));
                }
            }
        }

        java.nio.file.Files.write(java.nio.file.Paths.get("/tmp/opencode/channel_audit_all.txt"), auditLogs);

        System.out.println("================================================================================");
        System.out.println("📈 [PaymentChannel Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS)       : %d 封\n", totalCount);
        System.out.printf("  • 成功匹配支付渠道               : %d 笔\n", recognizedCount);
        System.out.println("  • 支付渠道分布 (Distribution)   : " + channelCounts);
        System.out.println("================================================================================");

        assertTrue(totalCount > 0);
        assertTrue(recognizedCount > 0);
    }
}
