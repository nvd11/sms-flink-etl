package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AmountExtractor 金额与币种提取器单元测试")
class AmountExtractorTest {

    private AmountExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new AmountExtractor();
    }

    private SmsRecord createRecord(String rawBody) {
        return new SmsRecord(1L, "test_uid", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), rawBody, Instant.now());
    }

    @ParameterizedTest(name = "[{index}] 提取金额: {0} => {1} {2}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡03日10:58消费14.89人民币，交易商户:支付宝-高德打车。', 14.89, CNY",
            "'106980095508【广发银行】您尾号3342信用卡14日10:32消费8988.08人民币，交易商户:财付通-美团。', 8988.08, CNY",
            "'106980095508【广发银行】您尾号3342信用卡09月27日发起退款人民币43.92元，到账情况点查询', 43.92, CNY",
            "'106980095508【广发银行】您尾号3342信用卡09月26日发起退款人民币8.90元，到账情况点查询', 8.90, CNY",
            "'10692576032【平安产险】尊敬的潘文林，您已付6646.00元的保单已承保，车牌号粤A-FH2500', 6646.00, CNY",
            "'10690661440018【中意人寿】尊敬的潘瑞成：您的理赔申请已通过审核，赔款金额110.79元将于0-5个工作日到账', 110.79, CNY",
            "'10690661440018【中意人寿】尊敬的潘文林：您的理赔申请已通过审核，赔款金额245元将于0-5个工作日到账', 245.00, CNY",
            "'【微信支付】微信零钱已向某某便利店成功付款8.00元。SubId：1', 8.00, CNY",
            "'【支付宝】花呗自动扣款通知：扣款成功299.00元。SubId：2', 299.00, CNY",
            "'10681128000113【分子借钱】尾号1962 用户交易成功 人民币2,743.3元预核准至您支付账户', 2743.30, CNY",
            "'106910095366【汇丰银行中国】温馨提示：您尾号0025的美元信用卡当月账单为5.99元，最低还款额为0.30元', 5.99, USD",
            "'106980095516【中国银联】付款验证码527521，任何人索取均为诈骗！尾号3342的银行卡向中国平安财产保险付款6646.00元，请勿泄露！', 6646.00, CNY"
    })
    void testExtractRealSmsAmounts(String rawBody, String expectedAmount, String expectedCurrency) {
        SmsRecord record = createRecord(rawBody);
        Map<String, Object> result = extractor.extract(record);

        assertNotNull(result, "提取结果不可为 null");
        assertFalse(result.isEmpty(), "真实动账报文必须成功提取出金额与币种: " + rawBody);

        BigDecimal amount = (BigDecimal) result.get("amount");
        String currency = (String) result.get("currency");

        assertEquals(new BigDecimal(expectedAmount), amount, "金额数值必须严格匹配");
        assertEquals(expectedCurrency, currency, "币种必须匹配");
    }

    @Test
    @DisplayName("测试非动账短信（纯验证码、营销广告、无金额通知）安全返回空 Map")
    void testNonTransactionMessagesReturnEmpty() {
        String[] nonTxSamples = {
                "106980095188000001【支付宝】支付宝验证码：283094，请勿向他人泄露您的验证码！唯一热线95188",
                "10693795818363665【平安保险】您的车险投保验证码为610569。投保人：潘文林，请妥善保管。",
                "com.tencent.mm广发信用卡: 交易成功提醒服务号UID：103362026-10-03 10:58:33",
                "106980095566【中国银行】中银E贷，额度最高30万，用款最长3年，申请拒收请回复R",
                "10086100【广州市人防办】为增强人民群众国防观念和防空意识，定于9月19日进行防空警报试鸣",
                "1068876090001288【腾讯云】尊敬的用户，您的腾讯云账号有1个资源包即将到期"
        };

        for (String sample : nonTxSamples) {
            SmsRecord record = createRecord(sample);
            Map<String, Object> result = extractor.extract(record);
            assertTrue(result.isEmpty(), "非交易短信不得提取出有效金额: " + sample);
        }
    }

    @Test
    @DisplayName("测试边界异常情况防御：null、空字符串、全空格")
    void testNullAndEmptyDefensive() {
        assertTrue(extractor.extract(null).isEmpty());
        assertTrue(extractor.extract(createRecord(null)).isEmpty());
        assertTrue(extractor.extract(createRecord("")).isEmpty());
        assertTrue(extractor.extract(createRecord("    ")).isEmpty());
    }

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量提取并审计金额与币种合理性")
    void testExtractAllSmsFromDevTable() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        int extractedCount = 0;
        int nonTxCount = 0;
        BigDecimal totalAmountCNY = BigDecimal.ZERO;
        BigDecimal totalAmountUSD = BigDecimal.ZERO;

        System.out.println("================================================================================");
        System.out.println("📊 [Dev Lakehouse SMS Audit] Reading all records from finance_dev.raw_sms_records...");
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

                if (!result.isEmpty()) {
                    extractedCount++;
                    BigDecimal amount = (BigDecimal) result.get("amount");
                    String currency = (String) result.get("currency");

                    assertNotNull(amount, "提取的金额不可为 null");
                    assertNotNull(currency, "提取的币种不可为 null");
                    assertTrue(amount.compareTo(BigDecimal.ZERO) > 0, "金额必须为正数");

                    if ("USD".equals(currency)) {
                        totalAmountUSD = totalAmountUSD.add(amount);
                    } else if ("CNY".equals(currency)) {
                        totalAmountCNY = totalAmountCNY.add(amount);
                    }

                    // 打印提取出来的样本
                    if (extractedCount <= 30 || extractedCount % 20 == 0) {
                        System.out.printf("  [#%3d | ID:%3d | %-4s] => %10s %-3s | %s\n",
                                extractedCount, id, sender != null ? sender : "N/A", amount, currency,
                                rawBody != null && rawBody.length() > 55 ? rawBody.substring(0, 55).replace("\n", " ") + "..." : rawBody);
                    }
                } else {
                    nonTxCount++;
                }
            }
        }

        System.out.println("================================================================================");
        System.out.println("📈 [Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS): %d 封\n", totalCount);
        System.out.printf("  • 成功提炼出金额 (Extracted): %d 笔动账 (占有效比例: %.1f%%)\n",
                extractedCount, totalCount > 0 ? (double) extractedCount * 100 / totalCount : 0);
        System.out.printf("  • 判定为非动账/纯通知 (Non-Tx): %d 封 (验证码/广告/服务号提醒)\n", nonTxCount);
        System.out.printf("  • 人民币总金额 (Total CNY): ￥%s\n", totalAmountCNY.toPlainString());
        System.out.printf("  • 美元总金额   (Total USD): $%s\n", totalAmountUSD.toPlainString());
        System.out.println("================================================================================");

        assertTrue(totalCount > 0, "Dev 表中必须有数据可供审计");
        assertTrue(extractedCount > 0, "必须成功提炼出至少部分动账金额");
    }
}
