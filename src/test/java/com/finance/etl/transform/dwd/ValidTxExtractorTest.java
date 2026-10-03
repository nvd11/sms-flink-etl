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

@DisplayName("ValidTxExtractor 有效性与交易类型提取器单元测试")
class ValidTxExtractorTest {

    private ValidTxExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new ValidTxExtractor();
    }

    private SmsRecord createRecord(String rawBody) {
        return new SmsRecord(1L, "uid", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), rawBody, Instant.now());
    }

    @ParameterizedTest(name = "[{index}] 交易有效性判定: {0} => valid: {1}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡03日10:58消费14.89人民币，交易商户:支付宝-高德打车。', true",
            "'106980095508【广发银行】您尾号3342信用卡09月27日发起退款人民币43.92元，到账情况点查询', true",
            "'10690661440018【中意人寿】尊敬的潘瑞成：您的理赔申请已通过审核，赔款金额110.79元将于0-5个工作日到账', true",
            "'106980095508【广发银行】您尾号3342信用卡16日00:43还款人民币17253.33元，到账后0', true",
            "'106980095508【广发银行】您尾号3342信用卡29日21:44消费6646.00人民币。分期请点', true",
            "'【微信支付】微信零钱已向某某便利店成功付款8.00元。SubId：1', true",
            "'【支付宝】花呗自动扣款通知：扣款成功299.00元。SubId：2', true"
    })
    void testValidTransactions(String rawBody, boolean expectedValid) {
        Map<String, Object> result = extractor.extract(createRecord(rawBody));

        assertEquals(expectedValid, result.get("is_valid_tx"), "is_valid_tx 判定必须匹配");
        assertFalse(result.containsKey("direction"), "ValidTxExtractor 恪守单一职责，严禁越权返回 direction");
        assertFalse(result.containsKey("tx_type"), "ValidTxExtractor 恪守单一职责，严禁越权返回 tx_type");
    }

    @Test
    @DisplayName("测试非真实交易短信一票否决为 false (验证码、套路贷、账单提醒)")
    void testInvalidNotices() {
        String[] invalidSamples = {
                "106980095188000001【支付宝】支付宝验证码：283094，请勿向他人泄露您的验证码！",
                "106980095516【中国银联】付款验证码527521，任何人索取均为诈骗！尾号3342的银行卡向中国平安财产保险付款6646.00元，请勿泄露！",
                "10692576032【平安产险】尊敬的潘文林，您已付6646.00元的保单已承保，车牌号粤A-FH2500",
                "10681128000113【分子借钱】尾号1962 用户交易成功 人民币2,743.3元预核准至您支付账户",
                "106828023000336【智花】1962用户预放款成功 人民币8958元额于8:00已发放至您支付账户",
                "106910095366【汇丰银行中国】温馨提示：您尾号0025的美元信用卡当月账单为5.99元，最低还款额为0.30元",
                "com.tencent.mm广发信用卡: 交易成功提醒服务号UID：103362026-10-03 10:58:33",
                "10086100【广州市人防办】定于9月19日进行防空警报试鸣"
        };

        for (String sample : invalidSamples) {
            Map<String, Object> result = extractor.extract(createRecord(sample));
            assertEquals(false, result.get("is_valid_tx"), "非动账短信必须判定 is_valid_tx=false: " + sample);
            assertNull(result.get("direction"), "非动账短信不应有资金流向");
        }
    }

    @Test
    @DisplayName("从 Dev 存储桶读取全部真实的 ODS 短信，批量审计交易有效性与动账类型判定")
    void testClassifyAllSmsFromDevTable() throws Exception {
        org.apache.iceberg.flink.TableLoader tableLoader =
                com.finance.etl.repository.IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        tableLoader.open();
        org.apache.iceberg.Table table = tableLoader.loadTable();

        int totalCount = 0;
        int validCount = 0;
        int invalidCount = 0;
        Map<String, Integer> directionCounts = new HashMap<>();
        Map<String, Integer> txTypeCounts = new HashMap<>();
        List<String> auditLogs = new ArrayList<>();

        System.out.println("================================================================================");
        System.out.println("🛡️ [Dev Lakehouse ValidTx Audit] Auditing transaction validity across all ODS records...");
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

                Boolean isValid = (Boolean) result.get("is_valid_tx");
                assertNotNull(isValid, "is_valid_tx 结果不可为 null");

                if (Boolean.TRUE.equals(isValid)) {
                    validCount++;
                    assertFalse(result.containsKey("direction"), "ValidTxExtractor 严禁输出 direction");
                    assertFalse(result.containsKey("tx_type"), "ValidTxExtractor 严禁输出 tx_type");

                    auditLogs.add(String.format("[VALID #%3d | ID:%3d | %-4s] => isValidTx: true  | %s",
                            validCount, id, sender != null ? sender : "N/A",
                            rawBody != null && rawBody.length() > 65 ? rawBody.substring(0, 65).replace("\n", " ") + "..." : rawBody));
                } else {
                    invalidCount++;
                }
            }
        }

        // 打印前 30 条判定为有效的样本
        for (int i = 0; i < Math.min(30, auditLogs.size()); i++) {
            System.out.println(auditLogs.get(i));
        }

        System.out.println("================================================================================");
        System.out.println("📈 [ValidTx Audit Summary]");
        System.out.printf("  • 总扫描记录数 (Total ODS)      : %d 封\n", totalCount);
        System.out.printf("  • 判定为真实动账 (Valid Tx)    : %d 笔 (占总短信比例: %.1f%%)\n",
                validCount, totalCount > 0 ? (double) validCount * 100 / totalCount : 0);
        System.out.printf("  • 判定为免动账/通知 (Invalid)   : %d 封 (验证码/广告/服务号/还款提醒)\n", invalidCount);
        System.out.println("================================================================================");

        assertTrue(totalCount > 0, "Dev 表中必须有数据可供审计");
        assertTrue(validCount > 0, "必须成功判定出至少部分有效动账");
        assertTrue(invalidCount > 0, "必须成功过滤出无意义的通知与广告短信");
    }
}
