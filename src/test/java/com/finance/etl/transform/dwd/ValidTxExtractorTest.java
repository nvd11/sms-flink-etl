package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
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

    @ParameterizedTest(name = "[{index}] 交易有效性判定: {0} => valid: {1}, dir: {2}, type: {3}")
    @CsvSource({
            "'106980095508【广发银行】您尾号3342信用卡03日10:58消费14.89人民币，交易商户:支付宝-高德打车。', true, OUTFLOW, EXPENSE",
            "'106980095508【广发银行】您尾号3342信用卡09月27日发起退款人民币43.92元，到账情况点查询', true, INFLOW, REFUND",
            "'10690661440018【中意人寿】尊敬的潘瑞成：您的理赔申请已通过审核，赔款金额110.79元将于0-5个工作日到账', true, INFLOW, INCOME",
            "'106980095508【广发银行】您尾号3342信用卡16日00:43还款人民币17253.33元，到账后0', true, OUTFLOW, TRANSFER",
            "'10692576032【平安产险】尊敬的潘文林，您已付6646.00元的保单已承保', true, OUTFLOW, EXPENSE",
            "'【微信支付】微信零钱已向某某便利店成功付款8.00元。SubId：1', true, OUTFLOW, EXPENSE",
            "'【支付宝】花呗自动扣款通知：扣款成功299.00元。SubId：2', true, OUTFLOW, EXPENSE"
    })
    void testValidTransactions(String rawBody, boolean expectedValid, String expectedDirection, String expectedType) {
        Map<String, Object> result = extractor.extract(createRecord(rawBody));

        assertEquals(expectedValid, result.get("is_valid_tx"));
        assertEquals(expectedDirection, result.get("direction"));
        assertEquals(expectedType, result.get("tx_type"));
    }

    @Test
    @DisplayName("测试非真实交易短信一票否决为 false (验证码、套路贷、账单提醒)")
    void testInvalidNotices() {
        String[] invalidSamples = {
                "106980095188000001【支付宝】支付宝验证码：283094，请勿向他人泄露您的验证码！",
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
}
