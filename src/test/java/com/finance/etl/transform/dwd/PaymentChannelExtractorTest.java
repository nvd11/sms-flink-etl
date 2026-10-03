package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
