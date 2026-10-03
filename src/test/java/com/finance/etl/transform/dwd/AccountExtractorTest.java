package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
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
}
