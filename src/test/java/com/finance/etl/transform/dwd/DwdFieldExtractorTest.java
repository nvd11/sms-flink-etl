package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DwdFieldExtractor 契约接口单元测试")
class DwdFieldExtractorTest {

    @Test
    @DisplayName("测试 DwdFieldExtractor 默认名称与纯函数提取契约")
    void testExtractorContract() {
        DwdFieldExtractor testExtractor = new DwdFieldExtractor() {
            @Override
            public Map<String, Object> extract(SmsRecord record) {
                if (record.getRawBody() != null && record.getRawBody().contains("消费")) {
                    return Map.of("test_field", "extracted_value");
                }
                return Map.of();
            }
        };

        assertTrue(testExtractor.getName().contains("DwdFieldExtractorTest"), "默认 getName() 应包含当前实现类名");

        SmsRecord matchRecord = new SmsRecord(1L, "uid1", "EMAIL_IMAP", "CGB", "SIM_1",
                Instant.now(), "您尾号3342信用卡消费10元", Instant.now());
        Map<String, Object> result = testExtractor.extract(matchRecord);
        assertNotNull(result, "提取结果绝不能为 null");
        assertEquals("extracted_value", result.get("test_field"));

        SmsRecord noMatchRecord = new SmsRecord(2L, "uid2", "EMAIL_IMAP", "OTHER", "SIM_1",
                Instant.now(), "您的验证码为123456", Instant.now());
        Map<String, Object> emptyResult = testExtractor.extract(noMatchRecord);
        assertNotNull(emptyResult, "无匹配时应返回空 Map，绝不能为 null");
        assertTrue(emptyResult.isEmpty());
    }
}
