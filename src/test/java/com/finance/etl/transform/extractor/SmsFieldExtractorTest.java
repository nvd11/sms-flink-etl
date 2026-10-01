package com.finance.etl.transform.extractor;

import com.finance.etl.model.RawEmail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SmsFieldExtractor 纯函数提取器组件族单元测试")
class SmsFieldExtractorTest {

    @Test
    @DisplayName("测试 RawBodyExtractor: 正文非空时取正文，正文为空时回退至主题")
    void testRawBodyExtractor() {
        RawBodyExtractor extractor = new RawBodyExtractor();

        RawEmail emailWithBody = new RawEmail();
        emailWithBody.setSubject("Subject Only");
        emailWithBody.setBody("Real Body Content");
        Map<String, Object> result1 = extractor.extract(emailWithBody);
        assertEquals("Real Body Content", result1.get("rawBody"));

        RawEmail emailEmptyBody = new RawEmail();
        emailEmptyBody.setSubject("Fallback Subject");
        emailEmptyBody.setBody("   ");
        Map<String, Object> result2 = extractor.extract(emailEmptyBody);
        assertEquals("Fallback Subject", result2.get("rawBody"));
    }

    @Test
    @DisplayName("测试 SenderExtractor: 精准识别广发、微信支付、支付宝与兜底")
    void testSenderExtractor() {
        SenderExtractor extractor = new SenderExtractor();

        RawEmail cgbEmail = new RawEmail();
        cgbEmail.setSubject("106980095508");
        cgbEmail.setBody("【广发银行】您尾号3342信用卡消费21.24元");
        assertEquals("95508", extractor.extract(cgbEmail).get("sender"));

        RawEmail wxEmail = new RawEmail();
        wxEmail.setSubject("com.tencent.mm");
        wxEmail.setBody("微信支付凭证通知");
        assertEquals("WECHAT_PAY", extractor.extract(wxEmail).get("sender"));

        RawEmail aliEmail = new RawEmail();
        aliEmail.setSubject("com.eg.android.AlipayGphone");
        aliEmail.setBody("支付宝交易动账");
        assertEquals("ALIPAY", extractor.extract(aliEmail).get("sender"));

        RawEmail otherEmail = new RawEmail();
        otherEmail.setSubject("10010");
        otherEmail.setBody("中国联通通知");
        assertEquals("OTHER", extractor.extract(otherEmail).get("sender"));
    }

    @Test
    @DisplayName("测试 SimSlotExtractor: 准确提取 SubId：1 与 SubId：2")
    void testSimSlotExtractor() {
        SimSlotExtractor extractor = new SimSlotExtractor();

        RawEmail sim2Email = new RawEmail();
        sim2Email.setBody("短信内容...SubId：22026-09-27 16:25:51");
        assertEquals("SIM_SLOT_2", extractor.extract(sim2Email).get("receiverPhone"));

        RawEmail sim1Email = new RawEmail();
        sim1Email.setBody("短信内容...SubId：12026-09-27 18:54:07");
        assertEquals("SIM_SLOT_1", extractor.extract(sim1Email).get("receiverPhone"));
    }

    @Test
    @DisplayName("测试 FingerprintExtractor: 生成稳定 64 位 SHA-256 幂等指纹")
    void testFingerprintExtractor() {
        FingerprintExtractor extractor = new FingerprintExtractor();

        RawEmail email = new RawEmail();
        email.setMessageId("<test-msg-12345@gmail.com>");
        email.setBody("【广发银行】您尾号3342消费100元");

        Map<String, Object> result1 = extractor.extract(email);
        Map<String, Object> result2 = extractor.extract(email);

        String fp1 = (String) result1.get("msgUid");
        String fp2 = (String) result2.get("msgUid");

        assertNotNull(fp1);
        assertEquals(64, fp1.length(), "SHA-256 十六进制字符串长度必须为 64 位");
        assertEquals(fp1, fp2, "输入相同时两次提取的幂等指纹必须完全相同");
    }
}
