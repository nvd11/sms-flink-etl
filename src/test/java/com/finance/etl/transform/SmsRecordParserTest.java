package com.finance.etl.transform;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 transform 包下的 SmsRecordParser 业务脱壳与动账提炼单元测试
 */
public class SmsRecordParserTest {

    @Test
    @DisplayName("测试 SmsRecordParser: 验证对广发银行(95508)消费动账邮件的脱壳与防重指纹")
    public void testParseGuangfaEmail() {
        SmsRecordParser parser = new SmsRecordParser();

        RawEmail email = new RawEmail();
        email.setImapUid(1088L);
        email.setMessageId("<cgb-tx-9988@gmail.com>");
        email.setSubject("【广发银行】信用卡消费通知");
        email.setBody("您尾号3342信用卡27日12:36消费459.00人民币，交易商户:支付宝-安庆市大观区惠佳数码产品经营部。");
        email.setReceivedAt(Instant.now());

        List<SmsRecord> records = parser.parse(email);
        assertNotNull(records);
        assertEquals(1, records.size());

        SmsRecord r = records.get(0);
        assertEquals("95508", r.getSender());
        assertEquals("EMAIL_IMAP", r.getChannel());
        assertEquals("SIM_SLOT_1", r.getReceiverPhone());
        assertNotNull(r.getMsgUid());
        assertEquals(64, r.getMsgUid().length(), "SHA-256 摘要指纹长度为 64");
        assertTrue(r.getRawBody().contains("459.00"));
    }

    @Test
    @DisplayName("测试 SmsRecordParser: 验证对微信支付与卡槽2(SIM2)的准确识别")
    public void testParseWeChatAndSimSlot2() {
        SmsRecordParser parser = new SmsRecordParser();

        RawEmail email = new RawEmail();
        email.setMessageId("<wx-sim2-1234@gmail.com>");
        email.setSubject("[SIM2] 微信支付扣款凭证");
        email.setBody("微信支付：您已成功向某某便利店付款 12.00 元。");
        email.setReceivedAt(Instant.now());

        List<SmsRecord> records = parser.parse(email);
        assertEquals(1, records.size());

        SmsRecord r = records.get(0);
        assertEquals("WECHAT_PAY", r.getSender());
        assertEquals("SIM_SLOT_2", r.getReceiverPhone(), "应识别出卡槽2");
    }
}
