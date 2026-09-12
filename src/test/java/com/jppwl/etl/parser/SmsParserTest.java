package com.jppwl.etl.parser;

import com.jppwl.etl.model.EmailMessage;
import com.jppwl.etl.model.SmsRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class SmsParserTest {

    private final SmsParser parser = new SmsParser();

    @Test
    public void testParseGuangfaExpenseSms() {
        String body = "106980095508\n【广发银行】您尾号3342信用卡12日10:40消费50.00人民币，交易商户:支付宝-广州市番禺区何贤纪念医院。\n\nSubId：1";
        EmailMessage msg = new EmailMessage("MSG-001", "106980095508", "小米 <nvd11@163.com>", LocalDateTime.of(2026, 9, 12, 10, 40, 31), body);

        SmsRecord record = parser.parse(msg);

        assertEquals("MSG-001", record.getEmailUid());
        assertEquals("SMS", record.getSourceType());
        assertEquals("106980095508", record.getSender());
        assertEquals("EXPENSE", record.getSmsCategory());
        assertEquals("3342", record.getParsedCardNo());
        assertEquals(new BigDecimal("50.00"), record.getParsedAmount());
        assertEquals("支付宝-广州市番禺区何贤纪念医院", record.getParsedMerchant());
    }

    @Test
    public void testParseGuangfaParkingExpense() {
        String body = "106980095508\n【广发银行】您尾号3342信用卡12日12:56消费18.00人民币，交易商户:财付通-易放停车。\n\nSubId：1";
        EmailMessage msg = new EmailMessage("MSG-002", "106980095508", "nvd11@163.com", LocalDateTime.of(2026, 9, 12, 12, 56, 16), body);

        SmsRecord record = parser.parse(msg);

        assertEquals("EXPENSE", record.getSmsCategory());
        assertEquals("3342", record.getParsedCardNo());
        assertEquals(new BigDecimal("18.00"), record.getParsedAmount());
        assertEquals("财付通-易放停车", record.getParsedMerchant());
    }

    @Test
    public void testParseAuthCodeSms() {
        String body = "95508\n【广发银行】验证码为024789，您正进行快捷绑卡签约小米支付，慎防诈骗，切勿转发或告知他人。\n\nSubId：1";
        EmailMessage msg = new EmailMessage("MSG-003", "95508", "nvd11@163.com", LocalDateTime.now(), body);

        SmsRecord record = parser.parse(msg);

        assertEquals("AUTH_CODE", record.getSmsCategory());
    }

    @Test
    public void testParseAlipayBillNotification() {
        String body = "com.eg.android.AlipayGphone\n广发银行信用卡(3342)已出账，点击查看本期账单金额\n出账日提醒\nUID：10340";
        EmailMessage msg = new EmailMessage("MSG-004", "com.eg.android.AlipayGphone", "nvd11@163.com", LocalDateTime.now(), body);

        SmsRecord record = parser.parse(msg);

        assertEquals("APP_NOTIFICATION", record.getSourceType());
        assertEquals("BILL", record.getSmsCategory());
    }
}
