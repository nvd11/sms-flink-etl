package com.finance.etl.transform.dwd;

import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.model.SmsRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SmsRecordToDwdTransactionMapper 领域模型全维度组装单元测试")
class SmsRecordToDwdTransactionMapperTest {

    @Test
    @DisplayName("测试真实广发消费短信全维度提取与组装为 FinancialTransaction")
    void testMapCgbExpenseSms() throws Exception {
        SmsRecordToDwdTransactionMapper mapper = new SmsRecordToDwdTransactionMapper();

        String raw = "106980095508【广发银行】您尾号3342信用卡03日12:10消费9.95人民币，交易商户:支付宝-高德打车。SubId：12026-10-03 12:10:27";
        SmsRecord record = new SmsRecord(441L, "msg-001", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), raw, Instant.now());

        FinancialTransaction tx = mapper.map(record);

        assertNotNull(tx);
        assertEquals(441L, tx.getRawRecordId());
        assertTrue(tx.getId().startsWith("tx_441_"));
        assertTrue(tx.getIsValidTx());
        assertEquals(new BigDecimal("9.95"), tx.getAmount());
        assertEquals("CNY", tx.getCurrency());
        assertEquals("OUTFLOW", tx.getDirection());
        assertEquals("EXPENSE", tx.getTxType());
        assertEquals("CREDIT_CARD", tx.getAccountType());
        assertEquals("3342", tx.getCardTail());
        assertEquals("ALIPAY", tx.getPaymentChannel());
        assertEquals("支付宝-高德打车", tx.getCounterparty());
        assertEquals("高德打车", tx.getCleanedMerchant());
        assertEquals("TRANSPORT", tx.getCategory());
        assertNotNull(tx.getTxTime());
        assertNotNull(tx.getEtlCreatedAt());
    }

    @Test
    @DisplayName("测试保险理赔短信组装：收入、非消费、归类 OTHER")
    void testMapInsuranceIncomeSms() throws Exception {
        SmsRecordToDwdTransactionMapper mapper = new SmsRecordToDwdTransactionMapper();

        String raw = "10690661440018【中意人寿】尊敬的潘瑞成：您2026年09月12日提交的理赔申请已通过审核，赔款金额279.95元将于0-5个工作日到账。SubId：12026-09-17 09:23:13";
        SmsRecord record = new SmsRecord(168L, "msg-002", "EMAIL_IMAP", "OTHER", "SIM_1", Instant.now(), raw, Instant.now());

        FinancialTransaction tx = mapper.map(record);

        assertNotNull(tx);
        assertTrue(tx.getIsValidTx());
        assertEquals(new BigDecimal("279.95"), tx.getAmount());
        assertEquals("INFLOW", tx.getDirection());
        assertEquals("INCOME", tx.getTxType());
        assertEquals("中意人寿", tx.getCleanedMerchant());
        assertEquals("OTHER", tx.getCategory());
    }

    @Test
    @DisplayName("测试 null 输入安全防卫")
    void testNullInput() throws Exception {
        SmsRecordToDwdTransactionMapper mapper = new SmsRecordToDwdTransactionMapper();
        assertNull(mapper.map(null));
    }
}
