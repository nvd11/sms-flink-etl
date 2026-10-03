package com.finance.etl.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * DWD 金融动账事实领域模型 (严格对齐 iceberg.finance.dwd_financial_transactions 表结构)
 * 标准 Flink POJO，支持高效序列化与列式映射。
 */
public class FinancialTransaction implements Serializable {
    private static final long serialVersionUID = 1L;

    // 1. 业务主键与血缘追溯 (Lineage)
    private String id;                  // 动账事实唯一标识主键 (如 'tx_317_1727957427000')
    private Long rawRecordId;           // 关联的 ODS 原始记录 ID (raw_sms_records.id)

    // 2. 时间维度 (Time Dimension)
    private Instant txTime;             // 真实交易发生的物理时间 (带时区微秒戳)

    // 3. 金额与财务维度 (Financial Metrics)
    private BigDecimal amount;          // 交易金额 (高精度数值，杜绝浮点失真)
    private String currency;            // 币种标准三字码: 'CNY', 'HKD', 'USD' (默认 CNY)
    private String direction;           // 资金方向: 'OUTFLOW' (支出), 'INFLOW' (收入)
    private String txType;              // 交易类型: 'EXPENSE', 'INCOME', 'TRANSFER', 'REFUND'

    // 4. 账户与渠道维度 (Account & Institution)
    private String institution;         // 金融机构代号: 'CGB', 'BOC', 'HSBC', 'ICBC', 'WECHAT_PAY', 'ALIPAY'
    private String accountType;         // 账户类型: 'CREDIT_CARD', 'DEBIT_CARD', 'WALLET', 'LOAN'
    private String cardTail;            // 卡号/账户尾号: 如 '3342'
    private String paymentChannel;      // 支付通路: 'ALIPAY', 'WECHAT_PAY', 'UNIONPAY', 'DIRECT'

    // 5. 对手方与消费场景维度 (Merchant & Categorization)
    private String counterparty;        // 交易对手/商户原名
    private String cleanedMerchant;     // 智能提取纯净商户名
    private String category;            // 消费大类: 'FOOD', 'TRANSPORT', 'SHOPPING', 'MEDICAL', 'OTHER'

    // 6. 治理与审计元数据 (Auditing)
    private Boolean isValidTx;          // 是否为有效动账 (区分真实交易与验证码/营销提醒)
    private Instant etlCreatedAt;       // 清洗入湖时间戳

    // Flink POJO 强制无参构造器
    public FinancialTransaction() {
    }

    public FinancialTransaction(String id, Long rawRecordId, Instant txTime, BigDecimal amount,
                                String currency, String direction, String txType, String institution,
                                String accountType, String cardTail, String paymentChannel,
                                String counterparty, String cleanedMerchant, String category,
                                Boolean isValidTx, Instant etlCreatedAt) {
        this.id = id;
        this.rawRecordId = rawRecordId;
        this.txTime = txTime;
        this.amount = amount;
        this.currency = currency;
        this.direction = direction;
        this.txType = txType;
        this.institution = institution;
        this.accountType = accountType;
        this.cardTail = cardTail;
        this.paymentChannel = paymentChannel;
        this.counterparty = counterparty;
        this.cleanedMerchant = cleanedMerchant;
        this.category = category;
        this.isValidTx = isValidTx;
        this.etlCreatedAt = etlCreatedAt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getRawRecordId() {
        return rawRecordId;
    }

    public void setRawRecordId(Long rawRecordId) {
        this.rawRecordId = rawRecordId;
    }

    public Instant getTxTime() {
        return txTime;
    }

    public void setTxTime(Instant txTime) {
        this.txTime = txTime;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getTxType() {
        return txType;
    }

    public void setTxType(String txType) {
        this.txType = txType;
    }

    public String getInstitution() {
        return institution;
    }

    public void setInstitution(String institution) {
        this.institution = institution;
    }

    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    public String getCardTail() {
        return cardTail;
    }

    public void setCardTail(String cardTail) {
        this.cardTail = cardTail;
    }

    public String getPaymentChannel() {
        return paymentChannel;
    }

    public void setPaymentChannel(String paymentChannel) {
        this.paymentChannel = paymentChannel;
    }

    public String getCounterparty() {
        return counterparty;
    }

    public void setCounterparty(String counterparty) {
        this.counterparty = counterparty;
    }

    public String getCleanedMerchant() {
        return cleanedMerchant;
    }

    public void setCleanedMerchant(String cleanedMerchant) {
        this.cleanedMerchant = cleanedMerchant;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Boolean getIsValidTx() {
        return isValidTx;
    }

    public void setIsValidTx(Boolean isValidTx) {
        this.isValidTx = isValidTx;
    }

    public Instant getEtlCreatedAt() {
        return etlCreatedAt;
    }

    public void setEtlCreatedAt(Instant etlCreatedAt) {
        this.etlCreatedAt = etlCreatedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FinancialTransaction that = (FinancialTransaction) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "FinancialTransaction{" +
                "id='" + id + '\'' +
                ", rawRecordId=" + rawRecordId +
                ", txTime=" + txTime +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", direction='" + direction + '\'' +
                ", txType='" + txType + '\'' +
                ", institution='" + institution + '\'' +
                ", accountType='" + accountType + '\'' +
                ", cardTail='" + cardTail + '\'' +
                ", paymentChannel='" + paymentChannel + '\'' +
                ", counterparty='" + counterparty + '\'' +
                ", cleanedMerchant='" + cleanedMerchant + '\'' +
                ", category='" + category + '\'' +
                ", isValidTx=" + isValidTx +
                ", etlCreatedAt=" + etlCreatedAt +
                '}';
    }
}
