package com.jppwl.etl.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

public class SmsRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private String emailUid;
    private String sourceType;
    private String sender;
    private LocalDateTime receivedAt;
    private String rawSubject;
    private String rawContent;
    private String smsCategory;
    private BigDecimal parsedAmount;
    private String parsedCurrency;
    private String parsedCardNo;
    private String parsedMerchant;
    private LocalDateTime createdAt;

    public SmsRecord() {
        this.parsedCurrency = "CNY";
        this.smsCategory = "NOTICE";
        this.createdAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEmailUid() {
        return emailUid;
    }

    public void setEmailUid(String emailUid) {
        this.emailUid = emailUid;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(LocalDateTime receivedAt) {
        this.receivedAt = receivedAt;
    }

    public String getRawSubject() {
        return rawSubject;
    }

    public void setRawSubject(String rawSubject) {
        this.rawSubject = rawSubject;
    }

    public String getRawContent() {
        return rawContent;
    }

    public void setRawContent(String rawContent) {
        this.rawContent = rawContent;
    }

    public String getSmsCategory() {
        return smsCategory;
    }

    public void setSmsCategory(String smsCategory) {
        this.smsCategory = smsCategory;
    }

    public BigDecimal getParsedAmount() {
        return parsedAmount;
    }

    public void setParsedAmount(BigDecimal parsedAmount) {
        this.parsedAmount = parsedAmount;
    }

    public String getParsedCurrency() {
        return parsedCurrency;
    }

    public void setParsedCurrency(String parsedCurrency) {
        this.parsedCurrency = parsedCurrency;
    }

    public String getParsedCardNo() {
        return parsedCardNo;
    }

    public void setParsedCardNo(String parsedCardNo) {
        this.parsedCardNo = parsedCardNo;
    }

    public String getParsedMerchant() {
        return parsedMerchant;
    }

    public void setParsedMerchant(String parsedMerchant) {
        this.parsedMerchant = parsedMerchant;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "SmsRecord{" +
                "emailUid='" + emailUid + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", sender='" + sender + '\'' +
                ", receivedAt=" + receivedAt +
                ", smsCategory='" + smsCategory + '\'' +
                ", parsedAmount=" + parsedAmount +
                ", parsedMerchant='" + parsedMerchant + '\'' +
                ", parsedCardNo='" + parsedCardNo + '\'' +
                '}';
    }
}
