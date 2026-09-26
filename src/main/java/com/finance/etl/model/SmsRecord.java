package com.finance.etl.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * 金融短信动账 ODS 层原始记录模型 (对齐 iceberg.finance.raw_sms_records 表结构)
 */
public class SmsRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private String msgUid;          // 唯一消息指纹 (防重业务键)
    private String channel;         // 采集渠道: 'EMAIL_IMAP'
    private String sender;          // 发送方 (如: 95508, 微信支付)
    private String receiverPhone;   // 本机卡槽接收手机号
    private Instant receivedAt;     // 短信到达物理时间 (UTC微秒时间戳)
    private String rawBody;         // 100% 原始短信报文正文
    private Instant createdAt;      // 本地入湖处理时间

    // Flink POJO 强制要求提供公开无参构造器
    public SmsRecord() {
    }

    public SmsRecord(Long id, String msgUid, String channel, String sender,
                     String receiverPhone, Instant receivedAt, String rawBody, Instant createdAt) {
        this.id = id;
        this.msgUid = msgUid;
        this.channel = channel;
        this.sender = sender;
        this.receiverPhone = receiverPhone;
        this.receivedAt = receivedAt;
        this.rawBody = rawBody;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMsgUid() {
        return msgUid;
    }

    public void setMsgUid(String msgUid) {
        this.msgUid = msgUid;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getReceiverPhone() {
        return receiverPhone;
    }

    public void setReceiverPhone(String receiverPhone) {
        this.receiverPhone = receiverPhone;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }

    public String getRawBody() {
        return rawBody;
    }

    public void setRawBody(String rawBody) {
        this.rawBody = rawBody;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SmsRecord smsRecord = (SmsRecord) o;
        return Objects.equals(msgUid, smsRecord.msgUid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgUid);
    }

    @Override
    public String toString() {
        return "SmsRecord{" +
                "id=" + id +
                ", msgUid='" + msgUid + '\'' +
                ", channel='" + channel + '\'' +
                ", sender='" + sender + '\'' +
                ", receiverPhone='" + receiverPhone + '\'' +
                ", receivedAt=" + receivedAt +
                ", rawBody='" + rawBody + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }
}
