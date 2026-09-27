package com.finance.etl.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * 原始邮件传输数据模型 (Raw Email DTO)
 * 纯粹记录从 Gmail IMAP 网络短连接中脱壳解析出的邮件协议元数据与正文内容
 * 确保网络连接断开后，下游 Flink 算子可安全在内存中流转与序列化
 */
public class RawEmail implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long imapUid;           // IMAP 邮箱目录下的唯一 UID 序列号
    private String messageId;       // 邮件协议头标准 Message-ID (如: <xxx@mail.gmail.com>)
    private String subject;         // 邮件主题标题 (常包含卡槽 SIM 标识或短信摘要)
    private String from;            // 邮件发送方地址 (如: 163 SMTP 中继发件人)
    private String to;              // 邮件收件方地址 (如: alice.h.y.he@gmail.com)
    private Instant sentAt;         // 邮件原始发送时间
    private Instant receivedAt;     // 邮件服务器物理接收时间
    private String body;            // 邮件纯文本正文内容 (解密与解开 Multipart 之后的内容)
    private String folderName;      // 所在邮件夹目录 (默认 "INBOX")

    // Flink POJO 规范必须提供公开无参构造器
    public RawEmail() {
    }

    public RawEmail(Long imapUid, String messageId, String subject, String from, String to,
                    Instant sentAt, Instant receivedAt, String body, String folderName) {
        this.imapUid = imapUid;
        this.messageId = messageId;
        this.subject = subject;
        this.from = from;
        this.to = to;
        this.sentAt = sentAt;
        this.receivedAt = receivedAt;
        this.body = body;
        this.folderName = folderName;
    }

    public Long getImapUid() {
        return imapUid;
    }

    public void setImapUid(Long imapUid) {
        this.imapUid = imapUid;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public void setSentAt(Instant sentAt) {
        this.sentAt = sentAt;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RawEmail rawEmail = (RawEmail) o;
        return Objects.equals(messageId, rawEmail.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId);
    }

    @Override
    public String toString() {
        return "RawEmail{" +
                "imapUid=" + imapUid +
                ", messageId='" + messageId + '\'' +
                ", subject='" + subject + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", sentAt=" + sentAt +
                ", receivedAt=" + receivedAt +
                ", bodyLength=" + (body != null ? body.length() : 0) +
                ", folderName='" + folderName + '\'' +
                '}';
    }
}
