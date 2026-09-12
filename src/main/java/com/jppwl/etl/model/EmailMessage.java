package com.jppwl.etl.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class EmailMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String messageId;
    private String subject;
    private String from;
    private LocalDateTime date;
    private String body;

    public EmailMessage() {}

    public EmailMessage(String messageId, String subject, String from, LocalDateTime date, String body) {
        this.messageId = messageId;
        this.subject = subject;
        this.from = from;
        this.date = date;
        this.body = body;
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

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "EmailMessage{" +
                "messageId='" + messageId + '\'' +
                ", subject='" + subject + '\'' +
                ", from='" + from + '\'' +
                ", date=" + date +
                '}';
    }
}
