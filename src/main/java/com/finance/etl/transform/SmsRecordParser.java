package com.finance.etl.transform;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 动账短信脱壳与实体解析器 (SmsRecordParser)
 * 职责：从 RawEmail 邮件报文中识别发件渠道（广发95508、微信支付、支付宝、招行95555等），生成 SHA-256 幂等防重指纹，构建 SmsRecord ODS 实体。
 * 原生实现 Flink 的 FlatMapFunction<RawEmail, SmsRecord>，可直接作为 Flink 转换算子挂载。
 */
public class SmsRecordParser implements FlatMapFunction<RawEmail, SmsRecord>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(RawEmail email, Collector<SmsRecord> out) throws Exception {
        List<SmsRecord> records = parse(email);
        for (SmsRecord record : records) {
            out.collect(record);
        }
    }

    /**
     * 将单封邮件解析为一条或多条 SmsRecord 记录
     */
    public List<SmsRecord> parse(RawEmail email) {
        List<SmsRecord> list = new ArrayList<>();
        if (email == null) {
            return list;
        }

        String subject = email.getSubject() != null ? email.getSubject() : "";
        String body = email.getBody() != null ? email.getBody() : "";
        String fullContent = subject + " " + body;

        // 识别业务发送方 (广发 95508, 微信支付, 支付宝, 招行等)
        String sender = "OTHER";
        if (fullContent.contains("95508") || fullContent.contains("广发银行")) {
            sender = "95508";
        } else if (fullContent.contains("微信支付") || fullContent.contains("财付通")) {
            sender = "WECHAT_PAY";
        } else if (fullContent.contains("支付宝") || fullContent.contains("蚂蚁金服")) {
            sender = "ALIPAY";
        } else if (fullContent.contains("95555") || fullContent.contains("招商银行")) {
            sender = "95555";
        }

        // 识别接收手机号 / 卡槽标识 (从 SmsForwarder 默认主题提取，例如: [SIM1] 或手机号)
        String receiverPhone = "SIM_SLOT_1";
        if (subject.contains("SIM2") || body.contains("卡槽2")) {
            receiverPhone = "SIM_SLOT_2";
        }

        // 生成 SHA-256 幂等防重指纹
        String fingerprint = generateSha256(email.getMessageId() + "_" + body.trim());

        SmsRecord record = new SmsRecord();
        record.setId(System.nanoTime());
        record.setMsgUid(fingerprint);
        record.setChannel("EMAIL_IMAP");
        record.setSender(sender);
        record.setReceiverPhone(receiverPhone);
        record.setReceivedAt(email.getReceivedAt() != null ? email.getReceivedAt() : Instant.now());
        record.setRawBody(body.trim().isEmpty() ? subject : body.trim());
        record.setCreatedAt(Instant.now());

        list.add(record);
        return list;
    }

    /**
     * 辅助工具：生成 SHA-256 唯一指纹
     */
    public String generateSha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                String h = Integer.toHexString(0xff & b);
                if (h.length() == 1) hex.append('0');
                hex.append(h);
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString().replace("-", "");
        }
    }
}
