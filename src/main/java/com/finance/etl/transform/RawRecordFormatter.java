package com.finance.etl.transform;

import com.finance.etl.model.SmsRecord;
import org.apache.flink.api.common.functions.MapFunction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

/**
 * Flink ODS 物理元数据规整与类型映射算子
 * 负责将输入的非结构化动账消息转换为符合 Lakehouse 表结构的 SmsRecord 实体
 */
public class RawRecordFormatter implements MapFunction<String, SmsRecord> {
    private static final long serialVersionUID = 1L;

    @Override
    public SmsRecord map(String rawMessage) throws Exception {
        if (rawMessage == null || rawMessage.trim().isEmpty()) {
            return null;
        }

        Instant now = Instant.now();
        String msgUid = generateMessageFingerprint(rawMessage);
        
        // 简单启发式提取发送方 (针对常见银行与服务号如 95508, 微信支付, 支付宝)
        String sender = "OTHER";
        if (rawMessage.contains("95508") || rawMessage.contains("广发银行")) {
            sender = "CGB";
        } else if (rawMessage.contains("微信支付") || rawMessage.contains("财付通")) {
            sender = "WECHAT_PAY";
        } else if (rawMessage.contains("支付宝") || rawMessage.contains("蚂蚁金服")) {
            sender = "ALIPAY";
        }

        SmsRecord record = new SmsRecord();
        record.setId(System.nanoTime());
        record.setMsgUid(msgUid);
        record.setChannel("EMAIL_IMAP");
        record.setSender(sender);
        record.setReceiverPhone("SIM_SLOT_1");
        record.setReceivedAt(now);
        record.setRawBody(rawMessage.trim());
        record.setCreatedAt(now);

        return record;
    }

    /**
     * 基于 SHA-256 生成唯一的报文业务指纹 (幂等去重键)
     */
    public static String generateMessageFingerprint(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            return String.valueOf(content.hashCode());
        }
    }
}
