package com.finance.etl.transform.extractor;

import com.finance.etl.model.RawEmail;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;

/**
 * 4. 业务唯一指纹生成器 (FingerprintExtractor)
 * 职责：结合 Message-ID 与真实短信正文计算不可篡改的 SHA-256 哈希值，作为全局去重防重键 (msgUid)。
 */
public class FingerprintExtractor implements SmsFieldExtractor {
    private static final long serialVersionUID = 1L;

    @Override
    public Map<String, Object> extract(RawEmail email) {
        if (email == null) {
            return Map.of("msgUid", UUID.randomUUID().toString().replace("-", ""));
        }

        String messageId = email.getMessageId() != null ? email.getMessageId() : "";
        String content = RawBodyExtractor.resolveContent(email);
        String source = messageId + "_" + content;

        String fingerprint = calculateSha256(source);
        return Map.of("msgUid", fingerprint);
    }

    private String calculateSha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                String h = Integer.toHexString(0xff & b);
                if (h.length() == 1) {
                    hex.append('0');
                }
                hex.append(h);
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString().replace("-", "");
        }
    }
}
