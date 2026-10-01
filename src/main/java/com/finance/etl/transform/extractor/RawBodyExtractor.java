package com.finance.etl.transform.extractor;

import com.finance.etl.model.RawEmail;
import java.util.Map;

/**
 * 1. 原始短信全文提取器 (RawBodyExtractor)
 * 职责：提取 100% 原始保真短信正文。优先取 email.getBody()，若为空则安全兜底至 email.getSubject()。
 */
public class RawBodyExtractor implements SmsFieldExtractor {
    private static final long serialVersionUID = 1L;

    /**
     * 静态辅助：解析并获得有效短信全文内容 (供其他依赖正文的提取器复用，确保逻辑基准绝对一致)
     */
    public static String resolveContent(RawEmail email) {
        if (email == null) {
            return "";
        }
        String body = email.getBody();
        if (body != null && !body.trim().isEmpty()) {
            return body.trim();
        }
        String subject = email.getSubject();
        return subject != null ? subject.trim() : "";
    }

    @Override
    public Map<String, Object> extract(RawEmail email) {
        return Map.of("rawBody", resolveContent(email));
    }
}
