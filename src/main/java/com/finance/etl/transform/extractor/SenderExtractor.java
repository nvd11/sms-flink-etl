package com.finance.etl.transform.extractor;

import com.finance.etl.model.RawEmail;
import java.util.Map;

/**
 * 2. 渠道发送方识别器 (SenderExtractor)
 * 职责：依据邮件主题与正文特征，启发式识别发送渠道 (95508, WECHAT_PAY, ALIPAY, 95555, 95566, HSBC, OTHER)。
 */
public class SenderExtractor implements SmsFieldExtractor {
    private static final long serialVersionUID = 1L;

    @Override
    public Map<String, Object> extract(RawEmail email) {
        if (email == null) {
            return Map.of("sender", "OTHER");
        }

        String subject = email.getSubject() != null ? email.getSubject() : "";
        String body = email.getBody() != null ? email.getBody() : "";
        String fullContent = subject + " " + body;

        String sender = "OTHER";
        if (fullContent.contains("95508") || fullContent.contains("广发银行") || fullContent.contains("广发信用卡")) {
            sender = "95508";
        } else if (fullContent.contains("com.tencent.mm") || fullContent.contains("微信支付") || fullContent.contains("财付通")) {
            sender = "WECHAT_PAY";
        } else if (fullContent.contains("Alipay") || fullContent.contains("支付宝") || fullContent.contains("蚂蚁金服")) {
            sender = "ALIPAY";
        } else if (fullContent.contains("95555") || fullContent.contains("招商银行")) {
            sender = "95555";
        } else if (fullContent.contains("95566") || fullContent.contains("中国银行")) {
            sender = "95566";
        } else if (fullContent.contains("95366") || fullContent.contains("汇丰银行") || fullContent.contains("汇丰中国") || fullContent.contains("HSBC")) {
            sender = "HSBC";
        }

        return Map.of("sender", sender);
    }
}
