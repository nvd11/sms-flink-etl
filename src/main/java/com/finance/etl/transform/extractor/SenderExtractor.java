package com.finance.etl.transform.extractor;

import com.finance.etl.model.RawEmail;
import java.util.Map;

/**
 * 2. 渠道发送方识别器 (SenderExtractor)
 * 职责：依据邮件主题与正文特征，启发式识别并规整为统一大写金融机构/支付渠道代码：
 * - CGB: 广发银行 (95508)
 * - CMB: 招商银行 (95555)
 * - BOC: 中国银行 (95566)
 * - HSBC: 汇丰银行 (95366)
 * - ICBC: 工商银行 (95588)
 * - CCB: 建设银行 (95533)
 * - ABC: 农业银行 (95599)
 * - WECHAT_PAY: 微信支付 / 财付通
 * - ALIPAY: 支付宝
 * - OTHER: 未知或非金融通知
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
            sender = "CGB";
        } else if (fullContent.contains("com.tencent.mm") || fullContent.contains("微信支付") || fullContent.contains("财付通")) {
            sender = "WECHAT_PAY";
        } else if (fullContent.contains("Alipay") || fullContent.contains("支付宝") || fullContent.contains("蚂蚁金服")) {
            sender = "ALIPAY";
        } else if (fullContent.contains("95555") || fullContent.contains("招商银行")) {
            sender = "CMB";
        } else if (fullContent.contains("95566") || fullContent.contains("中国银行")) {
            sender = "BOC";
        } else if (fullContent.contains("95366") || fullContent.contains("汇丰银行") || fullContent.contains("汇丰中国") || fullContent.contains("HSBC")) {
            sender = "HSBC";
        } else if (fullContent.contains("95588") || fullContent.contains("工商银行")) {
            sender = "ICBC";
        } else if (fullContent.contains("95533") || fullContent.contains("建设银行")) {
            sender = "CCB";
        } else if (fullContent.contains("95599") || fullContent.contains("农业银行")) {
            sender = "ABC";
        }

        return Map.of("sender", sender);
    }
}
