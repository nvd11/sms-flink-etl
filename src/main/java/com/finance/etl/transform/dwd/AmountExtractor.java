package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DWD 金融动账金额与币种高精度提取器 (AmountExtractor)
 * 职责：从 ODS 原始短信正文中，精确提炼交易金额 (BigDecimal) 与标准三字币种代码 (CNY/USD/HKD)。
 *
 * 核心设计原则：
 * 1. 严格使用确定性“金融语义锚点多级正则”，杜绝自然语言概率模型的模糊不确定性；
 * 2. 严格杜绝 Java 浮点数 (float/double) 精度损失，统一封装为 BigDecimal 并固定保留 2 位小数 (HALF_UP)；
 * 3. 精准免疫卡号尾号 (如 3342)、验证码 (如 527521)、日期时间 (如 2026-10-03) 的数字干扰；
 * 4. 健壮处理千分位逗号格式 (如 2,743.30元)。
 */
public class AmountExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AmountExtractor.class);

    // 核心金额提取正则模式链 (按优先级从高到低精确匹配)
    private static final List<Pattern> AMOUNT_PATTERNS = List.of(
            // 模式 1: 广发/中行/招行等经典格式: "消费31.00人民币", "消费14.89元", "消费 128.50 人民币"
            Pattern.compile("消费\\s*(?:人民币|RMB|￥)?\\s*([0-9,]+(?:\\.[0-9]+)?)\\s*(?:人民币|元)?"),

            // 模式 2: 退款与冲正格式: "发起退款人民币43.92元", "退款43.92元", "退款人民币 8.90 元"
            Pattern.compile("(?:发起退款|退款)\\s*(?:人民币|RMB|￥)?\\s*([0-9,]+(?:\\.[0-9]+)?)\\s*(?:人民币|元)?"),

            // 模式 3: 支付/扣款格式: "成功付款8.00元", "付款6646.00元", "您已付6646.00元的保单", "扣款成功299.00元", "扣款299.00元"
            Pattern.compile("(?:成功付款|您已付|付款|支付|扣款成功|扣款|已扣)\\s*(?:人民币|RMB|￥)?\\s*([0-9,]+(?:\\.[0-9]+)?)\\s*(?:元|人民币)?"),

            // 模式 4: 理赔与到账格式: "赔款金额110.79元", "赔款金额245元", "到账金额100.00元", "已入账100.00元"
            Pattern.compile("(?:赔款金额|理赔金额|到账金额|入账金额|已入账|到账)\\s*(?:人民币|RMB|￥)?\\s*([0-9,]+(?:\\.[0-9]+)?)\\s*(?:元|人民币)?"),

            // 模式 5: 交易成功/转账/账单格式: "交易成功 人民币2,743.3元", "当月账单为5.99元", "还款额为0.30元"
            Pattern.compile("(?:交易成功|账单为|还款额为|还款)\\s*(?:人民币|RMB|￥)?\\s*([0-9,]+(?:\\.[0-9]+)?)\\s*(?:元|人民币)?"),

            // 模式 6: 纯通用货币前缀格式: "人民币2,743.3元", "RMB 128.00"
            Pattern.compile("(?:人民币|RMB|￥)\\s*([0-9,]+(?:\\.[0-9]+)?)\\s*(?:元)?")
    );

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null || record.getRawBody() == null || record.getRawBody().trim().isEmpty()) {
            return Collections.emptyMap();
        }

        String text = record.getRawBody();

        // 1. 过滤明显的纯非动账报文 (如纯验证码通知，防止其中的验证码被误识别)
        if (isNonTransactionNotice(text)) {
            return Collections.emptyMap();
        }

        // 2. 逐级匹配金额模式
        BigDecimal extractedAmount = null;
        for (Pattern pattern : AMOUNT_PATTERNS) {
            Matcher matcher = pattern.matcher(text);
            if (matcher.find()) {
                String rawNum = matcher.group(1);
                extractedAmount = parseToBigDecimal(rawNum);
                if (extractedAmount != null && extractedAmount.compareTo(BigDecimal.ZERO) > 0) {
                    break;
                }
            }
        }

        if (extractedAmount == null) {
            return Collections.emptyMap();
        }

        // 3. 币种识别
        String currency = detectCurrency(text);

        Map<String, Object> result = new HashMap<>();
        result.put("amount", extractedAmount);
        result.put("currency", currency);

        LOG.debug("💰 [AmountExtractor] Extracted: {} {} from SMS [ID: {}]", currency, extractedAmount, record.getId());
        return result;
    }

    /**
     * 将解析出的数字字符串清洗并转化为高精度 BigDecimal (去除千分位逗号，四舍五入锁定 2 位小数)
     */
    private BigDecimal parseToBigDecimal(String rawNum) {
        if (rawNum == null) return null;
        try {
            String cleanNum = rawNum.replace(",", "").trim();
            if (cleanNum.isEmpty()) return null;
            return new BigDecimal(cleanNum).setScale(2, RoundingMode.HALF_UP);
        } catch (Exception e) {
            LOG.warn("⚠️ [AmountExtractor] Failed to parse amount string '{}': {}", rawNum, e.getMessage());
            return null;
        }
    }

    /**
     * 币种探测：识别美元、港币、欧元等外币，默认人民币 (CNY)
     */
    private String detectCurrency(String text) {
        if (text.contains("美元") || text.contains("USD") || text.contains("US$")) {
            return "USD";
        }
        if (text.contains("港币") || text.contains("HKD") || text.contains("HK$")) {
            return "HKD";
        }
        if (text.contains("欧元") || text.contains("EUR")) {
            return "EUR";
        }
        return "CNY";
    }

    /**
     * 前置安全过滤器：排除仅含有验证码、普通订阅通知而无资金实际流动的短信
     */
    private boolean isNonTransactionNotice(String text) {
        // 如果包含验证码，且不含明确动账消费动词，则属于纯验证码
        if (text.contains("验证码") && !text.contains("消费") && !text.contains("付款") && !text.contains("已付")) {
            return true;
        }
        // 排除微信/支付宝服务号纯模板消息
        if (text.startsWith("com.tencent.mm广发信用卡: 交易成功提醒服务号UID：")) {
            return true;
        }
        return false;
    }
}
