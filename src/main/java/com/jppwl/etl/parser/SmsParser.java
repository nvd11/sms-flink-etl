package com.jppwl.etl.parser;

import com.jppwl.etl.model.EmailMessage;
import com.jppwl.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SmsParser implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SmsParser.class);

    // 广发银行消费短信正则: 【广发银行】您尾号3342信用卡12日10:40消费50.00人民币，交易商户:支付宝-广州市番禺区何贤纪念医院。
    private static final Pattern CGB_EXPENSE_PATTERN = Pattern.compile(
            "尾号(?<card>\\d{4})[^\\d]*?(?:(?<day>\\d{1,2})日)?(?<time>\\d{1,2}:\\d{2})消费(?<amount>[\\d\\.]+)人民币(?:，交易商户:(?<merchant>[^。]+))?"
    );

    // 验证码短信正则
    private static final Pattern AUTH_CODE_PATTERN = Pattern.compile(
            "(?:验证码|动态码)[为是：:]\\s*(?<code>\\d{4,8})"
    );

    // 账单/出账提醒正则
    private static final Pattern BILL_PATTERN = Pattern.compile(
            "(?:账单|出账日提醒|已出账|应还款)"
    );

    // 通用消费金额正则
    private static final Pattern GENERAL_AMOUNT_PATTERN = Pattern.compile(
            "(?:消费|支出|支付|扣款)(?:人民币)?\\s*(?<amount>\\d+(?:\\.\\d{1,2})?)\\s*(?:元|人民币)?"
    );

    public SmsRecord parse(EmailMessage email) {
        SmsRecord record = new SmsRecord();
        record.setEmailUid(email.getMessageId());
        record.setRawSubject(email.getSubject());
        record.setRawContent(email.getBody() != null ? email.getBody() : "");
        record.setReceivedAt(email.getDate() != null ? email.getDate() : LocalDateTime.now());

        String subject = email.getSubject() != null ? email.getSubject().trim() : "";
        String body = email.getBody() != null ? email.getBody().trim() : "";

        // 识别数据源与发送方
        if (subject.startsWith("com.") || subject.contains("微信") || subject.contains("支付宝")) {
            record.setSourceType("APP_NOTIFICATION");
            record.setSender(subject);
        } else {
            record.setSourceType("SMS");
            record.setSender(!subject.isEmpty() ? subject : "UNKNOWN");
        }

        // 1. 广发银行短信精确匹配
        Matcher cgbMatcher = CGB_EXPENSE_PATTERN.matcher(body);
        if (cgbMatcher.find()) {
            record.setSmsCategory("EXPENSE");
            record.setParsedCardNo(cgbMatcher.group("card"));
            try {
                record.setParsedAmount(new BigDecimal(cgbMatcher.group("amount")));
            } catch (Exception e) {
                LOG.warn("Failed to parse amount: {}", cgbMatcher.group("amount"));
            }
            String merchant = cgbMatcher.group("merchant");
            if (merchant != null && !merchant.trim().isEmpty()) {
                record.setParsedMerchant(merchant.trim());
            }

            // 修正交易时间 (如果短信里带了日月时间)
            String timeStr = cgbMatcher.group("time");
            if (timeStr != null && email.getDate() != null) {
                try {
                    LocalTime time = LocalTime.parse(timeStr.length() == 4 ? "0" + timeStr : timeStr);
                    LocalDate date = email.getDate().toLocalDate();
                    String dayStr = cgbMatcher.group("day");
                    if (dayStr != null) {
                        date = date.withDayOfMonth(Integer.parseInt(dayStr));
                    }
                    record.setReceivedAt(LocalDateTime.of(date, time));
                } catch (Exception ignored) {}
            }
            return record;
        }

        // 2. 验证码分类识别
        if (AUTH_CODE_PATTERN.matcher(body).find() || body.contains("验证码")) {
            record.setSmsCategory("AUTH_CODE");
            return record;
        }

        // 3. 账单分类识别
        if (BILL_PATTERN.matcher(body).find() || BILL_PATTERN.matcher(subject).find()) {
            record.setSmsCategory("BILL");
            return record;
        }

        // 4. 通用金额兜底抽取
        Matcher genMatcher = GENERAL_AMOUNT_PATTERN.matcher(body);
        if (genMatcher.find()) {
            record.setSmsCategory("EXPENSE");
            try {
                record.setParsedAmount(new BigDecimal(genMatcher.group("amount")));
            } catch (Exception ignored) {}
        } else {
            record.setSmsCategory("NOTICE");
        }

        return record;
    }
}
