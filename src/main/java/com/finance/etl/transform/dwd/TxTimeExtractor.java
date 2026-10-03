package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DWD 交易时间精准提取与时区对齐器 (TxTimeExtractor)
 * 职责：从报文正文或物理到达元数据中，提取并构造带有标准 UTC 纳秒戳的权威交易时间 (tx_time: Instant)。
 *
 * 提取规则优先级：
 * 1. 优先提取手机端转发器注入的毫秒级物理接收时间戳: "SubId：12026-09-29 19:43:16" (解析并按 Asia/Shanghai 转为 UTC Instant)；
 * 2. 次优提取正文中银行注明的时间: "03日10:58消费" (结合 receivedAt 年月组合生成完整时间)；
 * 3. 兜底策略：使用 ODS 的物理到达时间 receivedAt；若仍为 null 则降级为 Instant.now()。
 */
public class TxTimeExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TxTimeExtractor.class);

    private static final ZoneId SHANGHAI_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter SUBID_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 模式 1: 转发器结尾时间戳 "SubId：12026-09-29 19:43:16"
    private static final Pattern SUBID_TIMESTAMP_PATTERN = Pattern.compile(
            "SubId[：:]\\s*[0-9]+?(\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2})"
    );

    // 模式 2: 正文中银行交易时间 "29日19:43消费", "16日00:43还款"
    private static final Pattern IN_BODY_TIME_PATTERN = Pattern.compile(
            "(\\d{1,2})日(\\d{1,2}):(\\d{2})"
    );

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null) {
            return Collections.emptyMap();
        }

        Instant txTime = null;
        String text = record.getRawBody();

        if (text != null && !text.isEmpty()) {
            // 1. 尝试从 SubId 提取精确的年月日时分秒
            Matcher subIdMatcher = SUBID_TIMESTAMP_PATTERN.matcher(text);
            if (subIdMatcher.find()) {
                String timeStr = subIdMatcher.group(1);
                try {
                    LocalDateTime ldt = LocalDateTime.parse(timeStr, SUBID_TIME_FORMATTER);
                    txTime = ldt.atZone(SHANGHAI_ZONE).toInstant();
                } catch (Exception e) {
                    LOG.debug("⚠️ [TxTimeExtractor] Failed to parse SubId time '{}'", timeStr);
                }
            }

            // 2. 尝试从正文的 "DD日HH:mm" 组合提取
            if (txTime == null) {
                Matcher bodyMatcher = IN_BODY_TIME_PATTERN.matcher(text);
                if (bodyMatcher.find()) {
                    try {
                        int day = Integer.parseInt(bodyMatcher.group(1));
                        int hour = Integer.parseInt(bodyMatcher.group(2));
                        int min = Integer.parseInt(bodyMatcher.group(3));

                        ZonedDateTime refTime = (record.getReceivedAt() != null ? record.getReceivedAt() : Instant.now())
                                .atZone(SHANGHAI_ZONE);

                        ZonedDateTime parsedZdt = refTime.withDayOfMonth(day)
                                .withHour(hour)
                                .withMinute(min)
                                .withSecond(0)
                                .withNano(0);
                        txTime = parsedZdt.toInstant();
                    } catch (Exception e) {
                        LOG.debug("⚠️ [TxTimeExtractor] Failed to combine in-body time");
                    }
                }
            }
        }

        // 3. 兜底策略：继承 ODS 物理时间
        if (txTime == null) {
            txTime = record.getReceivedAt() != null ? record.getReceivedAt() : Instant.now();
        }

        Map<String, Object> result = new HashMap<>();
        result.put("tx_time", txTime);

        LOG.debug("⏱️ [TxTimeExtractor] ID: {} => tx_time: {}", record.getId(), txTime);
        return result;
    }
}
