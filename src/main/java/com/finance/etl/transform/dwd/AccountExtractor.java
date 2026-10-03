package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DWD 账户类型与卡号尾号提取器 (AccountExtractor)
 * 职责：
 * 1. 提取交易卡片/账户的 4 位尾号 (card_tail: 如 "3342", "0025")；
 * 2. 识别账户类型 (account_type: CREDIT_CARD 信用卡, DEBIT_CARD 借记卡, WALLET 电子钱包/零钱, LOAN 贷款)。
 */
public class AccountExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AccountExtractor.class);

    private static final List<Pattern> CARD_TAIL_PATTERNS = List.of(
            // 模式 1: 银行/金融卡尾号: "尾号3342信用卡", "尾号为0025的美元信用卡", "尾号3342的银行卡", "尾号为2501的账户"
            Pattern.compile("尾号(?:为)?\\s*([0-9]{4})\\s*(?:的)?(?:信用卡|银行卡|储蓄卡|借记卡|账户|卡片|卡)"),

            // 模式 2: 紧贴卡种的尾号: "信用卡(3342)", "银行卡(8888)", "信用卡3342", "银行卡尾号3342"
            Pattern.compile("(?:信用卡|银行卡|储蓄卡|借记卡)(?:尾号|卡号|\\()?([0-9]{4})\\)?"),

            // 模式 3: 传统 "您尾号3342信用卡"
            Pattern.compile("(?:您|客户)?尾号(?:为|卡)?\\s*([0-9]{4})")
    );

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null || record.getRawBody() == null || record.getRawBody().trim().isEmpty()) {
            return Collections.emptyMap();
        }

        String text = record.getRawBody();

        // 快递/运单/包裹等非金融短信直接排除卡尾提取，避免将运单尾号当成银行卡
        if (text.contains("快递") || text.contains("运单") || text.contains("包裹") || text.contains("取件")) {
            return Collections.emptyMap();
        }

        Map<String, Object> result = new HashMap<>();

        // 1. 提取 4 位卡号尾号
        String cardTail = extractCardTail(text);
        if (cardTail != null) {
            result.put("card_tail", cardTail);
        }

        // 2. 识别账户类型
        String accountType = detectAccountType(text, cardTail);
        if (accountType != null) {
            result.put("account_type", accountType);
        }

        LOG.debug("💳 [AccountExtractor] ID: {} => cardTail: {}, accountType: {}",
                record.getId(), cardTail, accountType);
        return result;
    }

    private String extractCardTail(String text) {
        for (Pattern pattern : CARD_TAIL_PATTERNS) {
            Matcher matcher = pattern.matcher(text);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private String detectAccountType(String text, String cardTail) {
        if (text.contains("信用卡")) {
            return "CREDIT_CARD";
        }
        if (text.contains("储蓄卡") || text.contains("借记卡")) {
            return "DEBIT_CARD";
        }
        if (text.contains("微信零钱") || text.contains("零钱") || text.contains("花呗") || text.contains("余额宝")) {
            return "WALLET";
        }
        if (text.contains("贷款") || text.contains("中银E贷") || text.contains("供款账户")) {
            return "LOAN";
        }
        // 如果提取到了尾号且未明确说明，银行卡通常默认为借记卡/储蓄卡
        if (cardTail != null && text.contains("银行卡")) {
            return "DEBIT_CARD";
        }
        return null;
    }
}
