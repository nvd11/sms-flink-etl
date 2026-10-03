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
 * DWD 商户对手方与纯净商户名提炼提取器 (MerchantExtractor)
 * 职责：
 * 1. 提炼交易对手方原名 (counterparty: 如 "支付宝-高德打车", "财付通-知味园自选快餐", "中国平安财产保险")；
 * 2. 智能提炼纯净商户品牌名 (cleaned_merchant: 如 "高德打车", "知味园自选快餐", "中国平安", "中意人寿")。
 */
public class MerchantExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MerchantExtractor.class);

    // 模式 1: 广发/标准银行: "交易商户:支付宝-高德打车。" 或 "交易商户:广东永旺番禺广场店。"
    private static final Pattern MERCHANT_LABEL_PATTERN = Pattern.compile(
            "交易商户[：:]([^。，,;\n\r]+)"
    );

    // 模式 2: 银联/转账/付款场景: "向[中国平安财产保险]付款" 或 "向[某某便利店]成功付款"
    private static final Pattern TOWARDS_PAYEE_PATTERN = Pattern.compile(
            "向([^，,。]+?)(?:付款|支付|成功付款|转账)"
    );

    // 公司法律实体冗余后缀列表 (自长向短匹配清理)
    private static final List<String> CORPORATE_SUFFIXES = List.of(
            "网络科技有限公司", "电子商务有限公司", "信息科技有限公司", "供应链管理有限公司",
            "商贸有限公司", "投资有限公司", "科技有限公司", "餐饮管理有限公司",
            "物业管理有限公司", "通讯技术股份有限公司", "集团有限公司", "股份有限公司",
            "有限公司", "有限责任公司", "经营部"
    );

    // 知名品牌/商户直接映射规范表
    private static final Map<String, String> KNOWN_MERCHANT_MAP = Map.ofEntries(
            Map.entry("拼多多平台商户", "拼多多"),
            Map.entry("浙江天猫供应链管理有限公司", "天猫"),
            Map.entry("天猫超市", "天猫超市"),
            Map.entry("盒马", "盒马鲜生"),
            Map.entry("广州盒马鲜生网络科技有限公司", "盒马鲜生"),
            Map.entry("北京三快在线科技有限公司", "美团"),
            Map.entry("美团", "美团"),
            Map.entry("上海拉扎斯信息科技有限公司", "饿了么"),
            Map.entry("高德打车", "高德打车"),
            Map.entry("高德地图", "高德打车"),
            Map.entry("广州地铁集团有限公司", "广州地铁"),
            Map.entry("广州地铁", "广州地铁"),
            Map.entry("康成投资（中国）有限公司", "大润发"),
            Map.entry("康成投资", "大润发"),
            Map.entry("沃尔玛（中国）投资有限公司", "沃尔玛"),
            Map.entry("沃尔玛", "沃尔玛"),
            Map.entry("杭州今日卖场供应链管理有限公司", "今日卖场"),
            Map.entry("支付宝支付科技有限公司", "支付宝平台服务"),
            Map.entry("中国平安财产保险", "中国平安"),
            Map.entry("中国平安", "中国平安"),
            Map.entry("平安产险", "中国平安"),
            Map.entry("中意人寿", "中意人寿"),
            Map.entry("申能保险", "申能保险")
    );

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null || record.getRawBody() == null || record.getRawBody().trim().isEmpty()) {
            return Collections.emptyMap();
        }

        String text = record.getRawBody();
        String counterparty = extractCounterparty(text, record.getSender());
        if (counterparty == null || counterparty.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        counterparty = counterparty.trim();
        String cleanedMerchant = cleanMerchant(counterparty);

        Map<String, Object> result = new HashMap<>();
        result.put("counterparty", counterparty);
        result.put("cleaned_merchant", cleanedMerchant != null ? cleanedMerchant : counterparty);

        LOG.debug("🏷️ [MerchantExtractor] ID: {} => counterparty: '{}', cleaned: '{}'",
                record.getId(), counterparty, cleanedMerchant);
        return result;
    }

    private String extractCounterparty(String text, String sender) {
        // 1. 尝试从 "交易商户:xxx" 提取
        Matcher m1 = MERCHANT_LABEL_PATTERN.matcher(text);
        if (m1.find()) {
            return m1.group(1).trim();
        }

        // 2. 尝试从 "向xxx付款" 提取
        Matcher m2 = TOWARDS_PAYEE_PATTERN.matcher(text);
        if (m2.find()) {
            return m2.group(1).trim();
        }

        // 3. 保险理赔场景：中意人寿、申能保险、平安产险
        if (text.contains("中意人寿")) {
            return "中意人寿";
        }
        if (text.contains("申能保险")) {
            return "申能保险";
        }
        if (text.contains("平安产险") || text.contains("中国平安")) {
            return "中国平安";
        }

        // 4. 燃气缴费
        if (text.contains("港华燃气")) {
            return "港华燃气";
        }

        // 5. 话费账单
        if (text.contains("【话费账单】") || (text.contains("中国移动") && text.contains("消费"))) {
            return "中国移动";
        }

        return null;
    }

    private String cleanMerchant(String counterparty) {
        if (counterparty == null) {
            return null;
        }

        String raw = counterparty.trim();

        // 1. 剥离支付渠道前缀（如 "支付宝-高德打车" => "高德打车", "财付通-知味园自选快餐" => "知味园自选快餐"）
        if (raw.startsWith("支付宝-") || raw.startsWith("财付通-") || raw.startsWith("微信支付-") || raw.startsWith("银联-")) {
            raw = raw.substring(raw.indexOf('-') + 1).trim();
        }

        // 2. 检查规范化商户映射表
        if (KNOWN_MERCHANT_MAP.containsKey(raw)) {
            return KNOWN_MERCHANT_MAP.get(raw);
        }

        // 3. 剥离商户后缀修饰（如 "煲珠公收款" => "煲珠公"）
        if (raw.endsWith("收款")) {
            raw = raw.substring(0, raw.length() - 2).trim();
        }

        // 4. 剥离法律公司后缀
        for (String suffix : CORPORATE_SUFFIXES) {
            if (raw.endsWith(suffix) && raw.length() > suffix.length()) {
                raw = raw.substring(0, raw.length() - suffix.length()).trim();
                break;
            }
        }

        // 5. 若剥离后匹配已知映射表再次精炼
        if (KNOWN_MERCHANT_MAP.containsKey(raw)) {
            return KNOWN_MERCHANT_MAP.get(raw);
        }

        return raw.isEmpty() ? counterparty : raw;
    }
}
