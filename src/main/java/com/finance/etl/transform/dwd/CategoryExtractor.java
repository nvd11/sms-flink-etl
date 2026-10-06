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
 * DWD 交易消费分类提取器 (CategoryExtractor)
 * 职责：
 * 根据商户名称 (cleaned_merchant / counterparty) 与短信正文内容，
 * 智能归纳至 11 大消费类目：
 * 1. FOOD (餐饮美食/快餐/外卖/奶茶生鲜等)
 * 2. TRANSPORT (交通出行/高德打车/地铁/停车/充电等)
 * 3. ONLINE_SHOPPING (线上电商网购/淘宝/天猫/拼多多/京东/得物等)
 * 4. OFFLINE_SHOPPING (日常商超零售/盒马/沃尔玛/大润发/永旺/实体便利店等)
 * 5. MEDICAL (医疗健康/医院/大药房/慢病防治/体检/陪护等)
 * 6. COMMUNICATION (电信通信/手机话费/宽带缴费/10086/10010/10000等)
 * 7. INSURANCE (保险支出/车险保费/人寿险保费等)
 * 8. PROPERTY_MANAGEMENT (物业管理/小区物业费/管理费等)
 * 9. TRAVEL (旅游消费/文旅度假/景区门票/旅游文化等)
 * 10. PERSONAL_TRANSFER (个人转账/个人扫码支付/商户为个人姓名)
 * 11. OTHER (金融杂项、平台服务费、其他未分类)
 */
public class CategoryExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CategoryExtractor.class);

    // 匹配个人姓名商户的正则表达式 (2~4位纯中文人名，如 "交易商户:支付宝-王磊", "交易商户:财付通-郑春花")
    private static final Pattern PERSONAL_NAME_PATTERN = Pattern.compile("(?:交易商户|商户|收款方)[:：](?:支付宝|财付通|微信支付)?-?([\\u4e00-\\u9fa5]{2,4})[。，,\\s]");

    // 1. 交通出行类关键词
    private static final List<String> TRANSPORT_KEYWORDS = List.of(
            "打车", "高德", "地铁", "公交", "停车", "易充", "南网电动", "汽车文化",
            "滴滴", "顺易通", "科拓", "车主", "加油"
    );

    // 2. 医疗健康类关键词
    private static final List<String> MEDICAL_KEYWORDS = List.of(
            "医院", "大药房", "药店", "卫生服务", "门诊", "诊所", "慢病防治", "慢性病",
            "何贤", "番禺中心医院", "陪护"
    );

    // 3. 电信通信类关键词 (COMMUNICATION)
    private static final List<String> COMMUNICATION_KEYWORDS = List.of(
            "话费", "中国移动", "中国联通", "中国电信", "宽带", "通信", "10086", "10010", "10000"
    );

    // 4. 保险支出类关键词 (INSURANCE)
    private static final List<String> INSURANCE_KEYWORDS = List.of(
            "保费", "保单", "平安产险", "平安保险", "申能保险", "中意人寿", "人寿保险", "财产保险", "蚂蚁保"
    );

    // 5. 物业管理类关键词 (PROPERTY_MANAGEMENT)
    private static final List<String> PROPERTY_KEYWORDS = List.of(
            "物业管理", "物业服务", "浩丰物业", "物业费", "管理费"
    );

    // 6. 旅游消费类关键词 (TRAVEL)
    private static final List<String> TRAVEL_KEYWORDS = List.of(
            "文旅", "旅游文化", "景区", "门票", "旅行社", "度假", "苏荷文旅", "鼎基旅游"
    );

    // 7. 餐饮美食类关键词 (FOOD)
    private static final List<String> FOOD_KEYWORDS = List.of(
            "快餐", "餐饮", "饭堂", "小面", "米线", "美食", "萨莉亚", "煲珠公",
            "奶昔", "生果", "蛋挞", "面馆", "料理", "咖啡", "莲香楼", "丰味馆",
            "索迪斯", "饿了么", "拉扎斯", "美团", "三快", "茶理宜世", "奶茶"
    );

    // 8. 线上电商网购类关键词 (ONLINE_SHOPPING)
    private static final List<String> ONLINE_SHOPPING_KEYWORDS = List.of(
            "拼多多", "天猫", "淘宝", "京东", "得物", "唯品会", "抖音电商", "快手电商", "闲鱼",
            "苏宁易购", "当当网", "小米商城", "网易严选", "阿里", "电子商务", "艺牛"
    );

    // 9. 线下实体商超零售类关键词 (OFFLINE_SHOPPING)
    private static final List<String> OFFLINE_SHOPPING_KEYWORDS = List.of(
            "盒马", "超市", "沃尔玛", "永旺", "大润发", "山姆", "便利店", "康成投资",
            "今日卖场", "商贸", "箱包", "数码", "家具", "家居", "现代农业", "曼伦",
            "欧风麦甜", "所见所得", "百货", "商场"
    );

    // 其它金融/生活缴费类关键词（若未匹配以上，则兜底归为 OTHER）
    private static final List<String> OTHER_FINANCE_KEYWORDS = List.of(
            "理赔", "还款", "燃气", "支付宝平台", "手续费"
    );

    // 个人商户黑名单（防止误将通用机构名当成人名）
    private static final List<String> NON_PERSONAL_NAMES = List.of(
            "盒马", "美团", "高德", "易充", "京东", "淘宝", "天猫", "超市", "医院", "饭堂", "小面"
    );

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null || record.getRawBody() == null || record.getRawBody().trim().isEmpty()) {
            return Collections.emptyMap();
        }

        String text = record.getRawBody();
        String category = classifyCategory(text);

        Map<String, Object> result = new HashMap<>();
        result.put("category", category);

        LOG.debug("🏷️ [CategoryExtractor] ID: {} => category: {}", record.getId(), category);
        return result;
    }

    private String classifyCategory(String text) {
        // 1. 交通出行 (TRANSPORT) 优先级最高，避免“高德打车”被误判
        for (String kw : TRANSPORT_KEYWORDS) {
            if (text.contains(kw)) {
                return "TRANSPORT";
            }
        }

        // 2. 医疗健康 (MEDICAL)
        for (String kw : MEDICAL_KEYWORDS) {
            if (text.contains(kw)) {
                return "MEDICAL";
            }
        }

        // 3. 电信通信 (COMMUNICATION)
        for (String kw : COMMUNICATION_KEYWORDS) {
            if (text.contains(kw)) {
                return "COMMUNICATION";
            }
        }

        // 4. 保险支出 (INSURANCE)
        for (String kw : INSURANCE_KEYWORDS) {
            if (text.contains(kw)) {
                return "INSURANCE";
            }
        }

        // 5. 物业管理 (PROPERTY_MANAGEMENT)
        for (String kw : PROPERTY_KEYWORDS) {
            if (text.contains(kw)) {
                return "PROPERTY_MANAGEMENT";
            }
        }

        // 6. 旅游消费 (TRAVEL)
        for (String kw : TRAVEL_KEYWORDS) {
            if (text.contains(kw)) {
                return "TRAVEL";
            }
        }

        // 7. 餐饮美食 (FOOD)
        for (String kw : FOOD_KEYWORDS) {
            if (text.contains(kw)) {
                return "FOOD";
            }
        }

        // 8. 线上电商网购 (ONLINE_SHOPPING)
        for (String kw : ONLINE_SHOPPING_KEYWORDS) {
            if (text.contains(kw)) {
                return "ONLINE_SHOPPING";
            }
        }

        // 9. 线下日常商超零售 (OFFLINE_SHOPPING)
        for (String kw : OFFLINE_SHOPPING_KEYWORDS) {
            if (text.contains(kw)) {
                return "OFFLINE_SHOPPING";
            }
        }

        // 10. 个人转账 (PERSONAL_TRANSFER) - 智能识别个人姓名收款码
        Matcher personalMatcher = PERSONAL_NAME_PATTERN.matcher(text);
        if (personalMatcher.find()) {
            String name = personalMatcher.group(1);
            boolean isBlacklisted = NON_PERSONAL_NAMES.stream().anyMatch(name::contains);
            if (!isBlacklisted) {
                return "PERSONAL_TRANSFER";
            }
        }

        // 11. 其它公共事业、金融杂项、通知缴费等兜底归为 OTHER
        return "OTHER";
    }
}
