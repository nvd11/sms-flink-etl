package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DWD 交易消费分类提取器 (CategoryExtractor)
 * 职责：
 * 根据商户名称 (cleaned_merchant / counterparty) 与短信正文内容，
 * 智能归纳至 5 大消费类目：
 * 1. FOOD (餐饮美食/快餐/外卖/奶茶生鲜等)
 * 2. TRANSPORT (交通出行/高德打车/地铁/停车/充电等)
 * 3. SHOPPING (日常购物/天猫/拼多多/盒马/商超/电商等)
 * 4. MEDICAL (医疗健康/医院/大药房/慢病防治/体检等)
 * 5. OTHER (金融保险理赔、公共事业缴费、电信通信、其他未分类)
 */
public class CategoryExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CategoryExtractor.class);

    // 餐饮美食类关键词
    private static final List<String> FOOD_KEYWORDS = List.of(
            "快餐", "餐饮", "饭堂", "小面", "米线", "美食", "萨莉亚", "煲珠公",
            "奶昔", "生果", "蛋挞", "面馆", "料理", "咖啡", "莲香楼", "丰味馆",
            "索迪斯", "饿了么"
    );

    // 交通出行类关键词
    private static final List<String> TRANSPORT_KEYWORDS = List.of(
            "打车", "高德", "地铁", "公交", "停车", "易充", "南网电动", "汽车文化",
            "滴滴", "顺易通", "科拓", "车主", "加油"
    );

    // 医疗健康类关键词
    private static final List<String> MEDICAL_KEYWORDS = List.of(
            "医院", "大药房", "药店", "卫生服务", "门诊", "诊所", "慢病防治", "何贤", "番禺中心医院"
    );

    // 日常购物类关键词
    private static final List<String> SHOPPING_KEYWORDS = List.of(
            "拼多多", "天猫", "淘宝", "京东", "盒马", "超市", "沃尔玛", "永旺",
            "大润发", "康成投资", "今日卖场", "商贸", "箱包", "数码", "家具", "家居", "现代农业", "曼伦",
            "欧风麦甜", "所见所得"
    );

    // 其它金融/生活缴费类关键词（若未匹配以上，则兜底归为 OTHER）
    private static final List<String> OTHER_FINANCE_KEYWORDS = List.of(
            "保险", "人寿", "平安", "申能", "理赔", "还款", "燃气", "中国移动", "话费", "物业", "支付宝平台"
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

        // 3. 餐饮美食 (FOOD)
        for (String kw : FOOD_KEYWORDS) {
            if (text.contains(kw)) {
                return "FOOD";
            }
        }

        // 4. 日常购物 (SHOPPING)
        for (String kw : SHOPPING_KEYWORDS) {
            if (text.contains(kw)) {
                return "SHOPPING";
            }
        }

        // 5. 其它公共事业、金融保险、通知缴费等兜底归为 OTHER
        return "OTHER";
    }
}
