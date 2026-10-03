package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * DWD 支付通路中介提取器 (PaymentChannelExtractor)
 * 职责：辨析交易是经由支付宝、微信支付 (财付通)、银联云闪付还是银行直连渠道完成扣款。
 *
 * 映射标准代码：
 * - ALIPAY: 支付宝
 * - WECHAT_PAY: 微信支付 / 财付通
 * - UNIONPAY: 中国银联 / 云闪付
 * - DIRECT: 银行直连 / POS机刷卡 / 柜台直付
 */
public class PaymentChannelExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PaymentChannelExtractor.class);

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null || record.getRawBody() == null || record.getRawBody().trim().isEmpty()) {
            return Collections.emptyMap();
        }

        String text = record.getRawBody();
        String channel = "DIRECT"; // 默认银行直联

        if (text.contains("支付宝") || text.contains("花呗") || text.contains("蚂蚁")) {
            channel = "ALIPAY";
        } else if (text.contains("财付通") || text.contains("微信支付") || text.contains("微信零钱")) {
            channel = "WECHAT_PAY";
        } else if (text.contains("银联") || text.contains("云闪付")) {
            channel = "UNIONPAY";
        }

        Map<String, Object> result = new HashMap<>();
        result.put("payment_channel", channel);

        LOG.debug("🌐 [PaymentChannelExtractor] ID: {} => channel: {}", record.getId(), channel);
        return result;
    }
}
