package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * DWD 有效动账判定与交易类型分类器 (ValidTxExtractor)
 * 职责：
 * 1. 甄别报文是否为“真实资金变动的有效交易” (is_valid_tx: true/false)；
 * 2. 判定资金流动方向 (direction: OUTFLOW 支出 / INFLOW 收入)；
 * 3. 确定细分动账类型 (tx_type: EXPENSE 消费, REFUND 退款, INCOME 理赔到账, TRANSFER 转账/还款)。
 *
 * 过滤原则：
 * - 排除纯验证码 (如“验证码为527521”)；
 * - 排除营销宣传与套路贷广告 (如“分子借钱”、“智花”、“中银E贷”、“额度最高30万”)；
 * - 排除账单提醒/催缴通知 (如“当月账单为5.99元，最低还款额0.30元”，非实际交易)；
 * - 排除纯公众号无金额流水推送 (如“交易成功提醒服务号UID：10336”)。
 */
public class ValidTxExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ValidTxExtractor.class);

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null || record.getRawBody() == null || record.getRawBody().trim().isEmpty()) {
            return Collections.emptyMap();
        }

        String text = record.getRawBody();

        // 1. 负向一票否决：检查是否为纯广告、纯通知或纯验证码
        if (isFilteredNotice(text)) {
            Map<String, Object> result = new HashMap<>();
            result.put("is_valid_tx", false);
            return result;
        }

        boolean isValid = false;
        String direction = null;
        String txType = null;

        // 2. 退款与冲正判定 (优先于消费，防止“退款消费”被混淆)
        if (text.contains("退款") || text.contains("冲正")) {
            isValid = true;
            direction = "INFLOW";
            txType = "REFUND";
        }
        // 3. 还款判定 (信用卡还款属于资金流出/转移，且常含“到账后”，必须优先于通用“到账”)
        else if (text.contains("还款") && !text.contains("最低还款") && !text.contains("账单为") && !text.contains("账单应还")) {
            isValid = true;
            direction = "OUTFLOW";
            txType = "TRANSFER";
        }
        // 4. 理赔/赔款/入账收入判定
        else if (text.contains("赔款") || text.contains("理赔") || text.contains("到账") || text.contains("入账")) {
            isValid = true;
            direction = "INFLOW";
            txType = "INCOME";
        }
        // 5. 消费与支付判定 (高频核心场景)
        else if (text.contains("消费") || text.contains("付款") || text.contains("支付") || text.contains("已付") || text.contains("扣款")) {
            isValid = true;
            direction = "OUTFLOW";
            txType = "EXPENSE";
        }
        // 6. 转账买入判定
        else if (text.contains("转账") || text.contains("买入")) {
            isValid = true;
            direction = "OUTFLOW";
            txType = "TRANSFER";
        }

        Map<String, Object> result = new HashMap<>();
        result.put("is_valid_tx", isValid);
        if (isValid) {
            result.put("direction", direction);
            result.put("tx_type", txType);
        }

        LOG.debug("🛡️ [ValidTxExtractor] ID: {} => isValid: {}, direction: {}, txType: {}",
                record.getId(), isValid, direction, txType);
        return result;
    }

    /**
     * 负向一票否决规则
     */
    private boolean isFilteredNotice(String text) {
        // 1. 排除微信/支付宝服务号纯模板消息
        if (text.startsWith("com.tencent.mm广发信用卡: 交易成功提醒服务号UID：")) {
            return true;
        }
        // 2. 排除纯验证码短信
        if (text.contains("验证码") && !text.contains("消费") && !text.contains("付款") && !text.contains("已付")) {
            return true;
        }
        // 3. 排除虚假贷款/套路贷广告
        if (text.contains("预放款") || text.contains("预核准") || text.contains("贷款最高")
                || text.contains("额度最高") || text.contains("中银E贷") || text.contains("分子借钱") || text.contains("智花")) {
            return true;
        }
        // 4. 排除信用卡账单还款通知 (非交易流水)
        if (text.contains("当月账单") || text.contains("最低还款额") || text.contains("账单应还款金额") || text.contains("出账日提醒")) {
            return true;
        }
        // 5. 排除防空警报、政务通知、云服务提醒
        if (text.contains("防空警报") || text.contains("农业普查") || text.contains("资源包即将到期")) {
            return true;
        }
        return false;
    }
}
