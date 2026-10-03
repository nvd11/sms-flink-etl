package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * DWD 交易细分类型与资金流向提取器 (TxTypeExtractor)
 * 职责：
 * 1. 确定资金流向 (direction: OUTFLOW 资金流出/支出, INFLOW 资金流入/收入)；
 * 2. 识别细分动账类型 (tx_type: EXPENSE 消费支出, REFUND 退款冲正, INCOME 收入/理赔, TRANSFER 资金划转/还款)。
 *
 * 恪守单一职责原则 (SRP)：
 * - 专职负责识别流向与交易类型；
 * - 验证有效性由 ValidTxExtractor 负责。
 */
public class TxTypeExtractor implements DwdFieldExtractor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TxTypeExtractor.class);

    @Override
    public Map<String, Object> extract(SmsRecord record) {
        if (record == null || record.getRawBody() == null || record.getRawBody().trim().isEmpty()) {
            return Collections.emptyMap();
        }

        String text = record.getRawBody();

        // 1. 快速检查：纯非交易通知不返回类型
        if (isIgnoredNotice(text)) {
            return Collections.emptyMap();
        }

        String direction = null;
        String txType = null;

        // 2. 优先级 1: 退款与冲正 (优先于消费)
        if (text.contains("退款") || text.contains("冲正")) {
            direction = "INFLOW";
            txType = "REFUND";
        }
        // 3. 优先级 2: 信用卡还款 (流出，优先于通用“到账”)
        else if (text.contains("还款") && !text.contains("最低还款") && !text.contains("账单为") && !text.contains("账单应还")) {
            direction = "OUTFLOW";
            txType = "TRANSFER";
        }
        // 4. 优先级 3: 理赔/赔款/入账收入
        else if (text.contains("赔款") || text.contains("理赔") || text.contains("到账") || text.contains("入账")) {
            direction = "INFLOW";
            txType = "INCOME";
        }
        // 5. 优先级 4: 消费支出
        else if (text.contains("消费") || text.contains("付款") || text.contains("支付") || text.contains("已付") || text.contains("扣款")) {
            direction = "OUTFLOW";
            txType = "EXPENSE";
        }
        // 6. 优先级 5: 转账与理财买入
        else if (text.contains("转账") || text.contains("买入")) {
            direction = "OUTFLOW";
            txType = "TRANSFER";
        }

        if (direction == null || txType == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> result = new HashMap<>();
        result.put("direction", direction);
        result.put("tx_type", txType);

        LOG.debug("📊 [TxTypeExtractor] ID: {} => direction: {}, txType: {}",
                record.getId(), direction, txType);
        return result;
    }

    private boolean isIgnoredNotice(String text) {
        if (text.contains("验证码") && !text.contains("消费") && !text.contains("付款") && !text.contains("已付")) {
            return true;
        }
        if (text.startsWith("com.tencent.mm广发信用卡: 交易成功提醒服务号UID：")) {
            return true;
        }
        if (text.contains("当月账单") || text.contains("最低还款额") || text.contains("账单应还款金额")
                || text.contains("余额不足") || text.contains("未按时还款")) {
            return true;
        }
        return false;
    }
}
