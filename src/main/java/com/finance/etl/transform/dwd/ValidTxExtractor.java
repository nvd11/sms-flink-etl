package com.finance.etl.transform.dwd;

import com.finance.etl.model.SmsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * DWD 有效动账判定提取器 (ValidTxExtractor)
 * 职责：专职负责甄别报文是否为“真实资金变动的有效交易” (is_valid_tx: true/false)。
 *
 * 恪守单一职责原则 (SRP)：
 * - 仅输出 key: "is_valid_tx" (Boolean)；
 * - 绝不越权输出资金流向 (direction) 或交易细分类型 (tx_type)。
 *
 * 判定原则：
 * - 负向一票否决：过滤纯验证码、营销广告、套路贷、账单还款提醒、服务号无金额推送、防空政务通知；
 * - 正向特征核验：包含真实动账行为特征词 (消费、付款、退款、理赔、到账等)。
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

        // 1. 负向一票否决：检查是否为纯广告、纯通知、纯催缴或纯验证码
        if (isFilteredNotice(text)) {
            return Map.of("is_valid_tx", false);
        }

        // 2. 正向动账行为特征匹配
        boolean isValid = isPositiveTransaction(text);

        LOG.debug("🛡️ [ValidTxExtractor] ID: {} => isValidTx: {}", record.getId(), isValid);
        return Map.of("is_valid_tx", isValid);
    }

    /**
     * 正向动账行为特征识别
     */
    private boolean isPositiveTransaction(String text) {
        return text.contains("消费") || text.contains("付款") || text.contains("支付")
                || text.contains("已付") || text.contains("扣款") || text.contains("退款")
                || text.contains("冲正") || text.contains("赔款") || text.contains("理赔")
                || text.contains("到账") || text.contains("入账") || text.contains("还款")
                || text.contains("转账") || text.contains("买入");
    }

    /**
     * 负向一票否决规则
     */
    private boolean isFilteredNotice(String text) {
        // 1. 排除微信/支付宝服务号纯模板消息
        if (text.startsWith("com.tencent.mm广发信用卡: 交易成功提醒服务号UID：")) {
            return true;
        }
        // 2. 排除所有包含验证码、动态码的短信 (哪怕含有“向某某付款”，也是授权验证过程，严禁计入真实动账)
        if (text.contains("验证码") || text.contains("动态码") || text.contains("动态口令")) {
            return true;
        }
        // 3. 排除保单承保、订单发货、商品出库等商户凭据/收据类二次通知 (真实扣款已由银行/支付平台流水记录，避免重复翻倍)
        if (text.contains("已承保") || text.contains("已发货") || text.contains("订单已生成") || text.contains("电子发票")) {
            return true;
        }
        // 4. 排除虚假贷款/套路贷广告
        if (text.contains("预放款") || text.contains("预核准") || text.contains("贷款最高")
                || text.contains("额度最高") || text.contains("中银E贷") || text.contains("分子借钱") || text.contains("智花")) {
            return true;
        }
        // 5. 排除信用卡账单还款通知、供款提醒、余额不足催缴 (非真实交易流水)
        if (text.contains("当月账单") || text.contains("最低还款额") || text.contains("账单应还款金额")
                || text.contains("出账日提醒") || text.contains("余额不足") || text.contains("未按时还款")
                || text.contains("若已存足") || text.contains("补充账户余额")) {
            return true;
        }
        // 6. 排除防空警报、政务通知、云服务提醒
        if (text.contains("防空警报") || text.contains("农业普查") || text.contains("资源包即将到期")) {
            return true;
        }
        return false;
    }
}
