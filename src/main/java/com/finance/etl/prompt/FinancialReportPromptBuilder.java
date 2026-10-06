package com.finance.etl.prompt;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialReportContext;
import com.finance.etl.model.FinancialTransaction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;

/**
 * 财务研报提示词构建器 (FinancialReportPromptBuilder)
 * 职责：
 * 单一职责。专职负责将双流上下文 (FinancialReportContext) 与多维财务事实，
 * 组装为格式严密、零歧义、结构化且周期自适应的 Prompt 注入文本。
 */
public class FinancialReportPromptBuilder {

    /**
     * 核心统一构建函数：根据给定的财务报告上下文组装完整的 Prompt 注入文本
     *
     * @param context 包含周期类型、宏观大盘与全量明细的上下文领域对象
     * @return 结构化 Prompt 注入字符串
     */
    public static String buildPrompt(FinancialReportContext context) {
        Objects.requireNonNull(context, "FinancialReportContext must not be null");

        String periodType = context.getPeriodType() != null ? context.getPeriodType().trim().toUpperCase() : "DAILY";
        String periodValue = context.getPeriodValue() != null ? context.getPeriodValue() : "";
        DwsSummaryRecord macro = context.getMacroSummary();
        List<FinancialTransaction> transactions = context.getRawTransactions();

        StringBuilder sb = new StringBuilder();
        sb.append("================================================================================\n");
        sb.append("📊 【财务分析报告输入上下文 (Financial Lakehouse Context)】\n");
        sb.append("================================================================================\n");
        sb.append(String.format("• 报告周期类型 (Period Type) : %s\n", periodType));
        sb.append(String.format("• 报告周期标识 (Period Value): %s\n", periodValue));

        // 1. 注入周期专属的动态审计指引
        appendPeriodGuidance(sb, periodType);

        // 2. 注入宏观大盘平账指标
        appendMacroSection(sb, macro);

        // 3. 注入全量微观动账流水明细
        appendTransactionSection(sb, transactions);

        sb.append("================================================================================\n");
        return sb.toString();
    }

    /**
     * 针对 Daily、Weekly、Monthly 注入周期专属的分析提炼指引
     */
    private static void appendPeriodGuidance(StringBuilder sb, String periodType) {
        sb.append("\n【0. 周期专属分析指引 (Period Guidance)】:\n");
        switch (periodType) {
            case "DAILY" -> sb.append("""
                      * 目标：撰写睡前轻量财务复盘，重点分析全天生活消费主线。
                      * 案例提炼法则：聚焦全天全局消费，自主提炼金额最大或最显眼的 Top 3~5 核心商户。
                      * 工具调用：主动调用 `createMerchantBarChart` 绘制核心商户横向柱状图。
                    """);
            case "WEEKLY" -> sb.append("""
                      * 目标：撰写周度生活节律体检报告，对比工作日 vs 周末消费偏好。
                      * 案例提炼法则：采取【各大核心类目代表作策略 (Top 1 per Category)】，从餐饮、交通、商超、网购等活跃类目中各选 1 笔代表作，杜绝单笔大单垄断。
                      * 工具调用：必须主动调用 `createCategoryPieChart` 绘制大类分布环形饼图，并调用 `createMerchantBarChart` 绘制跨类目重点商户横向柱状图。
                    """);
            case "MONTHLY" -> sb.append("""
                      * 目标：撰写月度资产损益与现金流全景白皮书，评估储蓄率、负债结构与次月预算规划。
                      * 案例提炼法则：采取【各大核心类目代表作策略 (Top 1 per Category)】，全面覆盖吃、穿、住、行、用、健康各大生活支柱。
                      * 工具调用：必须主动调用 `createCategoryPieChart` 绘制全月大盘环形饼图，并调用 `createMerchantBarChart` 绘制核心商户横向柱状图。
                    """);
            default -> sb.append("  * 目标：进行标准财务数据复核与代表性商户分析。\n");
        }
    }

    /**
     * 格式化并组装宏观大盘平账指标
     */
    private static void appendMacroSection(StringBuilder sb, DwsSummaryRecord macro) {
        if (macro == null) {
            sb.append("\n【1. 宏观财务大盘核算指标】: (暂无宏观大盘数据)\n");
            return;
        }

        sb.append("\n【1. 宏观财务大盘核算指标 (Macro Financial Metrics)】:\n");
        sb.append(String.format("  - 有效消费总笔数: %d 笔\n", macro.getTxCount() != null ? macro.getTxCount() : 0));
        sb.append(String.format("  - 消费支出毛额  : ￥%s 元\n", formatDecimal(macro.getTotalExpense())));
        sb.append(String.format("  - 退款冲正抵扣  : ￥%s 元\n", formatDecimal(macro.getTotalRefund())));
        sb.append(String.format("  - 净消费开销总计: ￥%s 元 (审计核心平账基准)\n", formatDecimal(macro.getNetExpense())));
        sb.append(String.format("  - 理赔与被动收入: ￥%s 元 (正向资金回血)\n", formatDecimal(macro.getTotalIncome())));
        sb.append(String.format("  - 信用卡集中还款: ￥%s 元 (资产内部划转，非日常消费)\n", formatDecimal(macro.getTotalTransfer())));

        sb.append("\n【2. 十一大消费类目宏观大盘分布 (Category Breakdown)】:\n");
        BigDecimal net = macro.getNetExpense();
        sb.append(String.format("  1. 餐饮美食 (FOOD)                 : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getFoodExpense()), calcPct(macro.getFoodExpense(), net)));
        sb.append(String.format("  2. 交通出行 (TRANSPORT)            : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getTransportExpense()), calcPct(macro.getTransportExpense(), net)));
        sb.append(String.format("  3. 线上电商网购 (ONLINE_SHOPPING)     : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getOnlineShoppingExpense()), calcPct(macro.getOnlineShoppingExpense(), net)));
        sb.append(String.format("  4. 线下商超零售 (OFFLINE_SHOPPING)    : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getOfflineShoppingExpense()), calcPct(macro.getOfflineShoppingExpense(), net)));
        sb.append(String.format("  5. 医疗健康 (MEDICAL)              : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getMedicalExpense()), calcPct(macro.getMedicalExpense(), net)));
        sb.append(String.format("  6. 电信通信 (COMMUNICATION)        : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getCommunicationExpense()), calcPct(macro.getCommunicationExpense(), net)));
        sb.append(String.format("  7. 保险保费 (INSURANCE)            : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getInsuranceExpense()), calcPct(macro.getInsuranceExpense(), net)));
        sb.append(String.format("  8. 物业管理 (PROPERTY_MANAGEMENT)  : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getPropertyExpense()), calcPct(macro.getPropertyExpense(), net)));
        sb.append(String.format("  9. 旅游文旅 (TRAVEL)               : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getTravelExpense()), calcPct(macro.getTravelExpense(), net)));
        sb.append(String.format(" 10. 个人转账 (PERSONAL_TRANSFER)     : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getPersonalTransferExpense()), calcPct(macro.getPersonalTransferExpense(), net)));
        sb.append(String.format(" 11. 其他杂项支出 (OTHER)            : ￥%s 元 (占比: %s)\n",
                formatDecimal(macro.getOtherExpense()), calcPct(macro.getOtherExpense(), net)));

        if (macro.getMaxSingleAmount() != null && macro.getMaxSingleAmount().compareTo(BigDecimal.ZERO) > 0) {
            sb.append(String.format("  - 单笔最高支出: ￥%s 元 (商户: %s)\n",
                    formatDecimal(macro.getMaxSingleAmount()),
                    macro.getMaxMerchant() != null ? macro.getMaxMerchant() : "未知"));
        }
    }

    /**
     * 格式化并组装微观明细流水
     */
    private static void appendTransactionSection(StringBuilder sb, List<FinancialTransaction> transactions) {
        sb.append(String.format("\n【3. 周期内全量 DWD 动账交易明细流水 (共 %d 笔，请基于此自主提炼 Top 商户与重点案例)】:\n",
                transactions != null ? transactions.size() : 0));

        if (transactions == null || transactions.isEmpty()) {
            sb.append("  (暂无原始交易明细)\n");
            return;
        }

        for (int i = 0; i < transactions.size(); i++) {
            FinancialTransaction tx = transactions.get(i);
            sb.append(String.format("  [%2d] ID:%3d | 金额: ￥%8s %s | 类型: %-7s | 商户: %-12s | 类目: %-16s | 时间: %s | 渠道: %-10s | 对手方: %s\n",
                    i + 1,
                    tx.getId() != null ? tx.getId() : 0L,
                    formatDecimal(tx.getAmount()),
                    tx.getCurrency() != null ? tx.getCurrency() : "CNY",
                    tx.getTxType() != null ? tx.getTxType() : "EXPENSE",
                    tx.getCleanedMerchant() != null ? tx.getCleanedMerchant() : "N/A",
                    tx.getCategory() != null ? tx.getCategory() : "OTHER",
                    tx.getTxTime() != null ? tx.getTxTime().toString() : "N/A",
                    tx.getPaymentChannel() != null ? tx.getPaymentChannel() : "DIRECT",
                    tx.getCounterparty() != null ? tx.getCounterparty() : "N/A"));
        }
    }

    private static String formatDecimal(BigDecimal val) {
        return val != null ? val.setScale(2, RoundingMode.HALF_UP).toPlainString() : "0.00";
    }

    private static String calcPct(BigDecimal part, BigDecimal total) {
        if (part == null || total == null || total.compareTo(BigDecimal.ZERO) <= 0) {
            return "0.0%";
        }
        return part.multiply(new BigDecimal("100"))
                .divide(total, 1, RoundingMode.HALF_UP)
                .toPlainString() + "%";
    }
}
