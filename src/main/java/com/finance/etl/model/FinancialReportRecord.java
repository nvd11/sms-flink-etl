package com.finance.etl.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * ADS 智能财务研报事实记录实体模型 (FinancialReportRecord)
 * 职责：
 * 严格对齐 iceberg.finance.ads_financial_reports 与 iceberg.finance_dev.ads_financial_reports 物理表结构。
 * 承载每一次 Flink 批处理驱动 LLM 产出的指标快照、研报全文、图表短链及履约状态。
 */
public class FinancialReportRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String reportId;          // 主键，例如 'report_daily_2026-10-05', 'report_monthly_2026-09'
    private String periodType;        // 'DAILY', 'WEEKLY', 'MONTHLY'
    private String periodValue;       // '2026-10-05', '2026-W40', '2026-09'
    private LocalDate reportDate;     // 归属日期
    private BigDecimal totalExpense;  // 周期总消费支出 (CNY)
    private BigDecimal totalRefund;   // 周期退款冲正抵扣 (CNY)
    private BigDecimal netExpense;    // 周期净消费开销 (CNY)
    private BigDecimal totalIncome;   // 周期被动收入/理赔回血 (CNY)
    private BigDecimal totalTransfer; // 周期信用卡大额还款等 (CNY)
    private Long txCount;             // 有效消费总笔数
    private String metricsJson;       // 结构化分类与商户切片 JSON 快照
    private String summaryText;       // LLM (Gemini 3.8 Flash) 生成的完整 Markdown 研报与点评
    private String chartUrl;          // QuickChart 生成的高清短链图片 URL
    private String slackStatus;       // 'SENT', 'FAILED', 'SKIPPED'
    private Instant createdAt;        // 报告入湖时间戳

    public FinancialReportRecord() {
        this.createdAt = Instant.now();
    }

    public String getReportId() {
        return reportId;
    }

    public void setReportId(String reportId) {
        this.reportId = reportId;
    }

    public String getPeriodType() {
        return periodType;
    }

    public void setPeriodType(String periodType) {
        this.periodType = periodType;
    }

    public String getPeriodValue() {
        return periodValue;
    }

    public void setPeriodValue(String periodValue) {
        this.periodValue = periodValue;
    }

    public LocalDate getReportDate() {
        return reportDate;
    }

    public void setReportDate(LocalDate reportDate) {
        this.reportDate = reportDate;
    }

    public BigDecimal getTotalExpense() {
        return totalExpense;
    }

    public void setTotalExpense(BigDecimal totalExpense) {
        this.totalExpense = totalExpense;
    }

    public BigDecimal getTotalRefund() {
        return totalRefund;
    }

    public void setTotalRefund(BigDecimal totalRefund) {
        this.totalRefund = totalRefund;
    }

    public BigDecimal getNetExpense() {
        return netExpense;
    }

    public void setNetExpense(BigDecimal netExpense) {
        this.netExpense = netExpense;
    }

    public BigDecimal getTotalIncome() {
        return totalIncome;
    }

    public void setTotalIncome(BigDecimal totalIncome) {
        this.totalIncome = totalIncome;
    }

    public BigDecimal getTotalTransfer() {
        return totalTransfer;
    }

    public void setTotalTransfer(BigDecimal totalTransfer) {
        this.totalTransfer = totalTransfer;
    }

    public Long getTxCount() {
        return txCount;
    }

    public void setTxCount(Long txCount) {
        this.txCount = txCount;
    }

    public String getMetricsJson() {
        return metricsJson;
    }

    public void setMetricsJson(String metricsJson) {
        this.metricsJson = metricsJson;
    }

    public String getSummaryText() {
        return summaryText;
    }

    public void setSummaryText(String summaryText) {
        this.summaryText = summaryText;
    }

    public String getChartUrl() {
        return chartUrl;
    }

    public void setChartUrl(String chartUrl) {
        this.chartUrl = chartUrl;
    }

    /**
     * 解析 chart_url (兼容 JSON 数组字符串或历史单 URL 字符串)，返回全部图表 URL 列表
     */
    public List<String> getChartUrlList() {
        if (chartUrl == null || chartUrl.trim().isEmpty()) {
            return Collections.emptyList();
        }
        if (chartUrl.startsWith("[")) {
            List<String> list = new ArrayList<>();
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"(https?://[^\"]+)\"").matcher(chartUrl);
            while (m.find()) {
                list.add(m.group(1));
            }
            return list;
        }
        return Collections.singletonList(chartUrl);
    }

    /**
     * 获取首选主图 URL (第一张图)
     */
    public String getPrimaryChartUrl() {
        List<String> list = getChartUrlList();
        return list.isEmpty() ? null : list.get(0);
    }

    public String getSlackStatus() {
        return slackStatus;
    }

    public void setSlackStatus(String slackStatus) {
        this.slackStatus = slackStatus;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FinancialReportRecord that = (FinancialReportRecord) o;
        return Objects.equals(reportId, that.reportId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reportId);
    }
}
