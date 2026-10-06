package com.finance.etl.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 结构化财务分析报表上下文领域模型 (FinancialReportContext)
 * 职责：
 * 承载双流汇聚后的完整数据胶囊：
 * 1. 宏观 DWS 聚合平账指标 (DwsSummaryRecord)；
 * 2. 微观周期内全量 DWD 动账交易明细流水 (List<FinancialTransaction> rawTransactions)。
 * 拥有上帝视角的全部明细，赋予大模型自主洞察周期内 Top N、高频偏好、同店复购、商户排行与异动归因的最大自由度。
 */
public class FinancialReportContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private String periodType; // "DAILY", "WEEKLY", "MONTHLY"
    private String periodValue; // e.g. "2026-10-04", "2026-W40", "2026-09"
    private DwsSummaryRecord macroSummary; // 宏观平账大盘
    private List<FinancialTransaction> rawTransactions; // 周期内全量 DWD 动账事实明细

    public FinancialReportContext() {
        this.rawTransactions = new ArrayList<>();
    }

    public FinancialReportContext(String periodType, String periodValue,
                                  DwsSummaryRecord macroSummary,
                                  List<FinancialTransaction> rawTransactions) {
        this.periodType = periodType;
        this.periodValue = periodValue;
        this.macroSummary = macroSummary;
        this.rawTransactions = rawTransactions != null ? new ArrayList<>(rawTransactions) : new ArrayList<>();
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

    public DwsSummaryRecord getMacroSummary() {
        return macroSummary;
    }

    public void setMacroSummary(DwsSummaryRecord macroSummary) {
        this.macroSummary = macroSummary;
    }

    public List<FinancialTransaction> getRawTransactions() {
        return rawTransactions;
    }

    public void setRawTransactions(List<FinancialTransaction> rawTransactions) {
        this.rawTransactions = rawTransactions != null ? new ArrayList<>(rawTransactions) : new ArrayList<>();
    }

    /**
     * 将全量双流上下文组装为无损结构化 Prompt 注入文本，确保大模型获得 100% 原始事实与宏观大盘。
     * 核心实现已解耦收敛至专属构建器 FinancialReportPromptBuilder。
     */
    public String toPromptContext() {
        return com.finance.etl.prompt.FinancialReportPromptBuilder.buildPrompt(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FinancialReportContext that = (FinancialReportContext) o;
        return Objects.equals(periodType, that.periodType) &&
                Objects.equals(periodValue, that.periodValue) &&
                Objects.equals(macroSummary, that.macroSummary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(periodType, periodValue, macroSummary);
    }
}
