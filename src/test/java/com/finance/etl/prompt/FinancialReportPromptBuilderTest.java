package com.finance.etl.prompt;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialReportContext;
import com.finance.etl.model.FinancialTransaction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FinancialReportPromptBuilder 提示词生成器单元测试")
class FinancialReportPromptBuilderTest {

    @Test
    @DisplayName("测试针对 Daily 场景构建 Prompt 文本包含针对性指引与全量明细")
    void testBuildDailyPrompt() {
        DwsSummaryRecord macro = new DwsSummaryRecord(
                "DAILY", "2026-10-04", LocalDate.parse("2026-10-04"),
                456L, 475L, 8L,
                new BigDecimal("367.04"), new BigDecimal("5.90"), new BigDecimal("361.14"),
                BigDecimal.ZERO, BigDecimal.ZERO,
                new BigDecimal("44.90"), new BigDecimal("25.98"), new BigDecimal("32.39"), new BigDecimal("263.77"),
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                new BigDecimal("185.22"), "盒马鲜生"
        );

        FinancialTransaction tx = new FinancialTransaction();
        tx.setId(466L);
        tx.setAmount(new BigDecimal("185.22"));
        tx.setCleanedMerchant("盒马鲜生");
        tx.setCategory("OFFLINE_SHOPPING");
        tx.setTxTime(Instant.parse("2026-10-04T08:13:15Z"));

        FinancialReportContext context = new FinancialReportContext("DAILY", "2026-10-04", macro, List.of(tx));

        String prompt = FinancialReportPromptBuilder.buildPrompt(context);

        assertNotNull(prompt);
        assertTrue(prompt.contains("• 报告周期类型 (Period Type) : DAILY"));
        assertTrue(prompt.contains("睡前轻量财务复盘"));
        assertTrue(prompt.contains("361.14"));
        assertTrue(prompt.contains("盒马鲜生"));
    }

    @Test
    @DisplayName("测试针对 Monthly 场景构建 Prompt 文本包含各大核心类目代表作指引")
    void testBuildMonthlyPrompt() {
        FinancialReportContext context = new FinancialReportContext("MONTHLY", "2026-09", null, List.of());
        String prompt = FinancialReportPromptBuilder.buildPrompt(context);

        assertNotNull(prompt);
        assertTrue(prompt.contains("• 报告周期类型 (Period Type) : MONTHLY"));
        assertTrue(prompt.contains("各大核心类目代表作策略"));
        assertTrue(prompt.contains("createCategoryPieChart"));
    }
}
