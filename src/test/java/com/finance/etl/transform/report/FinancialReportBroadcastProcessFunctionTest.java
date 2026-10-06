package com.finance.etl.transform.report;

import com.finance.etl.agent.FinancialAdvisorAgent;
import com.finance.etl.client.SlackYuiClient;
import com.finance.etl.model.FinancialReportContext;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.service.FinancialAdvisorService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class FinancialReportBroadcastProcessFunctionTest {

    @Test
    @DisplayName("测试广播汇聚算子构造与基本触发逻辑")
    void testTriggerReportGenerationWithMock() {
        FinancialReportBroadcastProcessFunction func =
                new FinancialReportBroadcastProcessFunction("DAILY", "2026-10-05", false);

        AtomicBoolean agentCalled = new AtomicBoolean(false);

        // 构造匿名 Fake Service 模拟 Agent 输出
        FinancialAdvisorService fakeService = new FinancialAdvisorService() {
            @Override
            public String generateDailyReport(String contextData) {
                agentCalled.set(true);
                return "Mock Daily Report Content";
            }

            @Override
            public String generateWeeklyReport(String contextData) {
                agentCalled.set(true);
                return "Mock Weekly Report Content";
            }

            @Override
            public String generateMonthlyReport(String contextData) {
                agentCalled.set(true);
                return "Mock Monthly Report Content";
            }
        };

        FinancialAdvisorAgent fakeAgent = new FinancialAdvisorAgent(fakeService);
        func.setAgent(fakeAgent);

        FinancialTransaction tx = new FinancialTransaction();
        tx.setId(1L);
        tx.setTxTime(Instant.now());
        tx.setAmount(new BigDecimal("23.62"));
        tx.setCleanedMerchant("广州市番禺区何贤纪念医院");
        tx.setCategory("MEDICAL");
        func.getMicroTransactionsBuffer().add(tx);

        String report = func.triggerReportGeneration();

        assertNotNull(report);
        assertEquals("Mock Daily Report Content", report);
        assertTrue(agentCalled.get(), "Agent 必须被成功调用");
    }
}
