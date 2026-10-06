package com.finance.etl.agent;

import com.finance.etl.client.SlackYuiClient;
import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialReportContext;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.repository.FinancialLakehouseRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FinancialAdvisorAgent 财务智能体端到端纯研报生成测试")
class FinancialAdvisorAgentTest {

    private FinancialReportContext createSampleContext() {
        DwsSummaryRecord macro = new DwsSummaryRecord(
                "MONTHLY", "2026-09", null, 1L, 414L, 126L,
                new BigDecimal("22679.46"), // 总支出
                new BigDecimal("235.90"),   // 退款冲正
                new BigDecimal("22443.56"), // 净消费支出
                new BigDecimal("2790.59"),  // 理赔收入
                new BigDecimal("17253.33"), // 信用卡划转
                new BigDecimal("9648.44"),  // FOOD (含美团8988)
                new BigDecimal("1065.34"),  // TRANSPORT
                new BigDecimal("422.32"),   // ONLINE_SHOPPING
                new BigDecimal("2781.20"),  // OFFLINE_SHOPPING
                new BigDecimal("2360.67"),  // MEDICAL (含陪护1399)
                new BigDecimal("199.10"),   // COMMUNICATION (移动10086)
                new BigDecimal("5500.00"),  // INSURANCE (车险保费)
                new BigDecimal("30.00"),    // PROPERTY_MANAGEMENT (物业费)
                new BigDecimal("39.80"),    // TRAVEL (文旅)
                new BigDecimal("107.90"),   // PERSONAL_TRANSFER (个人扫码)
                new BigDecimal("288.79"),   // OTHER (平台杂项)
                new BigDecimal("8988.08"),  // 单笔峰值
                "美团"
        );

        FinancialTransaction tx1 = new FinancialTransaction();
        tx1.setId(95L);
        tx1.setAmount(new BigDecimal("8988.08"));
        tx1.setCleanedMerchant("美团");
        tx1.setCategory("FOOD");
        tx1.setTxTime(Instant.parse("2026-09-14T02:32:51Z"));
        tx1.setCounterparty("财付通-美团");

        FinancialTransaction tx2 = new FinancialTransaction();
        tx2.setId(121L);
        tx2.setAmount(new BigDecimal("1399.87"));
        tx2.setCleanedMerchant("陪护中心");
        tx2.setCategory("MEDICAL");
        tx2.setTxTime(Instant.parse("2026-09-15T12:44:50Z"));
        tx2.setCounterparty("财付通-陪护中心");

        FinancialTransaction tx3 = new FinancialTransaction();
        tx3.setId(402L);
        tx3.setAmount(new BigDecimal("472.42"));
        tx3.setCleanedMerchant("盒马鲜生");
        tx3.setCategory("OFFLINE_SHOPPING");
        tx3.setTxTime(Instant.parse("2026-09-29T19:49:26Z"));
        tx3.setCounterparty("支付宝-广州盒马鲜生网络科技有限公司");

        return new FinancialReportContext("MONTHLY", "2026-09", macro, List.of(tx1, tx2, tx3));
    }

    @Test
    @DisplayName("测试月度场景下 Agent 纯数据产出：自主思考、平账校验与图表 Tool Calling")
    void testGenerateMonthlyReport() {
        FinancialReportContext context = createSampleContext();
        FinancialAdvisorAgent agent = FinancialAdvisorAgent.fromConfig();
        assertNotNull(agent);

        String report = agent.generateReport(context);

        System.out.println("================================================================================");
        System.out.println("💌 【Yui 智能财务研报实盘生成结果预览 (月度)】");
        System.out.println("================================================================================");
        System.out.println(report);
        System.out.println("================================================================================");

        assertNotNull(report);
        assertFalse(report.trim().isEmpty());

        // 断言平账数据严格一致
        assertTrue(report.contains("22443.56") || report.contains("22,443.56") || report.contains("22443"),
                "研报必须包含准确的净支出金额 22443.56");
        assertTrue(report.contains("2790.59") || report.contains("2,790.59") || report.contains("2790"),
                "研报必须包含理赔到账金额 2790.59");

        // 断言包含 QuickChart 渲染短链（证明 Tool Calling 成功触发）
        assertTrue(report.contains("https://quickchart.io/chart/render/"),
                "研报必须包含自动调用 QuickChart 工具生成的图片短链");
    }

    @Test
    @DisplayName("🎯 实战测试用例：动态从 Trino View 查询最新一天流水，纯数据驱动 Agent 生成 Daily 研报并由外部测试端投递 Slack")
    void testGenerateLatestDailyReportFromView() throws Exception {
        try (FinancialLakehouseRepository lakehouseRepo = FinancialLakehouseRepository.fromConfig()) {

            // 1. 动态从 Trino View 查出最新有动账支出的自然日大盘
            DwsSummaryRecord latestDailyMacro = lakehouseRepo.queryLatestActiveDailySummary();
            assertNotNull(latestDailyMacro, "数据库 View 中必须能查出最新的日度大盘记录");
            assertNotNull(latestDailyMacro.getStatDate(), "统计日期不可为空");

            // 2. 从 DWD 物理事实表提取当天全量流水
            List<FinancialTransaction> allTransactions =
                    lakehouseRepo.queryDailyTransactions(latestDailyMacro.getStatDate());

            // 3. 打包成结构化领域上下文胶囊
            FinancialReportContext dailyContext = new FinancialReportContext(
                    "DAILY",
                    latestDailyMacro.getStatDate().toString(),
                    latestDailyMacro,
                    allTransactions
            );

            // 4. 调用 Agent 纯数据计算大脑（只负责生成研报 Markdown，绝不越界投递）
            FinancialAdvisorAgent agent = FinancialAdvisorAgent.fromConfig();
            assertNotNull(agent);

            System.out.println("================================================================================");
            System.out.printf("🚀 [Daily Report Test] 正在为数据库 View 最新的一天 (%s) 动态生成研报数据...\\n",
                    latestDailyMacro.getStatDate());
            System.out.println("================================================================================");

            String dailyReport = agent.generateReport(dailyContext);

            System.out.println("================================================================================");
            System.out.println("💌 【最新一天 Daily 研报生成结果预览】");
            System.out.println("================================================================================");
            System.out.println(dailyReport);
            System.out.println("================================================================================");

            // 5. 严密断言
            assertNotNull(dailyReport);
            assertFalse(dailyReport.trim().isEmpty());
            assertTrue(dailyReport.contains(latestDailyMacro.getNetExpense().toPlainString()) ||
                            dailyReport.contains(latestDailyMacro.getNetExpense().setScale(0, java.math.RoundingMode.HALF_UP).toString()),
                    "Daily 研报必须包含从数据库动态查得的精准平账净支出金额: " + latestDailyMacro.getNetExpense());

            // 6. 职责隔离演示：消息投递动作由外部客户端专职完成（完全解耦）
            // 自动将 Markdown 中的 QuickChart 图片转换为 Slack 原生 Image Block 卡片投递，实现图表内嵌直接渲染
            SlackYuiClient slackClient = SlackYuiClient.fromConfig();
            if (!slackClient.getBotToken().isEmpty() && !slackClient.getBotToken().startsWith("mock-")) {
                boolean delivered = slackClient.postReportWithBlocks(dailyReport);
                assertTrue(delivered, "解耦后的 Slack 原生图文富媒体卡片投递应该成功");
            }
        }
    }
}
