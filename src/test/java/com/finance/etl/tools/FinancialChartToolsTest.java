package com.finance.etl.tools;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FinancialChartTools 图表渲染工具集成测试")
class FinancialChartToolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialChartToolsTest.class);

    @Test
    @DisplayName("测试 FinancialChartTools 生成分类环形饼图与商户柱状图短链 (纯图表能力，无DAO依赖)")
    void testChartTools() {
        FinancialChartTools tools = FinancialChartTools.fromConfig();

        // 1. 环形饼图生成
        String pieUrl = tools.createCategoryPieChart(
                "9月分类开销占比",
                "购物,交通,餐饮,医疗",
                "2839.13,893.52,598.56,960.80"
        );
        LOG.info("🛠️ [Test Chart Pie URL]: {}", pieUrl);
        assertNotNull(pieUrl);
        assertTrue(pieUrl.startsWith("https://quickchart.io/chart/render/"));

        // 2. 商户柱状图生成
        String barUrl = tools.createMerchantBarChart(
                "9月Top商户消费",
                "美团,盒马鲜生,陪护中心",
                "9004.08,925.54,1399.87"
        );
        LOG.info("🛠️ [Test Chart Bar URL]: {}", barUrl);
        assertNotNull(barUrl);
        assertTrue(barUrl.startsWith("https://quickchart.io/chart/render/"));
    }
}
