package com.finance.etl.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("QuickChartClient 客户端短链生成测试")
class QuickChartClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(QuickChartClientTest.class);

    @Test
    @DisplayName("测试生成环形饼图 (Doughnut Chart) 短链")
    void testCreateDoughnutChart() {
        QuickChartClient client = QuickChartClient.fromConfig();
        List<String> labels = List.of("购物", "餐饮", "交通", "医疗");
        List<BigDecimal> values = List.of(
                new BigDecimal("2839.13"),
                new BigDecimal("598.56"),
                new BigDecimal("893.52"),
                new BigDecimal("960.80")
        );

        String shortUrl = client.createDoughnutChart("9月主要消费构成", labels, values);
        LOG.info("🍩 [QuickChart Doughnut Short URL]: {}", shortUrl);

        assertNotNull(shortUrl);
        assertTrue(shortUrl.startsWith("https://quickchart.io/chart/render/"), "必须返回 QuickChart 官方 short render 链接");
    }

    @Test
    @DisplayName("测试生成商户横向柱状图 (Horizontal Bar Chart) 短链")
    void testCreateHorizontalBarChart() {
        QuickChartClient client = QuickChartClient.fromConfig();
        List<String> labels = List.of("美团", "盒马鲜生", "陪护中心");
        List<BigDecimal> values = List.of(
                new BigDecimal("9004.08"),
                new BigDecimal("925.54"),
                new BigDecimal("1399.87")
        );

        String shortUrl = client.createHorizontalBarChart("9月商户支出Top 3", labels, values);
        LOG.info("📊 [QuickChart Bar Chart Short URL]: {}", shortUrl);

        assertNotNull(shortUrl);
        assertTrue(shortUrl.startsWith("https://quickchart.io/chart/render/"), "必须返回 QuickChart 官方 short render 链接");
    }
}
