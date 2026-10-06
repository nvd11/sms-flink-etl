package com.finance.etl.tools;

import com.finance.etl.client.QuickChartClient;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 图表渲染工具箱 (FinancialChartTools)
 * 职责：
 * 作为 LangChain4j 的外部工具集，包裹 QuickChartClient，
 * 向大模型 Agent 暴露生成环形饼图与商户排行榜柱状图短链的声明式工具能力。
 */
public class FinancialChartTools {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialChartTools.class);

    private final QuickChartClient chartClient;

    public FinancialChartTools(QuickChartClient chartClient) {
        this.chartClient = Objects.requireNonNull(chartClient, "chartClient must not be null");
    }

    public static FinancialChartTools fromConfig() {
        return new FinancialChartTools(QuickChartClient.fromConfig());
    }

    @Tool("根据给定的消费大类与金额生成环形饼图 (Doughnut Chart) 图片短链，供生成图表并在报表中展示")
    public String createCategoryPieChart(
            @P("图表标题，例如: 9月消费支出分类占比") String title,
            @P("英文逗号分隔的分类名称列表，例如: 购物,交通,餐饮,医疗,数码电器") String categories,
            @P("英文逗号分隔的金额数字列表，例如: 2839.13,893.52,598.56,960.80,0.00") String amounts
    ) {
        LOG.info("🛠️ [Chart Tool] Invoking createCategoryPieChart: title={}, categories={}, amounts={}", title, categories, amounts);
        try {
            if (categories == null || amounts == null) {
                return "Error: categories and amounts must not be null";
            }
            List<String> labelList = Arrays.stream(categories.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());

            List<BigDecimal> valueList = Arrays.stream(amounts.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(BigDecimal::new)
                    .collect(Collectors.toList());

            if (labelList.isEmpty() || labelList.size() != valueList.size()) {
                return "Error: categories and amounts count mismatch or empty";
            }

            // 过滤掉金额为 0 的项，让饼图更美观
            List<String> filteredLabels = new ArrayList<>();
            List<BigDecimal> filteredValues = new ArrayList<>();
            for (int i = 0; i < labelList.size(); i++) {
                if (valueList.get(i).compareTo(BigDecimal.ZERO) > 0) {
                    filteredLabels.add(labelList.get(i));
                    filteredValues.add(valueList.get(i));
                }
            }

            if (filteredLabels.isEmpty()) {
                filteredLabels = labelList;
                filteredValues = valueList;
            }

            return chartClient.createDoughnutChart(title, filteredLabels, filteredValues);
        } catch (Exception e) {
            LOG.error("❌ [Chart Tool] Failed to generate category pie chart: {}", e.getMessage(), e);
            return "Error generating pie chart: " + e.getMessage();
        }
    }

    @Tool("根据给定的商户排行榜数据生成横向柱状图 (Horizontal Bar Chart) 图片短链，供生成报表时展示前几名商户对比")
    public String createMerchantBarChart(
            @P("图表标题，例如: 9月主要商户支出 Top 5") String title,
            @P("英文逗号分隔的商户名称列表，例如: 美团,盒马鲜生,陪护中心,番禺何贤纪念医院") String merchants,
            @P("英文逗号分隔的消费金额列表，例如: 9004.08,925.54,1399.87,691.95") String amounts
    ) {
        LOG.info("🛠️ [Chart Tool] Invoking createMerchantBarChart: title={}, merchants={}, amounts={}", title, merchants, amounts);
        try {
            if (merchants == null || amounts == null) {
                return "Error: merchants and amounts must not be null";
            }
            List<String> labelList = Arrays.stream(merchants.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());

            List<BigDecimal> valueList = Arrays.stream(amounts.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(BigDecimal::new)
                    .collect(Collectors.toList());

            if (labelList.isEmpty() || labelList.size() != valueList.size()) {
                return "Error: merchants and amounts count mismatch or empty";
            }

            return chartClient.createHorizontalBarChart(title, labelList, valueList);
        } catch (Exception e) {
            LOG.error("❌ [Chart Tool] Failed to generate merchant bar chart: {}", e.getMessage(), e);
            return "Error generating merchant bar chart: " + e.getMessage();
        }
    }
}
