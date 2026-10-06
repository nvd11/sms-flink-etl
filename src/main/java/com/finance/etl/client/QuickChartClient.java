package com.finance.etl.client;

import com.finance.etl.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * QuickChart 客户端 (QuickChartClient)
 * 职责：
 * 封装 QuickChart 的官方 POST 端点 (https://quickchart.io/chart/create)，
 * 把图表配置换取为标准短链 URL (https://quickchart.io/chart/render/...)，
 * 彻底杜绝中文 GET 编码截断与 Slack 卡片裂图隐患。
 */
public class QuickChartClient {
    private static final Logger LOG = LoggerFactory.getLogger(QuickChartClient.class);
    private static final String DEFAULT_ENDPOINT = "https://quickchart.io/chart/create";

    private final String endpoint;
    private final HttpClient httpClient;

    public QuickChartClient(String endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public static QuickChartClient fromConfig() {
        String endpoint = ConfigUtils.get("QUICKCHART_ENDPOINT", DEFAULT_ENDPOINT);
        return new QuickChartClient(endpoint);
    }

    /**
     * 1. 交换环形饼图 (Doughnut Chart) 短链
     *
     * @param title  图表标题 (如: "9月支出类目分布")
     * @param labels 类目列表 (如: ["餐饮", "购物", "交通", "医疗"])
     * @param values 各分类金额 (如: [598.56, 2839.13, 893.52, 960.80])
     * @return QuickChart 短链 URL
     */
    public String createDoughnutChart(String title, List<String> labels, List<BigDecimal> values) {
        String labelsJson = labels.stream()
                .map(l -> "\"" + escapeJson(l) + "\"")
                .collect(Collectors.joining(",", "[", "]"));
        String dataJson = values.stream()
                .map(BigDecimal::toPlainString)
                .collect(Collectors.joining(",", "[", "]"));

        String chartConfigJson = String.format(
                "{" +
                "\"type\":\"doughnut\"," +
                "\"data\":{\"labels\":%s,\"datasets\":[{\"data\":%s}]}," +
                "\"options\":{" +
                "\"title\":{\"display\":true,\"text\":\"%s\",\"fontColor\":\"#333333\",\"fontSize\":16}," +
                "\"plugins\":{\"datalabels\":{\"display\":true,\"color\":\"#ffffff\",\"font\":{\"weight\":\"bold\"}}}," +
                "\"legend\":{\"position\":\"right\"}" +
                "}" +
                "}",
                labelsJson, dataJson, escapeJson(title)
        );

        return exchangeChartUrl(chartConfigJson);
    }

    /**
     * 2. 交换横向柱状图 (Horizontal Bar Chart) 短链
     *
     * @param title  图表标题 (如: "9月核心商户支出Top 5")
     * @param labels 商户名列表 (如: ["美团", "盒马鲜生", "陪护中心"])
     * @param values 消费金额列表 (如: [9004.08, 925.54, 1399.87])
     * @return QuickChart 短链 URL
     */
    public String createHorizontalBarChart(String title, List<String> labels, List<BigDecimal> values) {
        String labelsJson = labels.stream()
                .map(l -> "\"" + escapeJson(l) + "\"")
                .collect(Collectors.joining(",", "[", "]"));
        String dataJson = values.stream()
                .map(BigDecimal::toPlainString)
                .collect(Collectors.joining(",", "[", "]"));

        String chartConfigJson = String.format(
                "{" +
                "\"type\":\"horizontalBar\"," +
                "\"data\":{\"labels\":%s,\"datasets\":[{\"label\":\"消费金额(元)\",\"data\":%s,\"backgroundColor\":\"#36A2EB\"}]}," +
                "\"options\":{" +
                "\"title\":{\"display\":true,\"text\":\"%s\",\"fontColor\":\"#333333\",\"fontSize\":16}," +
                "\"scales\":{\"xAxes\":[{\"ticks\":{\"beginAtZero\":true}}]}," +
                "\"legend\":{\"display\":false}" +
                "}" +
                "}",
                labelsJson, dataJson, escapeJson(title)
        );

        return exchangeChartUrl(chartConfigJson);
    }

    /**
     * 底层方法：向 QuickChart POST 发起短链换取
     */
    public String exchangeChartUrl(String chartConfigJson) {
        String requestBody = "{\"chart\":" + chartConfigJson + "}";
        LOG.info("📊 [QuickChart] Exchanging short URL for chart config: {}", chartConfigJson);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("Content-Type", "application/json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(15))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                LOG.error("❌ [QuickChart] HTTP {} error: {}", response.statusCode(), response.body());
                throw new RuntimeException("QuickChart HTTP error: " + response.statusCode() + " " + response.body());
            }

            String body = response.body();
            // 解析返回的 JSON: {"success":true,"url":"https://quickchart.io/chart/render/..."}
            int urlIndex = body.indexOf("\"url\":\"");
            if (urlIndex != -1) {
                int endUrl = body.indexOf("\"", urlIndex + 7);
                if (endUrl != -1) {
                    String shortUrl = body.substring(urlIndex + 7, endUrl);
                    LOG.info("✅ [QuickChart] Acquired chart short URL: {}", shortUrl);
                    return shortUrl;
                }
            }

            throw new RuntimeException("Failed to extract 'url' from QuickChart response: " + body);
        } catch (IOException | InterruptedException e) {
            LOG.error("❌ [QuickChart] Network exception while creating chart: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to generate chart short URL", e);
        }
    }

    private static String escapeJson(String raw) {
        if (raw == null) return "";
        return raw.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
