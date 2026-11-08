package com.finance.etl.client;

import com.finance.etl.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;

/**
 * Slack 私聊客户端 (SlackYuiClient)
 * 职责：
 * 封装 Slack 原生 Web API (https://slack.com/api/chat.postMessage)，
 * 支持纯文本与 Block Kit 富文本卡片消息投递，
 * 以专属财务秘书 Yui 的身份推送到主人专属私聊频道 (默认 U0AM8G9AARF)。
 */
public class SlackYuiClient {
    private static final Logger LOG = LoggerFactory.getLogger(SlackYuiClient.class);
    private static final String DEFAULT_SLACK_API_URL = "https://slack.com/api/chat.postMessage";

    /**
     * 匹配 Markdown 图片语法: ![alt](url)
     */
    private static final Pattern MARKDOWN_IMG_PATTERN =
            Pattern.compile("!\\[(.*?)\\]\\((https?://[^\\s\\)]+)\\)");
    private static final String DEFAULT_MASTER_CHANNEL = "U0AM8G9AARF"; // 主人 Jason 专属 Slack 用户 ID

    private final String botToken;
    private final String defaultChannel;
    private final String apiUrl;
    private final HttpClient httpClient;

    public SlackYuiClient(String botToken, String defaultChannel, String apiUrl) {
        this.botToken = botToken != null ? botToken.trim() : "";
        this.defaultChannel = defaultChannel != null && !defaultChannel.trim().isEmpty() ?
                defaultChannel.trim() : DEFAULT_MASTER_CHANNEL;
        this.apiUrl = apiUrl != null && !apiUrl.trim().isEmpty() ? apiUrl.trim() : DEFAULT_SLACK_API_URL;
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * 工厂方法：从环境变量与 .env 配置中自动组装客户端
     */
    public static SlackYuiClient fromConfig() {
        String token = ConfigUtils.get("SLACK_YUI_BOT_TOKEN", "");
        String channel = ConfigUtils.get("SLACK_MASTER_CHANNEL", DEFAULT_MASTER_CHANNEL);
        String url = ConfigUtils.get("SLACK_API_URL", DEFAULT_SLACK_API_URL);
        return new SlackYuiClient(token, channel, url);
    }

    /**
     * 1. 发送标准 Markdown 纯文本私聊消息
     *
     * @param text 消息文本内容
     * @return 投递是否成功
     */
    public boolean postMessage(String text) {
        return postMessage(this.defaultChannel, text);
    }

    /**
     * 1.1 发送指定频道的标准 Markdown 纯文本消息
     */
    public boolean postMessage(String channel, String text) {
        Objects.requireNonNull(text, "text must not be null");
        if (botToken.isEmpty()) {
            LOG.warn("⚠️ [SlackYuiClient] SLACK_YUI_BOT_TOKEN not configured. Skipping Slack post.");
            return false;
        }

        String targetChannel = (channel != null && !channel.trim().isEmpty()) ? channel : this.defaultChannel;
        String jsonPayload = String.format("{\"channel\":\"%s\",\"text\":\"%s\"}",
                escapeJson(targetChannel), escapeJson(text));

        return executePost(jsonPayload);
    }

    /**
     * 2. 发送 Slack Block Kit 富文本卡片消息
     *
     * @param fallbackText 通知弹窗预览文本
     * @param blocksJson   符合 Slack Block Kit 规范的 blocks 数组 JSON 字符串
     * @return 投递是否成功
     */
    public boolean postBlockMessage(String fallbackText, String blocksJson) {
        return postBlockMessage(this.defaultChannel, fallbackText, blocksJson);
    }

    /**
     * 2.1 发送到指定频道的 Block Kit 富文本卡片消息
     */
    public boolean postBlockMessage(String channel, String fallbackText, String blocksJson) {
        Objects.requireNonNull(blocksJson, "blocksJson must not be null");
        if (botToken.isEmpty()) {
            LOG.warn("⚠️ [SlackYuiClient] SLACK_YUI_BOT_TOKEN not configured. Skipping Slack block post.");
            return false;
        }

        String targetChannel = (channel != null && !channel.trim().isEmpty()) ? channel : this.defaultChannel;
        String previewText = fallbackText != null ? fallbackText : "Yui 财务分析研报已就绪";

        String jsonPayload = String.format("{\"channel\":\"%s\",\"text\":\"%s\",\"blocks\":%s}",
                escapeJson(targetChannel), escapeJson(previewText), blocksJson.trim());

        return executePost(jsonPayload);
    }

    /**
     * 3. 智能解析 Markdown 报告，自动将嵌入的图片转化为 Slack Block Kit 原生图片块（Image Block），
     * 并在桌面端/移动端以富媒体卡片方式直观渲染高清图表。
     *
     * @param markdownReport Agent 生成的 Markdown 完整研报文本
     * @return 投递是否成功
     */
    public boolean postReportWithBlocks(String markdownReport) {
        return postReportWithBlocks(this.defaultChannel, markdownReport);
    }

    /**
     * 3.1 智能解析 Markdown 报告并投递至指定频道
     *
     * @param channel 目标 Channel ID 或私聊成员 ID
     * @param markdownReport Markdown 研报文本
     * @return 投递是否成功
     */
    public boolean postReportWithBlocks(String channel, String markdownReport) {
        Objects.requireNonNull(markdownReport, "markdownReport must not be null");
        if (botToken.isEmpty()) {
            LOG.warn("⚠️ [SlackYuiClient] SLACK_YUI_BOT_TOKEN not configured. Skipping Slack post.");
            return false;
        }

        // 第 1 道防线：逐张校验 Markdown 图片链接，剔除无效/不可下载的图表短链，
        // 避免 Slack 因单个 image_url 下载失败而把整条卡片判为 invalid_blocks 全量丢弃。
        String sanitizedMarkdown = stripUnreachableImages(markdownReport);
        String blocksJson = convertMarkdownToBlocksJson(sanitizedMarkdown);
        String previewText = "🌸 Yui 财务分析研报已就绪（含可视化图表）";

        boolean ok = postBlockMessage(channel, previewText, blocksJson);
        if (ok) {
            return true;
        }

        // 第 2 道防线：富文本卡片仍失败时，剥离全部图片后以纯文本重试一次，
        // 确保“宁可少一张图，也绝不整条消息丢失”。
        LOG.warn("♻️ [SlackYuiClient] Rich card delivery failed. Retrying once with text-only fallback (all images stripped)...");
        String textOnlyBlocks = convertMarkdownToBlocksJson(stripAllImages(markdownReport));
        boolean fallbackOk = postBlockMessage(channel, "🌸 Yui 财务分析研报已就绪（纯文本兜底）", textOnlyBlocks);
        if (fallbackOk) {
            LOG.info("✅ [SlackYuiClient] Text-only fallback delivered successfully to channel: {}", channel);
        } else {
            LOG.error("🚨 [ALERT] Slack delivery FAILED even after text-only fallback. Master will NOT receive this report. channel={}", channel);
        }
        return fallbackOk;
    }

    /**
     * 图片链接健康校验（第 1 道防线）：
     * 对 Markdown 中的每张图片发起 HEAD 探测，仅保留真正返回 200 且 Content-Type 为 image/* 的链接；
     * 失效链接会被替换为其 alt 文本，从而保住整条 Slack 卡片。
     */
    private String stripUnreachableImages(String markdown) {
        Matcher matcher = MARKDOWN_IMG_PATTERN.matcher(markdown);
        StringBuffer sb = new StringBuffer();
        int dropped = 0;
        while (matcher.find()) {
            String altText = matcher.group(1).trim();
            String imageUrl = matcher.group(2).trim();
            String replacement;
            if (isReachableImage(imageUrl)) {
                replacement = matcher.group(0);
            } else {
                dropped++;
                replacement = altText.isEmpty() ? "" : altText;
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        if (dropped > 0) {
            LOG.warn("🛡️ [SlackYuiClient] Dropped {} unreachable image(s) from Slack payload to protect card delivery.", dropped);
        }
        return sb.toString();
    }

    private boolean isReachableImage(String imageUrl) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(imageUrl))
                    .method("HEAD", HttpRequest.BodyPublishers.noBody())
                    .header("User-Agent", "sms-flink-etl-slack-guard/1.0")
                    .timeout(Duration.ofSeconds(8))
                    .build();
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            String contentType = response.headers().firstValue("content-type").orElse("").toLowerCase();
            boolean valid = response.statusCode() == 200 && contentType.startsWith("image/");
            if (!valid) {
                LOG.warn("⚠️ [SlackYuiClient] Image URL rejected (status={}, content-type='{}'): {}",
                        response.statusCode(), contentType, imageUrl);
            }
            return valid;
        } catch (Exception e) {
            LOG.warn("⚠️ [SlackYuiClient] Image URL validation error ({}): {}", e.getMessage(), imageUrl);
            return false;
        }
    }

    private static String stripAllImages(String markdown) {
        return MARKDOWN_IMG_PATTERN.matcher(markdown).replaceAll("");
    }

    /**
     * 将包含普通文本与 Markdown 图片语法的长文本转换为合法的 Slack Block Kit blocks JSON
     */
    public static String convertMarkdownToBlocksJson(String markdown) {
        if (markdown == null || markdown.trim().isEmpty()) {
            return "[]";
        }

        List<String> blockList = new ArrayList<>();
        Matcher matcher = MARKDOWN_IMG_PATTERN.matcher(markdown);
        int lastEnd = 0;

        while (matcher.find()) {
            int start = matcher.start();
            if (start > lastEnd) {
                String textSegment = markdown.substring(lastEnd, start).trim();
                if (!textSegment.isEmpty()) {
                    addSectionBlocks(blockList, textSegment);
                }
            }

            String altText = matcher.group(1).trim();
            if (altText.isEmpty()) {
                altText = "财务分析可视化图表";
            }
            String imageUrl = matcher.group(2).trim();

            String imageBlock = String.format(
                    "{\"type\":\"image\",\"title\":{\"type\":\"plain_text\",\"text\":\"%s\",\"emoji\":true}," +
                            "\"image_url\":\"%s\",\"alt_text\":\"%s\"}",
                    escapeJson(altText), escapeJson(imageUrl), escapeJson(altText)
            );
            blockList.add(imageBlock);

            lastEnd = matcher.end();
        }

        if (lastEnd < markdown.length()) {
            String trailingText = markdown.substring(lastEnd).trim();
            if (!trailingText.isEmpty()) {
                addSectionBlocks(blockList, trailingText);
            }
        }

        // 如果没有解析出任何 block，降级生成单一 section block
        if (blockList.isEmpty()) {
            addSectionBlocks(blockList, markdown.trim());
        }

        return "[" + String.join(",", blockList) + "]";
    }

    /**
     * Slack 的 section block 文本长度上限为 3000 字符，若超长则安全切片分段
     */
    private static void addSectionBlocks(List<String> blockList, String text) {
        int maxLen = 2900;
        int len = text.length();
        int offset = 0;

        while (offset < len) {
            int end = Math.min(offset + maxLen, len);
            // 尽量在换行符处截断以保持阅读完整性
            if (end < len) {
                int lastNewline = text.lastIndexOf('\n', end);
                if (lastNewline > offset + 500) {
                    end = lastNewline;
                }
            }
            String chunk = text.substring(offset, end).trim();
            if (!chunk.isEmpty()) {
                blockList.add(String.format(
                        "{\"type\":\"section\",\"text\":{\"type\":\"mrkdwn\",\"text\":\"%s\"}}",
                        escapeJson(chunk)
                ));
            }
            offset = end;
        }
    }

    private boolean executePost(String jsonPayload) {
        LOG.debug("📤 [SlackYuiClient] Sending payload to Slack API: {}", jsonPayload);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(this.apiUrl))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Authorization", "Bearer " + this.botToken)
                .timeout(Duration.ofSeconds(15))
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String responseBody = response.body();

            if (response.statusCode() == 200 && responseBody.contains("\"ok\":true")) {
                LOG.info("✅ [SlackYuiClient] Message successfully posted to Slack channel: {}", this.defaultChannel);
                return true;
            } else {
                LOG.error("❌ [SlackYuiClient] Failed to post Slack message (status: {}): {}",
                        response.statusCode(), responseBody);
                return false;
            }
        } catch (IOException | InterruptedException e) {
            LOG.error("❌ [SlackYuiClient] Network exception while connecting to Slack API: {}", e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    public String getBotToken() {
        return botToken;
    }

    public String getDefaultChannel() {
        return defaultChannel;
    }
}
