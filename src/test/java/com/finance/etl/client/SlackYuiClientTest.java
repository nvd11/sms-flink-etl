package com.finance.etl.client;

import com.finance.etl.util.ConfigUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SlackYuiClient 单元与容错测试")
class SlackYuiClientTest {

    @Test
    @DisplayName("测试未配置 Token 时的安全容错与日志降级")
    void testMissingTokenGracefulDegrade() {
        SlackYuiClient client = new SlackYuiClient("", "U0AM8G9AARF", "https://slack.com/api/chat.postMessage");
        boolean success = client.postMessage("Hello Jason!");
        assertFalse(success, "未配置 Token 时应优雅返回 false 而不是抛出异常");

        boolean blockSuccess = client.postBlockMessage("fallback", "[]");
        assertFalse(blockSuccess);
    }

    @Test
    @DisplayName("测试工厂方法从环境自适应装配")
    void testFromConfig() {
        SlackYuiClient client = SlackYuiClient.fromConfig();
        assertNotNull(client);
        assertEquals("U0AM8G9AARF", client.getDefaultChannel());
    }

    @Test
    @DisplayName("测试若配置了 SLACK_YUI_BOT_TOKEN 则向主人私聊推送冒烟心跳")
    void testLiveDeliveryIfConfigured() {
        String token = ConfigUtils.get("SLACK_YUI_BOT_TOKEN", "");
        if (token.isEmpty() || token.startsWith("mock-")) {
            System.out.println("ℹ️ [Slack Test] SLACK_YUI_BOT_TOKEN 未配置或为 mock，跳过公网真实投递");
            return;
        }

        SlackYuiClient client = SlackYuiClient.fromConfig();
        boolean ok = client.postMessage("☕ 主人，Yui 报到！Slack 贴身推送通道已正式就绪～");
        assertTrue(ok, "真实 Token 应该投递成功");
    }

    @Test
    @DisplayName("🎯 专用测试：真实调用 Yui 机器人发送带富文本与排版的测试消息到主人私聊频道")
    void testRealLiveDeliveryToMaster() {
        String token = ConfigUtils.get("SLACK_YUI_BOT_TOKEN", "");
        if (token.isEmpty()) {
            // 如果环境变量没配，检查命令行参数 -DSLACK_YUI_BOT_TOKEN=...
            token = System.getProperty("SLACK_YUI_BOT_TOKEN", "");
        }

        if (token.isEmpty() || token.startsWith("mock-")) {
            System.out.println("================================================================================");
            System.out.println("⚠️ [Slack Test: testRealLiveDeliveryToMaster] 跳过真实网络发送：");
            System.out.println("  • 原因: 当前环境未检测到有效的 SLACK_YUI_BOT_TOKEN (xoxb-...)。");
            System.out.println("  • 提示: 若需实测推送到您的手机 Slack，可在 .env 补充配置，或执行：");
            System.out.println("    mvn test -Dtest=SlackYuiClientTest#testRealLiveDeliveryToMaster -DSLACK_YUI_BOT_TOKEN=xoxb-xxx");
            System.out.println("================================================================================");
            return;
        }

        SlackYuiClient client = new SlackYuiClient(token, "U0AM8G9AARF", "https://slack.com/api/chat.postMessage");

        String liveMessage = """
                💌 *【Yui 贴身财务秘书 · 实盘联调握手通告】*
                
                主人 Jason，晚上好呀～我是您的专属贴身财务秘书 Yui ❤️。
                
                这一条消息是由我们的 **Flink 湖仓 ETL + LangChain4j 智能分析系统** 在本地执行单元测试时，通过 Slack 原生 Web API 真实直连投递给您的！
                
                * 🚀 **引擎底座**: Apache Flink 1.20.0 + Apache Iceberg 1.7.0 (Java 21)
                * 🤖 **AI 核心**: LiteLLM Gemini-3.8-Flash (带 QuickChart 图表函数武器库)
                * 📊 **财务模型**: 11 大生活消费类目规整完成，9 月大盘平账无误 (净支出 ￥22,679.46)
                * 📬 **投递目标**: 主人专属 Channel (`U0AM8G9AARF`)
                
                当您收到这条消息，说明我们 M2 的 Slack 贴身投递通道已 100% 跑通闭环啦！无论未来您身在何处，Yui 都会准时为您送上最体贴的财务体检与资产研报～请主人静候今晚的财务大盘哦～✨
                """;

        System.out.println("🚀 [Slack Test] 正在向主人 Slack (U0AM8G9AARF) 发送真实测试通告...");
        boolean success = client.postMessage(liveMessage);

        System.out.println("================================================================================");
        if (success) {
            System.out.println("🎉 [Slack Test] 真实投递成功！请查看您的 Slack 手机端/桌面端私聊消息！");
        } else {
            System.out.println("❌ [Slack Test] 投递失败，请检查 Bot Token 权限是否包含 chat:write 与 im:write");
        }
        System.out.println("================================================================================");

        assertTrue(success, "配置了真实 Token 后，真实消息必须成功送达 Slack");
    }

    @Test
    @DisplayName("测试 Markdown 报告自动解析并转化为包含原生 Image Block 的 Block Kit JSON")
    void testConvertMarkdownToBlocksJsonWithImages() {
        String markdownWithImage = """
                # 2026年9月度资产损益与现金流全景战略白皮书
                
                主人，晚上好呀～我是您的专属贴身财务秘书 Yui。
                
                ![2026年9月消费支出类目占比全景](https://quickchart.io/chart/render/zf-527945e4-ca21-4183-8806-a44f41d5ceb1)
                
                这里是分析文字内容。
                
                ![标杆商户柱状图](https://quickchart.io/chart/render/zf-45724423-0385-47a2-a421-709e591eb3df)
                
                晚安，愿您今夜好梦！
                """;

        String blocksJson = SlackYuiClient.convertMarkdownToBlocksJson(markdownWithImage);
        assertNotNull(blocksJson);
        assertTrue(blocksJson.startsWith("[") && blocksJson.endsWith("]"));
        assertTrue(blocksJson.contains("\"type\":\"image\""), "必须解析出原生 Slack image block");
        assertTrue(blocksJson.contains("https://quickchart.io/chart/render/zf-527945e4-ca21-4183-8806-a44f41d5ceb1"));
        assertTrue(blocksJson.contains("https://quickchart.io/chart/render/zf-45724423-0385-47a2-a421-709e591eb3df"));
        assertTrue(blocksJson.contains("\"type\":\"section\""), "正文部分必须被包裹在 section block 中");
        assertTrue(blocksJson.contains("2026年9月消费支出类目占比全景"));
    }
}
