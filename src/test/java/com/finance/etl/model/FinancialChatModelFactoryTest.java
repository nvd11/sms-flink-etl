package com.finance.etl.model;

import dev.langchain4j.model.chat.ChatLanguageModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FinancialChatModelFactory 财务大模型工厂单元与连通性测试")
class FinancialChatModelFactoryTest {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialChatModelFactoryTest.class);

    @Test
    @DisplayName("测试从环境配置一键生产 ChatLanguageModel 实例")
    void testFromConfigBuildsNonNullModel() {
        ChatLanguageModel model = FinancialChatModelFactory.fromConfig();
        assertNotNull(model, "生成的 ChatLanguageModel 实例绝不可为 null");
    }

    @Test
    @DisplayName("实测连通性：向私有 LiteLLM 统一网关发送 Ping 请求，验证 Gemini 3.8 Flash 正常响应")
    void testLiteLlmGatewayConnectivityWithGemini() {
        ChatLanguageModel model = FinancialChatModelFactory.fromConfig();

        LOG.info("📡 Sending Ping message to LiteLLM Gateway via LangChain4j...");
        String prompt = "你是 Yui，请用极简的一句话向主人打个招呼，证明网络链路与模型通畅。";

        String response = assertDoesNotThrow(() -> model.generate(prompt), "调用 LiteLLM 网关不应抛出异常");

        assertNotNull(response);
        assertFalse(response.trim().isEmpty(), "大模型响应内容不应为空");
        LOG.info("✨ [Gemini Response Received]: {}", response);
    }
}
