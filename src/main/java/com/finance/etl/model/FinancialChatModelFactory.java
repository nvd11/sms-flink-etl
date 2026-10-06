package com.finance.etl.model;

import com.finance.etl.util.ConfigUtils;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 财务大模型实例装配工厂 (FinancialChatModelFactory)
 * 职责：
 * 作为全局单一真理源，专职负责生产配置就绪的 ChatLanguageModel (Gemini 3.8 Flash) 实例。
 * 遵循姿势 A：直连私有 LiteLLM 统一网关，注入 Yui 专属凭证，配置低温与严谨财务审计日志。
 */
public class FinancialChatModelFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialChatModelFactory.class);

    // 默认私有统一网关配置
    public static final String DEFAULT_BASE_URL = "https://gw.jppwl.asia/litellm/v1";
    public static final String DEFAULT_API_KEY = "sk-WhW6BWdwKN_LITjCuAmgiA"; // Yui 专属 Virtual Key
    public static final String DEFAULT_MODEL_NAME = "gemini-3.8-flash";
    public static final double DEFAULT_TEMPERATURE = 0.3;

    /**
     * 工厂方法：从环境变量与 .env 自适应装配生产就绪的 ChatLanguageModel
     */
    public static ChatLanguageModel fromConfig() {
        String baseUrl = ConfigUtils.get("LLM_API_BASE", DEFAULT_BASE_URL);
        String apiKey = ConfigUtils.get("LLM_API_KEY", DEFAULT_API_KEY);
        String modelName = ConfigUtils.get("LLM_MODEL_NAME", DEFAULT_MODEL_NAME);
        double temperature = Double.parseDouble(ConfigUtils.get("LLM_TEMPERATURE", String.valueOf(DEFAULT_TEMPERATURE)));

        return create(baseUrl, apiKey, modelName, temperature);
    }

    /**
     * 参数化构造通用 ChatLanguageModel
     */
    public static ChatLanguageModel create(String baseUrl, String apiKey, String modelName, Double temperature) {
        LOG.info("🤖 [Model Factory] Assembling OpenAiChatModel for LiteLLM: endpoint='{}', model='{}', temp={}",
                baseUrl, modelName, temperature);

        return OpenAiChatModel.builder()
                .baseUrl(baseUrl)
                .apiKey(apiKey)
                .modelName(modelName)
                .temperature(temperature != null ? temperature : DEFAULT_TEMPERATURE)
                .timeout(Duration.ofSeconds(60))
                .maxRetries(2)
                .logRequests(true)
                .logResponses(true)
                .build();
    }
}
