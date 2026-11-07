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
 * 作为全局单一真理源，专职负责生产配置就绪的 ChatLanguageModel (Higress · DeepSeek V4.1 Flash 1017) 实例。
 * 遵循姿势 A：直连私有 LiteLLM 统一网关，注入 Yui 专属凭证，配置低温与严谨财务审计日志。
 */
public class FinancialChatModelFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialChatModelFactory.class);

    // 默认私有统一网关配置
    public static final String DEFAULT_BASE_URL = "http://100.105.130.0:31880/v1";
    // ⚠️ 安全红线：严禁在此硬编码任何真实 Key！必须通过 LLM_API_KEY 环境变量 / .env 注入。
    public static final String DEFAULT_API_KEY = "";
    public static final String DEFAULT_MODEL_NAME = "deepseek-v4.1-flash-1017";
    public static final double DEFAULT_TEMPERATURE = 0.3;
    public static final int DEFAULT_TIMEOUT_SECONDS = 180;
    public static final int DEFAULT_MAX_TOKENS = 8192;

    /**
     * 工厂方法：从环境变量与 .env 自适应装配生产就绪的 ChatLanguageModel
     */
    public static ChatLanguageModel fromConfig() {
        String baseUrl = ConfigUtils.get("LLM_API_BASE", DEFAULT_BASE_URL);
        String apiKey = ConfigUtils.get("LLM_API_KEY", DEFAULT_API_KEY);
        String modelName = ConfigUtils.get("LLM_MODEL_NAME", DEFAULT_MODEL_NAME);
        double temperature = Double.parseDouble(ConfigUtils.get("LLM_TEMPERATURE", String.valueOf(DEFAULT_TEMPERATURE)));
        int timeoutSeconds = ConfigUtils.getInt("LLM_TIMEOUT", DEFAULT_TIMEOUT_SECONDS);
        int maxTokens = ConfigUtils.getInt("LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS);
        String higressProvider = ConfigUtils.get("HIGRESS_LLM_PROVIDER", null);

        return create(baseUrl, apiKey, modelName, temperature, timeoutSeconds, maxTokens, higressProvider);
    }

    /**
     * 参数化构造通用 ChatLanguageModel (保持重载兼容)
     */
    public static ChatLanguageModel create(String baseUrl, String apiKey, String modelName, Double temperature) {
        return create(baseUrl, apiKey, modelName, temperature, DEFAULT_TIMEOUT_SECONDS, DEFAULT_MAX_TOKENS, null);
    }

    /**
     * 参数化构造通用 ChatLanguageModel (支持自定义超时)
     */
    public static ChatLanguageModel create(String baseUrl, String apiKey, String modelName, Double temperature, int timeoutSeconds) {
        return create(baseUrl, apiKey, modelName, temperature, timeoutSeconds, DEFAULT_MAX_TOKENS, null);
    }

    /**
     * 参数化构造通用 ChatLanguageModel (支持自定义超时与最大Token数)
     */
    public static ChatLanguageModel create(String baseUrl, String apiKey, String modelName, Double temperature, int timeoutSeconds, int maxTokens) {
        return create(baseUrl, apiKey, modelName, temperature, timeoutSeconds, maxTokens, null);
    }

    /**
     * 全参数构造通用 ChatLanguageModel (支持自定义 Header，如 Higress Provider 路由)
     */
    public static ChatLanguageModel create(String baseUrl, String apiKey, String modelName, Double temperature,
                                           int timeoutSeconds, int maxTokens, String higressProvider) {
        LOG.info("🤖 [Model Factory] Assembling OpenAiChatModel: endpoint='{}', model='{}', temp={}, timeout={}s, maxTokens={}, provider='{}'",
                baseUrl, modelName, temperature, timeoutSeconds, maxTokens, higressProvider);

        var builder = OpenAiChatModel.builder()
                .baseUrl(baseUrl)
                .apiKey(apiKey)
                .modelName(modelName)
                .temperature(temperature != null ? temperature : DEFAULT_TEMPERATURE)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .maxTokens(maxTokens)
                .maxRetries(2)
                .logRequests(true)
                .logResponses(true);

        if (higressProvider != null && !higressProvider.trim().isEmpty()) {
            builder.customHeaders(java.util.Map.of("x-higress-llm-provider", higressProvider.trim()));
        }

        return builder.build();
    }
}
