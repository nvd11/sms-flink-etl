package com.finance.etl.agent;

import com.finance.etl.model.FinancialChatModelFactory;
import com.finance.etl.model.FinancialReportContext;
import com.finance.etl.service.FinancialAdvisorService;
import com.finance.etl.tools.FinancialChartTools;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.service.AiServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 财务分析智能体核心本体 (FinancialAdvisorAgent)
 * 职责：
 * 纯粹的领域分析大脑 (Pure Logic)。
 * 整合 LangChain4j 的 ChatLanguageModel (直连 LiteLLM Gemini-3.8-Flash) 与 FinancialChartTools (@Tool 绘图武器库)，
 * 装配声明式 FinancialAdvisorService 代理，专职提供高质量、零幻觉的财务分析研报数据生成。
 * 【解法一 · 架构重构】：
 * 在 generateReport 方法内部根据 context.getPeriodType() 执行多态分流，
 * 分别调用对应 100% 纯净人设的专属方法 (Daily / Weekly / Monthly)，外部调用依然保持统一门面！
 */
public class FinancialAdvisorAgent {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialAdvisorAgent.class);

    private final FinancialAdvisorService aiService;

    public FinancialAdvisorAgent(FinancialAdvisorService aiService) {
        this.aiService = Objects.requireNonNull(aiService, "aiService must not be null");
    }

    /**
     * 静态工程装配方法：从环境配置与工具库全自动组装 Agent
     */
    public static FinancialAdvisorAgent fromConfig() {
        LOG.info("🤖 [FinancialAdvisorAgent] Assembling Agent from environment config & chart tools...");
        ChatLanguageModel model = FinancialChatModelFactory.fromConfig();
        FinancialChartTools chartTools = FinancialChartTools.fromConfig();

        FinancialAdvisorService service = AiServices.builder(FinancialAdvisorService.class)
                .chatLanguageModel(model)
                .tools(chartTools)
                .build();

        return new FinancialAdvisorAgent(service);
    }

    /**
     * 核心单一行为方法：根据双流汇聚的 FinancialReportContext 上下文生成完整财务研报数据
     * 内部利用 Java 21 模式匹配 switch 执行优雅多态分流
     *
     * @param context 双流结构化财务上下文 (包含宏观大盘与全量明细流水)
     * @return 包含 QuickChart 图表短链与专业 CFA 深度洞察的纯 Markdown 研报正文
     */
    public String generateReport(FinancialReportContext context) {
        Objects.requireNonNull(context, "context must not be null");
        String periodType = context.getPeriodType() != null ? context.getPeriodType().trim().toUpperCase() : "DAILY";
        LOG.info("🚀 [FinancialAdvisorAgent] Generating financial report for period={}:{}...",
                periodType, context.getPeriodValue());

        String promptContext = context.toPromptContext();

        // 🎯 核心解法一落地：根据周期参数，精准调用专职方法，彻底消除大模型头脑中的分支干扰！
        String report = switch (periodType) {
            case "DAILY" -> aiService.generateDailyReport(promptContext);
            case "WEEKLY" -> aiService.generateWeeklyReport(promptContext);
            case "MONTHLY" -> aiService.generateMonthlyReport(promptContext);
            default -> {
                LOG.warn("⚠️ [FinancialAdvisorAgent] Unrecognized periodType '{}', falling back to Daily report.", periodType);
                yield aiService.generateDailyReport(promptContext);
            }
        };

        LOG.info("✅ [FinancialAdvisorAgent] Financial report generated successfully for {} (length: {} chars)",
                periodType, report != null ? report.length() : 0);
        return report;
    }

    public FinancialAdvisorService getAiService() {
        return aiService;
    }
}
