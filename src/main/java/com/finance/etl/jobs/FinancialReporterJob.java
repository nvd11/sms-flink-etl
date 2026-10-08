package com.finance.etl.jobs;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.pipeline.FinancialReporterPipeline;
import com.finance.etl.repository.FinancialLakehouseRepository;
import com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction;
import com.finance.etl.util.ConfigUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;

/**
 * 财务研报 Flink 统一批作业启动器 (FinancialReporterJob)
 * 职责：
 * 1. 作为 Flink 批处理调度入口，支持外部命令行传参：
 *    - --period <DAILY|WEEKLY|MONTHLY>
 *    - --date <YYYY-MM-DD> (或月度 YYYY-MM)
 * 2. 探查 Trino DWS 宏观平账大盘指标，提取微观 DWD 事实交易明细；
 * 3. 构造双流输入 Source，委托 FinancialReporterPipeline 编排 Flink 拓扑；
 * 4. 触发 Agent 大脑生成研报，并通过 Slack 原生 Block Kit 渲染投递给主人。
 */
public class FinancialReporterJob {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialReporterJob.class);

    public static final String JOB_NAME = "financial-reporter-job";

    public static void main(String[] args) throws Exception {
        LOG.info("================================================================================");
        LOG.info("🚀 Starting Financial Reporter Flink Batch Job ({})", JOB_NAME);
        LOG.info("================================================================================");

        // 1. 解析作业运行参数
        String periodType = "DAILY";
        String periodValue = null;

        for (int i = 0; i < args.length; i++) {
            if ("--period".equalsIgnoreCase(args[i]) && i + 1 < args.length) {
                periodType = args[++i].toUpperCase();
            } else if ("--date".equalsIgnoreCase(args[i]) && i + 1 < args.length) {
                periodValue = args[++i];
            } else if ("--week".equalsIgnoreCase(args[i]) && i + 1 < args.length) {
                periodType = "WEEKLY";
                periodValue = args[++i];
            } else if ("--month".equalsIgnoreCase(args[i]) && i + 1 < args.length) {
                periodType = "MONTHLY";
                periodValue = args[++i];
            }
        }

        // 2. 探查湖仓数据源并解析待处理周期
        DwsSummaryRecord macroRecord;
        List<FinancialTransaction> microTransactions;

        try (FinancialLakehouseRepository repo = FinancialLakehouseRepository.fromConfig()) {
            if ("MONTHLY".equalsIgnoreCase(periodType)) {
                if (periodValue == null) {
                    macroRecord = repo.queryLatestActiveMonthlySummary();
                    if (macroRecord != null && macroRecord.getPeriodValue() != null) {
                        periodValue = macroRecord.getPeriodValue();
                    } else {
                        periodValue = "2026-10";
                    }
                } else {
                    macroRecord = repo.queryMonthlySummary(periodValue);
                }
                LOG.info("🔍 [Job Query] Querying monthly macro summary for: {}", periodValue);
                microTransactions = repo.queryMonthlyTransactions(periodValue);
            } else if ("WEEKLY".equalsIgnoreCase(periodType)) {
                if (periodValue == null) {
                    macroRecord = repo.queryLatestActiveWeeklySummary();
                    if (macroRecord != null && macroRecord.getPeriodValue() != null) {
                        periodValue = macroRecord.getPeriodValue();
                    } else {
                        periodValue = "2026-W41";
                    }
                } else {
                    macroRecord = repo.queryWeeklySummary(periodValue);
                }
                LOG.info("🔍 [Job Query] Querying weekly macro summary for: {}", periodValue);
                microTransactions = repo.queryWeeklyTransactions(periodValue);
            } else {
                periodType = "DAILY";
                if (periodValue == null) {
                    macroRecord = repo.queryLatestActiveDailySummary();
                    if (macroRecord != null && macroRecord.getStatDate() != null) {
                        periodValue = macroRecord.getStatDate().toString();
                    } else {
                        periodValue = LocalDate.now().toString();
                    }
                } else {
                    macroRecord = repo.queryDailySummary(LocalDate.parse(periodValue));
                }
                LOG.info("🔍 [Job Query] Querying daily transactions for date: {}", periodValue);
                microTransactions = repo.queryDailyTransactions(LocalDate.parse(periodValue));
            }
        }

        LOG.info("📊 [Job Metadata] Period: {}:{}, Raw Tx Count: {}", periodType, periodValue, microTransactions.size());

        // 3. 初始化 Flink 批处理环境
        int parallelism = ConfigUtils.getInt("FLINK_PARALLELISM", 1);
        Configuration flinkConf = new Configuration();
        flinkConf.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);

        // 4. 将探测到的宏观与微观数据包装为 Flink 有界 Source
        final DwsSummaryRecord finalMacro = macroRecord != null ? macroRecord : new DwsSummaryRecord();
        final List<FinancialTransaction> finalMicro = microTransactions;

        DataGeneratorSource<DwsSummaryRecord> macroSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, DwsSummaryRecord>) index -> finalMacro,
                1L,
                RateLimiterStrategy.noOp(),
                org.apache.flink.api.common.typeinfo.TypeInformation.of(DwsSummaryRecord.class)
        );

        DataGeneratorSource<FinancialTransaction> microSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, FinancialTransaction>) index -> finalMicro.get(index.intValue()),
                (long) finalMicro.size(),
                RateLimiterStrategy.noOp(),
                org.apache.flink.api.common.typeinfo.TypeInformation.of(FinancialTransaction.class)
        );

        // 5. 组装 Pipeline 拓扑并提交执行
        // 传入 finalMicro.size() 作为 expectedMicroRecordCount，使算子在批处理最后一笔精准 collect 发射给下游 IcebergSink
        FinancialReportBroadcastProcessFunction reportFunction =
                new FinancialReportBroadcastProcessFunction(periodType, periodValue, true, finalMicro.size());

        FinancialReporterPipeline pipeline =
                new FinancialReporterPipeline(macroSource, microSource, reportFunction);

        pipeline.build(env);

        LOG.info("🚀 [Job Execute] Submitting Flink Job Execution Graph...");
        env.execute(JOB_NAME + "-" + periodType.toLowerCase() + "-" + periodValue);
        LOG.info("================================================================================");
        LOG.info("🎉 Financial Reporter Flink Batch Job Completed Successfully!");
        LOG.info("================================================================================");
    }
}
