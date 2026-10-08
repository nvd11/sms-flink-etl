package com.finance.etl.jobs;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.pipeline.FinancialReporterPipeline;
import com.finance.etl.repository.FinancialLakehouseRepository;
import com.finance.etl.repository.IcebergCatalogFactory;
import com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FinancialReporterJob 端到端湖仓落盘集成测试")
class FinancialReporterJobIntegrationTest {

    @Test
    @DisplayName("测试 Flink 批处理运行完成并将研报记录成功写入 ads_financial_reports Iceberg 表")
    void testExecuteBatchJobAndPersistToIceberg() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        String periodType = "DAILY";
        String periodValue = "2026-10-05";

        DwsSummaryRecord macro = new DwsSummaryRecord();
        macro.setPeriodType(periodType);
        macro.setPeriodValue(periodValue);
        macro.setStatDate(LocalDate.parse(periodValue));
        macro.setNetExpense(new BigDecimal("23.62"));
        macro.setTxCount(1L);

        FinancialTransaction tx = new FinancialTransaction();
        tx.setId(101L);
        tx.setTxTime(Instant.now());
        tx.setAmount(new BigDecimal("23.62"));
        tx.setCleanedMerchant("广州市番禺区何贤纪念医院");
        tx.setCategory("MEDICAL");

        DataGeneratorSource<DwsSummaryRecord> macroSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, DwsSummaryRecord>) index -> macro,
                1L,
                RateLimiterStrategy.noOp(),
                org.apache.flink.api.common.typeinfo.TypeInformation.of(DwsSummaryRecord.class)
        );

        DataGeneratorSource<FinancialTransaction> microSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, FinancialTransaction>) index -> tx,
                1L,
                RateLimiterStrategy.noOp(),
                org.apache.flink.api.common.typeinfo.TypeInformation.of(FinancialTransaction.class)
        );

        // 预期仅 1 笔微观交易，postToSlack=false 避免单元测试频繁打扰 Slack
        FinancialReportBroadcastProcessFunction reportFunction =
                new FinancialReportBroadcastProcessFunction(periodType, periodValue, false, 1);

        FinancialReporterPipeline pipeline =
                new FinancialReporterPipeline(macroSource, microSource, reportFunction);

        pipeline.build(env);

        // 驱动 Flink 批处理执行
        env.execute("test-financial-reporter-iceberg-persist");

        // 验证 Iceberg 物理表中是否存在数据落盘
        try (JdbcCatalog catalog = IcebergCatalogFactory.createJdbcCatalog()) {
            TableIdentifier tableId = TableIdentifier.of("finance_dev", "ads_financial_reports");
            assertTrue(catalog.tableExists(tableId), "表 ads_financial_reports 必须存在");

            Table table = catalog.loadTable(tableId);
            long rowCount = 0;
            String persistedReportId = null;
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record r : records) {
                    rowCount++;
                    persistedReportId = r.get(0, String.class);
                }
            }

            assertTrue(rowCount > 0, "Flink 执行完毕后，ads_financial_reports 必须成功落盘至少 1 条记录");
            assertEquals("report_daily_2026-10-05", persistedReportId);
        }
    }

    @Test
    @DisplayName("🎯 实战全链路验收：从真实 dev 库动态查最新数据，执行 Flink 批处理双流拓扑，AI 生成图表并真实推 Slack，原生落盘 finance_dev")
    void testRealLivePipelineFromLakehouseWithSlackAndIcebergPersist() throws Exception {
        // 1. 严格锁定 Schema 为 finance_dev，绝对隔离生产环境
        System.setProperty("ICEBERG_CATALOG_SCHEMA", "finance_dev");

        // 2. 动态从 Trino View 查询最新有动账支出的自然日大盘与真实 DWD 流水
        DwsSummaryRecord latestDailyMacro;
        List<FinancialTransaction> realTransactions;

        try (FinancialLakehouseRepository repo = FinancialLakehouseRepository.fromConfig()) {
            latestDailyMacro = repo.queryLatestActiveDailySummary();
            assertNotNull(latestDailyMacro, "dev 数据库 View 中必须能查出最新的日度大盘记录");
            assertNotNull(latestDailyMacro.getStatDate(), "统计日期不可为空");

            LocalDate statDate = latestDailyMacro.getStatDate();
            realTransactions = repo.queryDailyTransactions(statDate);
            assertFalse(realTransactions.isEmpty(), "最新日期下的真实 DWD 动账交易明细不可为空");
        }

        String periodType = "DAILY";
        String periodValue = latestDailyMacro.getStatDate().toString();

        System.out.println("================================================================================");
        System.out.printf("🚀 [Real Live Pipeline Test] 正在针对 dev 库最新日期 (%s) 启动完整 Flink 双流批处理流水线...\n", periodValue);
        System.out.printf("  • 宏观净支出基准: ￥%s (笔数: %d)\n", latestDailyMacro.getNetExpense(), latestDailyMacro.getTxCount());
        System.out.printf("  • 微观真实流水数: %d 笔 (将全量装箱送入算子缓冲区)\n", realTransactions.size());
        System.out.println("================================================================================");

        // 3. 构建 Flink 批处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        // 4. 封装真实的宏观与微观有界 Source
        final DwsSummaryRecord finalMacro = latestDailyMacro;
        final List<FinancialTransaction> finalMicro = realTransactions;

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

        // 5. 开启 postToSlack = true，开启批完成最后一笔精准发射
        FinancialReportBroadcastProcessFunction reportFunction =
                new FinancialReportBroadcastProcessFunction(periodType, periodValue, true, finalMicro.size());

        // 6. 编排 Pipeline（默认自动挂载 Flink 原生 IcebergR2Sink 直写 ads_financial_reports）
        FinancialReporterPipeline pipeline =
                new FinancialReporterPipeline(macroSource, microSource, reportFunction);

        pipeline.build(env);

        // 7. 驱动执行 Flink 批处理
        env.execute("live-financial-reporter-job-" + periodValue);

        // 8. 物理校验 dev 湖仓表落盘记录
        try (JdbcCatalog catalog = IcebergCatalogFactory.createJdbcCatalog()) {
            TableIdentifier tableId = TableIdentifier.of("finance_dev", "ads_financial_reports");
            assertTrue(catalog.tableExists(tableId), "dev 环境表 ads_financial_reports 必须存在");

            Table table = catalog.loadTable(tableId);
            boolean foundTargetReport = false;
            String expectedReportId = "report_daily_" + periodValue;

            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record r : records) {
                    String repId = r.get(0, String.class);
                    if (expectedReportId.equals(repId)) {
                        foundTargetReport = true;
                        System.out.println("================================================================================");
                        System.out.printf("🎉 [Verification] 物理验证通过！最新报告已持久化落盘至 finance_dev.ads_financial_reports:\n");
                        System.out.printf("  • report_id    : %s\n", repId);
                        System.out.printf("  • period_value : %s\n", r.get(2, String.class));
                        System.out.printf("  • report_date  : %s\n", r.get(3, Object.class));
                        System.out.printf("  • net_expense  : ￥%s\n", r.get(6, Object.class));
                        System.out.printf("  • chart_url    : %s\n", r.get(12, String.class));
                        System.out.printf("  • slack_status : %s\n", r.get(13, String.class));
                        System.out.println("================================================================================");
                        assertEquals("SENT", r.get(13, String.class), "真实执行下 Slack 推送状态必须为 SENT");
                        assertNotNull(r.get(12, String.class), "QuickChart 短链不可为空");
                    }
                }
            }
            assertTrue(foundTargetReport, "必须在 finance_dev 湖仓中查到对应 report_id 为 " + expectedReportId + " 的落盘报告！");
        }
    }
}
