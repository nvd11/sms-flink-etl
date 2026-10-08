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
}
