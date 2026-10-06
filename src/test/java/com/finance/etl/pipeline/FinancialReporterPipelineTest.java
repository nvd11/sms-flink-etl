package com.finance.etl.pipeline;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.transform.report.FinancialReportBroadcastProcessFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class FinancialReporterPipelineTest {

    @Test
    @DisplayName("测试 FinancialReporterPipeline 双流 Flink 拓扑图成功构建")
    void testBuildPipelineGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        DwsSummaryRecord macro = new DwsSummaryRecord();
        macro.setNetExpense(new BigDecimal("100.00"));
        macro.setTxCount(1L);

        FinancialTransaction tx = new FinancialTransaction();
        tx.setId(1L);
        tx.setTxTime(java.time.Instant.now());
        tx.setAmount(new BigDecimal("100.00"));
        tx.setCleanedMerchant("测试商户");
        tx.setCategory("FOOD");

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

        FinancialReportBroadcastProcessFunction function =
                new FinancialReportBroadcastProcessFunction("DAILY", "2026-10-05", false);

        FinancialReporterPipeline pipeline =
                new FinancialReporterPipeline(macroSource, microSource, function);

        SingleOutputStreamOperator<String> stream = pipeline.build(env);

        assertNotNull(stream);
        assertNotNull(env.getExecutionPlan());
        assertTrue(env.getExecutionPlan().contains("FinancialReport-Broadcast-ProcessFunction"));
    }
}
