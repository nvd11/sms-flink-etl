package com.finance.etl.pipeline;

import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.sink.iceberg.IcebergR2Sink;
import com.finance.etl.transform.dwd.SmsRecordToDwdTransactionMapper;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SmsOdsToDwdPipeline 流水线编排实体单元测试")
class SmsOdsToDwdPipelineTest {

    @Test
    @DisplayName("测试 Pipeline 构造防御机制")
    void testConstructorValidation() {
        assertThrows(NullPointerException.class, () -> new SmsOdsToDwdPipeline(null));

        SmsRecord record = new SmsRecord(441L, "msg-001", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), "text", Instant.now());
        org.apache.flink.connector.datagen.source.DataGeneratorSource<SmsRecord> mockSource =
                new org.apache.flink.connector.datagen.source.DataGeneratorSource<>(
                        index -> record,
                        1,
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(SmsRecord.class)
                );

        assertThrows(NullPointerException.class, () -> new SmsOdsToDwdPipeline(mockSource, null));
    }

    @Test
    @DisplayName("测试组装并挂载 Sink 成功生成 Flink 执行图")
    void testAssembleAndAttachSink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String raw = "106980095508【广发银行】您尾号3342信用卡03日12:10消费9.95人民币，交易商户:支付宝-高德打车。SubId：12026-10-03 12:10:27";
        SmsRecord record = new SmsRecord(441L, "msg-001", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), raw, Instant.now());

        // 构造 Mock Source
        org.apache.flink.api.connector.source.util.ratelimit.RateLimiter rateLimiter;
        org.apache.flink.connector.datagen.source.DataGeneratorSource<SmsRecord> mockSource =
                new org.apache.flink.connector.datagen.source.DataGeneratorSource<>(
                        index -> record,
                        1,
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(SmsRecord.class)
                );

        IcebergR2Sink sink = IcebergR2Sink.fromConfig("dwd_financial_transactions", "id,tx_time");
        SmsOdsToDwdPipeline pipeline = new SmsOdsToDwdPipeline(
                mockSource,
                new SmsRecordToDwdTransactionMapper(),
                sink
        );

        assertNotNull(pipeline.getSource());
        assertNotNull(pipeline.getDwdMapper());
        assertNotNull(pipeline.getSink());

        DataStreamSink<?> streamSink = pipeline.assembleAndAttachSink(env);
        assertNotNull(streamSink);

        assertDoesNotThrow(() -> {
            String plan = env.getExecutionPlan();
            assertNotNull(plan);
            assertTrue(plan.contains("SmsRecord-To-DwdTransaction-Mapper"));
            assertTrue(plan.contains("FinancialTransaction-To-RowData-Mapper"));
        });
    }

    @Test
    @DisplayName("测试当 Sink 为 null 时自动降级为 Print Sink")
    void testFallbackToPrintSink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SmsRecord record = new SmsRecord(441L, "msg-001", "EMAIL_IMAP", "CGB", "SIM_1", Instant.now(), "text", Instant.now());
        org.apache.flink.connector.datagen.source.DataGeneratorSource<SmsRecord> mockSource =
                new org.apache.flink.connector.datagen.source.DataGeneratorSource<>(
                        index -> record,
                        1,
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(SmsRecord.class)
                );

        SmsOdsToDwdPipeline pipeline = new SmsOdsToDwdPipeline(mockSource);
        assertNull(pipeline.getSink());

        DataStreamSink<?> streamSink = pipeline.assembleAndAttachSink(env);
        assertNotNull(streamSink);
    }
}
