package com.finance.etl.sink.iceberg;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("IcebergR2Sink 门面实体单元测试")
class IcebergR2SinkTest {

    @Test
    @DisplayName("测试从环境配置装配 IcebergR2Sink 门面实体：验证 writeParallelism=1 与 Upsert 默认配置")
    void testFromConfig() {
        IcebergR2Sink sink = IcebergR2Sink.fromConfig();

        assertNotNull(sink);
        assertEquals(1, sink.getWriteParallelism(), "写端并发度必须严格锁定为 1 (漏斗形单写策略)");
        assertEquals("raw_sms_records", sink.getTableName(), "默认目标表必须为 raw_sms_records");
        assertEquals("finance_dev", sink.getSchemaName(), "开发测试环境下默认 schema 为 finance_dev");
        assertTrue(sink.isUpsert(), "默认应开启 Iceberg 原生 Upsert 模式");
        assertTrue(sink.getEqualityColumns().contains("msg_uid"), "默认 equality column 必须包含 msg_uid");
        assertTrue(sink.getEqualityColumns().contains("received_at"), "默认 equality column 必须包含分区源字段 received_at");
        assertNotNull(sink.getEndpoint());
        assertNotNull(sink.getCatalogUri());
    }

    @Test
    @DisplayName("测试构造器非空防御机制")
    void testConstructorValidation() {
        assertThrows(NullPointerException.class, () -> new IcebergR2Sink(
                null, "ak", "sk", "uri", "user", "pass", "s3a://wh", "finance", "finance_dev", "table", 1
        ));

        assertThrows(NullPointerException.class, () -> new IcebergR2Sink(
                "endpoint", null, "sk", "uri", "user", "pass", "s3a://wh", "finance", "finance_dev", "table", 1
        ));
    }

    @Test
    @DisplayName("测试 commitOffset 在 maxUid <= 0 时安全跳过，不发起网络提交")
    void testCommitOffsetZeroSafelySkips() {
        IcebergR2Sink sink = IcebergR2Sink.fromConfig();
        assertDoesNotThrow(() -> {
            sink.commitOffset("test-job", "EMAIL_IMAP", "test@gmail.com", 0L);
            sink.commitOffset("test-job", "EMAIL_IMAP", "test@gmail.com", -1L);
        }, "maxUid <= 0 时应安全跳过且不抛出异常");
    }
}
