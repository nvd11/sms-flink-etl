package com.finance.etl.source.iceberg;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.source.IcebergSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("IcebergR2Source 门面实体与 FLIP-27 数据源测试")
class IcebergR2SourceTest {

    @Test
    @DisplayName("测试从环境配置装配 IcebergR2Source 门面实体")
    void testFromConfig() {
        IcebergR2Source source = IcebergR2Source.fromConfig();

        assertNotNull(source, "IcebergR2Source 实例不能为 null");
        assertEquals("raw_sms_records", source.getTableName(), "默认目标表必须为 raw_sms_records");
        assertEquals("finance_dev", source.getSchemaName(), "开发测试环境下默认 schema 为 finance_dev");
        assertNull(source.getMinRecordId(), "默认全量扫描时 minRecordId 为空");
        assertNull(source.getStartSnapshotId(), "默认全量扫描时 startSnapshotId 为空");
    }

    @Test
    @DisplayName("测试增量构造器：基于 recordId 增量过滤")
    void testIncrementalFromId() {
        IcebergR2Source source = IcebergR2Source.incrementalFromId(200L);

        assertEquals(200L, source.getMinRecordId());
        assertEquals("raw_sms_records", source.getTableName());
    }

    @Test
    @DisplayName("测试增量构造器：基于 SnapshotId 增量过滤")
    void testIncrementalFromSnapshot() {
        IcebergR2Source source = IcebergR2Source.incrementalFromSnapshot(123456789L);

        assertEquals(123456789L, source.getStartSnapshotId());
    }

    @Test
    @DisplayName("测试 buildSource()：验证可成功实例化官方 FLIP-27 IcebergSource 算子")
    void testBuildSource() {
        IcebergR2Source source = IcebergR2Source.fromConfig();
        IcebergSource<RowData> flinkSource = source.buildSource();

        assertNotNull(flinkSource, "构建出的 Flink IcebergSource 不能为 null");
    }

    @Test
    @DisplayName("测试 buildStream()：在本地 Flink 批处理环境中挂载 IcebergSource 数据流")
    void testBuildStream() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        IcebergR2Source source = IcebergR2Source.fromConfig();
        DataStream<RowData> stream = source.buildStream(env);

        assertNotNull(stream, "挂载出的 DataStream<RowData> 实例不能为 null");
        assertEquals(2, stream.getParallelism(), "流算子并发度应继承环境并发度");
    }
}
