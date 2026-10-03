package com.finance.etl.source.iceberg;

import com.finance.etl.repository.IcebergCatalogFactory;
import com.finance.etl.util.ConfigUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Apache Iceberg on Cloudflare R2 湖仓读端门面实体 (IcebergR2Source)
 * 职责：与写端 IcebergR2Sink 镜像对称。
 * 封装 Apache Iceberg 官方生产级 FLIP-27 Source 实现 (IcebergSource<RowData>)。
 * 专职负责从 ODS 层 (raw_sms_records) 以零常驻、高吞吐批处理模式读取数据，供下游 DWD 流水线消费。
 *
 * 核心特性：
 * 1. 依托官方 FLIP-27 动态派单调度 (SimpleSplitAssigner / Work-Stealing)；
 * 2. 依据环境配置自适应路由物理存储桶 (sms-flink-etl vs sms-flink-etl-dev)；
 * 3. 支持基于谓词表达式 (Expression Filter) 的增量位点扫描 (如 id > lastOffset) 与分区剪枝；
 * 4. 支持指定快照区间 (startSnapshotId / endSnapshotId) 的精准增量捕获。
 */
public class IcebergR2Source implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergR2Source.class);

    private final String schemaName;
    private final String tableName;
    private final Long startSnapshotId;
    private final Long endSnapshotId;
    private final Long minRecordId;
    private final Long maxRecordId;

    public IcebergR2Source(String schemaName, String tableName) {
        this(schemaName, tableName, null, null, null, null);
    }

    public IcebergR2Source(String schemaName, String tableName, Long startSnapshotId, Long endSnapshotId, Long minRecordId) {
        this(schemaName, tableName, startSnapshotId, endSnapshotId, minRecordId, null);
    }

    public IcebergR2Source(String schemaName, String tableName, Long startSnapshotId, Long endSnapshotId, Long minRecordId, Long maxRecordId) {
        this.schemaName = Objects.requireNonNull(schemaName, "Schema name must not be null");
        this.tableName = Objects.requireNonNull(tableName, "Table name must not be null");
        this.startSnapshotId = startSnapshotId;
        this.endSnapshotId = endSnapshotId;
        this.minRecordId = minRecordId;
        this.maxRecordId = maxRecordId;
    }

    /**
     * 工厂方法：从环境配置与默认参数自适应装配 ODS 读取源
     */
    public static IcebergR2Source fromConfig() {
        String schemaName = ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance_dev");
        return new IcebergR2Source(schemaName, "raw_sms_records");
    }

    /**
     * 工厂方法：指定增量起始 recordId (即只读取 id > minRecordId 的新记录)
     */
    public static IcebergR2Source incrementalFromId(long minRecordId) {
        String schemaName = ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance_dev");
        return new IcebergR2Source(schemaName, "raw_sms_records", null, null, minRecordId, null);
    }

    /**
     * 工厂方法：指定增量起始 recordId 与单批次最大拉取上限 (minRecordId < id <= minRecordId + batchSize)
     */
    public static IcebergR2Source incrementalFromId(long minRecordId, int batchSize) {
        String schemaName = ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance_dev");
        Long upperLimit = batchSize > 0 ? (minRecordId + batchSize) : null;
        return new IcebergR2Source(schemaName, "raw_sms_records", null, null, minRecordId, upperLimit);
    }

    /**
     * 工厂方法：指定快照版本增量扫描
     */
    public static IcebergR2Source incrementalFromSnapshot(long startSnapshotId) {
        String schemaName = ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance_dev");
        return new IcebergR2Source(schemaName, "raw_sms_records", startSnapshotId, null, null);
    }

    /**
     * 核心构建：装配 Apache Iceberg 官方标准的 FLIP-27 IcebergSource 实例
     *
     * @return 配置就绪的 IcebergSource<RowData>
     */
    public IcebergSource<RowData> buildSource() {
        LOG.info("🧊 [Iceberg Source] Assembling Iceberg FLIP-27 Source for target table: {}.{} (minId: {}, startSnapshot: {})...",
                schemaName, tableName, minRecordId, startSnapshotId);

        TableLoader tableLoader = IcebergCatalogFactory.createTableLoader(schemaName, tableName);

        IcebergSource.Builder<RowData> builder = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .assignerFactory(new SimpleSplitAssignerFactory())
                .streaming(false); // 🎯 严格锁定为批处理 (Bounded Batch Mode)

        List<Expression> filters = new ArrayList<>();
        if (minRecordId != null && minRecordId > 0L) {
            LOG.info("🎯 [Iceberg Source] Applying incremental lower-bound filter: id > {}", minRecordId);
            filters.add(Expressions.greaterThan("id", minRecordId));
        }

        if (maxRecordId != null && maxRecordId > 0L) {
            LOG.info("🎯 [Iceberg Source] Applying incremental upper-bound filter: id <= {}", maxRecordId);
            filters.add(Expressions.lessThanOrEqual("id", maxRecordId));
        }

        if (!filters.isEmpty()) {
            builder.filters(filters);
        }

        if (startSnapshotId != null && startSnapshotId > 0L) {
            LOG.info("🎯 [Iceberg Source] Applying incremental snapshot window: startSnapshotId = {}", startSnapshotId);
            builder.startSnapshotId(startSnapshotId);
            if (endSnapshotId != null && endSnapshotId > 0L) {
                builder.endSnapshotId(endSnapshotId);
            }
        }

        return builder.build();
    }

    /**
     * 核心行为方法：将 Source 挂载进 Flink 执行环境，产出标准的 DataStream<RowData>
     *
     * @param env Flink 流执行环境
     * @return 包含 ODS 数据的 DataStream<RowData>
     */
    public DataStream<RowData> buildStream(StreamExecutionEnvironment env) {
        Objects.requireNonNull(env, "StreamExecutionEnvironment must not be null");

        IcebergSource<RowData> source = buildSource();
        String sourceName = "Iceberg-" + schemaName + "-" + tableName + "-Source";

        // 🎯 核心解决 Type Erasure：显式提供 RowData 的 TypeInformation
        return env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                sourceName,
                org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
                        org.apache.flink.table.types.logical.RowType.of(
                                new org.apache.flink.table.types.logical.BigIntType(),
                                new org.apache.flink.table.types.logical.VarCharType(org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
                                new org.apache.flink.table.types.logical.VarCharType(org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
                                new org.apache.flink.table.types.logical.VarCharType(org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
                                new org.apache.flink.table.types.logical.VarCharType(org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
                                new org.apache.flink.table.types.logical.LocalZonedTimestampType(6),
                                new org.apache.flink.table.types.logical.VarCharType(org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
                                new org.apache.flink.table.types.logical.LocalZonedTimestampType(6)
                        )
                )
        );
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public Long getStartSnapshotId() {
        return startSnapshotId;
    }

    public Long getEndSnapshotId() {
        return endSnapshotId;
    }

    public Long getMinRecordId() {
        return minRecordId;
    }
}
