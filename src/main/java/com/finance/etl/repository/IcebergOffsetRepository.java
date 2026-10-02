package com.finance.etl.repository;

import com.finance.etl.model.SyncOffset;
import com.finance.etl.util.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Apache Iceberg 湖仓水位元数据仓储服务 (IcebergOffsetRepository)
 * 职责：专职负责 etl_sync_offsets 表的增量水位读取与推进闭环。
 * 1. 启动时：通过 Iceberg 原生只读接口 (IcebergGenerics) 毫秒级直读 R2 湖仓中的最新 UID 水位；
 * 2. 跑完时：通过 Iceberg 官方写入与 Snapshot CAS 提交机制，原子追加本次成功消费的最大水位位点。
 * 独立收敛于 repository 包，彻底解耦 Source 读端与 Sink 写端 (0 跨层依赖倒挂，0 Trino 依赖)。
 */
public class IcebergOffsetRepository implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergOffsetRepository.class);

    private final JdbcCatalog catalog;
    private final String schemaName;
    private final String tableName;

    public IcebergOffsetRepository(JdbcCatalog catalog, String schemaName, String tableName) {
        this.catalog = Objects.requireNonNull(catalog, "JdbcCatalog must not be null");
        this.schemaName = Objects.requireNonNull(schemaName, "Schema name must not be null");
        this.tableName = Objects.requireNonNull(tableName, "Table name must not be null");
    }

    /**
     * 工厂方法：利用统一的 IcebergCatalogFactory 自动装配并初始化仓储服务
     */
    public static IcebergOffsetRepository fromConfig() {
        JdbcCatalog catalog = IcebergCatalogFactory.createJdbcCatalog();
        String schemaName = ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance_dev");
        return new IcebergOffsetRepository(catalog, schemaName, "etl_sync_offsets");
    }

    /**
     * 加载当前目标 Iceberg 表
     */
    public Table loadOffsetTable() {
        TableIdentifier tableId = TableIdentifier.of(schemaName, tableName);
        return catalog.loadTable(tableId);
    }

    /**
     * 1. 读水位：作业启动前调用，获取指定通道的最高已同步 UID (MAX(last_offset))
     *
     * @param jobName      作业名称，例如 'sms-gmail-r2'
     * @param channel      数据通道，例如 'EMAIL_IMAP'
     * @param sourceTarget 目标标识，例如 'alice.h.y.he@gmail.com'
     * @return 历史已同步的最大 UID；若表为空或无记录则返回 0L (触发安全冷启动)
     */
    public long getLatestOffset(String jobName, String channel, String sourceTarget) {
        LOG.info("🌊 [Offset Repo] Probing latest offset for job='{}', channel='{}', target='{}'...",
                jobName, channel, sourceTarget);
        try {
            Table table = loadOffsetTable();
            long maxOffset = 0L;

            try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                    .where(Expressions.and(
                            Expressions.equal("job_name", jobName),
                            Expressions.equal("channel", channel)
                    ))
                    .build()) {

                for (Record record : records) {
                    String target = (String) record.getField("source_target");
                    if (sourceTarget == null || sourceTarget.equals(target)) {
                        Long offset = (Long) record.getField("last_offset");
                        if (offset != null && offset > maxOffset) {
                            maxOffset = offset;
                        }
                    }
                }
            }

            LOG.info("🌊 [Offset Repo] Successfully discovered latest synced offset: {} from Iceberg {}.{}",
                    maxOffset, schemaName, tableName);
            return maxOffset;
        } catch (Exception e) {
            LOG.warn("⚠️ [Offset Repo] Failed to query Iceberg offset table ({}.{}): {}. Defaulting to 0L.",
                    schemaName, tableName, e.getMessage());
            return 0L;
        }
    }

    /**
     * 2. 写水位：作业执行成功 (env.execute() 返回) 后调用，原子提交最新增量位点
     *
     * @param offset 包含作业名、通道、最新 UID 及事件时间的实体对象
     */
    public void saveOffset(SyncOffset offset) {
        Objects.requireNonNull(offset, "SyncOffset must not be null");
        LOG.info("💾 [Offset Repo] Committing new offset snapshot to Iceberg: {}", offset);

        try {
            Table table = loadOffsetTable();

            // 1. 创建符合 etl_sync_offsets 结构的标准 GenericRecord
            GenericRecord record = GenericRecord.create(table.schema());
            record.setField("job_name", offset.getJobName());
            record.setField("channel", offset.getChannel());
            record.setField("source_target", offset.getSourceTarget());
            record.setField("last_offset", offset.getLastOffset());

            OffsetDateTime eventTime = offset.getLastEventTime() != null ?
                    offset.getLastEventTime().atOffset(ZoneOffset.UTC) : OffsetDateTime.now(ZoneOffset.UTC);
            OffsetDateTime updatedAt = offset.getUpdatedAt() != null ?
                    offset.getUpdatedAt().atOffset(ZoneOffset.UTC) : OffsetDateTime.now(ZoneOffset.UTC);

            record.setField("last_event_time", eventTime);
            record.setField("updated_at", updatedAt);

            // 2. 写入微型 Parquet 数据文件至 R2
            String filename = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
            OutputFile out = table.io().newOutputFile(filename);
            DataWriter<Record> writer = Parquet.writeData(out)
                    .forTable(table)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .build();

            try {
                writer.write(record);
            } finally {
                writer.close();
            }

            DataFile dataFile = writer.toDataFile();

            // 3. 执行 CAS 原子事务提交 (Snapshot Append)
            table.newAppend().appendFile(dataFile).commit();
            LOG.info("✅ [Offset Repo] Successfully committed offset snapshot to {}.{} (UID: {})",
                    schemaName, tableName, offset.getLastOffset());

        } catch (Exception e) {
            LOG.error("❌ [Offset Repo] Failed to commit offset snapshot to Iceberg: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to persist sync offset to Iceberg lakehouse", e);
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public void close() throws IOException {
        if (catalog != null) {
            catalog.close();
        }
    }
}
