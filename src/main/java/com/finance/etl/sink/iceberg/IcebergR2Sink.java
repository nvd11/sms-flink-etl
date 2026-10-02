package com.finance.etl.sink.iceberg;

import com.finance.etl.model.SyncOffset;
import com.finance.etl.repository.IcebergCatalogFactory;
import com.finance.etl.repository.IcebergOffsetRepository;
import com.finance.etl.util.ConfigUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Apache Iceberg on Cloudflare R2 湖仓落盘写端门面实体 (IcebergR2Sink)
 * 职责：与读端 ImapSource 形成绝对镜像对称规范。
 * 持有向 Cloudflare R2 (S3A 协议) 与 CockroachDB (JDBC Catalog) 写入数据所需的全部连接元数据。
 * 提供 append(DataStream<RowData>) 行为方法，挂载官方两阶段提交 (2PC) 算子链并返回标准的 DataStreamSink 算子节点。
 */
public class IcebergR2Sink implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergR2Sink.class);

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String catalogUri;
    private final String catalogUser;
    private final String catalogPassword;
    private final String warehouseDir;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final int writeParallelism;

    public IcebergR2Sink(String schemaName, String tableName, int writeParallelism) {
        this.schemaName = Objects.requireNonNull(schemaName, "Schema name must not be null");
        this.tableName = Objects.requireNonNull(tableName, "Table name must not be null");
        this.writeParallelism = writeParallelism;
        this.endpoint = ConfigUtils.get("R2_S3_ENDPOINT", "");
        this.accessKey = ConfigUtils.get("R2_S3_ACCESS_KEY_ID", "");
        this.secretKey = ConfigUtils.get("R2_S3_SECRET_ACCESS_KEY", "");
        this.catalogUri = ConfigUtils.get("ICEBERG_CATALOG_URI", "");
        this.catalogUser = ConfigUtils.get("ICEBERG_CATALOG_USER", "");
        this.catalogPassword = ConfigUtils.get("ICEBERG_CATALOG_PASSWORD", "");
        this.warehouseDir = ConfigUtils.get("ICEBERG_WAREHOUSE_DIR", "s3a://sms-flink-etl/iceberg/warehouse");
        this.catalogName = ConfigUtils.get("ICEBERG_CATALOG_NAME", "finance");
    }

    public IcebergR2Sink(String endpoint, String accessKey, String secretKey,
                         String catalogUri, String catalogUser, String catalogPassword,
                         String warehouseDir, String catalogName,
                         String schemaName, String tableName, int writeParallelism) {
        this.endpoint = Objects.requireNonNull(endpoint, "R2 endpoint must not be null");
        this.accessKey = Objects.requireNonNull(accessKey, "R2 access key must not be null");
        this.secretKey = Objects.requireNonNull(secretKey, "R2 secret key must not be null");
        this.catalogUri = Objects.requireNonNull(catalogUri, "Catalog URI must not be null");
        this.catalogUser = Objects.requireNonNull(catalogUser, "Catalog user must not be null");
        this.catalogPassword = Objects.requireNonNull(catalogPassword, "Catalog password must not be null");
        this.warehouseDir = warehouseDir;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.writeParallelism = writeParallelism;
    }

    /**
     * 工厂方法：直接从环境变量 / .env 中装配并返回一个配置就绪的 IcebergR2Sink 实体实例
     */
    public static IcebergR2Sink fromConfig() {
        return new IcebergR2Sink(
                ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance_dev"),
                "raw_sms_records",
                1 // 🎯 核心约束：漏斗形单并发 (消灭小文件碎片，零碎化单包落盘)
        );
    }

    /**
     * 核心行为方法：将上游 RowData 列式数据流挂载写入 Iceberg 表，返回 Flink 官方 DataStreamSink 算子节点
     *
     * @param rowStream 已完成字段投影映射的 DataStream<RowData>
     * @return 挂载完成的 DataStreamSink<Void>
     */
    public DataStreamSink<Void> append(DataStream<RowData> rowStream) {
        Objects.requireNonNull(rowStream, "Input DataStream<RowData> must not be null");

        LOG.info("🧊 [Iceberg Sink] Assembling Cloudflare R2 Iceberg Sink (table: {}.{}, writeParallelism={})...",
                schemaName, tableName, writeParallelism);

        // 🎯 核心委托：底层 S3A 文件系统、JDBC Catalog 与 TableLoader 装配全部收敛于工厂单一真理源
        TableLoader tableLoader = IcebergCatalogFactory.createTableLoader(schemaName, tableName);

        // 调用官方 FlinkSink，并显式锁定 writeParallelism(1)！返回 DataStreamSink 实例
        DataStreamSink<Void> sink = FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .writeParallelism(writeParallelism) // 🎯 核心控制点：收敛为单一 Writer
                .append();

        LOG.info("✅ [Iceberg Sink] Successfully mounted Iceberg Sink to target table: {}.{}", schemaName, tableName);
        return sink;
    }

    /**
     * 步骤 2：推进水位表 (只有在批处理作业 env.execute() 彻底成功后才被触发)
     *
     * @param jobName 作业标识，例如 'sms-gmail-r2'
     * @param channel 数据通道，例如 'EMAIL_IMAP'
     * @param target  目标标识，例如 'alice.h.y.he@gmail.com'
     * @param maxUid  本次成功入库的最大 UID (由 Flink 累加器汇聚得出)
     */
    public void commitOffset(String jobName, String channel, String target, long maxUid) {
        if (maxUid <= 0L) {
            LOG.info("ℹ️ [Iceberg Sink] No new records processed in this batch (maxUid={}). Watermark remains unchanged.", maxUid);
            return;
        }

        try (IcebergOffsetRepository repo = IcebergOffsetRepository.fromConfig()) {
            SyncOffset offset = new SyncOffset(
                    jobName,
                    channel,
                    target,
                    maxUid,
                    java.time.Instant.now(),
                    java.time.Instant.now()
            );
            repo.saveOffset(offset);
            LOG.info("🌊 [Iceberg Sink] Successfully advanced lakehouse watermark to UID: {}", maxUid);
        } catch (Exception e) {
            LOG.error("❌ [Iceberg Sink] Failed to advance watermark to {}: {}", maxUid, e.getMessage(), e);
            throw new RuntimeException("Failed to commit offset to Iceberg lakehouse", e);
        }
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getCatalogUri() {
        return catalogUri;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getWriteParallelism() {
        return writeParallelism;
    }
}
