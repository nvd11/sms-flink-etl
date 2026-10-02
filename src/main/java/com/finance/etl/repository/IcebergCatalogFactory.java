package com.finance.etl.repository;

import com.finance.etl.util.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Apache Iceberg 湖仓底层基础设施统一工厂 (IcebergCatalogFactory)
 * 职责：专职负责组装 Cloudflare R2 对象存储 (Hadoop S3A) 与 CockroachDB (JdbcCatalog) 的底层配置。
 * 消除 IcebergR2Sink 与 IcebergOffsetRepository 之间的全部重复样板代码，充当唯一的配置真理源：
 * 1. createHadoopConf(): 生产注入 S3A 认证与 s3:// 协议兼容的 Hadoop Configuration；
 * 2. createCatalogProperties(): 生产标准 JDBC Catalog 属性映射；
 * 3. createCatalogLoader(): 生产专供 Flink 流图两阶段提交使用的 CatalogLoader；
 * 4. createTableLoader(schemaName, tableName): 一键装配 Flink 官方标准的 TableLoader；
 * 5. createJdbcCatalog(): 一键装配供独立仓储 (IcebergOffsetRepository) 直读直写快照的 JdbcCatalog 实例。
 */
public final class IcebergCatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogFactory.class);

    private IcebergCatalogFactory() {
        // 工具类私有构造，禁止实例化
    }

    /**
     * 1. 统一构建 Hadoop S3A 核心文件系统配置 (直连 Cloudflare R2 对象存储)
     */
    public static Configuration createHadoopConf() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", ConfigUtils.get("R2_S3_ENDPOINT", ""));
        hadoopConf.set("fs.s3a.access.key", ConfigUtils.get("R2_S3_ACCESS_KEY_ID", ""));
        hadoopConf.set("fs.s3a.secret.key", ConfigUtils.get("R2_S3_SECRET_ACCESS_KEY", ""));
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"); // 🎯 核心兼任：支持 s3:// 协议前缀
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return hadoopConf;
    }

    /**
     * 2. 统一构建 CockroachDB JDBC Catalog 属性集
     */
    public static Map<String, String> createCatalogProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "jdbc");
        properties.put("uri", ConfigUtils.get("ICEBERG_CATALOG_URI", ""));
        properties.put("jdbc.user", ConfigUtils.get("ICEBERG_CATALOG_USER", ""));
        properties.put("jdbc.password", ConfigUtils.get("ICEBERG_CATALOG_PASSWORD", ""));
        properties.put("warehouse", ConfigUtils.get("ICEBERG_WAREHOUSE_DIR", "s3a://sms-flink-etl/iceberg/warehouse"));
        return properties;
    }

    /**
     * 3. 生产 Flink 官方标准的 CatalogLoader (供 FlinkSink / TableLoader 使用)
     */
    public static CatalogLoader createCatalogLoader(Configuration hadoopConf) {
        String catalogName = ConfigUtils.get("ICEBERG_CATALOG_NAME", "finance");
        Map<String, String> catalogProperties = createCatalogProperties();
        return CatalogLoader.custom(
                catalogName,
                catalogProperties,
                hadoopConf,
                "org.apache.iceberg.jdbc.JdbcCatalog"
        );
    }

    /**
     * 4. 一键装配生产级的 TableLoader (供 Flink IcebergSink 挂载写入目标表)
     */
    public static TableLoader createTableLoader(String schemaName, String tableName) {
        LOG.info("🧊 [Iceberg Factory] Assembling TableLoader for target table: {}.{}", schemaName, tableName);
        Configuration hadoopConf = createHadoopConf();
        CatalogLoader catalogLoader = createCatalogLoader(hadoopConf);
        TableIdentifier tableId = TableIdentifier.of(schemaName, tableName);
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

    /**
     * 5. 一键装配独立的 JdbcCatalog 实例 (供 IcebergOffsetRepository 进行纯 Java 只读扫描与 Snapshot 原子提交)
     */
    public static JdbcCatalog createJdbcCatalog() {
        Configuration hadoopConf = createHadoopConf();
        Map<String, String> catalogProperties = createCatalogProperties();
        String catalogName = ConfigUtils.get("ICEBERG_CATALOG_NAME", "finance");

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize(catalogName, catalogProperties);
        return catalog;
    }
}
