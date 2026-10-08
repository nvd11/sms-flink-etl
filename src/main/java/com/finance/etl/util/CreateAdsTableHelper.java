package com.finance.etl.util;

import com.finance.etl.repository.IcebergCatalogFactory;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * ADS 智能财务研报表初始化工具 (CreateAdsTableHelper)
 * 职责：
 * 优先使用 Trino JDBC 执行建表；若 Trino 视图或环境不可用，则自动降级通过 Iceberg JdbcCatalog 官方 API 创建物理表。
 */
public class CreateAdsTableHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CreateAdsTableHelper.class);

    public static void main(String[] args) {
        createTableViaTrino("finance_dev");
        createTableViaTrino("finance");
    }

    public static void createTableViaTrino(String schema) {
        String trinoUrl = ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/" + schema);
        String trinoUser = ConfigUtils.get("TRINO_USER", "jason");
        String trinoPass = ConfigUtils.get("TRINO_PASSWORD", null);

        String ddl = String.format("""
                CREATE TABLE IF NOT EXISTS iceberg.%s.ads_financial_reports (
                    report_id           VARCHAR,
                    period_type         VARCHAR,
                    period_value        VARCHAR,
                    report_date         DATE,
                    total_expense       DECIMAL(12, 2),
                    total_refund        DECIMAL(12, 2),
                    net_expense         DECIMAL(12, 2),
                    total_income        DECIMAL(12, 2),
                    total_transfer      DECIMAL(12, 2),
                    tx_count            BIGINT,
                    metrics_json        VARCHAR,
                    summary_text        VARCHAR,
                    chart_url           VARCHAR,
                    slack_status        VARCHAR,
                    created_at          TIMESTAMP(6) WITH TIME ZONE
                )
                WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['month(report_date)'],
                    sorted_by = ARRAY['report_date DESC']
                )
                """, schema);

        LOG.info("🔨 [CreateAdsTableHelper] Creating ads_financial_reports table via Trino in schema: {}...", schema);

        try (Connection conn = DriverManager.getConnection(trinoUrl, trinoUser, trinoPass);
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
            LOG.info("✅ [CreateAdsTableHelper] Table iceberg.{}.ads_financial_reports successfully created via Trino!", schema);
        } catch (Exception e) {
            LOG.warn("⚠️ [CreateAdsTableHelper] Trino execution failed: {}. Falling back to Iceberg Catalog API...", e.getMessage());
            createTableViaIcebergCatalog(schema);
        }
    }

    public static void createTableViaIcebergCatalog(String schema) {
        try (JdbcCatalog catalog = IcebergCatalogFactory.createJdbcCatalog()) {
            TableIdentifier tableId = TableIdentifier.of(schema, "ads_financial_reports");
            if (catalog.tableExists(tableId)) {
                LOG.info("✅ [CreateAdsTableHelper] Table {}.ads_financial_reports already exists in Iceberg Catalog.", schema);
                return;
            }

            Schema icebergSchema = new Schema(
                    Types.NestedField.required(1, "report_id", Types.StringType.get()),
                    Types.NestedField.optional(2, "period_type", Types.StringType.get()),
                    Types.NestedField.optional(3, "period_value", Types.StringType.get()),
                    Types.NestedField.optional(4, "report_date", Types.DateType.get()),
                    Types.NestedField.optional(5, "total_expense", Types.DecimalType.of(12, 2)),
                    Types.NestedField.optional(6, "total_refund", Types.DecimalType.of(12, 2)),
                    Types.NestedField.optional(7, "net_expense", Types.DecimalType.of(12, 2)),
                    Types.NestedField.optional(8, "total_income", Types.DecimalType.of(12, 2)),
                    Types.NestedField.optional(9, "total_transfer", Types.DecimalType.of(12, 2)),
                    Types.NestedField.optional(10, "tx_count", Types.LongType.get()),
                    Types.NestedField.optional(11, "metrics_json", Types.StringType.get()),
                    Types.NestedField.optional(12, "summary_text", Types.StringType.get()),
                    Types.NestedField.optional(13, "chart_url", Types.StringType.get()),
                    Types.NestedField.optional(14, "slack_status", Types.StringType.get()),
                    Types.NestedField.optional(15, "created_at", Types.TimestampType.withZone())
            );

            PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                    .month("report_date")
                    .build();

            SortOrder sortOrder = SortOrder.builderFor(icebergSchema)
                    .desc("report_date")
                    .build();

            Table table = catalog.createTable(tableId, icebergSchema, spec);
            LOG.info("✅ [CreateAdsTableHelper] Table {}.ads_financial_reports successfully created via Iceberg JdbcCatalog! Location: {}",
                    schema, table.location());
        } catch (Exception e) {
            LOG.error("❌ [CreateAdsTableHelper] Failed to create table via Iceberg Catalog: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
