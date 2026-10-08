package com.finance.etl.util;

import com.finance.etl.repository.IcebergCatalogFactory;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 快速探测 ads_financial_reports 数据表行数小工具
 */
public class CheckAdsTableRowsHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CheckAdsTableRowsHelper.class);

    public static void main(String[] args) {
        checkSchema("finance_dev");
    }

    public static void checkSchema(String schema) {
        try (JdbcCatalog catalog = IcebergCatalogFactory.createJdbcCatalog()) {
            TableIdentifier tableId = TableIdentifier.of(schema, "ads_financial_reports");
            if (!catalog.tableExists(tableId)) {
                LOG.warn("⚠️ 表 {}.ads_financial_reports 不存在！", schema);
                return;
            }

            Table table = catalog.loadTable(tableId);
            long count = 0;
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record r : records) {
                    count++;
                    System.out.printf("📄 [Row %d] report_id=%s, period=%s, net_expense=%s, date=%s, slack=%s\n",
                            count,
                            r.get(0, String.class),
                            r.get(2, String.class),
                            r.get(6, Object.class),
                            r.get(3, Object.class),
                            r.get(13, Object.class));
                }
            }
            System.out.printf("================================================================================\n");
            System.out.printf("📊 湖仓表 %s.ads_financial_reports 当前记录总行数: %d 条\n", schema, count);
            System.out.printf("================================================================================\n");
        } catch (Exception e) {
            LOG.error("❌ 读取失败: {}", e.getMessage(), e);
        }
    }
}
