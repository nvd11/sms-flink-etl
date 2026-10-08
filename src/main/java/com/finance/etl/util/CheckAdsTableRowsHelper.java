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

    public static void checkLakehouseDate(String dateStr) {
        try (com.finance.etl.repository.FinancialLakehouseRepository repo = com.finance.etl.repository.FinancialLakehouseRepository.fromConfig()) {
            java.time.LocalDate d = java.time.LocalDate.parse(dateStr);
            com.finance.etl.model.DwsSummaryRecord macro = repo.queryDailySummary(d);
            java.util.List<com.finance.etl.model.FinancialTransaction> txs = repo.queryDailyTransactions(d);
            System.out.println("================================================================================");
            System.out.printf("🔍 [Probe Lakehouse %s in finance_dev]\n", dateStr);
            if (macro != null) {
                System.out.printf("  • Macro Summary: netExpense=￥%s, totalExpense=￥%s, txCount=%s\n",
                        macro.getNetExpense(), macro.getTotalExpense(), macro.getTxCount());
                System.out.printf("  • Category: Medical=￥%s, Food=￥%s, Transport=￥%s, OnlineShopping=￥%s, Offline=￥%s, Other=￥%s\n",
                        macro.getMedicalExpense(), macro.getFoodExpense(), macro.getTransportExpense(), macro.getOnlineShoppingExpense(),
                        macro.getOfflineShoppingExpense(), macro.getOtherExpense());
            } else {
                System.out.println("  • Macro Summary: NULL (该日无宏观 DWS 记录)");
            }
            System.out.printf("  • Micro Transactions: %d 条\n", txs.size());
            for (com.finance.etl.model.FinancialTransaction tx : txs) {
                System.out.printf("    - ID=%s, Time=%s, Amt=￥%s, Type=%s, Cat=%s, Merchant=%s\n",
                        tx.getId(), tx.getTxTime(), tx.getAmount(), tx.getTxType(), tx.getCategory(), tx.getCleanedMerchant());
            }
            System.out.println("================================================================================");
        } catch (Exception e) {
            LOG.error("❌ 探测失败: {}", e.getMessage(), e);
        }
    }

    public static void probeDates(String schema) {
        String url = com.finance.etl.util.ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/finance");
        String user = com.finance.etl.util.ConfigUtils.get("TRINO_USER", "jason");
        String password = com.finance.etl.util.ConfigUtils.get("TRINO_PASSWORD", null);
        String sql = String.format("SELECT CAST(tx_time AS DATE) as dt, count(*) as cnt, sum(amount) as total_amt FROM iceberg.%s.dwd_financial_transactions GROUP BY 1 ORDER BY 1 DESC LIMIT 5", schema);

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("================================================================================");
            System.out.printf("📅 [Recent Dates in iceberg.%s.dwd_financial_transactions]:\n", schema);
            while (rs.next()) {
                System.out.printf("  • Date: %s | Count: %d | TotalAmt: ￥%s\n",
                        rs.getDate("dt"), rs.getLong("cnt"), rs.getBigDecimal("total_amt"));
            }
            System.out.println("================================================================================");
        } catch (Exception e) {
            LOG.error("❌ 探查日期失败 ({}): {}", schema, e.getMessage());
        }
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
