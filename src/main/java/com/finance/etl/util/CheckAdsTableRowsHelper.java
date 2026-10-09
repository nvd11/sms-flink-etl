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

    public static void checkEtlSyncOffsets(String schema) {
        String url = com.finance.etl.util.ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/finance");
        String user = com.finance.etl.util.ConfigUtils.get("TRINO_USER", "jason");
        String password = com.finance.etl.util.ConfigUtils.get("TRINO_PASSWORD", null);

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT job_name, channel, source_target, last_offset, last_event_time, updated_at FROM iceberg." + schema + ".etl_sync_offsets ORDER BY updated_at DESC")) {
            System.out.println("================================================================================");
            System.out.println("🌊 [Watermark Table: iceberg." + schema + ".etl_sync_offsets]:");
            boolean hasRows = false;
            while (rs.next()) {
                hasRows = true;
                System.out.printf("  • job: %s | channel: %s | target: %s | offset: %d | event_time: %s | updated: %s\n",
                        rs.getString("job_name"), rs.getString("channel"), rs.getString("source_target"),
                        rs.getLong("last_offset"), rs.getTimestamp("last_event_time"), rs.getTimestamp("updated_at"));
            }
            if (!hasRows) {
                System.out.println("  (表内暂无行记录)");
            }
            System.out.println("================================================================================");
        } catch (Exception e) {
            LOG.error("❌ 查询水位表失败 ({}): {}", schema, e.getMessage());
        }
    }

    public static void checkChartUrlsInWeeklySummary() {
        String url = com.finance.etl.util.ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/finance");
        String user = com.finance.etl.util.ConfigUtils.get("TRINO_USER", "jason");
        String password = com.finance.etl.util.ConfigUtils.get("TRINO_PASSWORD", null);

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT report_id, chart_url, summary_text FROM iceberg.finance_dev.ads_financial_reports WHERE report_id = 'report_weekly_2026-W41'")) {
            if (rs.next()) {
                String repId = rs.getString("report_id");
                String primaryChartUrl = rs.getString("chart_url");
                String summary = rs.getString("summary_text");

                System.out.println("================================================================================");
                System.out.printf("📊 [Chart URLs in %s]\n", repId);
                System.out.printf("  • 表字段 chart_url 存放的主图: %s\n", primaryChartUrl);

                java.util.regex.Pattern p = java.util.regex.Pattern.compile("(https?://quickchart\\.io/chart/render/[^\\s\\)\"]+)");
                java.util.regex.Matcher m = p.matcher(summary);
                int count = 0;
                while (m.find()) {
                    count++;
                    System.out.printf("  • summary_text 中的图表短链 [%d]: %s\n", count, m.group(1));
                }
                System.out.printf("📈 summary_text 中总共包含: %d 条图表 URL\n", count);
                System.out.println("================================================================================");
            }
        } catch (Exception e) {
            LOG.error("❌ 提取失败: {}", e.getMessage(), e);
        }
    }

    public static void testSelectStar() {
        String url = com.finance.etl.util.ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/finance");
        String user = com.finance.etl.util.ConfigUtils.get("TRINO_USER", "jason");
        String password = com.finance.etl.util.ConfigUtils.get("TRINO_PASSWORD", null);

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM iceberg.finance_dev.ads_financial_reports")) {
            java.sql.ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            System.out.println("================================================================================");
            System.out.println("📋 Column Metadata for SELECT *:");
            for (int i = 1; i <= colCount; i++) {
                System.out.printf("  %d: %s (%s, type=%d)\n", i, md.getColumnName(i), md.getColumnTypeName(i), md.getColumnType(i));
            }
            System.out.println("--------------------------------------------------------------------------------");
            while (rs.next()) {
                System.out.printf("Report [%s]:\n", rs.getString("report_id"));
                for (int i = 1; i <= colCount; i++) {
                    Object val = rs.getObject(i);
                    String valStr = val != null ? val.toString() : "[NULL]";
                    if (valStr.length() > 40) valStr = valStr.substring(0, 40) + "...";
                    System.out.printf("   col %d [%s] = %s\n", i, md.getColumnName(i), valStr);
                }
            }
            System.out.println("================================================================================");
        } catch (Exception e) {
            LOG.error("❌ SELECT * 失败: {}", e.getMessage(), e);
        }
    }

    public static void queryTrinoAdsTable() {
        String url = com.finance.etl.util.ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/finance");
        String user = com.finance.etl.util.ConfigUtils.get("TRINO_USER", "jason");
        String password = com.finance.etl.util.ConfigUtils.get("TRINO_PASSWORD", null);

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT report_id, metrics_json, summary_text, chart_url, length(summary_text) as summary_len FROM iceberg.finance_dev.ads_financial_reports")) {
            System.out.println("================================================================================");
            System.out.println("🔍 [Trino Query: ads_financial_reports]");
            while (rs.next()) {
                System.out.printf("  • report_id: %s\n    chart_url: %s\n    summary_len: %s\n    summary_text: %s\n",
                        rs.getString("report_id"),
                        rs.getString("chart_url"),
                        rs.getObject("summary_len"),
                        rs.getString("summary_text") != null ? (rs.getString("summary_text").substring(0, Math.min(50, rs.getString("summary_text").length())) + "...") : "null");
            }
            System.out.println("================================================================================");
        } catch (Exception e) {
            LOG.error("❌ Trino 查询失败: {}", e.getMessage(), e);
        }
    }

    public static void probeWeeklyWatermark(String schema) {
        String url = com.finance.etl.util.ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/finance");
        String user = com.finance.etl.util.ConfigUtils.get("TRINO_USER", "jason");
        String password = com.finance.etl.util.ConfigUtils.get("TRINO_PASSWORD", null);
        String sql = String.format("""
                SELECT
                    week_period,
                    week_start_date,
                    week_end_date,
                    min_id,
                    max_id,
                    tx_count,
                    total_expense,
                    net_expense
                FROM iceberg.%s.dws_financial_summary_weekly
                ORDER BY week_period DESC
                """, schema);

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("================================================================================");
            System.out.printf("🌊 [Weekly Watermark & Metrics in iceberg.%s.dws_financial_summary_weekly]:\n", schema);
            while (rs.next()) {
                System.out.printf("  • 周度: %s | 日期跨度: [%s ~ %s] | 水位[min_id ~ max_id]: [%s ~ %s] | 消费笔数: %s | 净支出: ￥%s\n",
                        rs.getString("week_period"),
                        rs.getDate("week_start_date"),
                        rs.getDate("week_end_date"),
                        rs.getLong("min_id"),
                        rs.getLong("max_id"),
                        rs.getLong("tx_count"),
                        rs.getBigDecimal("net_expense"));
            }
            System.out.println("================================================================================");
        } catch (Exception e) {
            LOG.error("❌ 探查周度水位线失败: {}", e.getMessage(), e);
        }
    }

    public static void probeWeeklyAndMonthly(String schema) {
        String url = com.finance.etl.util.ConfigUtils.get("TRINO_JDBC_URL", "jdbc:trino://10.0.1.113:30880/iceberg/finance");
        String user = com.finance.etl.util.ConfigUtils.get("TRINO_USER", "jason");
        String password = com.finance.etl.util.ConfigUtils.get("TRINO_PASSWORD", null);

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.Statement stmt = conn.createStatement()) {

            System.out.println("================================================================================");
            System.out.println("📅 [Probing Weekly View in " + schema + "]");
            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM iceberg." + schema + ".dws_financial_summary_weekly ORDER BY week_period DESC LIMIT 5")) {
                java.sql.ResultSetMetaData md = rs.getMetaData();
                int colCount = md.getColumnCount();
                System.out.print("Columns: ");
                for (int i = 1; i <= colCount; i++) System.out.print(md.getColumnName(i) + " ");
                System.out.println();
                while (rs.next()) {
                    System.out.printf("  • Week: %s | TxCount: %s | NetExpense: ￥%s\n",
                            rs.getString("week_period"), rs.getLong("tx_count"), rs.getBigDecimal("net_expense"));
                }
            }

            System.out.println("--------------------------------------------------------------------------------");
            System.out.println("📅 [Probing Monthly View in " + schema + "]");
            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM iceberg." + schema + ".dws_financial_summary_monthly ORDER BY stat_month DESC LIMIT 5")) {
                java.sql.ResultSetMetaData md = rs.getMetaData();
                int colCount = md.getColumnCount();
                System.out.print("Columns: ");
                for (int i = 1; i <= colCount; i++) System.out.print(md.getColumnName(i) + " ");
                System.out.println();
                while (rs.next()) {
                    System.out.printf("  • Month: %s | TxCount: %s | NetExpense: ￥%s\n",
                            rs.getString("stat_month"), rs.getLong("tx_count"), rs.getBigDecimal("net_expense_cny"));
                }
            }
            System.out.println("================================================================================");
        } catch (Exception e) {
            LOG.error("❌ 探测失败: {}", e.getMessage(), e);
        }
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
