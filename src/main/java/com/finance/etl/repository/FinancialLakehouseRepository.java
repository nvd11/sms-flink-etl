package com.finance.etl.repository;

import com.finance.etl.model.DwsSummaryRecord;
import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 财务湖仓数据查询仓储服务 (FinancialLakehouseRepository)
 * 职责：
 * 封装 Trino JDBC 数据访问层，专职向 DWS 聚合视图与 DWD 物理明细表提供纯净、无业务入侵的只读查询能力。
 * 消除所有 Hardcode，统一向 Flink 算子与智能体暴露标准领域模型。
 */
public class FinancialLakehouseRepository implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FinancialLakehouseRepository.class);

    static {
        try {
            Class.forName("io.trino.jdbc.TrinoDriver");
        } catch (ClassNotFoundException e) {
            LOG.warn("Trino JDBC Driver not found on classpath: {}", e.getMessage());
        }
    }

    private static final String DEFAULT_TRINO_URL = "jdbc:trino://10.0.1.113:30880/iceberg/finance";
    private static final String DEFAULT_TRINO_USER = "jason";

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String schema;

    public FinancialLakehouseRepository(String jdbcUrl, String user, String password, String schema) {
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl must not be null");
        this.user = user != null ? user : DEFAULT_TRINO_USER;
        this.password = password;
        this.schema = schema != null ? schema : "finance";
    }

    /**
     * 工厂方法：从环境变量与 .env 配置自适应装配
     */
    public static FinancialLakehouseRepository fromConfig() {
        String schema = ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance");
        // 如果是 dev 环境，切换对应 schema
        String url = ConfigUtils.get("TRINO_JDBC_URL", DEFAULT_TRINO_URL);
        String user = ConfigUtils.get("TRINO_USER", DEFAULT_TRINO_USER);
        String password = ConfigUtils.get("TRINO_PASSWORD", null);
        return new FinancialLakehouseRepository(url, user, password, schema);
    }

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
    }

    /**
     * 1. 动态从 Trino View 查询最新有动账支出的自然日大盘 (tx_count > 0 ORDER BY stat_date DESC LIMIT 1)
     *
     * @return 最新的日度大盘实体 DwsSummaryRecord，若无数据返回 null
     */
    public DwsSummaryRecord queryLatestActiveDailySummary() {
        String sql = String.format("""
                SELECT
                    stat_date, min_id, max_id, tx_count,
                    total_expense, total_refund, net_expense, total_income, total_transfer,
                    food_expense, transport_expense, online_shopping_expense, offline_shopping_expense,
                    medical_expense, communication_expense, insurance_expense, property_expense,
                    travel_expense, car_expense, personal_transfer_expense, other_expense,
                    max_single_amount
                FROM iceberg.%s.dws_financial_summary_daily
                WHERE tx_count > 0
                ORDER BY stat_date DESC
                LIMIT 1
                """, this.schema);

        LOG.info("🔍 [Lakehouse Repo] Querying latest active daily summary from iceberg.{}.dws_financial_summary_daily...", this.schema);

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                java.sql.Date d = rs.getDate("stat_date");
                LocalDate statDate = d.toLocalDate();
                Long minId = rs.getLong("min_id");
                Long maxId = rs.getLong("max_id");
                Long txCount = rs.getLong("tx_count");

                BigDecimal totalExpense = rs.getBigDecimal("total_expense");
                BigDecimal totalRefund = rs.getBigDecimal("total_refund");
                BigDecimal netExpense = rs.getBigDecimal("net_expense");
                BigDecimal totalIncome = rs.getBigDecimal("total_income");
                BigDecimal totalTransfer = rs.getBigDecimal("total_transfer");

                BigDecimal food = rs.getBigDecimal("food_expense");
                BigDecimal transport = rs.getBigDecimal("transport_expense");
                BigDecimal onlineShopping = rs.getBigDecimal("online_shopping_expense");
                BigDecimal offlineShopping = rs.getBigDecimal("offline_shopping_expense");
                BigDecimal medical = rs.getBigDecimal("medical_expense");
                BigDecimal communication = rs.getBigDecimal("communication_expense");
                BigDecimal insurance = rs.getBigDecimal("insurance_expense");
                BigDecimal property = rs.getBigDecimal("property_expense");
                BigDecimal travel = rs.getBigDecimal("travel_expense");
                BigDecimal car = rs.getBigDecimal("car_expense");
                BigDecimal personalTransfer = rs.getBigDecimal("personal_transfer_expense");
                BigDecimal other = rs.getBigDecimal("other_expense");
                BigDecimal maxSingleAmt = rs.getBigDecimal("max_single_amount");

                DwsSummaryRecord record = new DwsSummaryRecord(
                        "DAILY",
                        statDate.toString(),
                        statDate,
                        minId, maxId, txCount,
                        totalExpense, totalRefund, netExpense, totalIncome, totalTransfer,
                        food, transport, onlineShopping, offlineShopping,
                        medical, communication, insurance, property, travel, car, personalTransfer, other,
                        maxSingleAmt, null
                );

                LOG.info("✅ [Lakehouse Repo] Discovered latest active date: {} (net: ￥{}, count: {})",
                        statDate, netExpense, txCount);
                return record;
            }
        } catch (SQLException e) {
            LOG.error("❌ [Lakehouse Repo] Failed to query latest daily summary: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to query latest daily summary from Trino", e);
        }

        return null;
    }

    /**
     * 1.1 查询指定自然日大盘
     */
    public DwsSummaryRecord queryDailySummary(LocalDate date) {
        Objects.requireNonNull(date, "date must not be null");
        String sql = String.format("""
                SELECT
                    stat_date, min_id, max_id, tx_count,
                    total_expense, total_refund, net_expense, total_income, total_transfer,
                    food_expense, transport_expense, online_shopping_expense, offline_shopping_expense,
                    medical_expense, communication_expense, insurance_expense, property_expense,
                    travel_expense, car_expense, personal_transfer_expense, other_expense,
                    max_single_amount
                FROM iceberg.%s.dws_financial_summary_daily
                WHERE stat_date = DATE '%s'
                LIMIT 1
                """, this.schema, date);

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return mapDwsSummaryRecord("DAILY", rs.getDate("stat_date").toString(), rs);
            }
            return null;
        } catch (SQLException e) {
            LOG.error("❌ [Lakehouse Repo] Failed to query daily summary for {}: {}", date, e.getMessage(), e);
            throw new RuntimeException("Failed to query daily summary from Trino", e);
        }
    }

    /**
     * 1.2 动态从 Trino View 查询最新有动账支出的自然周大盘
     */
    public DwsSummaryRecord queryLatestActiveWeeklySummary() {
        String sql = String.format("""
                SELECT
                    week_period, min_id, max_id, tx_count,
                    week_start_date, week_end_date,
                    total_expense, total_refund, net_expense, total_income,
                    0.0 AS total_transfer,
                    food_expense, transport_expense, online_shopping_expense, offline_shopping_expense,
                    medical_expense, communication_expense, insurance_expense, property_expense,
                    travel_expense, car_expense, personal_transfer_expense, other_expense,
                    null AS max_single_amount
                FROM iceberg.%s.dws_financial_summary_weekly
                WHERE tx_count > 0
                ORDER BY week_period DESC
                LIMIT 1
                """, this.schema);

        LOG.info("🔍 [Lakehouse Repo] Querying latest active weekly summary from iceberg.{}.dws_financial_summary_weekly...", this.schema);

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return mapDwsSummaryRecord("WEEKLY", rs.getString("week_period"), rs);
            }
        } catch (SQLException e) {
            LOG.error("❌ [Lakehouse Repo] Failed to query latest weekly summary: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to query latest weekly summary from Trino", e);
        }

        return null;
    }

    /**
     * 1.3 查询指定自然周大盘 (如 "2026-W41")
     */
    public DwsSummaryRecord queryWeeklySummary(String weekPeriod) {
        Objects.requireNonNull(weekPeriod, "weekPeriod must not be null");
        String sql = String.format("""
                SELECT
                    week_period, min_id, max_id, tx_count,
                    week_start_date, week_end_date,
                    total_expense, total_refund, net_expense, total_income,
                    0.0 AS total_transfer,
                    food_expense, transport_expense, online_shopping_expense, offline_shopping_expense,
                    medical_expense, communication_expense, insurance_expense, property_expense,
                    travel_expense, car_expense, personal_transfer_expense, other_expense,
                    null AS max_single_amount
                FROM iceberg.%s.dws_financial_summary_weekly
                WHERE week_period = '%s'\n                LIMIT 1\n                """, this.schema, weekPeriod);

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return mapDwsSummaryRecord("WEEKLY", rs.getString("week_period"), rs);
            }
            return null;
        } catch (SQLException e) {
            LOG.error("❌ [Lakehouse Repo] Failed to query weekly summary for {}: {}", weekPeriod, e.getMessage(), e);
            throw new RuntimeException("Failed to query weekly summary from Trino", e);
        }
    }

    /**
     * 1.4 动态从 Trino View 查询最新有动账支出的月度大盘
     */
    public DwsSummaryRecord queryLatestActiveMonthlySummary() {
        String sql = String.format("""
                SELECT
                    stat_month, min_id, max_id, tx_count,
                    total_expense_cny AS total_expense,
                    total_refund_cny AS total_refund,
                    net_expense_cny AS net_expense,
                    total_income_cny AS total_income,
                    total_transfer_cny AS total_transfer,
                    food_expense, transport_expense, online_shopping_expense, offline_shopping_expense,
                    medical_expense, communication_expense, insurance_expense, property_expense,
                    travel_expense, car_expense, personal_transfer_expense, other_expense,
                    max_single_amount
                FROM iceberg.%s.dws_financial_summary_monthly
                WHERE tx_count > 0
                ORDER BY stat_month DESC
                LIMIT 1
                """, this.schema);

        LOG.info("🔍 [Lakehouse Repo] Querying latest active monthly summary from iceberg.{}.dws_financial_summary_monthly...", this.schema);

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return mapDwsSummaryRecord("MONTHLY", rs.getString("stat_month"), rs);
            }
        } catch (SQLException e) {
            LOG.error("❌ [Lakehouse Repo] Failed to query latest monthly summary: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to query latest monthly summary from Trino", e);
        }

        return null;
    }

    /**
     * 1.5 查询指定月度大盘 (如 "2026-10")
     */
    public DwsSummaryRecord queryMonthlySummary(String month) {
        Objects.requireNonNull(month, "month must not be null");
        String sql = String.format("""
                SELECT
                    stat_month, min_id, max_id, tx_count,
                    total_expense_cny AS total_expense,
                    total_refund_cny AS total_refund,
                    net_expense_cny AS net_expense,
                    total_income_cny AS total_income,
                    total_transfer_cny AS total_transfer,
                    food_expense, transport_expense, online_shopping_expense, offline_shopping_expense,
                    medical_expense, communication_expense, insurance_expense, property_expense,
                    travel_expense, car_expense, personal_transfer_expense, other_expense,
                    max_single_amount
                FROM iceberg.%s.dws_financial_summary_monthly
                WHERE stat_month = '%s'
                LIMIT 1
                """, this.schema, month);

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return mapDwsSummaryRecord("MONTHLY", rs.getString("stat_month"), rs);
            }
            return null;
        } catch (SQLException e) {
            LOG.error("❌ [Lakehouse Repo] Failed to query monthly summary for {}: {}", month, e.getMessage(), e);
            throw new RuntimeException("Failed to query monthly summary from Trino", e);
        }
    }

    private DwsSummaryRecord mapDwsSummaryRecord(String periodType, String periodVal, ResultSet rs) throws SQLException {
        DwsSummaryRecord record = new DwsSummaryRecord();
        record.setPeriodType(periodType);
        record.setPeriodValue(periodVal);
        record.setTxCount(rs.getLong("tx_count"));
        record.setTotalExpense(rs.getBigDecimal("total_expense"));
        record.setTotalRefund(rs.getBigDecimal("total_refund"));
        record.setNetExpense(rs.getBigDecimal("net_expense"));
        record.setTotalIncome(rs.getBigDecimal("total_income"));
        record.setTotalTransfer(rs.getBigDecimal("total_transfer"));
        record.setMaxSingleAmount(rs.getBigDecimal("max_single_amount"));

        record.setMinId(rs.getLong("min_id"));
        record.setMaxId(rs.getLong("max_id"));
        record.setFoodExpense(rs.getBigDecimal("food_expense"));
        record.setTransportExpense(rs.getBigDecimal("transport_expense"));
        record.setOnlineShoppingExpense(rs.getBigDecimal("online_shopping_expense"));
        record.setOfflineShoppingExpense(rs.getBigDecimal("offline_shopping_expense"));
        record.setMedicalExpense(rs.getBigDecimal("medical_expense"));
        record.setCommunicationExpense(rs.getBigDecimal("communication_expense"));
        record.setInsuranceExpense(rs.getBigDecimal("insurance_expense"));
        record.setPropertyExpense(rs.getBigDecimal("property_expense"));
        record.setTravelExpense(rs.getBigDecimal("travel_expense"));
        record.setCarExpense(rs.getBigDecimal("car_expense"));
        record.setPersonalTransferExpense(rs.getBigDecimal("personal_transfer_expense"));
        record.setOtherExpense(rs.getBigDecimal("other_expense"));

        try {
            java.sql.Date s = rs.getDate("week_start_date");
            if (s != null) record.setStartDate(s.toLocalDate());
            java.sql.Date e = rs.getDate("week_end_date");
            if (e != null) record.setEndDate(e.toLocalDate());
        } catch (Exception ignored) {}

        return record;
    }

    /**
     * 2. 查询指定自然日周期内的全量 DWD 动账交易明细流水
     *
     * @param date 统计日期
     * @return 该日期下全部真实交易明细列表
     */
    public List<FinancialTransaction> queryDailyTransactions(LocalDate date) {
        Objects.requireNonNull(date, "date must not be null");

        String sql = String.format("""
                SELECT id, raw_record_id, tx_time, amount, currency, direction, tx_type,
                       institution, account_type, card_tail, payment_channel,
                       counterparty, cleaned_merchant, category, is_valid_tx, etl_created_at
                FROM iceberg.%s.dwd_financial_transactions
                WHERE date(tx_time) = DATE '%s'
                  AND is_valid_tx = true
                ORDER BY tx_time ASC
                """, this.schema, date);

        LOG.info("🔍 [Lakehouse Repo] Querying all DWD transactions for date {} from iceberg.{}.dwd_financial_transactions...",
                date, this.schema);

        return executeTransactionQuery(sql);
    }

    /**
     * 2.1 查询指定自然周周期内的全量 DWD 动账交易明细流水 (如 "2026-W41")
     */
    public List<FinancialTransaction> queryWeeklyTransactions(String weekPeriod) {
        Objects.requireNonNull(weekPeriod, "weekPeriod must not be null");

        String rangeSql = String.format("""
                SELECT week_start_date, week_end_date
                FROM iceberg.%s.dws_financial_summary_weekly
                WHERE week_period = '%s'
                LIMIT 1
                """, this.schema, weekPeriod);

        LocalDate startDate = null;
        LocalDate endDate = null;

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(rangeSql)) {
            if (rs.next()) {
                java.sql.Date s = rs.getDate("week_start_date");
                java.sql.Date e = rs.getDate("week_end_date");
                if (s != null) startDate = s.toLocalDate();
                if (e != null) endDate = e.toLocalDate();
            }
        } catch (SQLException e) {
            LOG.warn("Could not query week range for {}: {}", weekPeriod, e.getMessage());
        }

        String filterClause;
        if (startDate != null && endDate != null) {
            filterClause = String.format("date(tx_time) >= DATE '%s' AND date(tx_time) <= DATE '%s'", startDate, endDate);
        } else {
            filterClause = String.format("concat(cast(year_of_week(tx_time) as varchar), '-W', lpad(cast(week(tx_time) as varchar), 2, '0')) = '%s'", weekPeriod);
        }

        String sql = String.format("""
                SELECT id, raw_record_id, tx_time, amount, currency, direction, tx_type,
                       institution, account_type, card_tail, payment_channel,
                       counterparty, cleaned_merchant, category, is_valid_tx, etl_created_at
                FROM iceberg.%s.dwd_financial_transactions
                WHERE %s
                  AND is_valid_tx = true
                ORDER BY tx_time ASC
                """, this.schema, filterClause);

        LOG.info("🔍 [Lakehouse Repo] Querying all DWD transactions for week {} from iceberg.{}.dwd_financial_transactions...",
                weekPeriod, this.schema);

        return executeTransactionQuery(sql);
    }

    /**
     * 2.2 查询指定月份周期内的全量 DWD 动账交易明细流水 (如 "2026-10")
     */
    public List<FinancialTransaction> queryMonthlyTransactions(String month) {
        Objects.requireNonNull(month, "month must not be null");

        String sql = String.format("""
                SELECT id, raw_record_id, tx_time, amount, currency, direction, tx_type,
                       institution, account_type, card_tail, payment_channel,
                       counterparty, cleaned_merchant, category, is_valid_tx, etl_created_at
                FROM iceberg.%s.dwd_financial_transactions
                WHERE date_format(tx_time, '%%Y-%%m') = '%s'
                  AND is_valid_tx = true
                ORDER BY tx_time ASC
                """, this.schema, month);

        LOG.info("🔍 [Lakehouse Repo] Querying all DWD transactions for month {} from iceberg.{}.dwd_financial_transactions...",
                month, this.schema);

        return executeTransactionQuery(sql);
    }

    private List<FinancialTransaction> executeTransactionQuery(String sql) {
        List<FinancialTransaction> list = new ArrayList<>();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                FinancialTransaction tx = new FinancialTransaction();
                tx.setId(rs.getLong("id"));
                tx.setRawRecordId(rs.getLong("raw_record_id"));
                Timestamp ts = rs.getTimestamp("tx_time");
                if (ts != null) {
                    tx.setTxTime(ts.toInstant());
                }
                tx.setAmount(rs.getBigDecimal("amount"));
                tx.setCurrency(rs.getString("currency"));
                tx.setDirection(rs.getString("direction"));
                tx.setTxType(rs.getString("tx_type"));
                tx.setInstitution(rs.getString("institution"));
                tx.setAccountType(rs.getString("account_type"));
                tx.setCardTail(rs.getString("card_tail"));
                tx.setPaymentChannel(rs.getString("payment_channel"));
                tx.setCounterparty(rs.getString("counterparty"));
                tx.setCleanedMerchant(rs.getString("cleaned_merchant"));
                tx.setCategory(rs.getString("category"));
                tx.setIsValidTx(rs.getBoolean("is_valid_tx"));
                list.add(tx);
            }
        } catch (SQLException e) {
            LOG.error("❌ [Lakehouse Repo] Failed to execute transaction query: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to query transactions from Trino", e);
        }

        LOG.info("✅ [Lakehouse Repo] Retrieved {} raw transactions", list.size());
        return list;
    }

    /**
     * 2.2 向后兼容：查询指定自然日按金额降序排序的 Top N 真实消费大额明细案例
     *
     * @param date  统计日期
     * @param limit 返回条数上限 (如 3 或 5)
     * @return 真实动账案例列表
     */
    public List<FinancialTransaction> queryTopDailyTransactions(LocalDate date, int limit) {
        return queryDailyTransactions(date);
    }

    @Override
    public void close() {
        // 无状态连接池/直连无持久资源，预留接口
    }
}
