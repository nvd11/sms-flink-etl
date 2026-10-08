package com.finance.etl.sink.iceberg;

import com.finance.etl.model.FinancialReportRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/**
 * ADS 智能财务研报事实列式映射器 (ReportRecordToRowDataMapper)
 * 职责：纯函数、无状态。
 * 将领域模型 FinancialReportRecord 转换为 Flink 底层列式内存行 GenericRowData。
 * 15 个字段与 iceberg.finance.ads_financial_reports 表结构严格按索引绝对对齐：
 * 0:  report_id (VARCHAR)
 * 1:  period_type (VARCHAR)
 * 2:  period_value (VARCHAR)
 * 3:  report_date (DATE) -> int (自 1970-01-01 以来的天数)
 * 4:  total_expense (DECIMAL(12, 2))
 * 5:  total_refund (DECIMAL(12, 2))
 * 6:  net_expense (DECIMAL(12, 2))
 * 7:  total_income (DECIMAL(12, 2))
 * 8:  total_transfer (DECIMAL(12, 2))
 * 9:  tx_count (BIGINT)
 * 10: metrics_json (VARCHAR)
 * 11: summary_text (VARCHAR)
 * 12: chart_url (VARCHAR)
 * 13: slack_status (VARCHAR)
 * 14: created_at (TIMESTAMP(6) WITH TIME ZONE)
 */
public class ReportRecordToRowDataMapper implements MapFunction<FinancialReportRecord, RowData> {
    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(FinancialReportRecord record) throws Exception {
        if (record == null) {
            return null;
        }

        GenericRowData row = new GenericRowData(15);

        // 0. report_id (VARCHAR)
        row.setField(0, record.getReportId() != null ? StringData.fromString(record.getReportId()) : null);

        // 1. period_type (VARCHAR)
        row.setField(1, record.getPeriodType() != null ? StringData.fromString(record.getPeriodType()) : null);

        // 2. period_value (VARCHAR)
        row.setField(2, record.getPeriodValue() != null ? StringData.fromString(record.getPeriodValue()) : null);

        // 3. report_date (DATE)
        if (record.getReportDate() != null) {
            row.setField(3, (int) record.getReportDate().toEpochDay());
        } else {
            row.setField(3, null);
        }

        // 4. total_expense (DECIMAL(12, 2))
        row.setField(4, record.getTotalExpense() != null ? DecimalData.fromBigDecimal(record.getTotalExpense(), 12, 2) : null);

        // 5. total_refund (DECIMAL(12, 2))
        row.setField(5, record.getTotalRefund() != null ? DecimalData.fromBigDecimal(record.getTotalRefund(), 12, 2) : null);

        // 6. net_expense (DECIMAL(12, 2))
        row.setField(6, record.getNetExpense() != null ? DecimalData.fromBigDecimal(record.getNetExpense(), 12, 2) : null);

        // 7. total_income (DECIMAL(12, 2))
        row.setField(7, record.getTotalIncome() != null ? DecimalData.fromBigDecimal(record.getTotalIncome(), 12, 2) : null);

        // 8. total_transfer (DECIMAL(12, 2))
        row.setField(8, record.getTotalTransfer() != null ? DecimalData.fromBigDecimal(record.getTotalTransfer(), 12, 2) : null);

        // 9. tx_count (BIGINT)
        row.setField(9, record.getTxCount() != null ? record.getTxCount() : 0L);

        // 10. metrics_json (VARCHAR)
        row.setField(10, record.getMetricsJson() != null ? StringData.fromString(record.getMetricsJson()) : null);

        // 11. summary_text (VARCHAR)
        row.setField(11, record.getSummaryText() != null ? StringData.fromString(record.getSummaryText()) : null);

        // 12. chart_url (VARCHAR)
        row.setField(12, record.getChartUrl() != null ? StringData.fromString(record.getChartUrl()) : null);

        // 13. slack_status (VARCHAR)
        row.setField(13, record.getSlackStatus() != null ? StringData.fromString(record.getSlackStatus()) : null);

        // 14. created_at (TIMESTAMP(6) WITH TIME ZONE)
        row.setField(14, record.getCreatedAt() != null ? TimestampData.fromInstant(record.getCreatedAt()) : TimestampData.fromInstant(java.time.Instant.now()));

        return row;
    }
}
