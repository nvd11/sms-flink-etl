package com.finance.etl.sink.iceberg;

import com.finance.etl.model.FinancialTransaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/**
 * DWD 动账事实列式映射器 (FinancialTransactionToRowDataMapper)
 * 职责：纯函数、无状态。
 * 将领域模型 FinancialTransaction 转换为 Flink 底层列式内存行 GenericRowData。
 * 16 个字段与 iceberg.finance.dwd_financial_transactions 表结构严格按索引绝对对齐：
 * 0:  id (BIGINT)
 * 1:  raw_record_id (BIGINT)
 * 2:  tx_time (TIMESTAMP(6) WITH TIME ZONE) -> 触发 Iceberg month(tx_time) 隐藏分区
 * 3:  amount (DECIMAL(12, 2))
 * 4:  currency (VARCHAR)
 * 5:  direction (VARCHAR)
 * 6:  tx_type (VARCHAR)
 * 7:  institution (VARCHAR)
 * 8:  account_type (VARCHAR)
 * 9:  card_tail (VARCHAR)
 * 10: payment_channel (VARCHAR)
 * 11: counterparty (VARCHAR)
 * 12: cleaned_merchant (VARCHAR)
 * 13: category (VARCHAR)
 * 14: is_valid_tx (BOOLEAN)
 * 15: etl_created_at (TIMESTAMP(6) WITH TIME ZONE)
 */
public class FinancialTransactionToRowDataMapper implements MapFunction<FinancialTransaction, RowData> {
    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(FinancialTransaction tx) throws Exception {
        if (tx == null) {
            return null;
        }

        GenericRowData row = new GenericRowData(16);

        // 0. id (BIGINT)
        row.setField(0, tx.getId());

        // 1. raw_record_id (BIGINT)
        row.setField(1, tx.getRawRecordId());

        // 2. tx_time (TIMESTAMP(6) WITH TIME ZONE)
        row.setField(2, tx.getTxTime() != null ? TimestampData.fromInstant(tx.getTxTime()) : null);

        // 3. amount (DECIMAL(12, 2))
        row.setField(3, tx.getAmount() != null ? DecimalData.fromBigDecimal(tx.getAmount(), 12, 2) : null);

        // 4. currency (VARCHAR)
        row.setField(4, tx.getCurrency() != null ? StringData.fromString(tx.getCurrency()) : null);

        // 5. direction (VARCHAR)
        row.setField(5, tx.getDirection() != null ? StringData.fromString(tx.getDirection()) : null);

        // 6. tx_type (VARCHAR)
        row.setField(6, tx.getTxType() != null ? StringData.fromString(tx.getTxType()) : null);

        // 7. institution (VARCHAR)
        row.setField(7, tx.getInstitution() != null ? StringData.fromString(tx.getInstitution()) : null);

        // 8. account_type (VARCHAR)
        row.setField(8, tx.getAccountType() != null ? StringData.fromString(tx.getAccountType()) : null);

        // 9. card_tail (VARCHAR)
        row.setField(9, tx.getCardTail() != null ? StringData.fromString(tx.getCardTail()) : null);

        // 10. payment_channel (VARCHAR)
        row.setField(10, tx.getPaymentChannel() != null ? StringData.fromString(tx.getPaymentChannel()) : null);

        // 11. counterparty (VARCHAR)
        row.setField(11, tx.getCounterparty() != null ? StringData.fromString(tx.getCounterparty()) : null);

        // 12. cleaned_merchant (VARCHAR)
        row.setField(12, tx.getCleanedMerchant() != null ? StringData.fromString(tx.getCleanedMerchant()) : null);

        // 13. category (VARCHAR)
        row.setField(13, tx.getCategory() != null ? StringData.fromString(tx.getCategory()) : null);

        // 14. is_valid_tx (BOOLEAN)
        row.setField(14, tx.getIsValidTx());

        // 15. etl_created_at (TIMESTAMP(6) WITH TIME ZONE)
        row.setField(15, tx.getEtlCreatedAt() != null ? TimestampData.fromInstant(tx.getEtlCreatedAt()) : null);

        return row;
    }
}
