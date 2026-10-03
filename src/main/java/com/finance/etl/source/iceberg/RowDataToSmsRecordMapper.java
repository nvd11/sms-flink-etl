package com.finance.etl.source.iceberg;

import com.finance.etl.model.SmsRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.RowData;

import java.time.Instant;

/**
 * 列式内存行反向映射器 (RowDataToSmsRecordMapper)
 * 职责：纯函数、无状态。
 * 将 IcebergSource<RowData> 读出的 ODS 原始行，转换为领域实体 SmsRecord。
 * 8 个字段严格与 iceberg.finance.raw_sms_records 表结构按物理索引绝对对齐：
 * 0: id (BIGINT)
 * 1: msg_uid (VARCHAR)
 * 2: channel (VARCHAR)
 * 3: sender (VARCHAR)
 * 4: receiver_phone (VARCHAR)
 * 5: received_at (TIMESTAMP(6) WITH TIME ZONE)
 * 6: raw_body (VARCHAR)
 * 7: created_at (TIMESTAMP(6) WITH TIME ZONE)
 */
public class RowDataToSmsRecordMapper implements MapFunction<RowData, SmsRecord> {
    private static final long serialVersionUID = 1L;

    @Override
    public SmsRecord map(RowData row) throws Exception {
        if (row == null) {
            return null;
        }

        Long id = row.isNullAt(0) ? null : row.getLong(0);
        String msgUid = row.isNullAt(1) ? null : row.getString(1).toString();
        String channel = row.isNullAt(2) ? null : row.getString(2).toString();
        String sender = row.isNullAt(3) ? null : row.getString(3).toString();
        String receiverPhone = row.isNullAt(4) ? null : row.getString(4).toString();

        Instant receivedAt = null;
        if (!row.isNullAt(5)) {
            receivedAt = row.getTimestamp(5, 6).toInstant();
        }

        String rawBody = row.isNullAt(6) ? null : row.getString(6).toString();

        Instant createdAt = null;
        if (!row.isNullAt(7)) {
            createdAt = row.getTimestamp(7, 6).toInstant();
        }

        return new SmsRecord(id, msgUid, channel, sender, receiverPhone, receivedAt, rawBody, createdAt);
    }
}
