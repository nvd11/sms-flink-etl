package com.finance.etl.sink.iceberg;

import com.finance.etl.model.SmsRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/**
 * 湖仓字段映射器 (SmsRecordToRowDataMapper)
 * 职责：纯函数、无状态。
 * 将领域实体 SmsRecord 转换为 Flink 底层列式内存行 GenericRowData。
 * 8 个字段与 iceberg.finance_dev.raw_sms_records 表结构严格按索引绝对对齐：
 * 0: id (BIGINT)
 * 1: msg_uid (VARCHAR)
 * 2: channel (VARCHAR)
 * 3: sender (VARCHAR)
 * 4: receiver_phone (VARCHAR)
 * 5: received_at (TIMESTAMP(6) WITH TIME ZONE)
 * 6: raw_body (VARCHAR)
 * 7: created_at (TIMESTAMP(6) WITH TIME ZONE)
 */
public class SmsRecordToRowDataMapper implements MapFunction<SmsRecord, RowData> {
    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(SmsRecord record) throws Exception {
        if (record == null) {
            return null;
        }

        GenericRowData row = new GenericRowData(8);

        // 0. id (BIGINT)
        row.setField(0, record.getId());

        // 1. msg_uid (VARCHAR)
        row.setField(1, record.getMsgUid() != null ? StringData.fromString(record.getMsgUid()) : null);

        // 2. channel (VARCHAR)
        row.setField(2, record.getChannel() != null ? StringData.fromString(record.getChannel()) : null);

        // 3. sender (VARCHAR)
        row.setField(3, record.getSender() != null ? StringData.fromString(record.getSender()) : null);

        // 4. receiver_phone (VARCHAR)
        row.setField(4, record.getReceiverPhone() != null ? StringData.fromString(record.getReceiverPhone()) : null);

        // 5. received_at (TIMESTAMP(6) WITH TIME ZONE) -> 触发 Iceberg month(received_at) 隐藏分区
        row.setField(5, record.getReceivedAt() != null ? TimestampData.fromInstant(record.getReceivedAt()) : null);

        // 6. raw_body (VARCHAR)
        row.setField(6, record.getRawBody() != null ? StringData.fromString(record.getRawBody()) : null);

        // 7. created_at (TIMESTAMP(6) WITH TIME ZONE)
        row.setField(7, record.getCreatedAt() != null ? TimestampData.fromInstant(record.getCreatedAt()) : null);

        return row;
    }
}
