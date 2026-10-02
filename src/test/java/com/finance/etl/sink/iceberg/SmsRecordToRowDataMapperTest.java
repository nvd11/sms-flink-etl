package com.finance.etl.sink.iceberg;

import com.finance.etl.model.SmsRecord;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SmsRecordToRowDataMapper 纯函数映射器单元测试")
class SmsRecordToRowDataMapperTest {

    private final SmsRecordToRowDataMapper mapper = new SmsRecordToRowDataMapper();

    @Test
    @DisplayName("测试完整 SmsRecord 映射至 GenericRowData：8个字段类型与值严格对齐")
    void testCompleteRecordMapping() throws Exception {
        Instant receivedAt = Instant.parse("2026-09-29T13:44:19Z");
        Instant createdAt = Instant.parse("2026-10-01T18:24:38Z");

        SmsRecord record = new SmsRecord();
        record.setId(408L);
        record.setMsgUid("3c02cd1ff392d99e00f0ee4bbdfa3a4930597f186753de3c18123e7846da61a9");
        record.setChannel("EMAIL_IMAP");
        record.setSender("CGB");
        record.setReceiverPhone("SIM_SLOT_1");
        record.setReceivedAt(receivedAt);
        record.setRawBody("【广发银行】您尾号3342信用卡29日21:44消费6646.00人民币。");
        record.setCreatedAt(createdAt);

        RowData row = mapper.map(record);

        assertNotNull(row, "转换结果不能为 null");
        assertEquals(8, row.getArity(), "RowData 字段总数必须与 Iceberg 表结构的 8 个字段完全一致");

        // 0: id (BIGINT)
        assertFalse(row.isNullAt(0));
        assertEquals(408L, row.getLong(0));

        // 1: msg_uid (VARCHAR)
        assertFalse(row.isNullAt(1));
        assertEquals("3c02cd1ff392d99e00f0ee4bbdfa3a4930597f186753de3c18123e7846da61a9", row.getString(1).toString());

        // 2: channel (VARCHAR)
        assertFalse(row.isNullAt(2));
        assertEquals("EMAIL_IMAP", row.getString(2).toString());

        // 3: sender (VARCHAR)
        assertFalse(row.isNullAt(3));
        assertEquals("CGB", row.getString(3).toString());

        // 4: receiver_phone (VARCHAR)
        assertFalse(row.isNullAt(4));
        assertEquals("SIM_SLOT_1", row.getString(4).toString());

        // 5: received_at (TIMESTAMP(6) WITH TIME ZONE)
        assertFalse(row.isNullAt(5));
        assertEquals(receivedAt, row.getTimestamp(5, 6).toInstant());

        // 6: raw_body (VARCHAR)
        assertFalse(row.isNullAt(6));
        assertEquals("【广发银行】您尾号3342信用卡29日21:44消费6646.00人民币。", row.getString(6).toString());

        // 7: created_at (TIMESTAMP(6) WITH TIME ZONE)
        assertFalse(row.isNullAt(7));
        assertEquals(createdAt, row.getTimestamp(7, 6).toInstant());
    }

    @Test
    @DisplayName("测试 null 安全性：输入 null 返回 null，字段为 null 保持 isNullAt")
    void testNullSafety() throws Exception {
        // 1. 输入 null 对象
        assertNull(mapper.map(null), "输入 null 应安全返回 null");

        // 2. 字段全为空的空实体
        SmsRecord emptyRecord = new SmsRecord();
        RowData emptyRow = mapper.map(emptyRecord);

        assertNotNull(emptyRow);
        assertEquals(8, emptyRow.getArity());
        for (int i = 0; i < 8; i++) {
            assertTrue(emptyRow.isNullAt(i), "索引 " + i + " 的字段应当安全保持 null");
        }
    }
}
