package com.finance.etl.source.iceberg;

import com.finance.etl.model.SmsRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RowDataToSmsRecordMapper 列式行向领域实体映射单元测试")
class RowDataToSmsRecordMapperTest {

    @Test
    @DisplayName("测试 RowData 到 SmsRecord 8 字段索引严格还原")
    void testMapAllFields() throws Exception {
        RowDataToSmsRecordMapper mapper = new RowDataToSmsRecordMapper();

        Instant now = Instant.now();
        GenericRowData row = new GenericRowData(8);
        row.setField(0, 441L);
        row.setField(1, StringData.fromString("uid-441"));
        row.setField(2, StringData.fromString("EMAIL_IMAP"));
        row.setField(3, StringData.fromString("CGB"));
        row.setField(4, StringData.fromString("SIM_1"));
        row.setField(5, TimestampData.fromInstant(now));
        row.setField(6, StringData.fromString("【广发银行】消费9.95元"));
        row.setField(7, TimestampData.fromInstant(now));

        SmsRecord record = mapper.map(row);

        assertNotNull(record);
        assertEquals(441L, record.getId());
        assertEquals("uid-441", record.getMsgUid());
        assertEquals("EMAIL_IMAP", record.getChannel());
        assertEquals("CGB", record.getSender());
        assertEquals("SIM_1", record.getReceiverPhone());
        assertEquals(now.toEpochMilli(), record.getReceivedAt().toEpochMilli());
        assertEquals("【广发银行】消费9.95元", record.getRawBody());
        assertEquals(now.toEpochMilli(), record.getCreatedAt().toEpochMilli());
    }

    @Test
    @DisplayName("测试 null 输入与空字段防御")
    void testNullAndEmptyRow() throws Exception {
        RowDataToSmsRecordMapper mapper = new RowDataToSmsRecordMapper();
        assertNull(mapper.map(null));

        GenericRowData emptyRow = new GenericRowData(8);
        SmsRecord record = mapper.map(emptyRow);
        assertNotNull(record);
        assertNull(record.getId());
        assertNull(record.getMsgUid());
        assertNull(record.getReceivedAt());
    }
}
