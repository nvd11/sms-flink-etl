package com.finance.etl.sink.iceberg;

import com.finance.etl.model.FinancialTransaction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FinancialTransactionToRowDataMapper 列式内存行对齐映射单元测试")
class FinancialTransactionToRowDataMapperTest {

    @Test
    @DisplayName("测试 FinancialTransaction 到 GenericRowData 16 列严格索引对齐")
    void testMapAllFields() throws Exception {
        FinancialTransactionToRowDataMapper mapper = new FinancialTransactionToRowDataMapper();

        Instant now = Instant.now();
        FinancialTransaction tx = new FinancialTransaction(
                "tx-20261003-001",
                441L,
                now,
                new BigDecimal("9.95"),
                "CNY",
                "OUTFLOW",
                "EXPENSE",
                "CGB",
                "CREDIT_CARD",
                "3342",
                "ALIPAY",
                "支付宝-高德打车",
                "高德打车",
                "TRANSPORT",
                true,
                now
        );

        RowData row = mapper.map(tx);

        assertNotNull(row);
        assertEquals(16, row.getArity(), "DWD 行列数必须严格等于 16 列");

        // 验证索引与类型 (0 号字段为 id)
        assertEquals("tx-20261003-001", row.getString(0).toString());
        assertEquals(441L, row.getLong(1));
        assertEquals(now.toEpochMilli(), row.getTimestamp(2, 6).getMillisecond());
        assertEquals(new BigDecimal("9.95"), row.getDecimal(3, 12, 2).toBigDecimal());
        assertEquals("CNY", row.getString(4).toString());
        assertEquals("OUTFLOW", row.getString(5).toString());
        assertEquals("EXPENSE", row.getString(6).toString());
        assertEquals("CGB", row.getString(7).toString());
        assertEquals("CREDIT_CARD", row.getString(8).toString());
        assertEquals("3342", row.getString(9).toString());
        assertEquals("ALIPAY", row.getString(10).toString());
        assertEquals("支付宝-高德打车", row.getString(11).toString());
        assertEquals("高德打车", row.getString(12).toString());
        assertEquals("TRANSPORT", row.getString(13).toString());
        assertTrue(row.getBoolean(14));
        assertEquals(now.toEpochMilli(), row.getTimestamp(15, 6).getMillisecond());
    }

    @Test
    @DisplayName("测试 null 对象输入安全返回 null")
    void testNullInput() throws Exception {
        FinancialTransactionToRowDataMapper mapper = new FinancialTransactionToRowDataMapper();
        assertNull(mapper.map(null));
    }
}
