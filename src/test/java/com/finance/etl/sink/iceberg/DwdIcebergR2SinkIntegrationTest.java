package com.finance.etl.sink.iceberg;

import com.finance.etl.model.FinancialTransaction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DWD IcebergR2Sink 集成拓扑构建测试")
class DwdIcebergR2SinkIntegrationTest {

    @Test
    @DisplayName("测试 DWD 数据流挂载到 IcebergR2Sink：验证 Flink 拓扑图生成成功")
    void testAppendDwdStreamBuildsTopology() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Instant now = Instant.now();
        FinancialTransaction tx = new FinancialTransaction(
                441L,
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

        // 1. 创建源流
        DataStream<FinancialTransaction> txStream = env.fromData(List.of(tx));

        // 2. 映射为 RowData
        DataStream<RowData> rowStream = txStream.map(new FinancialTransactionToRowDataMapper());

        // 3. 构建 Sink 并挂载
        IcebergR2Sink sink = IcebergR2Sink.fromConfig("dwd_financial_transactions", "id,tx_time");
        DataStreamSink<Void> dataStreamSink = sink.append(rowStream);

        assertNotNull(dataStreamSink);
        assertEquals("IcebergSink finance.finance_dev.dwd_financial_transactions", dataStreamSink.getTransformation().getName());
    }
}
