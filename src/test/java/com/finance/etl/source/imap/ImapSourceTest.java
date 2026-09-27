package com.finance.etl.source.imap;

import com.finance.etl.model.RawEmail;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 ImapSource 顶层连接器及其在 Flink MiniCluster 中运行的集成测试
 */
public class ImapSourceTest {

    @Test
    @DisplayName("测试 ImapSource 基础属性规范 (Boundedness, Serializer)")
    public void testImapSourceAttributes() {
        ImapSource source = ImapSource.builder()
                .host("imap.gmail.com")
                .port(993)
                .user("test@gmail.com")
                .password("")
                .build();

        assertEquals(Boundedness.BOUNDED, source.getBoundedness(), "批处理连接器必须显式声明为 BOUNDED");
        assertNotNull(source.getSplitSerializer(), "必须提供工单序列化器");
        assertNotNull(source.getEnumeratorCheckpointSerializer(), "必须提供检查点序列化器");
    }

    @Test
    @DisplayName("测试 ImapSource 接入 Flink: 验证 env.fromSource(...) 本地 MiniCluster 端到端闭环执行")
    public void testImapSourceInFlinkMiniCluster() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        ImapSource source = ImapSource.builder()
                .host("imap.gmail.com")
                .port(993)
                .user("test@gmail.com")
                .password("") // 空密码触发安全探活工单
                .build();

        DataStream<RawEmail> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Gmail-IMAP-FLIP27-Test-Source"
        );

        assertNotNull(stream, "fromSource 产出的 DataStream 不应为空");

        List<RawEmail> result = new ArrayList<>();
        assertDoesNotThrow(() -> {
            stream.executeAndCollect().forEachRemaining(result::add);
        }, "Flink 消费自定义 FLIP-27 Source 时不应抛出任何异常");
    }
}
