package com.finance.etl.pipeline;

import com.finance.etl.model.SmsRecord;
import com.finance.etl.source.imap.ImapSource;
import com.finance.etl.transform.SmsEmailParser;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 针对 pipeline 包下的 SmsGmailR2Pipeline 计算拓扑装配与 FLIP-27 流转单元测试
 */
public class SmsGmailR2PipelineTest {

    @Test
    @DisplayName("测试 SmsGmailR2Pipeline.buildStream(): 验证流拓扑生成与 Flink MiniCluster 批处理流转")
    public void testPipelineBuildStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        SmsEmailParser parser = new SmsEmailParser();
        ImapSource source = ImapSource.builder()
                .host("imap.gmail.com")
                .port(993)
                .user("test@gmail.com")
                .password("") // 空密码安全探活
                .build();
        SmsGmailR2Pipeline pipeline = new SmsGmailR2Pipeline(source, parser);

        DataStream<SmsRecord> stream = pipeline.buildStream(env);
        assertNotNull(stream, "构建出的 DataStream 不应为空");

        List<SmsRecord> collected = Collections.synchronizedList(new ArrayList<>());
        stream.executeAndCollect().forEachRemaining(collected::add);

        assertNotNull(collected);
    }
}
