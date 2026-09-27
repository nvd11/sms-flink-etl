package com.finance.etl.pipeline;

import com.finance.etl.model.SmsRecord;
import com.finance.etl.reader.GmailImapReader;
import com.finance.etl.transform.SmsRecordParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 pipeline 包下的 SmsGmailR2Pipeline 计算拓扑装配与心跳机制单元测试
 */
public class SmsGmailR2PipelineTest {

    @Test
    @DisplayName("测试 SmsGmailR2Pipeline: 验证空邮件时能够自动产生符合规范的兜底心跳记录")
    public void testHeartbeatRecordCreation() {
        SmsRecordParser parser = new SmsRecordParser();
        GmailImapReader reader = new GmailImapReader("imap.gmail.com", 993, "test@gmail.com", "", null, 7890, 10);
        SmsGmailR2Pipeline pipeline = new SmsGmailR2Pipeline(reader, parser);

        SmsRecord heartbeat = pipeline.createHeartbeatRecord();
        assertNotNull(heartbeat);
        assertEquals("SYSTEM_HEARTBEAT", heartbeat.getSender());
        assertEquals("EMAIL_IMAP", heartbeat.getChannel());
        assertTrue(heartbeat.getMsgUid().startsWith("HEARTBEAT_"));
        assertNotNull(heartbeat.getReceivedAt());
    }

    @Test
    @DisplayName("测试 SmsGmailR2Pipeline.buildStream(): 验证流拓扑生成与 Flink MiniCluster 批处理流转")
    public void testPipelineBuildStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SmsRecordParser parser = new SmsRecordParser();
        GmailImapReader reader = new GmailImapReader("imap.gmail.com", 993, "test@gmail.com", "", null, 7890, 10);
        SmsGmailR2Pipeline pipeline = new SmsGmailR2Pipeline(reader, parser);

        DataStream<SmsRecord> stream = pipeline.buildStream(env);
        assertNotNull(stream, "构建出的 DataStream 不应为空");

        List<SmsRecord> collected = Collections.synchronizedList(new ArrayList<>());
        stream.executeAndCollect().forEachRemaining(collected::add);

        assertFalse(collected.isEmpty(), "Pipeline 应至少产生一条记录（真实邮件或心跳兜底）");
    }
}
