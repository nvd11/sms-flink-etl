package com.finance.etl;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.transform.SmsEmailParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SMS Flink ETL - 本地 Flink 流计算流水线与 SmsEmailParser 算子集成测试 (JUnit 5)
 */
public class SmsPipelineTest {

    @Test
    @DisplayName("测试: 验证 Flink 本地 MiniCluster 流水线与 SmsEmailParser 算子端到端数据规整闭环")
    public void testFlinkLocalPipelineExecution() throws Exception {
        // 1. 在本地内存创建轻量 Flink MiniCluster 环境 (无需依赖外部集群)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 模拟由 IMAP 短连接拉取到的原始测试邮件流
        List<RawEmail> rawEmails = List.of(
            new RawEmail(101L, "<cgb-101@gmail.com>", "106980095508", "alice.h.y.he@gmail.com", "alice.h.y.he@gmail.com", Instant.now(), Instant.now(), "【广发银行】您尾号8888信用卡消费人民币128.50元，商户：山姆会员商店。SubId：1", "INBOX"),
            new RawEmail(102L, "<wx-102@gmail.com>", "com.tencent.mm", "alice.h.y.he@gmail.com", "alice.h.y.he@gmail.com", Instant.now(), Instant.now(), "【微信支付】微信零钱已向某某便利店成功付款8.00元。SubId：1", "INBOX"),
            new RawEmail(103L, "<ali-103@gmail.com>", "com.eg.android.AlipayGphone", "alice.h.y.he@gmail.com", "alice.h.y.he@gmail.com", Instant.now(), Instant.now(), "【支付宝】花呗自动扣款通知：扣款成功299.00元。SubId：2", "INBOX")
        );

        DataStream<RawEmail> inputStream = env.fromData(rawEmails);

        // 3. 接入正式生产级动账解析算子 SmsEmailParser
        DataStream<SmsRecord> recordStream = inputStream.flatMap(new SmsEmailParser());

        // 4. 将结果收集至本地内存列表进行严格断言
        List<SmsRecord> results = Collections.synchronizedList(new ArrayList<>());
        recordStream.executeAndCollect().forEachRemaining(results::add);

        // 5. 断言流计算结果
        assertEquals(3, results.size(), "经过 Flink 批处理算子规整后的记录数应为 3");

        for (SmsRecord record : results) {
            assertNotNull(record.getMsgUid(), "流计算输出每条记录都必须带有防重 UID");
            assertEquals(64, record.getMsgUid().length(), "防重 UID 必须为 64 位 SHA-256 哈希值");
            assertNotNull(record.getSender(), "流计算输出每条记录都必须有发送方分类");
            assertNotNull(record.getReceivedAt(), "流计算输出每条记录都必须有微秒时间戳");
            assertNotNull(record.getReceiverPhone(), "流计算输出每条记录都必须有卡槽标识");
            assertTrue(record.getRawBody().length() > 10, "原始报文文本长度合理");
        }
    }
}
