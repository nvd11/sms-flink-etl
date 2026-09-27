package com.finance.etl;

import com.finance.etl.model.SmsRecord;
import com.finance.etl.transform.RawRecordFormatter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SMS Flink ETL - 本地极速单元测试与算子集成测试 (JUnit 5)
 */
public class SmsPipelineTest {

    @Test
    @DisplayName("测试 1: 验证 RawRecordFormatter 对动账报文的基础字段映射与指纹生成")
    public void testRawRecordFormatterMapping() throws Exception {
        RawRecordFormatter formatter = new RawRecordFormatter();

        // 模拟广发信用卡 95508 消费动账短信
        String cgbSms = "【广发银行】您尾号1234信用卡于09月26日18:30在星巴克消费人民币38.00元。";
        SmsRecord cgbRecord = formatter.map(cgbSms);

        assertNotNull(cgbRecord, "规整后的实体不应为空");
        assertEquals("95508", cgbRecord.getSender(), "发送方应识别为广发 95508");
        assertEquals("EMAIL_IMAP", cgbRecord.getChannel(), "采集通道应为 EMAIL_IMAP");
        assertNotNull(cgbRecord.getMsgUid(), "必须生成 SHA-256 唯一指纹键");
        assertEquals(64, cgbRecord.getMsgUid().length(), "SHA-256 16进制摘要长度应为 64");
        assertEquals(cgbSms, cgbRecord.getRawBody(), "原始报文必须 100% 原始保真");
        assertNotNull(cgbRecord.getReceivedAt(), "到达物理时间戳不可为空");

        // 模拟微信支付凭证
        String wechatMsg = "【微信支付】财付通扣款凭证：扫码向商户付款人民币15.50元。";
        SmsRecord wechatRecord = formatter.map(wechatMsg);
        assertEquals("WECHAT_PAY", wechatRecord.getSender(), "发送方应识别为微信支付");

        // 模拟支付宝账单通知
        String alipayMsg = "【支付宝】蚂蚁金服提醒：您有一笔生活缴费支出45.00元。";
        SmsRecord alipayRecord = formatter.map(alipayMsg);
        assertEquals("ALIPAY", alipayRecord.getSender(), "发送方应识别为支付宝");

        // 验证幂等指纹一致性 (相同报文产生相同指纹)
        String fingerprint1 = RawRecordFormatter.generateMessageFingerprint(cgbSms);
        String fingerprint2 = RawRecordFormatter.generateMessageFingerprint(cgbSms);
        assertEquals(fingerprint1, fingerprint2, "相同内容多次生成指纹必须严格一致");
    }

    @Test
    @DisplayName("测试 2: 验证 Flink 本地 MiniCluster 流计算流水线端到端运行与算子闭环")
    public void testFlinkLocalPipelineExecution() throws Exception {
        // 1. 在本地内存创建轻量 Flink MiniCluster 环境 (无需依赖外部集群)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 模拟由 IMAP 短连接拉取到的原始测试邮件流
        List<String> rawMessages = Arrays.asList(
            "【广发银行】您尾号8888信用卡消费人民币128.50元，商户：山姆会员商店。",
            "【微信支付】微信零钱已向某某便利店成功付款8.00元。",
            "【支付宝】花呗自动扣款通知：扣款成功299.00元。"
        );

        DataStream<String> inputStream = env.fromData(rawMessages);

        // 3. 接入真正的业务规整算子
        DataStream<SmsRecord> recordStream = inputStream.map(new RawRecordFormatter());

        // 4. 将结果收集至本地内存列表进行严格断言
        List<SmsRecord> results = Collections.synchronizedList(new ArrayList<>());
        recordStream.executeAndCollect().forEachRemaining(results::add);

        // 5. 断言流计算结果
        assertEquals(3, results.size(), "经过 Flink 批处理算子规整后的记录数应为 3");

        for (SmsRecord record : results) {
            assertNotNull(record.getMsgUid(), "流计算输出每条记录都必须带有防重 UID");
            assertNotNull(record.getSender(), "流计算输出每条记录都必须有发送方分类");
            assertNotNull(record.getReceivedAt(), "流计算输出每条记录都必须有微秒时间戳");
            assertTrue(record.getRawBody().length() > 10, "原始报文文本长度合理");
        }
    }
}
