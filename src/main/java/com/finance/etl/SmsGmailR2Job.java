package com.finance.etl;

import com.finance.etl.util.ConfigUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;

/**
 * SMS Flink ETL - 金融短信动账批处理入湖流水线主作业 (sms-gmail-r2)
 * 数据链路: 小米15 (SmsForwarder) -> Gmail IMAP -> Flink on NUC -> Cloudflare R2 (Apache Iceberg) -> Trino
 */
public class SmsGmailR2Job {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2Job.class);

    public static void main(String[] args) throws Exception {
        LOG.info("================================================================================");
        LOG.info("🚀 Starting SMS Gmail to Cloudflare R2 Lakehouse ETL Batch Job (sms-gmail-r2)...");
        LOG.info("================================================================================");

        String user = ConfigUtils.get("GMAIL_IMAP_USER", "alice.h.y.he@gmail.com");
        LOG.info("📧 Configured Gmail IMAP Buffer Account: {}", user);

        // 1. 初始化 Flink 批处理流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 匹配 NUC TaskManager 的 2 个并行 Slots

        // 2. 模拟短信动账业务数据流 (初始 Hello World 冒烟骨架)
        DataStream<String> mockSmsStream = env.fromCollection(Arrays.asList(
            "[SMS-ODS-INIT] Hello Boss Jason! This is the official sms-gmail-r2 financial pipeline.",
            "[SMS-ODS-INIT] Target Storage: Cloudflare R2 (Iceberg table: iceberg.finance.raw_sms_records)",
            "[SMS-ODS-INIT] Source Buffer: Gmail (" + user + ") via IMAP SSL short-polling.",
            "[SMS-ODS-INIT] Timestamp: " + Instant.now()
        ));

        // 3. 数据转换与打印输出
        mockSmsStream
            .map(msg -> {
                String processed = "[Nova-Worker-Slot] [sms-gmail-r2] " + msg;
                LOG.info(processed);
                return processed;
            })
            .print();

        // 4. 提交作业执行
        LOG.info("🚀 Submitting sms-gmail-r2 JobGraph to Flink execution runtime...");
        env.execute("SMS-Gmail-R2-Lakehouse-Batch-Job");

        LOG.info("================================================================================");
        LOG.info("✅ SMS Gmail to R2 Batch Job Execution Finished Successfully!");
        LOG.info("================================================================================");
    }
}
