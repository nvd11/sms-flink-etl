package com.finance.etl.jobs;

import com.finance.etl.model.SmsRecord;
import com.finance.etl.pipeline.SmsGmailR2Pipeline;
import com.finance.etl.source.imap.ImapSource;
import com.finance.etl.transform.SmsRecordParser;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SMS Flink ETL - 金融短信动账批处理入湖流水线主作业 (sms-gmail-r2)
 * 数据链路: 小米15 (SmsForwarder) -> Gmail IMAP -> Flink on NUC -> Cloudflare R2 (Apache Iceberg) -> Trino
 * 极简 main 入口：纯编排驱动器，负责初始化环境、装配实体对象并驱动执行。
 */
public class SmsGmailR2Job {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2Job.class);

    public static void main(String[] args) throws Exception {
        LOG.info("================================================================================");
        LOG.info("🚀 Starting SMS Gmail to Cloudflare R2 Lakehouse ETL Batch Job (sms-gmail-r2)...");
        LOG.info("================================================================================");

        // 1. 初始化 Flink 执行环境并强制锁定为批处理运行模式 (纯批处理，算完即焚)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // 🎯 核心铁律：显式声明为 BATCH 模式！
        // 💡 架构注解 (Mode B 零常驻批处理)：
        // 当前作业由 K3s CronJob 拉起单 Pod 执行 (java -jar)，Flink 运行时底层通过环境嗅探自动降级为内嵌式 MiniCluster。
        // 此处的 parallelism 并非触发拉起多个 K8s Pod，而是在单 Pod JVM 进程内分配 2 个并发 TaskSlot 工作线程，
        // 既充分压榨 NUC 的双核算力并行执行 Map/Sink 转换，又免除跨 Pod 的网络 IPC/Shuffle 开销，算完即焚彻底归还内存。
        env.setParallelism(2);

        // 2. 组装实体对象 (FLIP-27 Source + Parser -> Pipeline)
        ImapSource source = ImapSource.fromConfig();
        LOG.info("📧 Configured Gmail IMAP Buffer Account: {}", source.getUser());

        SmsRecordParser parser = new SmsRecordParser();
        SmsGmailR2Pipeline pipeline = new SmsGmailR2Pipeline(source, parser);

        // 3. 编排并挂载数据流
        DataStream<SmsRecord> smsStream = pipeline.buildStream(env);
        smsStream.print();

        // 4. 提交作业执行
        LOG.info("🚀 Submitting sms-gmail-r2 JobGraph to Flink execution runtime...");
        env.execute("SMS-Gmail-R2-Lakehouse-Batch-Job");

        LOG.info("================================================================================");
        LOG.info("✅ SMS Gmail to R2 Batch Job Execution Finished Successfully!");
        LOG.info("================================================================================");
    }
}
