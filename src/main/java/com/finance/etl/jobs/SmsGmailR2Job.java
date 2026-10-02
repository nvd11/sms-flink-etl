package com.finance.etl.jobs;

import com.finance.etl.model.SmsRecord;
import com.finance.etl.pipeline.SmsGmailR2Pipeline;
import com.finance.etl.sink.iceberg.IcebergR2Sink;
import com.finance.etl.source.imap.ImapSource;
import com.finance.etl.transform.SmsEmailParser;
import com.finance.etl.util.ConfigUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
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
        // 💡 架构注解 (Mode B 零常驻批处理 + Slot 物理对齐)：
        // 并行度从环境配置 FLINK_PARALLELISM 中读取 (默认 2)。此处有一个极易踩中的隐形大坑：
        // 仅调用 env.setParallelism(N) 只会改变 JobGraph 的 subtask 数量，而本地 MiniCluster 的
        // TaskExecutor 默认仅持有 1 个 TaskSlot——同一 vertex 的多个 subtask 又不允许共享同一 slot，
        // 于是 N 个 subtask 会在唯一 slot 上排队接力执行 (实测两个 subtask 先后部署、共用同一 allocation id)，
        // "并行"彻底退化为物理串行，总耗时随时长波动 (17s ~ 31s)。
        // 解药：通过 Configuration 显式将 TaskManagerOptions.NUM_TASK_SLOTS 对齐至 parallelism，
        // 确保每个 subtask 都有专属物理 slot 线程，实现真·多线程并行 IMAP 拉取。
        int parallelism = ConfigUtils.getInt("FLINK_PARALLELISM", 2);
        Configuration flinkConf = new Configuration();
        flinkConf.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // 🎯 核心铁律：显式声明为 BATCH 模式！
        env.setParallelism(parallelism);
        LOG.info("⚡ [Flink Runtime] Configured parallelism: {} slot thread(s) (TaskExecutor slots aligned: {})",
                parallelism, parallelism);

        // 2. 组装实体对象 (FLIP-27 Source + Demo Parser + Iceberg Sink -> Pipeline)
        ImapSource source = ImapSource.fromConfig();
        LOG.info("📧 Configured Gmail IMAP Buffer Account: {}", source.getUser());

        SmsEmailParser parser = new SmsEmailParser();
        IcebergR2Sink sink = IcebergR2Sink.fromConfig();
        SmsGmailR2Pipeline pipeline = new SmsGmailR2Pipeline(source, parser, sink);

        // 3. 编排并挂载数据流 (同时保留 stdout 打印方便日志观测，并挂载 Iceberg 湖仓 Sink 落盘)
        DataStream<SmsRecord> smsStream = pipeline.buildStream(env);
        smsStream.print();

        DataStream<RowData> rowStream = smsStream
                .map(new com.finance.etl.sink.iceberg.SmsRecordToRowDataMapper())
                .name("SmsRecord-To-RowData-Mapper");
        sink.append(rowStream);

        // 4. 提交作业执行 (步骤 1：业务数据写入落盘，阻塞等待全部 TaskSlot 跑完)
        LOG.info("🚀 Submitting sms-gmail-r2 JobGraph to Flink execution runtime...");
        JobExecutionResult executionResult = env.execute("SMS-Gmail-R2-Lakehouse-Batch-Job");

        // 5. 提交水位位点 (步骤 2：作业 100% 成功后，从分布式累加器提取最大 UID 推进水位，严格保障 At-Least-Once)
        Long maxUid = executionResult.getAccumulatorResult(SmsEmailParser.ACCUMULATOR_MAX_UID);
        LOG.info("🌊 [Job Completion] Batch executed successfully. Global MAX(UID) from accumulator: {}", maxUid);
        sink.commitOffset("sms-gmail-r2", "EMAIL_IMAP", source.getUser(), maxUid != null ? maxUid : 0L);

        LOG.info("================================================================================");
        LOG.info("✅ SMS Gmail to R2 Batch Job Execution Finished Successfully!");
        LOG.info("================================================================================");
    }
}
