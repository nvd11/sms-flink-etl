package com.finance.etl.jobs;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;

/**
 * SMS Flink ETL - 冒烟测试与探活 Hello World 批流作业 (包: com.finance.etl.jobs)
 */
public class HelloWorldJob {
    private static final Logger LOG = LoggerFactory.getLogger(HelloWorldJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("==========================================================");
        LOG.info("🚀 Starting SMS Flink ETL - Hello World Verification Job");
        LOG.info("==========================================================");

        // 1. 初始化 Flink 流执行环境并强制锁定为批处理运行模式
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // 🎯 显式切换为批处理运行模式

        // 💡 架构注解 (Mode B 零常驻批处理)：
        // 当前作业由 K3s CronJob 拉起单 Pod 执行 (java -jar)，Flink 运行时底层通过环境嗅探自动降级为内嵌式 MiniCluster。
        // 此处的 parallelism 并非触发拉起多个 K8s Pod，而是在单 Pod JVM 进程内分配 2 个并发 TaskSlot 工作线程。
        env.setParallelism(2);

        // 3. 构建模拟动账探活测试数据源
        DataStream<String> sampleStream = env.fromData(Arrays.asList(
            "Hello Boss Jason! Flink 1.19 is running gracefully on NUC Nova (10.0.1.113)!",
            "Cindy reporting: Financial Lakehouse ODS pipeline is ready to rumble.",
            "Verified Stack: AWS Scheduler -> GitHub Actions -> NUC Flink -> Cloudflare R2 Iceberg -> Trino.",
            "Heartbeat Pulse Timestamp: " + Instant.now()
        ));

        // 4. 数据转换与计算工人执行日志
        sampleStream
            .map(item -> {
                String result = "[Nova-Worker-Slot] Processed message: " + item;
                LOG.info(result);
                return result;
            })
            .print();

        // 5. 触发作业提交与执行
        LOG.info("🚀 Submitting SMS Flink ETL JobGraph to Cluster Dispatcher...");
        env.execute("SMS-Flink-ETL-HelloWorld-Verification");

        LOG.info("==========================================================");
        LOG.info("✅ SMS Flink ETL - JobGraph Dispatched & Accepted by Cluster! (Workers processing in parallel)");
        LOG.info("==========================================================");
    }
}
