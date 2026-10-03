package com.finance.etl.jobs;

import com.finance.etl.model.FinancialTransaction;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.repository.IcebergOffsetRepository;
import com.finance.etl.sink.iceberg.FinancialTransactionToRowDataMapper;
import com.finance.etl.sink.iceberg.IcebergR2Sink;
import com.finance.etl.source.iceberg.IcebergR2Source;
import com.finance.etl.source.iceberg.RowDataToSmsRecordMapper;
import com.finance.etl.transform.dwd.SmsRecordToDwdTransactionMapper;
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
 * SMS Flink ETL - ODS 到 DWD 动账事实批处理入湖流水线主作业 (sms-ods-to-dwd)
 * 数据链路: ODS (raw_sms_records) -> Flink DWD Pipeline (7 大维度提取器) -> DWD (dwd_financial_transactions)
 * 遵循极简 main 入口设计规范：纯编排驱动器，负责初始化环境、装配实体对象、增量水位探查并驱动批处理执行。
 */
public class SmsOdsToDwdJob {
    private static final Logger LOG = LoggerFactory.getLogger(SmsOdsToDwdJob.class);

    public static final String JOB_NAME = "sms-ods-to-dwd";
    public static final String CHANNEL_ICEBERG_ODS = "ICEBERG_ODS";
    public static final String SOURCE_TARGET_ODS = "raw_sms_records";

    public static void main(String[] args) throws Exception {
        LOG.info("================================================================================");
        LOG.info("🚀 Starting SMS ODS to DWD Lakehouse ETL Batch Job ({})", JOB_NAME);
        LOG.info("================================================================================");

        // 1. 初始化 Flink 执行环境并强制锁定为批处理运行模式 (Mode B: 零常驻批处理，算完即焚)
        int parallelism = ConfigUtils.getInt("FLINK_PARALLELISM", 2);
        Configuration flinkConf = new Configuration();
        flinkConf.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // 🎯 核心铁律：显式声明为 BATCH 模式！
        env.setParallelism(parallelism);
        LOG.info("⚡ [Flink Runtime] Configured parallelism: {} slot thread(s) (TaskExecutor slots aligned: {})",
                parallelism, parallelism);

        // 2. 探查水位：从 etl_sync_offsets 读取上一次成功清洗的 MAX(id)
        long lastOffset;
        try (IcebergOffsetRepository offsetRepo = IcebergOffsetRepository.fromConfig()) {
            lastOffset = offsetRepo.getLatestOffset(JOB_NAME, CHANNEL_ICEBERG_ODS, SOURCE_TARGET_ODS);
        }
        LOG.info("🌊 [DWD Watermark] Current last processed ODS record ID: {}", lastOffset);

        // 3. 读取单批次处理上限 (DWD_MAX_BATCH_SIZE，默认 500 条)
        int maxBatchSize = ConfigUtils.getInt("DWD_MAX_BATCH_SIZE", 500);
        LOG.info("📦 [DWD Batch Limit] Max records per batch: {}", maxBatchSize);

        // 4. 装配读端：从 ODS raw_sms_records 增量读取 (仅读取 lastOffset < id <= lastOffset + maxBatchSize)，并反向还原为 SmsRecord 领域模型
        IcebergR2Source odsSource = IcebergR2Source.incrementalFromId(lastOffset, maxBatchSize);
        LOG.info("🧊 [ODS Source] Reading from table: {}.{} with filter ({} < id <= {})",
                odsSource.getSchemaName(), odsSource.getTableName(), lastOffset, (lastOffset + maxBatchSize));

        DataStream<RowData> odsRowStream = odsSource.buildStream(env);
        DataStream<SmsRecord> smsStream = odsRowStream
                .map(new RowDataToSmsRecordMapper())
                .name("ODS-RowData-To-SmsRecord");

        // 4. 挂载 DWD 业务规整转换算子 (编排 7 大单一职责提取器 + 收集当前批次 MAX(id) 累加器)
        SmsRecordToDwdTransactionMapper dwdMapper = new SmsRecordToDwdTransactionMapper();
        DataStream<FinancialTransaction> txStream = smsStream
                .map(dwdMapper)
                .name("SmsRecord-To-DwdTransaction-Mapper");

        // 打印流方便日志观测与调试
        txStream.print();

        // 5. 装配写端：映射为 16 列 RowData，并挂载写入 dwd_financial_transactions 表
        String dwdTableName = "dwd_financial_transactions";
        IcebergR2Sink dwdSink = IcebergR2Sink.fromConfig(dwdTableName, "id,tx_time");
        LOG.info("🧊 [DWD Sink] Mounting sink for table: {}.{} (parallelism: 1, upsert: true)",
                dwdSink.getSchemaName(), dwdTableName);

        DataStream<RowData> dwdRowStream = txStream
                .map(new FinancialTransactionToRowDataMapper())
                .name("FinancialTransaction-To-RowData-Mapper");

        dwdSink.append(dwdRowStream);

        // 6. 提交作业执行并捕获 Spring Batch 风格的元数据审计足迹
        LOG.info("🚀 Submitting {} JobGraph to Flink execution runtime...", JOB_NAME);
        java.time.Instant startTime = java.time.Instant.now();
        String executionId = "exec_dwd_" + startTime.toEpochMilli();
        JobExecutionResult executionResult = null;
        String status = "RUNNING";
        String errorMsg = null;
        Long maxRecordId = null;

        try {
            executionResult = env.execute("SMS-ODS-To-DWD-Lakehouse-Batch-Job");
            status = "SUCCESS";
            maxRecordId = executionResult.getAccumulatorResult(SmsRecordToDwdTransactionMapper.ACCUMULATOR_MAX_RECORD_ID);
        } catch (Exception e) {
            status = "FAILED";
            errorMsg = e.getMessage();
            throw e;
        } finally {
            java.time.Instant endTime = java.time.Instant.now();
            long runtimeMs = executionResult != null ? executionResult.getNetRuntime() :
                    (endTime.toEpochMilli() - startTime.toEpochMilli());

            // 7. 提交新水位：作业 100% 成功后，推进水位
            if ("SUCCESS".equals(status) && maxRecordId != null && maxRecordId > lastOffset) {
                dwdSink.commitOffset(JOB_NAME, CHANNEL_ICEBERG_ODS, SOURCE_TARGET_ODS, maxRecordId);
                LOG.info("🌊 [Watermark Advanced] Updated {} watermark to {}", JOB_NAME, maxRecordId);
            } else if ("SUCCESS".equals(status)) {
                LOG.info("ℹ️ [Watermark Unchanged] No newer records processed in this batch (maxRecordId={}).", maxRecordId);
            }

            // 8. 持久化记录本次作业的执行足迹 (写入 etl_job_executions 表)
            try (com.finance.etl.repository.JobExecutionAuditRepository auditRepo =
                         com.finance.etl.repository.JobExecutionAuditRepository.fromConfig()) {
                com.finance.etl.model.JobExecutionRecord auditRecord = new com.finance.etl.model.JobExecutionRecord(
                        executionId,
                        JOB_NAME,
                        status,
                        startTime,
                        endTime,
                        runtimeMs,
                        maxRecordId != null && maxRecordId > lastOffset ? (maxRecordId - lastOffset) : 0L,
                        maxRecordId != null && maxRecordId > lastOffset ? (maxRecordId - lastOffset) : 0L,
                        maxRecordId != null && maxRecordId > lastOffset ? maxRecordId : lastOffset,
                        String.format("{\"parallelism\":%d,\"maxBatchSize\":%d}", parallelism, maxBatchSize),
                        errorMsg,
                        java.time.Instant.now()
                );
                auditRepo.recordExecution(auditRecord);
            } catch (Exception ex) {
                LOG.warn("⚠️ Failed to record job execution audit: {}", ex.getMessage());
            }
        }

        LOG.info("================================================================================");
        LOG.info("✅ SMS ODS to DWD Batch Job Execution Finished Successfully in {} ms!",
                executionResult.getNetRuntime());
        LOG.info("================================================================================");
    }
}
