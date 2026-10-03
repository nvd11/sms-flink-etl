package com.finance.etl.repository;

import com.finance.etl.model.JobExecutionRecord;
import com.finance.etl.util.ConfigUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

/**
 * 批处理作业执行足迹审计仓储 (JobExecutionAuditRepository)
 * 职责：
 * 专职负责将 Flink 批处理作业的每次执行状态、耗时、数据量、Worker 运行指标持久化写入
 * iceberg.finance.etl_job_executions / iceberg.finance_dev.etl_job_executions 表。
 */
public class JobExecutionAuditRepository implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionAuditRepository.class);

    private final JdbcCatalog catalog;
    private final String schemaName;
    private final String tableName;

    public JobExecutionAuditRepository(JdbcCatalog catalog, String schemaName, String tableName) {
        this.catalog = Objects.requireNonNull(catalog, "JdbcCatalog must not be null");
        this.schemaName = Objects.requireNonNull(schemaName, "Schema name must not be null");
        this.tableName = Objects.requireNonNull(tableName, "Table name must not be null");
    }

    public static JobExecutionAuditRepository fromConfig() {
        JdbcCatalog catalog = IcebergCatalogFactory.createJdbcCatalog();
        String schemaName = ConfigUtils.get("ICEBERG_CATALOG_SCHEMA", "finance_dev");
        return new JobExecutionAuditRepository(catalog, schemaName, "etl_job_executions");
    }

    public Table loadTable() {
        TableIdentifier tableId = TableIdentifier.of(schemaName, tableName);
        return catalog.loadTable(tableId);
    }

    /**
     * 写入一条执行足迹记录
     */
    public void recordExecution(JobExecutionRecord exec) {
        if (exec == null) {
            return;
        }

        LOG.info("📝 [Job Execution Audit] Recording execution for job='{}', id='{}', status='{}'...",
                exec.getJobName(), exec.getExecutionId(), exec.getStatus());

        try {
            Table table = loadTable();

            GenericRecord record = GenericRecord.create(table.schema());
            record.setField("execution_id", exec.getExecutionId());
            record.setField("job_name", exec.getJobName());
            record.setField("status", exec.getStatus());

            OffsetDateTime startTime = exec.getStartTime() != null ?
                    exec.getStartTime().atOffset(ZoneOffset.UTC) : null;
            OffsetDateTime endTime = exec.getEndTime() != null ?
                    exec.getEndTime().atOffset(ZoneOffset.UTC) : null;
            OffsetDateTime createdAt = exec.getCreatedAt() != null ?
                    exec.getCreatedAt().atOffset(ZoneOffset.UTC) : OffsetDateTime.now(ZoneOffset.UTC);

            record.setField("start_time", startTime);
            record.setField("end_time", endTime);
            record.setField("net_runtime_ms", exec.getNetRuntimeMs());
            record.setField("records_in", exec.getRecordsIn());
            record.setField("records_out", exec.getRecordsOut());
            record.setField("last_offset", exec.getLastOffset());
            record.setField("workers_summary", exec.getWorkersSummary());
            record.setField("error_message", exec.getErrorMessage());
            record.setField("created_at", createdAt);

            String filename = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
            OutputFile out = table.io().newOutputFile(filename);
            DataWriter<Record> writer = Parquet.writeData(out)
                    .forTable(table)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .build();

            try {
                writer.write(record);
            } finally {
                writer.close();
            }

            DataFile dataFile = writer.toDataFile();
            table.newAppend().appendFile(dataFile).commit();

            LOG.info("✅ [Job Execution Audit] Successfully committed execution audit to {}.{} (status: {})",
                    schemaName, tableName, exec.getStatus());
        } catch (Exception e) {
            LOG.error("❌ [Job Execution Audit] Failed to record execution for job {}: {}",
                    exec.getJobName(), e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                LOG.warn("⚠️ Failed to close catalog", e);
            }
        }
    }
}
