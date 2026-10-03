package com.finance.etl.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * 批处理作业执行足迹审计模型 (对标 Spring Batch BATCH_JOB_EXECUTION)
 * 对应表: iceberg.finance.etl_job_executions / iceberg.finance_dev.etl_job_executions
 */
public class JobExecutionRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String executionId;
    private String jobName;
    private String status;           // 'SUCCESS', 'FAILED', 'RUNNING'
    private Instant startTime;
    private Instant endTime;
    private Long netRuntimeMs;
    private Long recordsIn;
    private Long recordsOut;
    private Long lastOffset;
    private String workersSummary;   // 各并行 Worker 指标 JSON
    private String errorMessage;
    private Instant createdAt;

    public JobExecutionRecord() {
    }

    public JobExecutionRecord(String executionId, String jobName, String status,
                              Instant startTime, Instant endTime, Long netRuntimeMs,
                              Long recordsIn, Long recordsOut, Long lastOffset,
                              String workersSummary, String errorMessage, Instant createdAt) {
        this.executionId = executionId;
        this.jobName = jobName;
        this.status = status;
        this.startTime = startTime;
        this.endTime = endTime;
        this.netRuntimeMs = netRuntimeMs;
        this.recordsIn = recordsIn;
        this.recordsOut = recordsOut;
        this.lastOffset = lastOffset;
        this.workersSummary = workersSummary;
        this.errorMessage = errorMessage;
        this.createdAt = createdAt;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    public Long getNetRuntimeMs() {
        return netRuntimeMs;
    }

    public void setNetRuntimeMs(Long netRuntimeMs) {
        this.netRuntimeMs = netRuntimeMs;
    }

    public Long getRecordsIn() {
        return recordsIn;
    }

    public void setRecordsIn(Long recordsIn) {
        this.recordsIn = recordsIn;
    }

    public Long getRecordsOut() {
        return recordsOut;
    }

    public void setRecordsOut(Long recordsOut) {
        this.recordsOut = recordsOut;
    }

    public Long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(Long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public String getWorkersSummary() {
        return workersSummary;
    }

    public void setWorkersSummary(String workersSummary) {
        this.workersSummary = workersSummary;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "JobExecutionRecord{" +
                "executionId='" + executionId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", status='" + status + '\'' +
                ", netRuntimeMs=" + netRuntimeMs +
                ", recordsIn=" + recordsIn +
                ", recordsOut=" + recordsOut +
                ", lastOffset=" + lastOffset +
                '}';
    }
}
