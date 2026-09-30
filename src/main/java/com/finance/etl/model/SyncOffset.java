package com.finance.etl.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * ETL 管道断点续传水位状态模型 (对齐 iceberg.finance.etl_sync_offsets 表结构)
 * 职责：专用于持久化各数据源通道的物理消费 offset，解耦业务实体与调度元数据。
 */
public class SyncOffset implements Serializable {
    private static final long serialVersionUID = 1L;

    private String jobName;         // 作业名称，例如 'sms-gmail-r2'
    private String channel;         // 通道类型，例如 'EMAIL_IMAP'
    private String sourceTarget;    // 目标标识，例如 'alice.h.y.he@gmail.com'
    private Long lastOffset;        // 增量水位游标 (IMAP 下为最后已读 UID)
    private Instant lastEventTime;  // 批次内最后一条数据的到达时间
    private Instant updatedAt;      // 本条元数据写入时间

    public SyncOffset() {
    }

    public SyncOffset(String jobName, String channel, String sourceTarget,
                      Long lastOffset, Instant lastEventTime, Instant updatedAt) {
        this.jobName = jobName;
        this.channel = channel;
        this.sourceTarget = sourceTarget;
        this.lastOffset = lastOffset;
        this.lastEventTime = lastEventTime;
        this.updatedAt = updatedAt;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getSourceTarget() {
        return sourceTarget;
    }

    public void setSourceTarget(String sourceTarget) {
        this.sourceTarget = sourceTarget;
    }

    public Long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(Long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public Instant getLastEventTime() {
        return lastEventTime;
    }

    public void setLastEventTime(Instant lastEventTime) {
        this.lastEventTime = lastEventTime;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return "SyncOffset{" +
                "jobName='" + jobName + '\'' +
                ", channel='" + channel + '\'' +
                ", sourceTarget='" + sourceTarget + '\'' +
                ", lastOffset=" + lastOffset +
                ", lastEventTime=" + lastEventTime +
                ", updatedAt=" + updatedAt +
                '}';
    }
}
