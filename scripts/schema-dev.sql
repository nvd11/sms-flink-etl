-- ====================================================================
-- Development & Test Environment DDL Specification for Apache Iceberg & Trino
-- Schema: iceberg.finance_dev (Dedicated to Local Dev & JUnit Integration Tests)
-- Storage: Cloudflare R2 (Bucket: "sms-flink-etl" / Prefix: "iceberg/finance_dev")
-- ====================================================================

-- 1. 创建本地开发与单测专用独立的 Schema (完全隔离生产 finance)
CREATE SCHEMA IF NOT EXISTS iceberg.finance_dev
WITH (location = 's3://sms-flink-etl/iceberg/finance_dev');

-- 2. 开发测试用 ODS 原始报文资产表
CREATE TABLE IF NOT EXISTS iceberg.finance_dev.raw_sms_records (
    id              BIGINT,                              -- 全局递增序列 ID
    msg_uid         VARCHAR,                             -- RFC 2822 Message-ID 或全局唯一消息指纹 (防重业务唯一键)
    channel         VARCHAR,                             -- 采集通道: 'EMAIL_IMAP', 'SMS_DIRECT', 'WEBHOOK'
    sender          VARCHAR,                             -- 机构/渠道大写代号: CGB, CMB, BOC, HSBC, WECHAT_PAY, ALIPAY, OTHER
    receiver_phone  VARCHAR,                             -- 接收短信的本机手机号码 / 卡槽标识 (SIM_SLOT_1, SIM_SLOT_2)
    received_at     TIMESTAMP(6) WITH TIME ZONE,         -- 原始短信到达物理时间 (带时区微秒戳)
    raw_body        VARCHAR,                             -- 原始短信全文报文 (100% 原始保真)
    created_at      TIMESTAMP(6) WITH TIME ZONE          -- 本系统入湖落地时间
)
WITH (
    format = 'PARQUET',                                  -- 底层存储格式: Parquet 列存
    partitioning = ARRAY['month(received_at)'],          -- Iceberg 隐藏分区: 按短信到达月份自动分区
    sorted_by = ARRAY['received_at']                     -- 块内排序加速时间切片检索
);

-- 3. 开发测试用专职 ETL 管道同步水位元数据表
CREATE TABLE IF NOT EXISTS iceberg.finance_dev.etl_sync_offsets (
    job_name        VARCHAR,                             -- 作业唯一标识，例如: 'sms-gmail-r2'
    channel         VARCHAR,                             -- 采集通道，例如: 'EMAIL_IMAP'
    source_target   VARCHAR,                             -- 采集目标标识，例如: 'alice.h.y.he@gmail.com'
    last_offset     BIGINT,                              -- 增量水位游标 (对于 IMAP 而言即最后已成功同步的 UID)
    last_event_time TIMESTAMP(6) WITH TIME ZONE,         -- 该批次最后一条数据的业务时间戳
    updated_at      TIMESTAMP(6) WITH TIME ZONE          -- 本次元数据位点更新入湖时间
)
WITH (
    format = 'PARQUET'
);
