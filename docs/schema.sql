-- ====================================================================
-- Standard DDL & Query Specification for Apache Iceberg & Trino
-- Storage: Cloudflare R2 (S3-Compatible Bucket: "sms-flink-etl")
-- Query Engine: Trino on NUC (K3s via ArgoCD)
-- Layer: ODS (Operational Data Store) / Raw Message Lakehouse Layer
-- ====================================================================

-- 1. 创建湖仓 Database / Schema (挂载于 R2 存储桶上)
CREATE SCHEMA IF NOT EXISTS iceberg.finance
WITH (location = 's3://sms-flink-etl/iceberg/finance');

-- 2. 纯粹的 ODS 原始报文表 (Apache Iceberg 表格式 · Parquet 列式存储)
-- 严格遵循经典数据湖仓规范与数仓分层，字段全覆盖、零业务派生冗余
CREATE TABLE IF NOT EXISTS iceberg.finance.raw_sms_records (
    id              BIGINT,                              -- 全局自增或业务递增序列 ID
    msg_uid         VARCHAR,                             -- 邮件 Message-ID 或全局唯一消息指纹 (防重业务唯一键)
    channel         VARCHAR,                             -- 采集通道: 'EMAIL_IMAP'
    sender          VARCHAR,                             -- 发送方原始号码: 95508, 106xxxx 等
    receiver_phone  VARCHAR,                             -- 接收短信的本机手机号码 (区分双卡/多卡归属)
    received_at     TIMESTAMP(6) WITH TIME ZONE,         -- 原始短信到达物理时间 (带时区微秒戳)
    raw_body        VARCHAR,                             -- 原始短信全文报文 (100% 原始保真)
    created_at      TIMESTAMP(6) WITH TIME ZONE          -- 本系统入湖落地时间
)
WITH (
    format = 'PARQUET',                                  -- 底层存储格式: Parquet 列存
    partitioning = ARRAY['month(received_at)'],          -- Iceberg 隐藏分区: 按短信到达月份自动分区
    sorted_by = ARRAY['received_at']                     -- 块内排序加速时间切片检索
);

-- ====================================================================
-- 3. Flink SQL Batch Sink 注册示例 (S3A 直连 Cloudflare R2)
-- ====================================================================
-- CREATE CATALOG r2_iceberg WITH (
--     'type'='iceberg',
--     'catalog-type'='hadoop',
--     'warehouse'='s3a://sms-flink-etl/iceberg/warehouse',
--     'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
--     's3.endpoint'='https://8ac25a3a0ac482af1dbd6c65e118693e.r2.cloudflarestorage.com',
--     's3.path-style-access'='true'
-- );

-- ====================================================================
-- 4. Trino 高级查询与金融审计实操参考
-- ====================================================================

-- 4.1 基础检索与排重查询 (基于唯一 msg_uid 获取最新事实)
-- SELECT * FROM iceberg.finance.raw_sms_records
-- WHERE received_at >= CURRENT_DATE - INTERVAL '7' DAY
-- ORDER BY received_at DESC;

-- 4.2 金融审计时间旅行 (Time Travel - 回溯任意历史切片)
-- 调阅截至指定时间点的数据快照：
-- SELECT COUNT(*), MAX(received_at) 
-- FROM iceberg.finance.raw_sms_records 
-- FOR TIMESTAMP AS OF TIMESTAMP '2026-09-25 18:00:00 UTC';

-- 4.3 查看 Iceberg 快照树与元数据历史
-- SELECT snapshot_id, committed_at, operation, summary['total-records'] AS total_records
-- FROM iceberg.finance."raw_sms_records$snapshots"
-- ORDER BY committed_at DESC;

-- 4.4 下游 DWD 明细层消费 (在 Trino 中直接进行正则抽取与视图清洗)
-- CREATE OR REPLACE VIEW iceberg.finance.v_dwd_transactions AS
-- SELECT
--     msg_uid,
--     sender,
--     received_at,
--     -- 针对广发 95508 正则抽取示例
--     regexp_extract(raw_body, '尾号(\d{4})', 1) AS card_last4,
--     CAST(regexp_extract(raw_body, '([0-9]+\.[0-9]{2})元', 1) AS DECIMAL(10,2)) AS amount,
--     raw_body
-- FROM iceberg.finance.raw_sms_records;
