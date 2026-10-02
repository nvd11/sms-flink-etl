-- ====================================================================
-- Standard DDL & Query Specification for Apache Iceberg & Trino
-- Storage: Cloudflare R2 (S3-Compatible Bucket: "sms-flink-etl")
-- Query Engine: Trino on NUC (K3s via ArgoCD)
-- Layer: ODS (Operational Data Store) & ETL Metadata Layer
-- ====================================================================

-- 1. 创建湖仓 Database / Schema (挂载于 R2 存储桶上)
CREATE SCHEMA IF NOT EXISTS iceberg.finance
WITH (location = 's3://sms-flink-etl/iceberg/finance');

-- 2. 纯粹的 ODS 原始报文业务资产表 (保持纯正领域模型，杜绝协议层概念污染)
CREATE TABLE IF NOT EXISTS iceberg.finance.raw_sms_records (
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

-- 3. 专职 ETL 管道同步水位元数据表 (关注点分离: 专用于记录批处理断点续传 offset)
CREATE TABLE IF NOT EXISTS iceberg.finance.etl_sync_offsets (
    job_name        VARCHAR,                             -- 作业唯一标识，例如: 'sms-gmail-r2', 'video-cleanup'
    channel         VARCHAR,                             -- 采集通道，例如: 'EMAIL_IMAP', 'KAFKA', 'FILE'
    source_target   VARCHAR,                             -- 采集目标标识，例如: 'alice.h.y.he@gmail.com'
    last_offset     BIGINT,                              -- 🎯 增量水位游标 (对于 IMAP 而言即最后已成功同步的 UID)
    last_event_time TIMESTAMP(6) WITH TIME ZONE,         -- 该批次最后一条数据的业务时间戳
    updated_at      TIMESTAMP(6) WITH TIME ZONE          -- 本次元数据位点更新入湖时间
)
WITH (
    format = 'PARQUET'
);

-- 4. DWD 金融动账明细事实表 (Data Warehouse Detail · 纯粹金融领域明细资产)
CREATE TABLE IF NOT EXISTS iceberg.finance.dwd_financial_transactions (
    -- 1. 业务主键与血缘追溯 (Lineage)
    tx_id               VARCHAR,                             -- 动账唯一流水号 (如 'tx_317')
    raw_record_id       BIGINT,                              -- 🎯 唯一血缘外键 (关联 raw_sms_records.id)

    -- 2. 时间维度 (Time Dimension · 权威交易时间)
    tx_time             TIMESTAMP(6) WITH TIME ZONE,         -- 真实动账发生的物理时间 (带时区微秒戳)

    -- 3. 金额与财务维度 (Financial Metrics)
    amount              DECIMAL(12, 2),                      -- 交易金额 (高精度数字，严格杜绝浮点失真)
    currency            VARCHAR,                             -- 币种标准三字码: 'CNY', 'HKD', 'USD' (默认 CNY)
    direction           VARCHAR,                             -- 资金方向: 'OUTFLOW' (支出), 'INFLOW' (收入)
    tx_type             VARCHAR,                             -- 交易明细类型: 'EXPENSE' (消费), 'INCOME' (收入), 'TRANSFER' (转账), 'REFUND' (退款)

    -- 4. 账户与渠道维度 (Account & Institution)
    institution         VARCHAR,                             -- 金融机构代号: 'CGB', 'BOC', 'HSBC', 'ICBC', 'WECHAT_PAY', 'ALIPAY'
    account_type        VARCHAR,                             -- 账户类型: 'CREDIT_CARD', 'DEBIT_CARD', 'WALLET', 'LOAN'
    card_tail           VARCHAR,                             -- 卡号/账户尾号: 如 '3342'
    payment_channel     VARCHAR,                             -- 支付通路: 'ALIPAY', 'WECHAT_PAY', 'UNIONPAY', 'DIRECT'

    -- 5. 对手方与消费场景维度 (Merchant & Categorization)
    counterparty        VARCHAR,                             -- 交易对手/商户原名 (如: '财付通-煲珠公收款')
    cleaned_merchant    VARCHAR,                             -- 智能提取纯净商户名 (如: '煲珠公')
    category            VARCHAR,                             -- 消费大类: 'FOOD', 'TRANSPORT', 'SHOPPING', 'MEDICAL', 'OTHER'

    -- 6. 治理与审计元数据 (Auditing)
    is_valid_tx         BOOLEAN,                             -- 是否为有效动账 (区分真实交易与验证码/营销提醒)
    etl_created_at      TIMESTAMP(6) WITH TIME ZONE          -- 清洗入湖时间戳
)
WITH (
    format = 'PARQUET',                                      -- 底层列式存储: Parquet 列存
    partitioning = ARRAY['month(tx_time)'],                  -- 🎯 依托 Iceberg 隐藏分区，原生支撑极速范围剪枝
    sorted_by = ARRAY['tx_time DESC']                        -- 块内按交易时间倒序排列
);

-- ====================================================================
-- 5. Trino 高级查询与湖仓运维实操参考
-- ====================================================================

-- 4.1 批处理增量水位探查 (查询当前数据源的最新同步位点)
-- SELECT COALESCE(MAX(last_offset), 0) AS last_offset 
-- FROM iceberg.finance.etl_sync_offsets 
-- WHERE job_name = 'sms-gmail-r2' AND channel = 'EMAIL_IMAP';

-- 4.2 基础检索与排重查询 (基于唯一 msg_uid 获取最新动账事实)
-- SELECT * FROM iceberg.finance.raw_sms_records
-- WHERE received_at >= CURRENT_DATE - INTERVAL '7' DAY
-- ORDER BY received_at DESC;

-- 4.3 金融审计时间旅行 (Time Travel - 回溯任意历史切片)
-- 调阅截至指定时间点的数据快照：
-- SELECT COUNT(*), MAX(received_at) 
-- FROM iceberg.finance.raw_sms_records 
-- FOR TIMESTAMP AS OF TIMESTAMP '2026-09-25 18:00:00 UTC';

-- 4.4 查看 Iceberg 快照树与元数据历史
-- SELECT snapshot_id, committed_at, operation, summary['total-records'] AS total_records
-- FROM iceberg.finance."raw_sms_records$snapshots"
-- ORDER BY committed_at DESC;

-- 4.5 下游 DWD 明细层消费 (在 Trino 中直接进行正则抽取与视图清洗)
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
