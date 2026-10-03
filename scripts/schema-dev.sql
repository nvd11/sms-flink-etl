-- ====================================================================
-- Development & Test Environment DDL Specification for Apache Iceberg & Trino
-- Schema: iceberg.finance_dev (Dedicated to Local Dev & JUnit Integration Tests)
-- Storage: Cloudflare R2 (Bucket: "sms-flink-etl-dev" / Prefix: "iceberg/finance_dev")
-- ====================================================================

-- 1. 创建本地开发与单测专用独立的 Schema (完全物理隔离生产 finance)
CREATE SCHEMA IF NOT EXISTS iceberg.finance_dev
WITH (location = 's3://sms-flink-etl-dev/iceberg/finance_dev');

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

-- 4. 开发测试用 DWD 金融动账明细事实表
CREATE TABLE IF NOT EXISTS iceberg.finance_dev.dwd_financial_transactions (
    -- 1. 业务主键与血缘追溯 (Lineage)
    id                  BIGINT,                              -- 动账事实全局单调递增 ID (直接对齐 raw_sms_records.id)
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
