-- ====================================================================
-- DWD Financial Transactions Lakehouse Fact Table DDL Specification
-- Target Schemas: 
--   • Production:  iceberg.finance.dwd_financial_transactions
--                  Location: s3://sms-flink-etl/iceberg/finance/dwd_financial_transactions
--   • Development: iceberg.finance_dev.dwd_financial_transactions
--                  Location: s3://sms-flink-etl-dev/iceberg/finance_dev/dwd_financial_transactions
-- Engine: Trino / Apache Iceberg / Flink 1.19
-- ====================================================================

-- 1. 生产环境 DWD 动账事实表
CREATE TABLE IF NOT EXISTS iceberg.finance.dwd_financial_transactions (
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

-- 2. 开发环境 DWD 动账事实表
CREATE TABLE IF NOT EXISTS iceberg.finance_dev.dwd_financial_transactions (
    id                  BIGINT,
    raw_record_id       BIGINT,
    tx_time             TIMESTAMP(6) WITH TIME ZONE,
    amount              DECIMAL(12, 2),
    currency            VARCHAR,
    direction           VARCHAR,
    tx_type             VARCHAR,
    institution         VARCHAR,
    account_type        VARCHAR,
    card_tail           VARCHAR,
    payment_channel     VARCHAR,
    counterparty        VARCHAR,
    cleaned_merchant    VARCHAR,
    category            VARCHAR,
    is_valid_tx         BOOLEAN,
    etl_created_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(tx_time)'],
    sorted_by = ARRAY['tx_time DESC']
);
