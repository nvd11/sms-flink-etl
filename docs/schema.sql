-- ==========================================================
-- Standard DDL for CockroachDB / PostgreSQL (Production Target)
-- Database: "finance-db"
-- ==========================================================

CREATE TABLE IF NOT EXISTS raw_sms_records (
    id BIGSERIAL PRIMARY KEY,
    email_uid VARCHAR(128) NOT NULL UNIQUE,       -- 邮件 Message-ID 或消息唯一指纹 (幂等防重键)
    source_type VARCHAR(32) NOT NULL,            -- 来源类型: SMS 或 APP_NOTIFICATION
    sender VARCHAR(64) NOT NULL,                 -- 发送方号码或包名 (例: 95508, com.tencent.mm)
    received_at TIMESTAMPTZ NOT NULL,            -- 实际动账/通知发生时间
    raw_subject VARCHAR(255),                    -- 原始邮件主题
    raw_content TEXT NOT NULL,                   -- 原始短信/通知全文报文
    sms_category VARCHAR(32) DEFAULT 'EXPENSE',  -- 分类: EXPENSE(消费支出), BILL(出账账单), AUTH_CODE(验证码), NOTICE(通知)
    parsed_amount DECIMAL(12, 2) DEFAULT NULL,   -- 正则提取金额
    parsed_currency VARCHAR(8) DEFAULT 'CNY',    -- 币种 (CNY, USD等)
    parsed_card_no VARCHAR(16) DEFAULT NULL,     -- 银行卡尾号 (例: 3342)
    parsed_merchant VARCHAR(128) DEFAULT NULL,   -- 交易商户/对方账户名称
    extra_metadata JSONB DEFAULT '{}'::jsonb,    -- 扩展非结构化元数据 (JSONB 自由字段)
    created_at TIMESTAMPTZ DEFAULT clock_timestamp() -- 入库落盘时间
);

-- 常用查询索引
CREATE INDEX IF NOT EXISTS idx_raw_sms_received_at ON raw_sms_records (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_sms_sender ON raw_sms_records (sender);
CREATE INDEX IF NOT EXISTS idx_raw_sms_category ON raw_sms_records (sms_category);
CREATE INDEX IF NOT EXISTS idx_raw_sms_card_no ON raw_sms_records (parsed_card_no);

-- ==========================================================
-- Upsert 幂等写入样例 (CockroachDB / PostgreSQL 原生语法)
-- ==========================================================
-- INSERT INTO raw_sms_records (
--     email_uid, source_type, sender, received_at, raw_subject, 
--     raw_content, sms_category, parsed_amount, parsed_card_no, parsed_merchant
-- ) VALUES (
--     'uid_12345', 'SMS', '95508', '2026-09-12 18:00:00+08', '95508',
--     '您尾号3342广发卡消费50.00元...', 'EXPENSE', 50.00, '3342', '何贤纪念医院'
-- ) ON CONFLICT (email_uid) DO UPDATE SET
--     parsed_amount = EXCLUDED.parsed_amount,
--     parsed_merchant = EXCLUDED.parsed_merchant,
--     sms_category = EXCLUDED.sms_category;
