-- ==========================================================
-- Standard DDL for CockroachDB / PostgreSQL (Production Target)
-- Database: "finance-db"
-- Layer: ODS (Operational Data Store) / Raw Message Layer
-- ==========================================================

-- 纯粹的 ODS 原始报文表 (严格遵循数仓分层与范式设计，不包含任何业务派生/解析字段)
CREATE TABLE IF NOT EXISTS raw_sms_records (
    id              BIGSERIAL PRIMARY KEY,
    msg_uid         VARCHAR(128) NOT NULL UNIQUE,      -- 邮件 Message-ID 或全局唯一消息指纹 (幂等防重主键)
    source_type     VARCHAR(32) NOT NULL,             -- 消息形态: SMS 或 APP_NOTIFICATION
    channel         VARCHAR(32) NOT NULL DEFAULT 'EMAIL_IMAP', -- 采集通道
    sender          VARCHAR(64) NOT NULL,             -- 发送方原始标识: 95508, com.tencent.mm, com.eg.android.AlipayGphone
    receiver_phone  VARCHAR(32),                      -- 接收短信的本机手机号码 (区分双卡/多卡归属)
    received_at     TIMESTAMPTZ NOT NULL,             -- 原始邮件/通知到达物理时间
    raw_subject     VARCHAR(255),                     -- 原始邮件主题
    raw_body        TEXT NOT NULL,                    -- 原始短信/通知全文报文 (100% 原始保真)
    extra_metadata  JSONB DEFAULT '{}'::jsonb,        -- 原始非结构化扩展元数据 (如原始 Headers, SIM 卡槽, 通知副标题等)
    created_at      TIMESTAMPTZ DEFAULT clock_timestamp() -- 本系统落地入库时间
);

-- 检索与审计索引 (纯物理维度索引)
CREATE INDEX IF NOT EXISTS idx_raw_sms_received_at ON raw_sms_records (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_sms_sender ON raw_sms_records (sender);
CREATE INDEX IF NOT EXISTS idx_raw_sms_receiver_phone ON raw_sms_records (receiver_phone);
CREATE INDEX IF NOT EXISTS idx_raw_sms_source_type ON raw_sms_records (source_type);

-- ==========================================================
-- 幂等写入标准语法 (CockroachDB / PostgreSQL 原生语法)
-- 当 msg_uid 冲突时，不做任何修改直接跳过，保证 Raw 数据不可变性 (Append-Only)
-- ==========================================================
-- INSERT INTO raw_sms_records (
--     msg_uid, source_type, channel, sender, receiver_phone, 
--     received_at, raw_subject, raw_body, extra_metadata
-- ) VALUES (
--     'uid_12345', 'SMS', 'EMAIL_IMAP', '95508', '18520521962',
--     '2026-09-12 18:00:00+08', '95508',
--     '您尾号3342广发卡消费人民币50.00元...', '{"sim_slot": 1}'::jsonb
-- ) ON CONFLICT (msg_uid) DO NOTHING;
