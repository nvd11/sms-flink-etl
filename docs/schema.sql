-- MySQL DDL for raw_sms_records
CREATE TABLE IF NOT EXISTS raw_sms_records (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    email_uid VARCHAR(128) NOT NULL UNIQUE COMMENT '邮件Message-ID或消息唯一指纹(幂等防重)',
    source_type VARCHAR(32) NOT NULL COMMENT 'SMS 或 APP_NOTIFICATION',
    sender VARCHAR(64) NOT NULL COMMENT '发送方号码或包名(95508, com.tencent.mm)',
    received_at DATETIME NOT NULL COMMENT '实际动账交易时间',
    raw_subject VARCHAR(255) COMMENT '原始邮件主题',
    raw_content TEXT NOT NULL COMMENT '原始全文报文',
    sms_category VARCHAR(32) DEFAULT 'EXPENSE' COMMENT '分类: EXPENSE, BILL, AUTH_CODE, NOTICE',
    parsed_amount DECIMAL(10, 2) DEFAULT NULL COMMENT '提取的交易金额',
    parsed_currency VARCHAR(8) DEFAULT 'CNY' COMMENT '币种',
    parsed_card_no VARCHAR(16) DEFAULT NULL COMMENT '涉及卡号尾号',
    parsed_merchant VARCHAR(128) DEFAULT NULL COMMENT '交易商户名称',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '数据入库时间',
    INDEX idx_received_at (received_at),
    INDEX idx_sender (sender),
    INDEX idx_category (sms_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Raw短信与动账凭证明细表';
