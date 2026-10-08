-- ====================================================================
-- ADS 智能财务研报持久化事实表 (Financial Reports Fact Table DDL Specification)
-- Target Schemas:
--   • Production:  iceberg.finance.ads_financial_reports
--                  Location: s3://sms-flink-etl/iceberg/finance/ads_financial_reports
--   • Development: iceberg.finance_dev.ads_financial_reports
--                  Location: s3://sms-flink-etl-dev/iceberg/finance_dev/ads_financial_reports
-- Engine: Trino / Apache Iceberg V2 / Flink 1.20
-- ====================================================================

-- 1. 生产环境 ADS 财务研报事实表
CREATE TABLE IF NOT EXISTS iceberg.finance.ads_financial_reports (
    -- 1. 业务主键与周期标识
    report_id           VARCHAR,                             -- 报告唯一主键 (如 'report_daily_2026-10-05', 'report_monthly_2026-09')
    period_type         VARCHAR,                             -- 周期类型: 'DAILY', 'WEEKLY', 'MONTHLY'
    period_value        VARCHAR,                             -- 周期取值标识 (如 '2026-10-05', '2026-W40', '2026-09')
    report_date         DATE,                                -- 报告归属日期 (用于分区与日期范围检索)

    -- 2. 宏观财务大盘核算指标快照 (CNY)
    total_expense       DECIMAL(12, 2),                      -- 周期消费支出毛额
    total_refund        DECIMAL(12, 2),                      -- 周期退款冲正抵扣
    net_expense         DECIMAL(12, 2),                      -- 周期净消费开销 (平账核心基准)
    total_income        DECIMAL(12, 2),                      -- 周期被动收入/理赔回血
    total_transfer      DECIMAL(12, 2),                      -- 周期信用卡还款等内部划转 (非日常消费)
    tx_count            BIGINT,                              -- 周期内有效消费笔数

    -- 3. 结构化分类明细与 AI 分析产物
    metrics_json        VARCHAR,                             -- 关键分类金额与商户结构快照 JSON
    summary_text        VARCHAR,                             -- LLM (Gemini 3.8 Flash) 生成的完整 Markdown 研报与 Yui 温存点评
    chart_url           VARCHAR,                             -- QuickChart 生成的高清图表短链图片 URL

    -- 4. 履约与审计元数据
    slack_status        VARCHAR,                             -- Slack 推送状态: 'SENT', 'FAILED', 'SKIPPED'
    created_at          TIMESTAMP(6) WITH TIME ZONE          -- 报告生成入湖时间戳
)
WITH (
    format = 'PARQUET',                                      -- 底层列式存储: Parquet
    partitioning = ARRAY['month(report_date)'],              -- 按归属月进行 Iceberg 隐藏分区剪枝
    sorted_by = ARRAY['report_date DESC']                    -- 块内按报告日期倒序排列
);

-- 2. 开发测试环境 ADS 财务研报事实表
CREATE TABLE IF NOT EXISTS iceberg.finance_dev.ads_financial_reports (
    report_id           VARCHAR,
    period_type         VARCHAR,
    period_value        VARCHAR,
    report_date         DATE,
    total_expense       DECIMAL(12, 2),
    total_refund        DECIMAL(12, 2),
    net_expense         DECIMAL(12, 2),
    total_income        DECIMAL(12, 2),
    total_transfer      DECIMAL(12, 2),
    tx_count            BIGINT,
    metrics_json        VARCHAR,
    summary_text        VARCHAR,
    chart_url           VARCHAR,
    slack_status        VARCHAR,
    created_at          TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(report_date)'],
    sorted_by = ARRAY['report_date DESC']
);
