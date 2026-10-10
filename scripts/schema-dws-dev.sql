-- ====================================================================
-- DWS 财务服务汇总层 (Data Warehouse Summary) 聚合视图规范 (DEV 环境)
-- 数据基石: iceberg.finance_dev.dwd_financial_transactions
-- 核心增强: 引入 min_id 与 max_id 物理行号聚合，完美支撑水位追踪与防漏单
-- ====================================================================

-- 1. 每日财务汇总视图 (Daily Financial Summary)
CREATE OR REPLACE VIEW iceberg.finance_dev.dws_financial_summary_daily AS
SELECT 
    date(tx_time) AS stat_date,
    min(id) AS min_id,
    max(id) AS max_id,
    day_of_week(tx_time) AS day_of_week,
    day_of_week(tx_time) IN (6, 7) AS is_weekend,
    count(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE', 1, null)) AS tx_count,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and currency='CNY', amount, 0)) AS total_expense,
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='REFUND' and currency='CNY', amount, 0)) AS total_refund,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and currency='CNY', amount, 0)) - 
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='REFUND' and currency='CNY', amount, 0)) AS net_expense,
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='INCOME' and currency='CNY', amount, 0)) AS total_income,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='TRANSFER' and currency='CNY', amount, 0)) AS total_transfer,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='FOOD', amount, 0)) AS food_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='TRANSPORT', amount, 0)) AS transport_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='ONLINE_SHOPPING', amount, 0)) AS online_shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='OFFLINE_SHOPPING', amount, 0)) AS offline_shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='SHOPPING', amount, 0)) AS shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='MEDICAL', amount, 0)) AS medical_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='COMMUNICATION', amount, 0)) AS communication_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='INSURANCE', amount, 0)) AS insurance_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='PROPERTY_MANAGEMENT', amount, 0)) AS property_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='TRAVEL', amount, 0)) AS travel_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='CAR_EXPENSE', amount, 0)) AS car_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='PERSONAL_TRANSFER', amount, 0)) AS personal_transfer_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='OTHER', amount, 0)) AS other_expense,
    max(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE', amount, null)) AS max_single_amount
FROM iceberg.finance_dev.dwd_financial_transactions
GROUP BY date(tx_time), day_of_week(tx_time);

-- 2. 每周财务汇总视图 (Weekly Financial Summary - ISO-8601 标准自然周)
CREATE OR REPLACE VIEW iceberg.finance_dev.dws_financial_summary_weekly AS
SELECT 
    year_of_week(tx_time) AS stat_year,
    week(tx_time) AS week,
    concat(cast(year_of_week(tx_time) as varchar), '-W', lpad(cast(week(tx_time) as varchar), 2, '0')) AS week_period,
    min(date(tx_time)) AS week_start_date,
    max(date(tx_time)) AS week_end_date,
    min(id) AS min_id,
    max(id) AS max_id,
    count(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE', 1, null)) AS tx_count,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and currency='CNY', amount, 0)) AS total_expense,
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='REFUND' and currency='CNY', amount, 0)) AS total_refund,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and currency='CNY', amount, 0)) - 
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='REFUND' and currency='CNY', amount, 0)) AS net_expense,
    round((sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and currency='CNY', amount, 0)) - 
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='REFUND' and currency='CNY', amount, 0))) / 7.0, 2) AS avg_daily_expense,
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='INCOME' and currency='CNY', amount, 0)) AS total_income,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and day_of_week(tx_time) in (6, 7), amount, 0)) AS weekend_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and day_of_week(tx_time) between 1 and 5, amount, 0)) AS weekday_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='FOOD', amount, 0)) AS food_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='TRANSPORT', amount, 0)) AS transport_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='ONLINE_SHOPPING', amount, 0)) AS online_shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='OFFLINE_SHOPPING', amount, 0)) AS offline_shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='SHOPPING', amount, 0)) AS shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='MEDICAL', amount, 0)) AS medical_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='COMMUNICATION', amount, 0)) AS communication_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='INSURANCE', amount, 0)) AS insurance_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='PROPERTY_MANAGEMENT', amount, 0)) AS property_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='TRAVEL', amount, 0)) AS travel_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='CAR_EXPENSE', amount, 0)) AS car_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='PERSONAL_TRANSFER', amount, 0)) AS personal_transfer_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='OTHER', amount, 0)) AS other_expense
FROM iceberg.finance_dev.dwd_financial_transactions
GROUP BY year_of_week(tx_time), week(tx_time);

-- 3. 每月财务资产负债大盘视图 (Monthly Financial Summary)
CREATE OR REPLACE VIEW iceberg.finance_dev.dws_financial_summary_monthly AS
SELECT 
    date_format(tx_time, '%Y-%m') AS stat_month,
    min(id) AS min_id,
    max(id) AS max_id,
    count(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE', 1, null)) AS tx_count,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and currency='CNY', amount, 0)) AS total_expense_cny,
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='REFUND' and currency='CNY', amount, 0)) AS total_refund_cny,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and currency='CNY', amount, 0)) - 
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='REFUND' and currency='CNY', amount, 0)) AS net_expense_cny,
    sum(if(is_valid_tx and direction='INFLOW' and tx_type='INCOME' and currency='CNY', amount, 0)) AS total_income_cny,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='TRANSFER' and currency='CNY', amount, 0)) AS total_transfer_cny,
    sum(if(is_valid_tx and currency='USD', amount, 0)) AS total_expense_usd,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and card_tail='3342', amount, 0)) AS cgb_expense_cny,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='FOOD', amount, 0)) AS food_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='TRANSPORT', amount, 0)) AS transport_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='ONLINE_SHOPPING', amount, 0)) AS online_shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='OFFLINE_SHOPPING', amount, 0)) AS offline_shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='SHOPPING', amount, 0)) AS shopping_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='MEDICAL', amount, 0)) AS medical_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='COMMUNICATION', amount, 0)) AS communication_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='INSURANCE', amount, 0)) AS insurance_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='PROPERTY_MANAGEMENT', amount, 0)) AS property_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='TRAVEL', amount, 0)) AS travel_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='CAR_EXPENSE', amount, 0)) AS car_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='PERSONAL_TRANSFER', amount, 0)) AS personal_transfer_expense,
    sum(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE' and category='OTHER', amount, 0)) AS other_expense,
    max(if(is_valid_tx and direction='OUTFLOW' and tx_type='EXPENSE', amount, null)) AS max_single_amount
FROM iceberg.finance_dev.dwd_financial_transactions
GROUP BY date_format(tx_time, '%Y-%m');

-- 4. 核心商户排行榜视图 (Merchant Spending Ranking Mart)
CREATE OR REPLACE VIEW iceberg.finance_dev.dws_merchant_spending_ranking AS
SELECT 
    date_format(tx_time, '%Y-%m') AS stat_month,
    coalesce(cleaned_merchant, counterparty, '未知商户') AS merchant_name,
    min(id) AS min_id,
    max(id) AS max_id,
    count(1) AS tx_count,
    sum(amount) AS total_amount_cny,
    round(avg(amount), 2) AS avg_amount_cny
FROM iceberg.finance_dev.dwd_financial_transactions
WHERE is_valid_tx = true and direction = 'OUTFLOW' and tx_type = 'EXPENSE' and currency = 'CNY'
GROUP BY date_format(tx_time, '%Y-%m'), coalesce(cleaned_merchant, counterparty, '未知商户');
