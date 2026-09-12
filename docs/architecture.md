# Java Flink on K3s 短信流式 ETL 架构设计详解

## 1. 架构目标与背景
实现个人动账与信用卡消费凭证的端到端自动化摄入与结构化流处理。
* 接收端：手机端 SmsForwarder 抓取银行短信与 App 通知，通过 SMTP 发送至中继邮箱。
* 传输层：Alice 专属 Gmail 邮箱作为消息汇聚中枢。
* 流计算层：部署在 K3s 集群上的 Java Flink 应用，持续监听并解析消息。
* 存储层：以关系型数据库（MySQL HeatWave / PostgreSQL）存储 Raw 短信日志。

## 2. 核心流计算算子设计

### 2.1 Source 算子 (Email / Message Stream Source)
* 实现定时长轮询 / IDLE 监听模式，读取未读邮件；
* 提取邮件元数据（Message-ID、From、Subject、Date、Body）；
* 支持状态存储与 Checkpoint，记录已处理的最新邮件 UID。

### 2.2 Transform 算子 (Pattern Extractor)
* 正则模式库：
  * 广发银行短信：提取卡号、时间、金额与商户
  * 微信支付通知：提取服务号标题、商户名称及金额大文本
  * 支付宝出账通知：提取卡号、出账日期与提醒内容
* 异常降级处理：未能正则命中的未知短信打标为 NOTICE，完整保留原始报文供后续模型分析。

### 2.3 Sink 算子 (JDBC Upsert Sink)
* 采用 JdbcExecutionOptions 设置小批量批量写入（例如 1 秒刷新或满 10 条写入）；
* 利用 ON DUPLICATE KEY UPDATE 或 ON CONFLICT (email_uid) DO NOTHING 保证流计算幂等性。
