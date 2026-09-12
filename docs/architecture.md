# Java Flink on GitHub Actions Cron 短信批处理 ETL 架构设计规格书

本文档作为 **SMS Flink ETL** 项目的核心技术架构设计规范，全面记录系统的设计背景、选型推演、架构决策记录 (ADR) 以及各模块技术规范。

---

## 1. 业务背景与架构愿景

* **业务定位**：构建个人高可靠财务动账数据管道，将银行交易（广发信用卡 95508）、微信支付及支付宝动账凭证汇聚入统一结构化数据仓库。
* **数据流向**：
  `手机端 (SmsForwarder) ➔ 163 邮箱 SMTP ➔ Alice Gmail ➔ GitHub Actions Cron (Flink BATCH) ➔ CockroachDB ("finance-db")`
* **核心原则**：
  1. **架构先行**：先打磨好技术选型、拓扑流向与 DDL，再进入工程实施；
  2. **轻量与弹性 (Zero-Cost Serverless)**：借助 Public 仓库红利彻底解放本地硬件算力，算完即焚，不花一分钱；
  3. **数据幂等与高保真**：保留完整短信原文，依赖唯一指纹杜绝重复记账。

---

## 2. 核心架构决策记录 (Architectural Decision Records - ADR)

### ADR-001: 计算范式由 7x24 常驻流转为周期性批处理 (Cron BATCH)
* **背景**：原方案假定 Flink 需 7x24 小时保持运行。但个人短信动账属于低频稀疏事件流（日均 5~20 条）。若常驻流集群，JVM 空载即需 1~2GB 内存开销。
* **决策**：采用 **Flink 批处理模式 (`RuntimeExecutionMode.BATCH`)**，配置 **每 4 小时调度一次 (一天 6 批)**。
* **收益**：
  * 对标 **GCP Dataflow 弹性批处理哲学**：定时动态调起 Runner，处理完毕进程正常退出并回收算力，平时维持 **0 CPU、0 内存** 纯净状态；
  * 彻底消解 Watermark 乱序、窗口漂移与流作业复杂状态快照的维护负担。

### ADR-002: 数据源接入采用短连接主动拉取 (Short-Polling)
* **背景**：IMAP 协议在长连接保持下易受网络重置困扰，一旦断流极易引发作业重启。
* **决策**：每 4 小时启动时，建立一次性短连接（耗时约 1~2 秒），根据当前数据库内已有的 `MAX(email_uid)` 获取新邮件，抓取完成后立刻优雅断开释放连接。

### ADR-003: 目标数据库选定 CockroachDB Serverless
* **选型对比**：
  * **本地自建 Postgres**：需在本地维护存储介质与备份，存在运维心智负担；
  * **OCI MySQL HeatWave (50GB)**：Always Free 实例，但目前主要用于 LiteLLM 生产调用流水；
  * **CockroachDB Serverless (10GB)**：托管于 AWS 新加坡机房，支持原生 PostgreSQL 14 语法协议，自带 10GB 终身免费配额与每月 50M RU 免费算力，具备闲置自动 Scale to Zero 能力。
* **决策**：选定 **CockroachDB Serverless** 作为动账专属数据库。已初始化独立数据库 **`finance-db`** 与专用业务账号 **`finance_user`**。
* **特性红利**：
  * 原生支持 **`JSONB`**，可对多变的非结构化短信特征建立灵活扩展字段；
  * 原生支持 `ON CONFLICT (email_uid) DO UPDATE` 语法，提供端到端幂等写入保证。

### ADR-004: 调度与计算底座锁定 GitHub Actions Cron (Public Repo 终身免费)
* **背景评估**：
  * 原方案考虑部署在本地 NUC 的 K3s CronJob 上，虽然可行，但存在两项客观约束：(1) 本地家庭网络拉取海外 Gmail 需走代理穿透；(2) 本地硬件不可关机/断网。
  * 本仓库为 **公开开源项目 (Public Repository)**，享有 GitHub 官方赋予的 **无限制分钟数 (Unlimited Minutes)** 政策。
* **决策**：将调度器与计算执行载体全面上云，锁定为 **GitHub Actions Scheduled Workflow**。
* **架构收益**：
  1. **本地硬件与集群零开销**：NUC、K3s、OCI 资源开销全部为 0，本地 PC 或 NUC 关机完全不影响动账入库；
  2. **绝对纯净的海外云网络链路**：GitHub Actions Runner 位于海外原生云机房，访问 Gmail IMAP（Google 海外服务器）与 CockroachDB（AWS 新加坡）均为原生公网高速互通，彻底免除国内代理断流和 GFW 阻断问题；
  3. **自带高可用与开箱即用可视化**：GitHub 官方提供运行历史列表、控制台日志、执行耗时分析及失败邮件主动告警；同时自带 `workflow_dispatch` 手工一键即席补跑能力。

### ADR-005: 凭证与机密隔离标准 (GitHub Repository Secrets)
* **决策**：虽然代码库公开透明，但任何生产凭证严禁硬编码。
* **注入规范**：
  * `GMAIL_IMAP_USER`、`GMAIL_IMAP_PASS`（Alice Gmail 应用授权码）
  * `DB_URL`、`DB_USER`、`DB_PASS`（CockroachDB 连接串）
  统一在 GitHub Repository Settings ➔ Secrets and variables ➔ Actions 中进行强加密保存，仅在 Workflow 运行时作为环境变量注入 JVM 内存。

---

## 3. Flink 处理流水线与算子规格设计

```text
[ EmailImapBatchSource ] 
          │ (输出 EmailMessage: uid, sender, subject, body, date)
          ▼
   [ SmsParserFlatMap ]
          │ (正则模式提取: 卡号、金额、商户、动账分类)
          ▼
   [ SmsRecordBuilder ]
          │ (组装 SmsRecord 实体 + 填充 extra_metadata JSONB)
          ▼
    [ CockroachJdbcSink ]
          │ (Batch Upsert: ON CONFLICT (email_uid) DO UPDATE)
          ▼
      [ DB 落盘完成 ]
```

### 3.1 Source 算子 (`EmailImapBatchSource`)
* 协议：Jakarta Mail / IMAP over SSL (Port 993)；
* 认证：应用专用密码授权 (`alice.h.y.he@gmail.com`)；
* 检索策略：
  1. 优先读取数据库当前最高 `email_uid`；
  2. 构造 `SearchTerm` 仅拉取增量或 `UNSEEN` 邮件；
  3. 读取完成后批量 Emits 邮件对象进入下游，随后关闭 Folder 与 Store 连接。

### 3.2 Transform 算子 (`SmsParserFlatMap`)
* 规则引擎维护预定义正则表达式字典：
  * **广发银行信用卡 (95508)**：
    * 模式：`您尾号(?<card>\d{4})广发卡(?<time>[^消费]+)消费人民币(?<amount>[\d\.]+)元，商户：(?<merchant>[^。]+)`
    * 提取结果：`card_no="3342"`, `amount=50.00`, `merchant="何贤纪念医院"`, `category="EXPENSE"`
  * **微信支付通知 (`com.tencent.mm`)**：
    * 提取服务号交易提醒的大文本、金额及收款方。
  * **验证码短信**：
    * 匹配包含“验证码”、“校验码”之短信，分类打标为 `AUTH_CODE`。
* 兜底机制：未匹配到模式的通知打标为 `NOTICE`，完整保留原始报文进入 `raw_content`，绝不丢失任何审计信息。

### 3.3 Sink 算子 (`CockroachJdbcSink`)
* 驱动：PostgreSQL 官方 JDBC 驱动；
* 批量策略：`JdbcExecutionOptions.builder().withBatchSize(50).withBatchIntervalMs(200).build()`；
* 幂等 SQL：
  ```sql
  INSERT INTO raw_sms_records (
      email_uid, source_type, sender, received_at, raw_subject,
      raw_content, sms_category, parsed_amount, parsed_currency,
      parsed_card_no, parsed_merchant, extra_metadata
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
  ON CONFLICT (email_uid) DO UPDATE SET
      parsed_amount = EXCLUDED.parsed_amount,
      parsed_merchant = EXCLUDED.parsed_merchant,
      sms_category = EXCLUDED.sms_category,
      extra_metadata = EXCLUDED.extra_metadata;
  ```

---

## 4. 数据库 Schema 规范与迁移控制

* 当前 `finance-db` 保持**纯净库**状态；
* 完整数据表定义见 [`docs/schema.sql`](schema.sql)；
* 所有字段均设计有时区支持（`TIMESTAMPTZ`）与高精度数值支持（`NUMERIC(12, 2)`），严禁使用浮点数存储金融金额。
