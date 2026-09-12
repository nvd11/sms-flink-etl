# Java Flink on K3s 短信流式/批处理 ETL 架构设计规格书

本文档作为 **SMS Flink ETL** 项目的核心技术架构设计规范，全面记录系统的设计背景、选型推演、架构决策记录 (ADR) 以及各模块技术规范。

---

## 1. 业务背景与架构愿景

* **业务定位**：构建个人高可靠财务动账数据管道，将银行交易（广发信用卡 95508）、微信支付及支付宝动账凭证汇聚入统一结构化数据仓库。
* **数据流向**：
  `手机端 (SmsForwarder) ➔ 163 邮箱 SMTP ➔ Alice Gmail ➔ K3s Flink ETL ➔ CockroachDB ("finance-db")`
* **核心原则**：
  1. **架构先行**：先打磨好技术选型、拓扑流向与 DDL，再进入工程实施；
  2. **轻量与弹性**：杜绝资源空耗与过度设计，计算随用随起，算完即焚；
  3. **数据幂等与高保真**：保留完整短信原文，依赖唯一指纹杜绝重复记账。

---

## 2. 核心架构决策记录 (Architectural Decision Records - ADR)

### ADR-001: 计算范式由 7x24 常驻流转为周期性批处理 (Cron BATCH)
* **背景**：原方案假定 Flink 需 7x24 小时保持运行。但个人短信动账属于低频稀疏事件流（日均 5~20 条）。若常驻 Flink 集群，JVM 空载即需 1~2GB 内存开销。
* **决策**：采用 **Flink 批处理模式 (`RuntimeExecutionMode.BATCH`)**，配置 **每 4 小时调度一次 (一天 6 批)**。
* **收益**：
  * 对标 **GCP Dataflow 弹性批处理哲学**：由 Kubernetes CronJob 动态调起单 Pod，处理完毕进程正常退出，Pod 被回收，平时维持 **0 CPU、0 内存** 纯净状态；
  * 彻底消解 Watermark 乱序、窗口漂移与流作业复杂状态快照的维护负担。

### ADR-002: 数据源接入采用短连接主动拉取 (Short-Polling)
* **背景**：IMAP 协议在网络抖动或家庭代理切换时极易发生 TCP 连接重置。若在流作业中维护长连接，极易引发连续 Failover 重启。
* **决策**：每 4 小时启动时，建立一次性短连接（耗时约 1~2 秒），根据当前数据库内已有的 `MAX(email_uid)` 获取新邮件，抓取完成后立刻优雅断开释放连接。

### ADR-003: 目标数据库选定 CockroachDB Serverless
* **选型对比**：
  * **本地自建 Postgres**：需在 NUC 上自建维护 StatefulSet/PV，存在运维心智负担；
  * **OCI MySQL HeatWave (50GB)**：Always Free 实例，但目前主要用于 LiteLLM 生产调用流水；
  * **CockroachDB Serverless (10GB)**：托管于 AWS 新加坡机房，支持原生 PostgreSQL 14 语法协议，自带 10GB 终身免费配额与每月 50M RU 免费算力，具备闲置自动 Scale to Zero 能力。
* **决策**：选定 **CockroachDB Serverless** 作为动账专属数据库。已初始化独立数据库 **`finance-db`** 与专用业务账号 **`finance_user`**。
* **特性红利**：
  * 原生支持 **`JSONB`**，可对多变的非结构化短信特征建立灵活扩展字段；
  * 原生支持 `ON CONFLICT (email_uid) DO UPDATE` 语法，提供端到端幂等写入保证。

### ADR-004: 运行节点正式选定锁定本地 NUC (kubernetes.io/hostname=nuc)
* **背景**：K3s 业务集群跨越云端与家庭边缘。其中腾讯云控制面仅 4G 内存，绝不可容纳 JVM 算力；而候选节点主要为新加坡 OCI ARM (24G) 与本地 NUC (16G)。
* **决策**：正式拍板将 Flink 批处理调度在 **本地 NUC 节点**（`100.104.150.19`），通过 Pod Template 的 `nodeSelector: kubernetes.io/hostname: nuc` 进行精确绑定。
* **收益与合理性**：
  1. **内存极度宽裕**：NUC 拥有高达 **13 GiB 的空闲可用物理内存**（当前仅使用 2.0G），可从容承载 Flink JVM 进程的拉起与 GC，零内存瓶颈风险；
  2. **原生 x86_64 指令集优势**：NUC 为标准 Intel x86_64 硬件体系，规避了 ARM64 跨平台镜像编译或依赖项不兼容的隐患，可直接以最高性能原生运行标准 Flink 官方镜像；
  3. **短连接消解家宽短板**：由于前置决策定下了“每 4 小时批处理短连接拉取”，拉取操作仅需 1~2 秒，经本地家庭代理即可顺畅穿透，彻底避开了长连接在家庭网络下的掉线风险。

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
