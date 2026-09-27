# SMS Flink ETL (个人动账与金融短信轻量级 Lakehouse 架构规划)

基于 **AWS EventBridge Scheduler (云端精准调度) + GitHub Actions Workflow (编排中枢) + Flink on NUC (K3s 弹性批处理) + Trino on NUC (ArgoCD 湖仓查询) + Cloudflare R2 (Iceberg 零出口费存储)** 的现代化个人全自动动账记账与开放数据湖仓 (Lakehouse) 架构。

本项目设计并落地将来自手机端（小米 15 HyperOS + SmsForwarder）经网易 163 SMTP 中继汇聚至 Alice Gmail（`alice.h.y.he@gmail.com`）的银行交易动账短信（广发 95508）、微信支付凭证及支付宝出账通知；由云端 **AWS EventBridge Scheduler** 每 4 小时吹哨，通过 API 触发 **GitHub Actions Workflow** 调度中枢，安全唤醒本地 **Intel NUC (`Nova`)** K3s 节点上的 **Flink 批处理流水线** 进行增量抓取、结构化清洗与元数据规整，写入开放湖仓表格式 **Apache Iceberg** 并持久化沉淀于 **Cloudflare R2** 对象存储中，同时由同驻 NUC 的 **Trino MPP 查询引擎** 提供极致标准的 ANSI SQL 查询与金融审计切片分析体验。

---

## 🏛️ 最新端到端架构拓扑

```text
+-----------------------------------------------------------------------------------+
|  [采集端] 小米 15 (HyperOS)                                                       |
|  - 广发信用卡动账短信 (95508)                                                     |
|  - 微信支付凭证 (com.tencent.mm 服务号通知)                                       |
|  - 支付宝动账/出账通知 (com.eg.android.AlipayGphone)                              |
+-----------------------------------------+-----------------------------------------+
                                          | SmsForwarder 自动邮件中继 (163 SMTP)
                                          v
+-----------------------------------------------------------------------------------+
|  [汇聚中心] Alice 专属 Gmail 邮箱 (alice.h.y.he@gmail.com)                        |
|  - 作为高可靠的海外原始报文缓冲区 (无需自建重型 MQ)                              |
+-----------------------------------------------------------------------------------+

================================= ⏰ 调度与触发链路 =================================
+-----------------------------------------------------------------------------------+
|  1. [云端精准定时器] AWS EventBridge Scheduler (新加坡机房 · 每月 1400 万次免费)   |
|  - 调度规则: cron(0 0,4,8,12,16,20 * * ? *) · Asia/Shanghai (北京时间每 4 小时)    |
|  - 动作: 到点自动向 GitHub 官方 API 发起 dispatch 调用，0 本地常驻负担             |
+-----------------------------------------+-----------------------------------------+
                                          | HTTPS POST 触发 GitHub Workflow Dispatch
                                          v
+-----------------------------------------------------------------------------------+
|  2. [调度编排中枢] GitHub Actions Workflow (Public Repo · 100% 终身免费)          |
|  - 工作流: .github/workflows/trigger-nuc-etl.yml                                  |
|  - 职责: 接收调度事件、执行健康状态巡检，向 NUC 触发 Flink 批处理 Job 执行         |
+-----------------------------------------+-----------------------------------------+
                                          | 调度唤醒指令 (Webhook over TLS)
                                          v
================================= 💻 算力与执行底座 =================================
+-----------------------------------------------------------------------------------+
|  3. [本地算力中心] Intel NUC (`Nova` · 10.0.1.113 · 16GB RAM · 可用 12GB+ · x86)   |
|  - 集群身份: K3s 业务集群 (`tencent-dp1-cluster` Worker 节点 · ArgoCD GitOps 纳管)  |
|                                                                                   |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │  Flink 批处理流水线 (K3s CronJob/Job · 运行 15 秒即焚 · 纯静默零常驻占用)    │  |
|  │  - 基础环境: JDK 21 运行时 (--add-opens 模块全通)                           │  |
|  │  - 算子链路: EmailImapBatchSource ➔ RawRecordFormatter ➔ IcebergBatchSink   │  |
|  │  - 内存配额: 临时借用 JVM -Xmx1536m，完成批写入后立即 100% 释放系统内存     │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                          │ S3A over TLS 直写 (0 出口流量费)
|                                          v
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │  Trino 湖仓查询引擎 (ArgoCD GitOps · 单节点轻量常驻 · JVM -Xmx3G · 占 960MB)│  |
|  │  - 功能: 直读 R2 上 Iceberg 表的 Parquet 列存文件与 Snapshot 快照树         │  |
|  │  - 体验: 开放 ANSI SQL、Time Travel 审计历史切片回溯、DBeaver 秒级交互查询  │  |
|  │  - 控制台: Trino Web Dashboard (`10.0.1.113:30880` / `trino.jppwl.asia`)    │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
+-----------------------------------------+-----------------------------------------+
                                          | 远端对象存储挂载与持久化
                                          v
+-----------------------------------------------------------------------------------+
|  [数据湖仓底座 ODS 层] Cloudflare R2 (10GB Always Free · Zero Egress)              |
|  - 存储桶: "sms-flink-etl" (APAC 亚太机房)                                        |
|  - 表格式: Apache Iceberg (Parquet 列式存储 + Snapshot 元数据树)                  |
|  - 核心表: iceberg.finance.raw_sms_records (纯粹 ODS 原始报文不可变表)            |
|  - 核心红利: 10GB 免费空间、100万写/1000万读、全球首创 0 出口流量费 ($0 Egress)    |
+-----------------------------------------------------------------------------------+
```

---

## 💡 最新核心架构决策记录 (ADR Summary)

1. **三级调度触发闭环：AWS Scheduler ➔ GitHub Actions ➔ NUC Flink**
   * **决策**：由 **AWS EventBridge Scheduler**（每月 1400 万次免费、原生 Asia/Shanghai 北京时间时钟）充当云端定时吹哨人；触发 **GitHub Actions Workflow** 调度中枢；最后唤醒本地 **Intel NUC** 上的 Flink 批处理容器。
   * **收益**：彻底消解本地常驻定时器断电走偏风险，享受云端开箱即用的可视化执行历史与告警，实现真正端到端的高可用自动化。

2. **Flink 纯批处理算完即焚：0 常驻内存开销**
   * **决策**：Flink 仅在收到调度信号时在 NUC 上启动，运行 10~20 秒完成增量短信抓取、Iceberg 提交后立即以 `Completed` 状态正常退出。
   * **收益**：平时在 NUC 物理机上维持 **0 CPU、0 内存** 绝对静默，绝不霸占主机宝贵资源。

3. **湖仓存储与查询解耦：Cloudflare R2 + Apache Iceberg + Trino on NUC**
   * **决策**：选用 **Cloudflare R2** 专属存储桶 `sms-flink-etl` 承载 **Apache Iceberg (Parquet)** 表；由本地 NUC 常驻的 **Trino**（常驻仅 960MB）提供秒级 SQL 交互与时间旅行 (Time Travel) 审计查询。
   * **收益**：享受 0 出口流量费 ($0 Egress) 与 10GB 免费存储红利，杜绝云厂商数据锁定。

4. **机密与凭证隔离：K8s Secret & 单一真理源**
   * **决策**：生产凭证（Gmail 授权码、Cloudflare R2 S3 密钥）由 K8s Secret 注入 Pod 内存，内网配置全量收敛于私有资产真理源 `cloud_accounts_and_spaces.md`。

---

## 🗂️ 文档与目录导引

* 详细架构设计规格书：[`docs/architecture.md`](docs/architecture.md)
* 全自动化部署与运维指南：[`docs/deployment.md`](docs/deployment.md)
* Trino on NUC 部署规格书：[`docs/trino-deployment.md`](docs/trino-deployment.md)
* Flink on NUC 部署规格书：[`docs/flink-deployment.md`](docs/flink-deployment.md)
* Apache Iceberg / Trino DDL 规范与查询参考：[`scripts/schema.sql`](scripts/schema.sql)
