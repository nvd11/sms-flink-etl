# Flink + Trino on NUC (ArgoCD GitOps) + Cloudflare R2 湖仓架构设计规格书

本文档作为 **SMS Flink ETL** 项目的核心技术架构设计规范，全面记录系统的设计背景、选型推演、架构决策记录 (ADR) 以及各模块技术规范。

---

## 1. 业务背景与架构愿景

* **业务定位**：构建个人高可靠财务动账数据湖仓 (Lakehouse) 管道，将银行交易（广发信用卡 95508）、微信支付及支付宝动账凭证汇聚入统一开放的 Parquet / Iceberg 表格中。
* **数据流向**：
  `手机端 (SmsForwarder) ➔ 163 邮箱 SMTP ➔ Alice Gmail ➔ NUC K3s Flink (ArgoCD) ➔ Cloudflare R2 (Iceberg) ➔ NUC K3s Trino (ArgoCD)`
* **核心原则**：
  1. **现代湖仓一体 (Lakehouse First)**：对标汇丰金融级数据湖仓演进哲学，全面拥抱开放标准，杜绝闭源专有格式与厂商锁定；
  2. **双引擎本地统一治理 (Double Engine Hub)**：本地 NUC 节点作为专属计算中心，通过 **ArgoCD GitOps** 统一纳管 **Flink (计算清洗)** 与 **Trino (交互查询)** 双引擎；
  3. **数据不可变性与审计可追溯**：基于 Iceberg 快照隔离机制，保留完整短信原文，支持毫秒级时间旅行 (Time Travel) 历史切片回溯。

---

## 2. 核心架构决策记录 (Architectural Decision Records - ADR)

### ADR-001: 湖仓计算引擎锁定 Flink on NUC (ArgoCD GitOps 交付)
* **背景评估**：
  * 原方案考虑在云端 Runner 执行动态批计算，虽然免除了本地常驻，但缺乏持久化的 Web 监控面板与集群统一治理体验；
  * 手机端 (OPPO) 虽然算力强劲，但属于电池设备，不宜常驻重型 Java 守护；
  * 本地 **Intel NUC (`Nova` · 10.0.1.113)** 拥有 16GB 物理内存（**实测闲置可用高达 13.4GB**），原生 x86_64 架构，是业务集群 Worker 节点的完美载体。
* **决策**：在 NUC 节点上通过 **ArgoCD GitOps** 纳管部署 **Apache Flink 计算引擎**。
* **架构收益**：
  * **充足闲置内存变现**：仅需分配轻量 JVM 内存 `-Xmx2G`，在 NUC 13.4GB 空闲内存面前轻如鸿毛；
  * **开箱即用小松鼠监控**：原生暴露 Flink Web Dashboard (`10.0.1.113:8081` / `flink.jppwl.asia`)，清晰直观地观测批处理吞吐、Checkpoint 状态与作业拓扑；
  * **声明式统一运维**：受控于 `my-argocd-manifests` 仓库，版本升级与配置调优全部实现 GitOps 自动化。

### ADR-002: 数据源接入采用短连接主动拉取 (Short-Polling)
* **背景**：IMAP 协议在长连接保持下易受网络重置困扰，一旦断流极易引发作业重启。
* **决策**：定时启动时，建立一次性短连接（耗时约 1~2 秒），根据当前已同步的水位或 `UNSEEN` 邮件获取增量，抓取完成后立刻优雅断开释放连接。

### ADR-003: 存储与表格式选定 Cloudflare R2 + Apache Iceberg
* **选型背景与对比**：
  * **传统关系库 (CockroachDB / PgSQL)**：虽然支持单行事务，但属于封闭存储；无法享受列式存储高压缩比、元数据剪枝以及开放数据湖生态互通红利；
  * **AWS S3 / GCP GCS**：公有云对象存储均存在昂贵不可控的 **出网流量费 (Egress Fee)**；
  * **Cloudflare R2**：全球唯一彻底 **免除出口流量费 (Zero Egress)** 的 S3 兼容对象存储，同时提供 **10GB 终身免费存储空间**、每月 **100 万次写 + 1000 万次读** 免费 API 配额；
  * **Apache Iceberg**：现代开源数据湖仓事实标准，具备表级 ACID 快照、隐藏分区、Schema Evolution 以及时间旅行审计追溯能力。
* **决策**：选定 **Cloudflare R2** 专属存储桶 `sms-flink-etl` 承载 **Apache Iceberg (Parquet)** 表。
* **特性红利**：
  * **开放标准与零锁定**：100% 开放的 Parquet 列存，跨引擎自由读取（Trino, Flink, Spark, DuckDB）；
  * **极高压缩比**：动账报文经 Snappy/ZSTD 压缩，单批占用几十 KB，10GB 空间足以容纳数十年海量动账；
  * **金融级审计血统**：支持基于 Snapshot 的 Time Travel 语法，可一键调阅任意历史时间点的动账状态。

### ADR-004: 查询引擎与控制面锁定 Trino on NUC (ArgoCD GitOps 交付)
* **决策**：选用业界成熟度最高的分布式计算引擎 **Trino**，与 Flink 同宿于本地 **Intel NUC (`Nova`)** K3s 节点上，由 **ArgoCD GitOps** 自动化部署。
* **架构收益**：
  1. **双引擎协同共生**：Flink 负责入湖清洗，Trino 负责交互查询。Flink (2G) + Trino (3G) 叠加仅消耗 ~5GB，NUC 依然富余 **8GB+ 内存**，从容不迫；
  2. **声明式 GitOps 运维**：所有 Deployment、Service、ConfigMap 和 Catalog 配置收敛至 `my-argocd-manifests` 仓库，修改 Git 自动热同步；
  3. **标准 ANSI SQL 与 Web UI**：内置精美 Web Dashboard 监控 (`10.0.1.113:8080`)；支持 DBeaver、Python 以及集成 Kong Ingress (`trino.jppwl.asia` 带权威免费 SSL) 进行随时随地查账；
  4. **元数据下推与高效剪枝**：Trino 借助 Iceberg Manifest 直接完成分区裁剪与 Min/Max 过滤，毫秒级响应。

### ADR-005: 持续交付体系——GitHub Actions CI + ArgoCD GitOps CD
* **决策**：明确划分 CI 与 CD 的职责边界：
  * **GitHub Actions (CI)**：代码推送触发自动化单测 (`mvn test`)、打包 Fat JAR、构建 Docker 容器镜像并推送到镜像仓库；
  * **ArgoCD (CD)**：自动检测镜像或 GitOps 清单变更，将 Flink 作业与 Trino 服务滚动更新至 NUC 集群。

### ADR-006: 凭证与机密隔离标准 (K8s Secret & 单一真理源)
* **决策**：虽代码库公开透明，但任何生产凭证严禁硬编码。
* **注入规范**：
  * `GMAIL_IMAP_USER`、`GMAIL_IMAP_PASS`（Alice Gmail 应用授权码）
  * `R2_S3_ENDPOINT`、`R2_S3_ACCESS_KEY_ID`、`R2_S3_SECRET_ACCESS_KEY`、`R2_BUCKET_NAME`（Cloudflare R2 S3 凭据）
  统一由 K8s Secret 注入 Pod 内存，内网配置全量收敛于私有资产真理源 `cloud_accounts_and_spaces.md`。

---

## 3. Flink 处理流水线与数仓分层设计

系统严格遵循数据仓库经典分层架构与数据湖仓范式：
* **ODS 层 (Operational Data Store · 原始报文层)**：本工程核心目标，**纯粹落盘原始报文与物理元数据，不掺杂任何业务解析/派生字段**，落地为 Iceberg 不可变表；
* **DWD 层 (Data Warehouse Detail · 明细事实层)**：下游衍生任务或 Trino 视图负责读取 ODS 表，执行正则模式提取、动账归类与结构化清洗（金额、商户、卡号、交易时间），落地为结构化消费事实表。

```text
[ 采集端 (SmsForwarder ➔ Gmail) ]
              │
              ▼
[ EmailImapBatchSource ] (短连接拉取原始邮件 DTO)
              │
              ▼
[ RawRecordFormatter ] (物理元数据规整与类型映射)
              │
              ▼
[ Flink IcebergBatchSink ] (S3A 直连 Cloudflare R2 写入 Iceberg 表)
              │  (Append-Only Parquet + Metadata Snapshot 提交)
              ▼
============================ ODS 数据湖仓底座已落稳 ============================
              │
              ▼
[ Trino on NUC (ArgoCD) ] (挂载 R2 Catalog · 提供标准 SQL 与 Time Travel)
              │
              ▼
(下游 DWD 任务消费 / 正则解析) ➔ 消费明细事实表 (fct_transactions)
```

### 3.1 ODS Source 算子 (`EmailImapBatchSource`)
* 协议：Jakarta Mail / IMAP over SSL (Port 993)；
* 认证：应用专用密码授权 (`alice.h.y.he@gmail.com`)；
* 检索策略：
  1. 获取当前未读或增量邮件；
  2. 批量拉取发件人、收件号码、正文全文与时间戳后即刻关闭连接。

### 3.2 ODS Sink 算子 (`Flink IcebergBatchSink`)
* 格式：Apache Iceberg (Parquet 列式编码 + Snappy 压缩)；
* 存储底座：Cloudflare R2（S3 兼容协议）；
* 写入语义：**Batch Append-Only 事务性提交**。原子更新 Snapshot，天然保证数据真实保真且具备快照隔离能力。

---

## 4. 表结构 Schema 规范与分区策略

* **表规范**：`iceberg.finance.raw_sms_records`
* **分区策略**：采用 Iceberg 隐藏分区（Hidden Partitioning）特性，按接收月份分区 `month(received_at)`，兼顾文件紧凑度与查询剪枝效率；
* **完整 Iceberg DDL 定义**：详见 [`docs/schema.sql`](schema.sql)；
* **时间标准**：统一采用带时区微秒时间戳（`TIMESTAMP(6) WITH TIME ZONE`）。
