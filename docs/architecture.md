# Flink + Trino on NUC (ArgoCD GitOps) + Cloudflare R2 湖仓架构设计规格书

本文档作为 **SMS Flink ETL** 项目的核心技术架构设计规范，全面记录系统的设计背景、选型推演、架构决策记录 (ADR) 以及各模块技术规范。

---

## 1. 业务背景与架构愿景

* **业务定位**：构建个人高可靠财务动账数据湖仓 (Lakehouse) 管道，将银行交易（广发信用卡 95508）、微信支付及支付宝动账凭证汇聚入统一开放的 Parquet / Iceberg 表格中。
* **数据流向**：
  `手机端 (SmsForwarder) ➔ 163 邮箱 SMTP ➔ Alice Gmail ➔ AWS EventBridge Scheduler (定时吹哨) ➔ GitHub Actions Workflow (调度中枢) ➔ NUC K3s Flink (批处理入湖) ➔ Cloudflare R2 (Iceberg) ➔ NUC K3s Trino (查询分析)`
* **核心原则**：
  1. **现代湖仓一体 (Lakehouse First)**：对标汇丰金融级数据湖仓演进哲学，全面拥抱开放标准，杜绝闭源专有格式与厂商锁定；
  2. **双引擎本地统一治理 (Double Engine Hub)**：本地 NUC 节点作为专属计算中心，通过 **ArgoCD GitOps** 统一纳管 **Flink (计算清洗)** 与 **Trino (交互查询)** 双引擎；
  3. **三级弹性调度闭环 (Zero-Cost Serverless Trigger)**：借助 AWS EventBridge Scheduler 与 GitHub Actions 官方 API 实现云端吹哨与状态监控，本地 Flink 无需任何常驻监听服务，算完即焚；
  4. **数据不可变性与审计可追溯**：基于 Iceberg 快照隔离机制，保留完整短信原文，支持毫秒级时间旅行 (Time Travel) 历史切片回溯。

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

### ADR-005: 触发与调度链路锁定 AWS EventBridge Scheduler ➔ GitHub Actions ➔ NUC Flink
* **背景评估**：
  * 若完全依赖本地 NUC 系统时钟自调度，一旦家庭网络临时断网、关机或宿主机时钟漂移，可能发生定时漏跑；
  * 若在本地常驻一个 HTTP 服务专门监听外部调度，又会违背 Flink 零常驻的精简初衷。
* **决策**：建立 **三级弹性调度闭环体系**：
  1. **云端定时吹哨人 (AWS EventBridge Scheduler)**：在 AWS 新加坡机房配置原生支持 `Asia/Shanghai` 的定时调度任务（`cron(0 0,4,8,12,16,20 * * ? *)`），享有每月 **1400 万次永久免费** 配额，到点自动向 GitHub 官方 API 发起 dispatch 请求；
  2. **调度编排中枢 (GitHub Actions Workflow)**：接收 GitHub 官方的 `workflow_dispatch` 事件，执行环境预检与监控记录，随后向本地 NUC 发送批处理唤醒信号；
  3. **批计算执行端 (NUC K3s Flink)**：本地 K3s 动态拉起批处理 Job，运行 10~20 秒将增量短信规整并写入 Cloudflare R2，完成后优雅退出并 100% 释放内存。
* **架构收益**：
  * **完全零本地常驻监听负担**：无需在 NUC 长期常驻任何 Web 监听进程，由 GitHub 价值数亿美元的全球云基础设施替我们 7x24 小时守候；
  * **开箱即用可视化日志与告警**：每一次定时触发在 GitHub 界面上均有完整的执行历史图表与失败邮件告警。

### ADR-006: 持续交付体系——GitHub Actions CI + ArgoCD GitOps CD
* **决策**：明确划分 CI 与 CD 的职责边界：
  * **GitHub Actions (CI)**：代码推送触发自动化单测 (`mvn test`)、打包 Fat JAR、构建 Docker 容器镜像并推送到镜像仓库；
  * **ArgoCD (CD)**：自动检测镜像或 GitOps 清单变更，将 Flink 作业与 Trino 服务滚动更新至 NUC 集群。

### ADR-007: 凭证与机密隔离标准 (K8s Secret & 单一真理源)
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

## 4. 代码工程结构与模块化设计规范 (Clean Layering & Multi-Job Extensibility)

为了彻底杜绝面向过程脚本式写法、避免静态函数泛滥，同时支撑未来多作业横向扩展（如增加**视频过期清理任务 VideoCleanupJob**、账单核对任务等），系统采用清晰严谨的分层架构设计：**将“执行入口驱动层 (jobs)”、“流图拓扑编排层 (pipeline)”、“协议/介质驱动层 (source)”及“存储落地层 (sink)”彻底解耦**。

### 4.1 包结构拓扑与多作业扩展蓝图 (Package Topology)

```text
com.finance.etl
│
├── jobs                <-- [执行入口驱动层] 纯粹的 Application/Job Launcher (main 入口)
│   ├── HelloWorldJob.java        # 探活与基线冒烟测试驱动器
│   ├── SmsGmailR2Job.java        # 金融动账入湖作业入口
│   └── VideoCleanupJob.java      # [未来扩展示例] 监控视频过期清理作业入口
│
├── pipeline            <-- [计算拓扑编排层] 纯粹的 Flink DAG 算子装配与数据流编排
│   ├── SmsGmailR2Pipeline.java   # 协调 IMAP 采集、动账规整并组装 DataStream 拓扑
│   └── VideoCleanupPipeline.java # [未来扩展示例] 编排视频元数据流与清理过滤拓扑
│
├── source              <-- [数据输入连接器层 / Source] 按协议/介质二级分包，遵循 FLIP-27 标准
│   ├── imap                      # 邮件协议数据源 (给动账入湖 Job 使用)
│   │   ├── ImapSource.java           # 顶层门面工厂 (实现 Source<RawEmail, ImapSplit, ...>)
│   │   ├── ImapSourceReader.java     # Worker 读取工人 (实现 SourceReader)
│   │   ├── ImapSplit.java            # 任务分片工单 (实现 SourceSplit)
│   │   ├── ImapSplitEnumerator.java  # Master 调度总管 (实现 SplitEnumerator)
│   │   └── ImapSplitSerializer.java  # 工单版本化编解码器
│   │
│   └── video / file              # [未来扩展示例] 视频/文件存储数据源 (给清理 Job 使用)
│       ├── VideoFileSource.java      # 扫描 NAS / S3 / 本地磁盘视频元数据 Source
│       ├── VideoFileSourceReader.java# 读取视频文件属性 (创建时间、时长、大小)
│       └── VideoFileSplit.java       # 目录/文件块分片工单
│
├── transform           <-- [业务清洗层 / Transform] 业务规则、实体映射与防重指纹
│   ├── SmsRecordParser.java      # 邮件元数据解析与动账凭证提炼
│   ├── RawRecordFormatter.java   # 辅助算子：字符串规整与历史兼容算子
│   └── VideoRetentionFilter.java # [未来扩展示例] 视频过期策略过滤算子 (如保留7天)
│
├── sink                <-- [湖仓/落地存储层 / Sink] 开放表格与对象存储直连
│   ├── IcebergR2SinkBuilder.java # Cloudflare R2 Iceberg 湖仓追加 Sink
│   └── FileDeletionSink.java     # [未来扩展示例] 视频文件物理清理 / 冷归档 Sink
│
├── model               <-- [领域模型层] 核心 DTO 与 Lakehouse ODS 物理表模型
│   ├── RawEmail.java             # 邮件协议原始 DTO
│   ├── SmsRecord.java            # 湖仓 ODS 物理表实体模型
│   └── VideoFileMeta.java        # [未来扩展示例] 视频文件元数据实体
│
└── util                <-- [通用基础设施层] 辅助工具与配置解析
    └── ConfigUtils.java          # 环境变量与配置加载器 (.env / OS ENV)
```

### 4.2 层次与面向对象职责分工矩阵

| 层次 / 包名 | 代表类名 | 角色与类型 | 核心职责 | 依赖与可测试性 |
| :--- | :--- | :--- | :--- | :--- |
| **`jobs`** | `SmsGmailR2Job`<br>`VideoCleanupJob` | 作业驱动入口 (Main Driver) | 负责命令行参数解析、Flink 执行环境初始化（BATCH 模式、并发度）、通过工厂实例化组件并调用 `env.execute()`。 | 极简，无任何静态业务实现，专注于应用生命周期控制。 |
| **`pipeline`** | `SmsGmailR2Pipeline`<br>`VideoCleanupPipeline` | 计算拓扑编排器 (DAG Orchestrator) | 持有 `source`、`transform` 及 `sink` 实例，负责组装 Flink `DataStream` 算子拓扑、空数据心跳保活及异常兜底。 | 高内聚，脱离静态入口，天然支持在测试中注入 Mock 组件执行拓扑验证。 |
| **`source`** | `source.imap.*`<br>`source.video.*` | 数据源连接器 (FLIP-27 Connector) | 按协议/介质独立子包。内部严格遵循 FLIP-27 规范，分离 `SplitEnumerator`（Master 调度）与 `SourceReader`（Worker 读取），通过 `SplitSerializer` 完成网络传输。 | 独立子包物理隔离，新增视频/文件等数据源对原有代码 0 侵入。 |
| **`transform`** | `SmsRecordParser`<br>`VideoRetentionFilter` | 业务解析实体 (Domain Transformer) | 实现具体的业务清洗、正则解析、过期策略判断或模型转换。 | 纯业务逻辑，实现 `Serializable`，直接作为 Flink 函数算子复用。 |
| **`sink`** | `IcebergR2SinkBuilder`<br>`FileDeletionSink` | 落地存储构造器 (Lakehouse / Action Sink) | 封装目标存储协议（Cloudflare R2 S3A 认证、Iceberg Commit）或物理动作（本地/远程文件删除）。 | 隔离复杂的外部存储认证与底层连接池配置。 |

### 4.3 架构收益与多作业演进红利
1. **开闭原则（OCP）终极落地**：
   - 增加新业务（如视频清理、外币对账）时，只需在 `jobs` 增加入口、在 `pipeline` 增加编排、在 `source` 扩充对应协议子包，**旧业务代码零修改、零风险**。
2. **连接器生态按“协议/介质”自收敛**：
   - `source.imap` 只管邮件通信协议；
   - `source.video` 只管文件目录与流媒体探测；
   - 职责边界如刀刻般分明，彻底消除了包污染。
3. **CI/CD 构建粒度清晰**：
   - `build-helloworld-job.yml` 关注 `jobs/HelloWorldJob.java`；
   - `build-sms-gmail-r2-job.yml` 关注 `jobs/SmsGmailR2Job.java`、`source/imap/**`、`pipeline/SmsGmailR2Pipeline.java` 等关联模块。

---

## 5. 表结构 Schema 规范与关注点分离 (Domain vs Pipeline Metadata)

系统在数据湖仓表模型设计上严格遵循**领域驱动设计（DDD）与关注点分离原则（SoC）**，坚决杜绝用传输通道层（如 IMAP 协议、Kafka 分区）的临时元数据污染业务资产模型。

* **业务资产主表**：`iceberg.finance.raw_sms_records`（完整 DDL 详见 [`scripts/schema.sql`](../scripts/schema.sql)）
  - **纯粹领域资产模型**：纯粹保存短信业务属性（`id`, `msg_uid`, `channel`, `sender`, `receiver_phone`, `received_at`, `raw_body`, `created_at`）；
  - 严禁将 `imap_uid` 等特定通信协议参数硬塞入业务表，保证未来从企业微信、Android 蓝牙或外部 Webhook 接入短信时，Schema 永久稳定纯洁；
  - 隐藏分区与排序：按短信物理到达月份自动隐藏分区 `month(received_at)`，块内按到达时间物理排序 `sorted_by = ARRAY['received_at']`。

* **调度同步状态表**：`iceberg.finance.etl_sync_offsets`
  - **集中式水位游标中心**：专职解耦并持久化各 Pipeline 批处理任务的消费位点；
  - 包含字段：`job_name`（作业标识）、`channel`（通道类型）、`source_target`（目标标识）、`last_offset`（增量水位游标，如 IMAP UID / Kafka Offset）、`last_event_time`、`updated_at`；
  - 批处理任务启动时，通过 `SELECT COALESCE(MAX(last_offset), 0) FROM iceberg.finance.etl_sync_offsets WHERE job_name = 'sms-gmail-r2'` 秒级获取上一次成功提交的水位线，实现可靠的断点续传。

* **时间标准**：统一采用带时区微秒时间戳（`TIMESTAMP(6) WITH TIME ZONE`）。
