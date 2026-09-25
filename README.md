# SMS Flink ETL (个人动账与金融短信轻量级 Lakehouse 架构规划)

基于 **Apache Flink + Trino on NUC (ArgoCD GitOps 双引擎治理)**、**Cloudflare R2 (10GB Always Free · Zero Egress)** 与 **Apache Iceberg (Parquet)** 的现代化个人全自动动账记账与开放数据湖仓 (Lakehouse) 架构。

本项目设计并落地将来自手机端（小米 15 HyperOS + SmsForwarder）经网易 163 SMTP 中继汇聚至 Alice Gmail（`alice.h.y.he@gmail.com`）的银行交易动账短信（广发 95508）、微信支付凭证及支付宝出账通知，由本地 NUC 节点上通过 **ArgoCD GitOps** 纳管的 **Flink 计算引擎** 进行定时增量摄入、结构化清洗与元数据规整，写入开放湖仓表格式 **Apache Iceberg** 并持久化沉淀于 **Cloudflare R2** 对象存储中，同时由同驻 NUC 的 **Trino MPP 查询引擎** 提供极致标准的 ANSI SQL 查询与金融审计切片分析体验。

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
+-----------------------------------------+-----------------------------------------+
                                          | 定时触发 / 增量检索 (IMAP over TLS)
                                          v
+-----------------------------------------------------------------------------------+
|  [统一计算与控制底座] Intel NUC (`Nova` · 16GB RAM · 13.4GB 空闲 · x86_64)          |
|  - 集群归属: K3s 业务集群 (`tencent-dp1-cluster` Worker 节点)                     |
|  - 全局治理: 阿里云 ArgoCD 控制面声明式 GitOps 统一纳管 (`my-argocd-manifests`)    |
|                                                                                   |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │  1. 湖仓计算引擎: Apache Flink (ArgoCD 纳管部署 · 轻量 JVM -Xmx2G)          │  |
|  │  - 模式: K3s 批处理 CronJob / Flink Application                             │  |
|  │  - 算子流水: EmailImapSource ➔ RawRecordFormatter ➔ IcebergBatchSink       │  |
|  │  - 可观测性: Flink Web Dashboard (`10.0.1.113:8081` / `flink.jppwl.asia`)  │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
|                                          │ S3A over TLS 直写 (零出网流量费)
|                                          v
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │  2. 湖仓查询引擎: Trino MPP Engine (ArgoCD 纳管部署 · 轻量 JVM -Xmx3G)      │  |
|  │  - 功能: 直读 R2 上 Iceberg 表的 Parquet 列存文件与 Snapshot 快照树         │  |
|  │  - 体验: 开放 ANSI SQL、Time Travel 审计历史切片回溯、DBeaver 交互秒级响应  │  |
|  │  - 可观测性: Trino Web Dashboard (`10.0.1.113:8080` / `trino.jppwl.asia`)  │  |
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

1. **计算与查询双引擎同宿：锁定 Intel NUC (Nova · ArgoCD GitOps 交付)**
   * **决策**：将 **Flink (计算清洗)** 与 **Trino (交互查询)** 双引擎统一纳管在本地 **Intel NUC (`Nova` · 10.0.1.113)** 节点上，由 **ArgoCD GitOps** 实现全自动声明式部署与持续交付。
   * **收益**：
     * **充足闲置资源变现**：NUC 实测拥有 **13.4GB 闲置可用内存**，分配 Flink 2G + Trino 3G，总占用仅 ~5GB，系统仍保留 8GB+ 充裕内存；
     * **标准大数据双看板体验**：同时享有 Flink Web UI (`:8081`) 与 Trino Web UI (`:8080`)，可集成 Kong 网关暴露专属域名；
     * **企业级 GitOps 纪律**：所有 Deployment、Service、ConfigMap 和 Catalog 全部受控于 `my-argocd-manifests` 仓库。

2. **存储与湖仓底座：选定 Cloudflare R2 + Apache Iceberg**
   * **决策**：摒弃传统封闭关系型数据库，全面拥抱 **现代化开放数据湖仓 (Lakehouse)** 架构。选用 **Cloudflare R2** 专属存储桶 `sms-flink-etl` 承载 **Apache Iceberg** 表。
   * **收益**：
     * **免除厂商锁定 (No Vendor Lock-in)**：开放 Parquet 列存标准，对标汇丰金融级 BigLake 湖仓战略；
     * **彻底白嫖出网流量 (Zero Egress)**：Cloudflare R2 提供 10GB 永久免费空间、每月 100 万次写 + 1000 万次读，且出网流量永久 100% 免费；
     * **ACID 快照与时间旅行**：天然支持 Snapshot Isolation 与审计切片回溯，满足金融级数据可回溯性。

3. **CI/CD 流水线职责明确：GitHub Actions 回归标准构建交付**
   * **决策**：GitHub Actions 回归专业 CI/CD 职责——代码提交触发单测 (`mvn test`)、打包 Fat JAR、构建多架构 Docker 镜像推送至容器镜像仓库，并触发 ArgoCD 自动滚动更新 NUC 集群。

4. **机密与凭证隔离：集中收敛于内网单一真理源**
   * **决策**：Gmail 专用密码与 Cloudflare R2 S3 凭据由 K8s Secret 注入 Pod 内存，内网配置全量收敛于私有资产真理源 `cloud_accounts_and_spaces.md`。

---

## 🗂️ 文档与目录导引

* 详细架构设计规格书：[`docs/architecture.md`](docs/architecture.md)
* ArgoCD GitOps 与 NUC 部署运维指南：[`docs/deployment.md`](docs/deployment.md)
* Apache Iceberg / Trino DDL 规范与查询参考：[`docs/schema.sql`](docs/schema.sql)
