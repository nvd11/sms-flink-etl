# SMS Flink ETL (个人动账与金融短信轻量级 ETL 架构规划)

基于 **Java Flink** 与 **K3s Serverless CronJob** 的个人全自动动账记账与批流统一 ETL 架构。

本项目设计并落地将来自手机端（小米 15 HyperOS + SmsForwarder）经网易 163 SMTP 中继汇聚至 Alice Gmail（`alice.h.y.he@gmail.com`）的银行交易动账短信（广发 95508）、微信支付凭证及支付宝出账通知，进行结构化清洗、正则抽取与幂等持久化，沉淀入统一的 Raw 动账明细数仓中。

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
                                          | 每 4 小时短连接拉取 (0 */4 * * *)
                                          | 耗时 1~2 秒，避免 7x24 长连接与断流雪崩
                                          v
+-----------------------------------------------------------------------------------+
|  [调度与弹性算力] K3s 业务集群 (`tencent-dp1-cluster`)                            |
|  - 调度器: Kubernetes CronJob (类比 GCP Dataflow Serverless 弹性理念)             |
|  - 计算节点: 锁定本地 NUC (13G 闲置内存 · nodeSelector: kubernetes.io/hostname=nuc) |
|                                                                                   |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │  临时拉起单 Pod: Java Flink (RuntimeExecutionMode.BATCH)                    │  |
|  │  1. Ingestion: 短连接检索 IMAP 增量邮件 (UID > max_uid / UNSEEN)            │  |
|  │  2. FlatMap: 正则提取金额 (Amount)、商户 (Merchant)、卡号 (CardNo)、类别    │  |
|  │  3. Formatting: 组装结构化实体 + JSONB 自由扩展元数据                       │  |
|  │  4. Sink: Flink JDBC Batch Sink 执行幂等 Upsert                             │  |
|  │  5. 退出下线: 进程 Exit 0，Pod 自动标记 Completed 并回收算力 (0 常驻消耗)   │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
+-----------------------------------------+-----------------------------------------+
                                          | JDBC SSL 写入 (AWS 新加坡)
                                          v
+-----------------------------------------------------------------------------------+
|  [目标数据仓库] CockroachDB Serverless (Always Free 10GB · AWS 新加坡)            |
|  - 数据库名: "finance-db"                                                         |
|  - 专属用户: finance_user                                                         |
|  - 核心表: raw_sms_records (PostgreSQL 14 兼容方言，含 JSONB 与 email_uid 唯一键)  |
+-----------------------------------------------------------------------------------+
```

---

## 💡 最新核心架构决策记录 (ADR Summary)

1. **计算范式：周期性批处理 (BATCH Mode) 替代 7x24 常驻流**
   * **决策**：个人短信动账属于低频、偶发事件流（每天 5~20 笔）。采用 **每 4 小时调度一次 (一天 6 次)** 的节奏，由 K3s CronJob 动态调起单 Pod 批计算任务，跑完即释放。
   * **收益**：避免 JVM 7x24 空转常驻霸占内存；避免邮件协议长连接在网络抖动时导致流引擎频繁 Failover 重启。

2. **数据源摄入：短连接轮询增量拉取**
   * **决策**：Flink 启动时通过 IMAP 建立 1~2 秒短连接，依据当前数据库内记录的 `MAX(email_uid)` 仅拉取增量或 `UNSEEN` 邮件，抓取完毕即主动断开。

3. **目标数据库选定：CockroachDB Serverless (10GB Always Free)**
   * **决策**：舍弃本地自建运维与 MySQL 混合存储，选用已完成探活的 **CockroachDB Serverless**（AWS 新加坡节点，10GB 空间，每月 50M RU 免费额度）。
   * **专用库与账号**：已开辟独立数据库 `"finance-db"` 及业务用户 `finance_user`（目前保持纯净无表，由迁移管理受控初始化）。
   * **SQL 特权**：全面拥抱 PostgreSQL 协议生态，原生支持 `JSONB` 灵活存取各类未知短信元数据，并使用 `ON CONFLICT (email_uid) DO UPDATE` 实现端到端 Exactly-Once 语义。

4. **算力节点选定：锁定本地 NUC 节点 (`kubernetes.io/hostname=nuc`)**
   * **决策**：Flink 批处理 CronJob 钉死在 K3s 集群的 **本地 NUC 节点**（`100.104.150.19`）。
   * **收益**：
     * **13GB 充沛可用内存**：彻底杜绝 JVM 对云端小节点（如腾讯云 4G 控制面）的内存挤压；
     * **原生 x86_64 体系**：无须任何 ARM 交叉编译或指令集兼容性顾虑，Flink 官方镜像开箱即用；
     * **短连接极低风险**：4 小时一次的 1~2 秒短请求，经本地代理环境平稳穿透，无网络掉线顾虑。

5. **部署与运维规范：统一通过 ArgoCD GitOps 声明式交付**
   * **决策**：Flink on K3s 基础设施与 CronJob 清单严禁手工 `kubectl apply`，必须纳管入主人的中央 GitOps 仓库（[`my-argocd-manifests`](https://github.com/nvd11/my-argocd-manifests)），由 ArgoCD 自动轮询与收敛。
   * **收益**：
     * **配置即代码**：与集群内其他核心工作负载（fastapi-svc、quarkus-svc、Kong、Redis）遵循 100% 一致的 GitOps 规范；
     * **自愈与防漂移**：ArgoCD 持续检测集群真实状态与 Git 仓库差异，自动修正任何非预期的配置漂移；
     * **统一控制台与审计**：在 ArgoCD 控制台（`https://argo.jppwl.asia`）统一监控同步健康度，支持秒级一键回滚。

---

## 🗂️ 文档与目录导引

* 详细架构设计与算子流水线：[`docs/architecture.md`](docs/architecture.md)
* Flink Batch 作业部署与 GitOps 交付指南：[`docs/deployment.md`](docs/deployment.md)
* 标准 PostgreSQL / CockroachDB DDL 规范：[`docs/schema.sql`](docs/schema.sql)
