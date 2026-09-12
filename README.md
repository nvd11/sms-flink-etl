# SMS Flink ETL (个人动账与金融短信轻量级 ETL 架构规划)

基于 **Java Flink (BATCH Mode)** 与 **GitHub Actions Cron (Public Repo 终身无限免费)** 的个人全自动动账记账与流式/批处理 ETL 架构。

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
                                          | 每 4 小时定时触发 (0 */4 * * *)
                                          | 海外原生云网络直连，零墙零代理，毫秒响应
                                          v
+-----------------------------------------------------------------------------------+
|  [调度与算力底座] GitHub Actions Scheduled Runner (Public Repo · 100% 终身免费)    |
|  - 调度器: GitHub Actions Cron Workflow (`.github/workflows/scheduled-etl.yml`)   |
|  - 计算节点: GitHub-hosted Ubuntu Runner (2 vCPU, 7GB RAM, 算完即释放)             |
|  - 资源开销: 本地 NUC / K3s 集群 0 占用，家庭网络即使断网/关机依然稳定运行         |
|                                                                                   |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │  一次性拉起执行: Java Flink (RuntimeExecutionMode.BATCH)                    │  |
|  │  1. Ingestion: 短连接检索 IMAP 增量邮件 (UID > max_uid / UNSEEN)            │  |
|  │  2. FlatMap: 正则提取金额 (Amount)、商户 (Merchant)、卡号 (CardNo)、类别    │  |
|  │  3. Formatting: 组装结构化实体 + JSONB 自由扩展元数据                       │  |
|  │  4. Sink: Flink JDBC Batch Sink 执行幂等 Upsert                             │  |
|  │  5. 退出结算: 进程正常退出 (耗时 10~20 秒)，GitHub 归档执行日志与监控指标    │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
+-----------------------------------------+-----------------------------------------+
                                          | JDBC over TLS 写入 (AWS 新加坡)
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
   * **决策**：个人短信动账属于低频、偶发事件流（每天 5~20 笔）。采用 **每 4 小时调度一次 (一天 6 次)** 的节奏，由 定时任务 动态调起批计算任务，跑完即释放。
   * **收益**：避免 JVM 7x24 空转常驻霸占内存；消解流状态快照维护负担。

2. **数据源摄入：短连接轮询增量拉取**
   * **决策**：任务启动时通过 IMAP 建立 1~2 秒短连接，依据当前数据库内记录的 `MAX(email_uid)` 仅拉取增量或 `UNSEEN` 邮件，抓取完毕即主动断开。

3. **目标数据库选定：CockroachDB Serverless (10GB Always Free)**
   * **决策**：选用已完成探活的 **CockroachDB Serverless**（AWS 新加坡节点，10GB 空间，每月 50M RU 免费额度）。
   * **专用库与账号**：已开辟独立数据库 `"finance-db"` 及业务用户 `finance_user`（目前保持纯净无表，由迁移管理受控初始化）。
   * **SQL 特权**：全面拥抱 PostgreSQL 协议生态，原生支持 `JSONB` 灵活存取各类未知短信元数据，并使用 `ON CONFLICT (email_uid) DO UPDATE` 实现端到端 Exactly-Once 语义。

4. **调度与运行底座：锁定 GitHub Actions Cron (Public Repo 零成本上云)**
   * **决策**：由于本项目为 **公开开源仓库 (Public Repo)**，根据 GitHub 官方政策，GitHub Actions **享受 100% 终身免费、无分钟上限 (Unlimited Minutes)**！
   * **收益**：
     * **本地与集群 0 负担**：本地 NUC 与云端 K3s 不占用任何 CPU、内存或 Pod 额度，本地断电/关机对定时记账完全无影响；
     * **海外原生网络优势**：GitHub Actions Runner 位于海外云机房，拉取海外 Gmail (`imap.gmail.com:993`) 与直连 AWS 新加坡 CockroachDB 属于纯海外骨干网光纤直连，零延迟、零网络墙干扰、零连接重置风险；
     * **开箱即用的运维面板**：GitHub Actions 界面天然自带执行历史日志、运行耗时曲线、邮件失败告警与手工即席触发 (`workflow_dispatch`) 按钮。

5. **机密与凭证隔离：GitHub Repository Secrets**
   * **决策**：代码与 DDL 虽开源公开，但数据库连接串与 Gmail 应用专用密码严格隔离在 GitHub 仓库密钥（Secrets）中，执行时以环境变量安全注入，杜绝凭证泄露。

---

## 🗂️ 文档与目录导引

* 详细架构设计规格书：[`docs/architecture.md`](docs/architecture.md)
* GitHub Actions Cron 部署与运维指南：[`docs/deployment.md`](docs/deployment.md)
* 标准 PostgreSQL / CockroachDB DDL 规范：[`docs/schema.sql`](docs/schema.sql)
