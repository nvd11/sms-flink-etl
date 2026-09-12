# Java Flink on GitHub Actions Cron 自动化部署与运维指南

本文档详细规范在 **`sms-flink-etl`** 仓库中开发一个全新或升级现有 Flink Batch 作业时，基于 **GitHub Actions Cron (Public Repo 终身无限免费)** 的全自动化运行生命周期。

---

## 1. 为什么 Public Repo 选用 GitHub Actions Cron 是终极选择？

1. **🔥 100% 终身免费，无时长上限**：
   * 根据 GitHub 官方政策，Public 仓库的 GitHub Actions 消耗 **0 分钟额度**，完全无需担心账单或配额用尽。
2. **🌍 海外原生网络骨干直连 (Zero-Wall & Zero-Proxy)**：
   * GitHub Actions Runner 位于海外原生云机房，直连海外 Google Gmail IMAP (`imap.gmail.com:993`) 与 AWS 新加坡 CockroachDB，完全不需要像本地家庭宽带那样折腾软路由或科学上网代理，稳定性 100%。
3. **💻 本地硬件彻底解耦 (0 机器负担)**：
   * 本地 NUC、Main PC、K3s 集群、OCI ARM 无需常驻任何进程或分配 Pod，即使您出门在外、电脑全关机，云端定时清洗入库照常毫秒不差地运转。
4. **📊 开箱即用的运维监控面板**：
   * GitHub 界面天生具备执行历史排查、stdout 完整日志查看、运行耗时曲线、邮件失败通知，并支持在 Web 界面一键手动触发 (`workflow_dispatch`)。

---

## 2. 定时工作流规格设计 (`.github/workflows/scheduled-etl.yml`)

在仓库中声明标准定时工作流：

```yaml
name: SMS Flink Batch ETL Scheduled Runner

on:
  schedule:
    # 🎯 核心定时调度：每 4 小时触发执行一次 (UTC 00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
    # 对应北京时间: 08:00, 12:00, 16:00, 20:00, 00:00, 04:00
    - cron: '0 */4 * * *'
  workflow_dispatch: # 🎯 支持在 GitHub 网页上一键手动点击触发 (用于即席对账测试)

concurrency:
  group: sms-flink-etl-cron
  cancel-in-progress: false # 禁止重叠运行

jobs:
  run-etl:
    name: Execute Flink Batch Ingestion & DB Upsert
    runs-on: ubuntu-latest
    timeout-minutes: 5 # 限制单次任务最大 5 分钟超时，防止挂起

    steps:
      - name: Checkout Source Code
        uses: actions/checkout@v4

      - name: Set up JDK 17
        uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: 'temurin'
          cache: 'maven' # 智能缓存 Maven 依赖，加速后续运行

      - name: Run Tests and Build Fat JAR
        run: |
          mvn clean test
          mvn package -DskipTests

      - name: Execute Flink Batch ETL Application
        env:
          GMAIL_IMAP_USER: ${{ secrets.GMAIL_IMAP_USER }}
          GMAIL_IMAP_PASS: ${{ secrets.GMAIL_IMAP_PASS }}
          DB_URL: ${{ secrets.DB_URL }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASS: ${{ secrets.DB_PASS }}
        run: |
          echo "Starting Flink Batch ETL..."
          java -jar target/sms-flink-etl-1.0.0.jar \
            --imap.user "$GMAIL_IMAP_USER" \
            --imap.password "$GMAIL_IMAP_PASS" \
            --db.url "$DB_URL" \
            --db.user "$DB_USER" \
            --db.password "$DB_PASS"
          echo "Flink Batch Job Completed Successfully!"
```

---

## 3. 机密凭证配置清单 (GitHub Secrets)

进入 GitHub 仓库页面：`Settings` ➔ `Secrets and variables` ➔ `Actions` ➔ `New repository secret`：

| Secret 变量名 | 对应机密信息 | 来源说明 |
| :--- | :--- | :--- |
| **`GMAIL_IMAP_USER`** | `alice.h.y.he@gmail.com` | Alice 专属 Gmail 接收邮箱 |
| **`GMAIL_IMAP_PASS`** | `[REDACTED-GMAIL-APP-PASSWORD]` | Google 账号生成的 16 位专用应用密码 |
| **`DB_URL`** | `jdbc:postgresql://brief-titan-32937.j77.aws-ap-southeast-1.cockroachlabs.cloud:26257/finance-db?sslmode=require` | CockroachDB Serverless JDBC 连接串 |
| **`DB_USER`** | `finance_user` | 财务专用独立账号 |
| **`DB_PASS`** | `qqMHLcNdtzrNB1hh` | 财务专用独立账号密码 |

---

## 4. 研发日常迭代与交付流程

1. **开发新 Job 或调优正则**：
   * 在本地修改正则匹配规则（如广发新卡号、微信新模板）；
   * 本地运行单测：`mvn test`；
2. **Git 提交推送**：
   * `git commit -am "feat: support new merchant regex pattern"`；
   * `git push origin main`；
3. **即刻生效**：
   * 下一次 4 小时到达时，GitHub Actions 自动拉取最新代码并在云端执行；
   * 亦可在 GitHub Web 界面点击 **"Run workflow"** 立即进行单次验证！
