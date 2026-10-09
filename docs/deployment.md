# Flink + Trino on NUC (ArgoCD GitOps) 全自动部署与运维指南

本文档详细规范在 **`sms-flink-etl`** 仓库中开发一个全新或升级现有 Flink 作业时，基于 **NUC K3s 双引擎 (Flink + Trino)**、**ArgoCD GitOps**、**Cloudflare R2 (Iceberg Lakehouse)** 的全生命周期交付与运维规范。

---

## 1. 整体部署拓扑与资源配置

本地 Intel NUC (`Nova` · 10.0.1.113) 拥有 **16GB 物理内存（实测闲置可用 13.4GB，x86_64 架构）**，作为 K3s 业务集群 (`tencent-dp1-cluster`) 的高算力 Worker 节点，通过 **ArgoCD GitOps** 统一纳管部署两大大数据核心引擎：

```text
                     ┌────────────────────────────────────────┐
                     │ 阿里云控制面: ArgoCD (aliyun-k3s)       │
                     └──────────────────┬─────────────────────┘
                                        │ GitOps 声明式同步监控
                                        ▼
+─────────────────────────────────────────────────────────────────────────────+
|  本地 Intel NUC (`Nova` · 10.0.1.113 · 16GB RAM · 可用 13.4GB)               |
|                                                                             |
|  ┌───────────────────────────────┐     ┌─────────────────────────────────┐  |
|  │ 1. Apache Flink 计算引擎      │     │ 2. Trino MPP 查询引擎           │  |
|  │ - 角色: 动账增量摄入与入湖     │     │ - 角色: 开放 ANSI SQL 交互分析   │  |
|  │ - 资源: 轻量 JVM (-Xmx2G)     │     │ - 资源: 轻量 JVM (-Xmx3G)       │  |
|  │ - 端口: 8081 (Web Dashboard)  │     │ - 端口: 8080 (Web Dashboard)    │  |
|  │ - 域名: flink.jppwl.asia      │     │ - 域名: trino.jppwl.asia        │  |
|  └───────────────┬───────────────┘     └────────────────┬────────────────┘  |
+──────────────────┼──────────────────────────────────────┼───────────────────+
                   │ 增量写入 (Parquet)                    │ 元数据下推直读
                   └──────────────────┬───────────────────┘
                                      ▼ (S3 API over TLS)
+─────────────────────────────────────────────────────────────────────────────+
|  [云端开放湖仓] Cloudflare R2 (`sms-flink-etl` · 10GB Always Free · Zero Egress)  |
+─────────────────────────────────────────────────────────────────────────────+
```

---

## 2. 三级触发与调度全链路配置 (AWS Scheduler ➔ GitHub Actions ➔ NUC Flink)

为了确保调度绝对准时、杜绝本地时钟漂移、且完全免除在 NUC 上常驻任何外部监听服务的负担，系统采用 **云端吹哨 ➔ GitHub 编排 ➔ NUC 落地** 的三级弹性调度体系：

```text
 ┌────────────────────────────────────────────────────────┐
 │ 1. 云端定时吹哨人: AWS EventBridge Scheduler           │
 │ - 区域: ap-southeast-1 (新加坡机房 · 终身 1400万次免费) │
 │ - 规则: cron(0 0,4,8,12,16,20 * * ? *) · Asia/Shanghai │
 │ - 动作: 到点调用 GitHub 官方 API 触发 workflow_dispatch│
 └───────────────────────────┬────────────────────────────┘
                             │ HTTPS POST /dispatches
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │ 2. 调度编排中枢: GitHub Actions Workflow               │
 │ - 宿主: GitHub 官方托管服务器 (7x24 免费公网监听)       │
 │ - 工作流: .github/workflows/trigger-nuc-etl.yml        │
 │ - 动作: 执行运行前预检，向本地 NUC 发送批处理唤醒指令  │
 └───────────────────────────┬────────────────────────────┘
                             │ 唤醒指令 (Webhook over TLS)
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │ 3. 批计算执行端: 本地 Intel NUC (K3s Flink Pod)        │
 │ - 调度: 动态创建 Job/Pod，运行 15 秒拉取入湖并退出      │
 │ - 内存: 算完即归还 100% 内存，真正做到 0 常驻占用      │
 └────────────────────────────────────────────────────────┘
```

### 2.1 AWS EventBridge Scheduler 触发配置规范
* **调度时区**: `Asia/Shanghai`（原生北京时间，1400 万次/月 终身免费配额）
* **调度器清单**:
  1. `test-sms-flink-cron`: `cron(0 0,4,8,12,16,20 * * ? *)` · 每 4 小时触发一次 ODS & DWD 增量入湖批处理
  2. `sms-flink-report-daily`: `cron(0 22 * * ? *)` · 每日 22:00 自动触发 Flink Daily 财务分析与 Slack 推送
  3. `sms-flink-report-weekly`: `cron(0 9 ? * MON *)` · 每周一 09:00 自动触发 Flink Weekly 财务复盘与 Slack 推送
  4. `sms-flink-report-monthly`: `cron(30 9 1 * ? *)` · 每月 1 号 09:30 自动触发 Flink Monthly 财务月度分析与 Slack 推送
* **触发目标 (Target)**: 调用 GitHub 官方工作流触发 API
  * **目标 URL**: `https://api.github.com/repos/nvd11/sms-flink-etl/actions/workflows/trigger-nuc-batch-runner.yml/dispatches`
  * **HTTP Method**: `POST`
  * **Headers**:
    * `Accept: application/vnd.github+json`
    * `Authorization: Bearer <GITHUB_PAT>`
  * **Payload Body (按周期动态传入)**:
    ```json
    {
      "ref": "main",
      "inputs": {
        "target_env": "finance",
        "pipeline_stage": "report",
        "report_period": "DAILY"
      }
    }
    ```

### 2.2 GitHub Actions 调度中枢工作流 (`.github/workflows/trigger-nuc-batch-runner.yml`)
```yaml
name: SMS Flink Batch Orchestration Runner

on:
  workflow_dispatch: # 🎯 由 AWS EventBridge Scheduler 远程调用触发，亦支持网页一键手动补跑
  schedule:
    - cron: '0 */4 * * *' # 🎯 GitHub 自带 Cron 容灾备用兜底

jobs:
  dispatch-to-nuc:
    name: Dispatch Batch Trigger to NUC K3s
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - name: Trigger NUC K3s Flink Batch Execution
        env:
          NUC_WEBHOOK_URL: ${{ secrets.NUC_WEBHOOK_URL }}
          NUC_WEBHOOK_TOKEN: ${{ secrets.NUC_WEBHOOK_TOKEN }}
        run: |
          echo "Sending execution trigger signal to NUC K3s..."
          curl -s -X POST "$NUC_WEBHOOK_URL" \
            -H "Authorization: Bearer $NUC_WEBHOOK_TOKEN" \
            -H "Content-Type: application/json" \
            -d '{"action": "trigger-batch", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'
          echo "Trigger signal acknowledged!"
```

---

## 3. Flink on NUC 部署规格 (ArgoCD GitOps)

在 `my-argocd-manifests` 仓库中声明 Flink 部署清单（支持 K3s CronJob 或 Flink Session 批作业）：

### 3.1 K3s CronJob 调度规格 (`infrastructure/flink-sms-etl/cronjob.yaml`)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: flink-sms-batch-etl
  namespace: default
spec:
  # 🎯 每 4 小时触发一次 (对应北京时间: 08:00, 12:00, 16:00, 20:00, 00:00, 04:00)
  schedule: "0 */4 * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 5
  jobTemplate:
    spec:
      template:
        metadata:
          labels:
            app: flink-sms-etl
        spec:
          nodeSelector:
            kubernetes.io/hostname: nuc  # 锁定 NUC 节点调度
          restartPolicy: OnFailure
          containers:
            - name: flink-batch-job
              image: ghcr.io/nvd11/sms-flink-etl:latest
              imagePullPolicy: IfNotPresent
              resources:
                requests:
                  cpu: "500m"
                  memory: "1Gi"
                limits:
                  cpu: "2000m"
                  memory: "2Gi"
              envFrom:
                - secretRef:
                    name: sms-lakehouse-secrets
              command:
                - java
                - -Xmx1536m
                - -jar
                - /app/sms-flink-etl-1.0.0.jar
```

---

## 4. Trino on NUC 部署规格 (ArgoCD GitOps)

Trino 作为湖仓计算控制面，部署为常驻服务，提供 Web UI 与 JDBC 端口。

### 4.1 Trino Iceberg Catalog 配置 (`infrastructure/trino/catalog-iceberg-cm.yaml`)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-catalog-iceberg
  namespace: default
data:
  iceberg.properties: |
    connector.name=iceberg
    iceberg.file-format=PARQUET

    # Cloudflare R2 S3 兼容连接配置
    fs.native-s3.enabled=true
    s3.endpoint=https://8ac25a3a0ac482af1dbd6c65e118693e.r2.cloudflarestorage.com
    s3.path-style-access=true
    s3.aws-access-key=35db4e6908dafcfefb36a4384fc87634
    s3.aws-secret-key=aea44e90549d73e4aaadf7de32d02448924fa923ee61eb341b10f789c6233478
    s3.region=auto
```

### 4.2 Trino 访问与交互方式
1. **Web UI 面板**：浏览器直接打开 `http://10.0.1.113:8080`（实时查看查询计划、Worker 状态与资源消耗）；
2. **Kong Ingress 公网安全访问**：通过 `https://trino.jppwl.asia`（带 Cloudflare SSL 保护与 SSO 鉴权）；
3. **客户端直接连线 (DBeaver / Python / DataGrip)**：
   * JDBC URL：`jdbc:trino://10.0.1.113:8080/iceberg/finance`
   * 用户名：`jason`（无密码或可选 LDAP/Logto 认证）。

---

## 5. 机密与凭证清单 (K8s Secret)

在 NUC 所在 K3s 集群中创建统一机密对象 `sms-lakehouse-secrets`：

```bash
kubectl create secret generic sms-lakehouse-secrets \
  --from-literal=GMAIL_IMAP_USER="alice.h.y.he@gmail.com" \
  --from-literal=GMAIL_IMAP_PASS="[REDACTED-APP-PASSWORD]" \
  --from-literal=R2_S3_ENDPOINT="https://8ac25a3a0ac482af1dbd6c65e118693e.r2.cloudflarestorage.com" \
  --from-literal=R2_S3_ACCESS_KEY_ID="35db4e6908dafcfefb36a4384fc87634" \
  --from-literal=R2_S3_SECRET_ACCESS_KEY="aea44e90549d73e4aaadf7de32d02448924fa923ee61eb341b10f789c6233478" \
  --from-literal=R2_BUCKET_NAME="sms-flink-etl"
```

> 📌 **注**：以上凭据已永久收敛备份于内网真理源 `cloud_accounts_and_spaces.md`。

---

## 6. 持续交付 CI/CD 流水线 (`.github/workflows/ci.yml`)

GitHub Actions 承担标准持续集成职责：

```yaml
name: SMS Flink Lakehouse CI Pipeline

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: '21'
          distribution: 'temurin'
          cache: 'maven'

      - name: Build and Test
        run: mvn clean package

      - name: Build Docker Image
        run: docker build -t ghcr.io/nvd11/sms-flink-etl:latest .

      - name: Push to GHCR
        run: |
          echo "${{ secrets.GITHUB_TOKEN }}" | docker login ghcr.io -u ${{ github.actor }} --password-stdin
          docker push ghcr.io/nvd11/sms-flink-etl:latest
```

推送成功后，**ArgoCD** 自动感知镜像更新并同步至 NUC 节点，实现真正的无人值守持续交付！
