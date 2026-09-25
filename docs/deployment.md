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

## 2. Flink on NUC 部署规格 (ArgoCD GitOps)

在 `my-argocd-manifests` 仓库中声明 Flink 部署清单（支持 K3s CronJob 或 Flink Session 批作业）：

### 2.1 K3s CronJob 调度规格 (`infrastructure/flink-sms-etl/cronjob.yaml`)

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

## 3. Trino on NUC 部署规格 (ArgoCD GitOps)

Trino 作为湖仓计算控制面，部署为常驻服务，提供 Web UI 与 JDBC 端口。

### 3.1 Trino Iceberg Catalog 配置 (`infrastructure/trino/catalog-iceberg-cm.yaml`)

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

### 3.2 Trino 访问与交互方式
1. **Web UI 面板**：浏览器直接打开 `http://10.0.1.113:8080`（实时查看查询计划、Worker 状态与资源消耗）；
2. **Kong Ingress 公网安全访问**：通过 `https://trino.jppwl.asia`（带 Cloudflare SSL 保护与 SSO 鉴权）；
3. **客户端直接连线 (DBeaver / Python / DataGrip)**：
   * JDBC URL：`jdbc:trino://10.0.1.113:8080/iceberg/finance`
   * 用户名：`jason`（无密码或可选 LDAP/Logto 认证）。

---

## 4. 机密与凭证清单 (K8s Secret)

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

## 5. 持续交付 CI/CD 流水线 (`.github/workflows/ci.yml`)

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
          java-version: '17'
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
