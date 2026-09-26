# Flink on NUC (ArgoCD GitOps) 湖仓计算引擎部署规格书

本文档作为 **SMS Flink ETL** 开放数据湖仓体系中 **Apache Flink 计算引擎** 的标准实施蓝图与部署操作指南 (SOP)。详细规范如何在本地 **Intel NUC (`Nova` · 10.0.1.113)** 节点上，通过 **ArgoCD GitOps** 声明式交付一套轻量、高效、基于 **JDK 21** 运行的 Flink 动账批处理入湖流水线，直写 **Cloudflare R2 (Apache Iceberg)**。

---

## 1. 架构拓扑与运行模式

### 1.1 物理节点与资源定位
* **宿主机**: Intel NUC (`Nova` · 局域网 IP: `10.0.1.113`, Tailscale: `100.104.150.19`)
* **硬件环境**: Intel Core i5-7260U (2C4T x86_64) · 16GB RAM · 800GB NVMe
* **内存余量**: **实测当前闲置可用内存高达 12GB+** (Trino 仅占 960MB)
* **K3s 集群身份**: `tencent-dp1-cluster` (业务集群) Worker 节点
* **GitOps 控制面**: 阿里云 Master 上的 **ArgoCD (`aliyun-k3s`)**，受控仓库为 `my-argocd-manifests`

```text
  [ 小米 15 / SmsForwarder ] 
                │ (163 SMTP 中继)
                ▼
  [ Alice 专属 Gmail 邮箱 (alice.h.y.he@gmail.com) ]
                │
  ┌─────────────┴─────────────────────────────────────────────────────────────┐
  │ ⏰ 触发机制: AWS EventBridge Scheduler (Asia/Shanghai) ➔ GitHub Actions   │
  │              ➔ NUC Webhook 唤醒 Flink 批处理 Job (15 秒即焚)               │
  └─────────────┬─────────────────────────────────────────────────────────────┘
                │ 定时检索 (IMAP over TLS: 993)
                ▼
+─────────────────────────────────────────────────────────────────────────────+
|  本地 Intel NUC (`Nova` · Worker 节点 · tencent-dp1-cluster)                  |
|                                                                             |
|  [ K3s CronJob/Job: flink-sms-batch-etl ] (Namespace: default)              |
|  - 基础镜像: eclipse-temurin:21-jre-jammy (JDK 21 运行时)                    |
|  - 算力限制: Requests 500m/1Gi · Limits 2000m/2Gi (JVM 堆 -Xmx1536m)        |
|  - 算子流水:                                                                |
|    1. EmailImapBatchSource: 增量抓取未读动账邮件                             |
|    2. RawRecordFormatter: 物理元数据规整与时间时区映射                      |
|    3. IcebergBatchSink: 基于 S3A 写入 Parquet 并提交 Snapshot               |
|  - 执行形态: 耗时 10~20 秒，处理完成正常退出并回收所有内存资源               |
+─────────────────────────────────────────┬───────────────────────────────────+
                                          │ S3A over TLS (零出网流量费)
                                          ▼
+─────────────────────────────────────────────────────────────────────────────+
|  [云端开放湖仓] Cloudflare R2 (Bucket: "sms-flink-etl" · APAC 亚太机房)       |
|  - 表格式: Apache Iceberg (Parquet 列式存储 + Snapshot 元数据树)             |
|  - 目录: s3a://sms-flink-etl/iceberg/warehouse/finance/raw_sms_records/      |
+─────────────────────────────────────────────────────────────────────────────+
```

---

## 2. JDK 21 运行时与 JVM 核心参数

本项目全面采用 **JDK 21 (LTS)** 构建与运行。由于 Java 21 对 JDK 内部模块实施了严格封装（Strong Encapsulation），Flink 内存管理与 Hadoop S3A 依赖需要显式开放反射通道：

### 2.1 模块开放参数 (`--add-opens`)
在容器启动 JVM 时强制挂载以下参数，避免 `InaccessibleObjectException`：
```text
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.util.concurrent=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

### 2.2 内存配额规格
* **容器 Limit**: `2Gi` (cgroup 保护阈值)
* **JVM 最大堆内存 (`-Xmx`)**: `1536m`
* **垃圾收集器**: `-XX:+UseG1GC`（快速回收短生命周期的邮件报文对象）

---

## 3. Cloudflare R2 S3A 存储配置集

Flink 的 Iceberg Sink 底层依赖 Hadoop S3A 文件系统协议。在 Flink 配置中注入以下核心参数：

```properties
# Hadoop S3A 核心配置
fs.s3a.endpoint=https://8ac25a3a0ac482af1dbd6c65e118693e.r2.cloudflarestorage.com
fs.s3a.path.style.access=true
fs.s3a.connection.ssl.enabled=true
fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

# R2 S3 凭据 (通过环境变量注入)
fs.s3a.access.key=${R2_S3_ACCESS_KEY_ID}
fs.s3a.secret.key=${R2_S3_SECRET_ACCESS_KEY}

# Iceberg Catalog 配置 (与 Trino 共享 CockroachDB 元数据)
iceberg.catalog.name=finance
iceberg.catalog.type=jdbc
iceberg.catalog.uri=jdbc:postgresql://brief-titan-32937.j77.aws-ap-southeast-1.cockroachlabs.cloud:26257/finance-db?sslmode=require
iceberg.catalog.warehouse=s3a://sms-flink-etl/iceberg/warehouse
```

---

## 4. GitOps K8s 资源清单全貌

所有清单存放于 GitOps 仓库 `my-argocd-manifests` 中的 `infrastructure/flink-sms-etl/` 路径下。

### 4.1 Secret 机密对象声明 (`infrastructure/flink-sms-etl/secret.yaml`)
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: flink-sms-secrets
  namespace: default
type: Opaque
stringData:
  GMAIL_IMAP_USER: "alice.h.y.he@gmail.com"
  GMAIL_IMAP_PASS: "[REDACTED-GMAIL-APP-PASSWORD]"
  R2_S3_ENDPOINT: "https://8ac25a3a0ac482af1dbd6c65e118693e.r2.cloudflarestorage.com"
  R2_S3_ACCESS_KEY_ID: "35db4e6908dafcfefb36a4384fc87634"
  R2_S3_SECRET_ACCESS_KEY: "aea44e90549d73e4aaadf7de32d02448924fa923ee61eb341b10f789c6233478"
  R2_BUCKET_NAME: "sms-flink-etl"
```

### 4.2 K3s CronJob 定时调度声明 (`infrastructure/flink-sms-etl/cronjob.yaml`)
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: flink-sms-batch-etl
  namespace: default
  labels:
    app: flink-sms-etl
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
            kubernetes.io/hostname: "nuc" # 🔒 核心调度约束：必须钉在 Intel NUC 节点上
          restartPolicy: OnFailure
          containers:
            - name: flink-batch-runner
              image: ghcr.io/nvd11/sms-flink-etl:latest
              imagePullPolicy: Always
              resources:
                requests:
                  cpu: "500m"
                  memory: "1Gi"
                limits:
                  cpu: "2000m"
                  memory: "2Gi"
              envFrom:
                - secretRef:
                    name: flink-sms-secrets
              command:
                - java
                - --add-opens=java.base/java.lang=ALL-UNNAMED
                - --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
                - --add-opens=java.base/java.util=ALL-UNNAMED
                - --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
                - --add-opens=java.base/java.nio=ALL-UNNAMED
                - --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
                - -Xmx1536m
                - -jar
                - /app/sms-flink-etl-1.0.0.jar
```

### 4.3 ArgoCD 统一纳管 Application (`argocd-apps/flink-sms-etl-app.yaml`)
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flink-sms-etl
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "4"
spec:
  project: default
  source:
    repoURL: 'https://github.com/nvd11/my-argocd-manifests.git'
    path: infrastructure/flink-sms-etl
    targetRevision: HEAD
  destination:
    name: 'tencent-dp1-cluster'
    namespace: default
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

---

## 5. Docker 镜像打包标准 (`Dockerfile`)

```dockerfile
# 阶段 1: 运行底座使用轻量 JRE 21
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# 安装必要的 ca 证书与时区数据
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai

# 拷贝由 CI 构建完成的 Fat JAR
COPY target/sms-flink-etl-1.0.0.jar /app/sms-flink-etl-1.0.0.jar

USER 1000:1000

ENTRYPOINT ["java", "-jar", "/app/sms-flink-etl-1.0.0.jar"]
```

---

## 6. 部署落地与验收验证操作标准 (SOP)

### 步骤 1: CI 自动构建并推送镜像
推送代码至 GitHub `main` 分支，由 GitHub Actions 自动化触发构建并将镜像推送至 `ghcr.io/nvd11/sms-flink-etl:latest`。

### 步骤 2: GitOps 自动同步
在 `my-argocd-manifests` 提交清单后，ArgoCD 自动在 `tencent-dp1-cluster` 创建 `flink-sms-batch-etl` CronJob。

### 步骤 3: 手动即席触发单次运行测试
在集群控制面或本地执行即席 Job 调试：
```bash
# 从 CronJob 手工创建一次即席测试 Job
sudo k3s kubectl create job --from=cronjob/flink-sms-batch-etl manual-sms-test-01

# 观察 Pod 实时运行日志
sudo k3s kubectl logs -f job/manual-sms-test-01

# 确认退出状态为 Completed
sudo k3s kubectl get pod -l job-name=manual-sms-test-01
```

### 步骤 4: Trino 侧即时联调对账
在 DBeaver 或使用 Trino CLI 执行查询，验证数据是否已原子性入湖：
```sql
SELECT count(*), max(received_at) FROM iceberg.finance.raw_sms_records;
```

---

## 7. 常见故障排除与避坑指南 (Troubleshooting)

1. **`java.lang.reflect.InaccessibleObjectException`**
   * **原因**: Java 21 模块强行封装。
   * **解决**: 确认 CronJob 的 `command` 中已完整挂载 `--add-opens` 六大核心参数。
2. **`AWS S3 403 / Access Denied on S3A`**
   * **原因**: 未配置 Path-Style Access。
   * **解决**: 检查 `fs.s3a.path.style.access` 是否为 `true`。
3. **IMAP 握手超时 (Connection Timeout)**
   * **原因**: NUC 本地家庭网络访问海外 Google IMAP (`imap.gmail.com:993`) 出现丢包。
   * **解决**: 在 JVM 启动参数中配置 SOCKS5 代理：`-DsocksProxyHost=10.0.1.105 -DsocksProxyPort=7890`（直指 Radxa Mihomo 透明代理中心）。
