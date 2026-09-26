# Trino on NUC (ArgoCD GitOps) 极简湖仓查询引擎部署规格书

本文档作为 **SMS Flink ETL** 开放数据湖仓体系中 **Trino 查询引擎** 的标准实施蓝图与部署操作指南 (SOP)。详细规范如何在本地 **Intel NUC (`Nova` · 10.0.1.113)** 节点上，通过 **ArgoCD GitOps** 声明式交付一套轻量、高能、直读 **Cloudflare R2 (Apache Iceberg)** 的企业级 SQL 查询控制面。

---

## 1. 架构拓扑与硬件定位

### 1.1 物理节点与集群归属
* **宿主机**: Intel NUC (`Nova` · 局域网 IP: `10.0.1.113`, Tailscale: `100.104.150.19`)
* **硬件规格**: Intel Core i5-7260U (2C4T x86_64) · 16GB RAM · 800GB NVMe
* **内存余量**: **实测当前闲置可用内存高达 13.4GB** (系统基底仅占 2.1GB)
* **K3s 集群身份**: `tencent-dp1-cluster` (业务集群) Worker 节点
* **GitOps 控制面**: 托管于阿里云主节点的 **ArgoCD (`aliyun-k3s`)**，受控仓库为 `my-argocd-manifests`

```text
  [ 终端用户 / DBeaver / Python ] 
                │
                ├─────────────────────────────────────────┐
                ▼ (局域网 HTTP: 10.0.1.113:8080)          ▼ (公网 HTTPS: trino.jppwl.asia)
+─────────────────────────────────────────────────────────+───────────────────────────+
|  本地 Intel NUC (`Nova` · Worker 节点 · tencent-dp1-cluster)                          |
|                                                                                     |
|  [ Kong Gateway ] (HTTPRoute 域名转发 / SSL 终结)                                    |
|          │                                                                          |
|          ▼                                                                          |
|  [ Trino Pod: Coordinator + Worker 合一 ] (Namespace: default)                      |
|  - 镜像: trinodb/trino:latest (官方 x86_64 容器)                                      |
|  - 算力限制: Requests 1C2G / Limits 2C4G (JVM 堆内存 -Xmx3G)                         |
|  - Catalog 挂载: /etc/trino/catalog/iceberg.properties                              |
+─────────────────────────────────────────┬───────────────────────────────────────────+
                                          │ S3A over TLS (Range GET 剪枝直读)
                                          ▼
+─────────────────────────────────────────────────────────────────────────────────────+
|  [云端开放湖仓] Cloudflare R2 (Bucket: "sms-flink-etl" · APAC 亚太机房 · 0 流量费)    |
|  - 表格式: Apache Iceberg (Parquet 列式存储 + Snapshot 快照元数据树)                 |
+─────────────────────────────────────────────────────────────────────────────────────+
```

---

## 2. 算力规划与 JVM 轻量调优

Trino 官方默认针对大厂多 Worker 集群配置（通常默认要求 16GB 堆内存）。在个人与边缘湖仓场景中，必须实施**轻量裁剪**以确保常驻稳定性：

| 参数项 | 企业集群默认值 | 本地 NUC 裁剪值 | 说明 |
| :--- | :--- | :--- | :--- |
| **JVM 堆内存 (`-Xmx`)** | 16G ~ 64G | **`-Xmx3072m` (3GB)** | 满足百万级行内内存聚合与 Hash Join |
| **容器内存限制 (Limits)** | 32GB+ | **`4Gi`** | 预留 1GB 给 OS 及 JVM DirectMemory 堆外内存 |
| **单查询最大内存** | 5GB | **`1.5GB`** | `query.max-memory-per-node=1536MB`，防止恶意 SQL 打满 |
| **节点运行模式** | 分离式 Master-Worker | **单节点协同自驱** | `node-scheduler.include-coordinator=true` |
| **并发任务线程数** | 16 ~ 32 | **`4`** | `task.concurrency=4`，完全适配 NUC 4 线程 CPU |

---

## 3. 配置文件全集规格 (Trino Configuration)

所有配置文件统一由 Kubernetes `ConfigMap` 进行管理并注入容器 `/etc/trino/` 目录。

### 3.1 节点标识 (`node.properties`)
```properties
node.environment=production
node.id=nuc-trino-coordinator-01
node.data-dir=/data/trino
```

### 3.2 JVM 参数 (`jvm.config`)
```properties
-server
-Xmx3072m
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
-Dsun.reflect.inflationThreshold=0
```

### 3.3 核心服务配置 (`config.properties`)
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=2560MB
query.max-memory-per-node=1536MB
query.max-total-memory-per-node=2048MB
discovery.uri=http://localhost:8080
task.concurrency=4
```

### 3.4 Cloudflare R2 Iceberg Catalog (`etc/catalog/iceberg.properties`)
```properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.catalog-name=finance
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://brief-titan-32937.j77.aws-ap-southeast-1.cockroachlabs.cloud:26257/finance-db?sslmode=require
iceberg.jdbc-catalog.connection-user=finance_user
iceberg.jdbc-catalog.connection-password=qqMHLcNdtzrNB1hh
iceberg.jdbc-catalog.default-warehouse-dir=s3://sms-flink-etl/iceberg/warehouse

# S3 协议与 Cloudflare R2 兼容性设置
fs.s3.enabled=true
s3.endpoint=https://8ac25a3a0ac482af1dbd6c65e118693e.r2.cloudflarestorage.com
s3.path-style-access=true
s3.region=auto

# 访问凭据 (从单一真理源 cloud_accounts_and_spaces.md 注入)
s3.aws-access-key=35db4e6908dafcfefb36a4384fc87634
s3.aws-secret-key=aea44e90549d73e4aaadf7de32d02448924fa923ee61eb341b10f789c6233478
```

---

## 4. GitOps K8s 资源清单全貌

所有清单存放于 GitOps 仓库 `my-argocd-manifests` 中的 `infrastructure/trino/` 路径下。

### 4.1 Deployment 声明 (`infrastructure/trino/deployment.yaml`)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trino
  namespace: default
  labels:
    app: trino
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: trino
  template:
    metadata:
      labels:
        app: trino
    spec:
      nodeSelector:
        kubernetes.io/hostname: "nuc" # 核心调度约束：钉在 Intel NUC 节点上
      containers:
        - name: trino-coordinator
          image: trinodb/trino:latest
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
          resources:
            requests:
              cpu: "500m"
              memory: "2Gi"
            limits:
              cpu: "2000m"
              memory: "4Gi"
          readinessProbe:
            httpGet:
              path: /v1/info
              port: 8080
            initialDelaySeconds: 20
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /v1/info
              port: 8080
            initialDelaySeconds: 40
            periodSeconds: 20
          volumeMounts:
            - name: trino-config-volume
              mountPath: /etc/trino
            - name: trino-catalog-volume
              mountPath: /etc/trino/catalog
      volumes:
        - name: trino-config-volume
          configMap:
            name: trino-config
        - name: trino-catalog-volume
          configMap:
            name: trino-catalog-iceberg
```

### 4.2 Service 暴露 (`infrastructure/trino/service.yaml`)
```yaml
apiVersion: v1
kind: Service
metadata:
  name: trino
  namespace: trino
  labels:
    app: trino
spec:
  type: NodePort
  ports:
    - name: http
      port: 8080
      targetPort: 8080
      nodePort: 30880 # 🎯 局域网访问使用 30880 (避免与宿主机 Kodi 8080 冲突)
  selector:
    app: trino
```

### 4.3 Kong HTTPRoute 网关路由 (`infrastructure/trino/httproute.yaml`)
```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: trino-route
  namespace: default
spec:
  parentRefs:
    - name: kong-main-gateway
      namespace: default
  hostnames:
    - "trino.jppwl.asia"
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: trino-service
          port: 8080
```

### 4.4 ArgoCD 统一纳管 Application (`argocd-apps/trino-app.yaml`)
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: trino
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "3"
spec:
  project: default
  source:
    repoURL: 'https://github.com/nvd11/my-argocd-manifests.git'
    path: infrastructure/trino
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
  ignoreDifferences:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      jsonPointers:
        - /spec/parentRefs/0/group
        - /spec/parentRefs/0/kind
        - /status
```

---

## 5. 部署落地与验收验证操作标准 (SOP)

### 步骤 1: 提交通知 GitOps
1. 在 `my-argocd-manifests` 仓库创建 `infrastructure/trino/` 目录并放置上述资源清单；
2. 在 `argocd-apps/` 目录下放置 `trino-app.yaml`；
3. 执行 Git 提交并推送到 GitHub。

### 步骤 2: ArgoCD 自动同步核验
登录 ArgoCD 控制台（`https://argo.jppwl.asia`）或在命令行执行：
```bash
argocd app sync trino
argocd app wait trino --health
```

### 步骤 3: NUC 宿主机 Pod 运行状态探活
通过 SSH 登入 NUC 检查进程与资源：
```bash
# 检查 Pod 是否在 NUC 正常启动
sudo k3s kubectl get pods -l app=trino -o wide

# 检查内存真实消耗情况
free -h
```

### 步骤 4: 湖仓 SQL 连通性测试 (CLI / DBeaver)
1. **浏览器查看控制台**：打开 `http://10.0.1.113:8080`，确认 Active Workers 显示为 `1`；
2. **DBeaver 连线测试**：
   * 驱动选择: `Trino`
   * 主机: `10.0.1.113`，端口: `8080`
   * Catalog: `iceberg`，Schema: `finance`
   * 用户名: `jason`
3. **执行烟雾测试查询**：
   ```sql
   -- 1. 查看表清单
   SHOW TABLES FROM iceberg.finance;

   -- 2. 验证元数据探活
   SELECT * FROM iceberg.finance.raw_sms_records LIMIT 5;
   ```

---

## 6. 常见故障排除与避坑指南 (Troubleshooting)

1. **S3 403 Forbidden / Access Denied**
   * **排查重点**: 检查 `iceberg.properties` 中的 `s3.path-style-access` 是否已强制设置为 `true`。Cloudflare R2 不支持 S3 默认的虚拟主机名桶路由（Virtual-hosted-style），必须开启 Path-style。
2. **JVM 报 OOMKilled (Exit Code 137)**
   * **排查重点**: 确认 K8s `resources.limits.memory` 是否给足了 `4Gi`。如果仅给 `3Gi` 而 JVM 堆开到 `3072m`，堆外内存膨胀会直接触发 Linux cgroup 杀进程。
3. **Iceberg Catalog 元数据指针找不到**
   * **排查重点**: 确保存储桶内根路径存在，首次创建 Schema 时必须显式声明 `WITH (location = 's3://sms-flink-etl/iceberg/finance')`。
