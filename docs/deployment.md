# Java Flink Batch Job 自动化部署与运维指南 (GitOps CI/CD)

本文档详细规范在 **`sms-flink-etl`** 仓库中开发一个全新或升级现有 Flink Batch 作业时，从代码编写、CI 镜像构建、ArgoCD GitOps 同步到 K3s NUC 节点调度执行的完整生命周期。

---

## 1. 端到端自动化交付全景

```text
[ 开发者 / 统一仓库 ] 
  ├── 编写业务 Job (实现 Flink BATCH Mode)
  ├── 编写单测 (mvn test 验证动账正则抽取)
  └── git push origin main
            │
            ▼
[ 阶段一：GitHub Actions 自动化 CI ]
  ├── 1. 环境校验与依赖拉取 (Java 17 / Maven)
  ├── 2. 自动化单元测试 (SmsParserTest)
  ├── 3. 构建可执行 Fat JAR (包含 Flink、JDBC、PostgreSQL 驱动)
  └── 4. 针对 x86_64 构建原生 Docker 镜像并推送至 GHCR:
         ghcr.io/nvd11/sms-flink-etl:<git-sha> 及 latest
            │
            ▼
[ 阶段二：声明式资源图纸与 GitOps 纳管 ]
  ├── 本仓库 k8s/cronjob.yaml 声明 CronJob 规范:
  │   • 调度周期: 0 */4 * * * (每 4 小时一次)
  │   • 节点绑定: nodeSelector: kubernetes.io/hostname: nuc
  │   • 凭证解耦: 引用 finance-db-secret 与 gmail-imap-secret
  └── 中央 GitOps 仓库 (my-argocd-manifests) 通过 App-of-Apps 监听
            │
            ▼
[ 阶段三：ArgoCD 自动化收敛 (CD) ]
  ├── 持续轮询 Git，发现新 Commit / 镜像版本
  └── 自动将最新 CronJob 清单对齐至 K3s 集群 (自动自愈，状态保持 Synced)
            │
            ▼
[ 阶段四：运行时按需调度与算力释放 (Serverless Lifecycle) ]
  ├── 整点到达 (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
  ├── K3s 在 NUC 节点拉起单 Pod 容器，启动原生 x86 JVM
  ├── 耗时 10~20 秒: 短连接抓邮件 ➔ 正则清洗 ➔ Upsert 写入 CockroachDB
  └── 任务完毕进程 System.exit(0)，Pod 自动进入 Completed，完全归还 CPU/内存！
```

---

## 2. 阶段一：代码开发与自动化 CI 构建

### 2.1 编写新的批处理作业 (Batch Job)
作业应遵循 Flink 批处理标准，使用统一的 `StreamExecutionEnvironment` 并显式声明为 `BATCH` 运行模式：
```java
public class MyNewBatchJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 显式指定批处理模式 (对标 Dataflow 弹性批处理)
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 1. Ingestion: 短连接拉取增量数据
        // 2. Transformation: 算子清洗与特征提取
        // 3. Sink: 写入 CockroachDB finance-db (Upsert 幂等)
        
        env.execute("My-New-Batch-Job");
    }
}
```

### 2.2 GitHub Actions 自动化构建清单 (`.github/workflows/ci.yml`)
每次合并至 `main` 分支时触发：
1. **执行单元测试**：`mvn clean test`；
2. **构建可执行 Fat JAR**：`mvn package -DskipTests`（产出 `sms-flink-etl.jar`）；
3. **构建 Docker 镜像**：基于 `openjdk:17-slim` 或官方 Flink 基础镜像；由于部署节点为本地 Intel NUC，镜像目标平台直接设定为 **`linux/amd64`**，享有原生 x86 指令集最高吞吐；
4. **镜像推送**：推送到 GitHub Packages（`ghcr.io/nvd11/sms-flink-etl:latest`）。

---

## 3. 阶段二：K3s 声明式清单设计 (`k8s/`)

在项目 `k8s/` 目录下维护 Kubernetes 声明式配置：

### 3.1 CronJob 资源声明样例 (`k8s/cronjob.yaml`)
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: sms-flink-batch-etl
  namespace: default
spec:
  schedule: "0 */4 * * *" # 每 4 小时触发执行一次
  concurrencyPolicy: Forbid # 禁止并发重叠执行
  successfulJobsHistoryLimit: 3 # 保留最近 3 次成功 Pod 日志
  failedJobsHistoryLimit: 3 # 保留最近 3 次失败 Pod 日志
  jobTemplate:
    spec:
      template:
        spec:
          # 🎯 核心调度锁定：精确钉死在本地家庭 NUC 节点 (13G 内存充沛区)
          nodeSelector:
            kubernetes.io/hostname: "nuc"
          restartPolicy: OnFailure
          containers:
            - name: flink-etl-job
              image: ghcr.io/nvd11/sms-flink-etl:latest
              imagePullPolicy: Always
              command: ["java", "-cp", "/opt/flink/sms-flink-etl.jar", "com.jppwl.etl.SmsFlinkEtlApp"]
              envFrom:
                - secretRef:
                    name: finance-db-secret # 注入 CockroachDB 账号密码
                - secretRef:
                    name: gmail-imap-secret # 注入 Alice Gmail 授权凭证
              resources:
                requests:
                  memory: "512Mi"
                  cpu: "200m"
                limits:
                  memory: "2Gi" # NUC 闲置内存 13G，配给 2G 充分呼吸，防止 OOM
                  cpu: "1500m"
```

### 3.2 敏感凭证解耦 (Secrets)
数据库明文与邮件应用密码严禁提交进公共仓库或镜像，统一由 K8s Secret 提供注入：
* **`finance-db-secret`**：
  * `DB_URL`: `jdbc:postgresql://brief-titan-32937.j77.aws-ap-southeast-1.cockroachlabs.cloud:26257/finance-db?sslmode=require`
  * `DB_USER`: `finance_user`
  * `DB_PASSWORD`: `qqMHLcNdtzrNB1hh`
* **`gmail-imap-secret`**：
  * `IMAP_HOST`: `imap.gmail.com`
  * `IMAP_USER`: `alice.h.y.he@gmail.com`
  * `IMAP_PASS`: `[REDACTED-GMAIL-APP-KEY]`

---

## 4. 阶段三：中央 ArgoCD GitOps 持续收敛 (CD)

系统严格遵循主人已有的 GitOps 治理规范：

1. **中央托管**：
   在主人的中央配置仓库 [`my-argocd-manifests`](https://github.com/nvd11/my-argocd-manifests) 中的 `argocd-apps/` 目录下增加：
   `argocd-apps/sms-flink-etl-app.yaml`
2. **声明式绑定**：
   ```yaml
   apiVersion: argoproj.io/v1alpha1
   kind: Application
   metadata:
     name: sms-flink-etl
     namespace: argocd
   spec:
     project: default
     source:
       repoURL: 'https://github.com/nvd11/sms-flink-etl.git'
       targetRevision: main
       path: k8s
     destination:
       server: 'https://kubernetes.default.svc'
       namespace: default
     syncPolicy:
       automated:
         prune: true
         selfHeal: true
   ```
3. **零手工运维**：
   集群任何变动均以 Git 为唯一真理源。ArgoCD 自动轮询并将 NUC 节点上的工作负载收敛至 Git 声明，生产环境严禁直接运行 `kubectl apply`。
4. **控制台可观测性**：
   可通过主人的 ArgoCD Web 控制台（`https://argo.jppwl.asia`）实时查验拓扑健康度与同步状态。

---

## 5. 阶段四：横向扩展新 Job 的最佳实践 (Multi-Job Extension)

当需要为系统添加新的分析任务（例如：`MonthlyBillSummaryJob` 每月对账任务，或者 `SuspiciousTransactionAlertJob` 异动告警任务）时：

1. **共享 Fat JAR**：
   在同一个代码工程内编写新的主类，复用通用的邮件连接器、正则解析器与 CockroachDB 数据模型，Maven 打包输出包含所有类。
2. **新增 CronJob 清单**：
   在 `k8s/` 目录下新增对应的 YAML（如 `k8s/monthly-summary-cronjob.yaml`）：
   * 仅需修改 `schedule: "0 2 1 * *"`（每月 1 号凌晨 2 点执行）；
   * 修改启动命令的类名：`command: ["java", "-cp", "/opt/flink/sms-flink-etl.jar", "com.jppwl.etl.jobs.MonthlyBillSummaryJob"]`；
   * 保持 `nodeSelector: kubernetes.io/hostname: "nuc"`。
3. **Git Commit & Push**：
   提交到仓库后，ArgoCD 自动在 K3s 集群中拉起第二套定时 Cron 机制，多个 Job 相互独立、互不干扰、按需调度！
