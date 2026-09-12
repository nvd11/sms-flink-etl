# SMS Flink ETL (个人动账流式处理流水线)

[![Flink](https://img.shields.io/badge/Apache%20Flink-1.19%2B-blue.svg)](https://flink.apache.org/)
[![Java](https://img.shields.io/badge/Java-17%20%2F%2021-orange.svg)](https://openjdk.org/)
[![K3s](https://img.shields.io/badge/K3s-Kubernetes-green.svg)](https://k3s.io/)
[![GitOps](https://img.shields.io/badge/GitOps-ArgoCD-red.svg)](https://argoproj.github.io/cd/)

基于 **Java Flink** 与 **Flink on K3s** 的个人全自动实时动账记账与流式 ETL 平台。

从 Alice 专属汇聚邮箱（小米 15 手机端 SmsForwarder 自动转发）实时摄入银行动账短信（95508）、微信支付凭证及支付宝动账通知，经过流式清洗、正则解析抽取与幂等去重，沉淀入库至 Raw 短信明细表。

---

## 🏛️ 系统架构拓扑

```
+-------------------------------------------------------------------+
|  数据源：小米 15 (HyperOS)                                         |
|  - 银行动账短信 (95508)                                            |
|  - 微信/支付宝消费凭证                                             |
+---------------------------------+---------------------------------+
                                  | SmsForwarder 自动转发
                                  v
+---------------------------------+---------------------------------+
|  汇聚中心：Alice Gmail (alice.h.y.he@gmail.com)                   |
+---------------------------------+---------------------------------+
                                  | IMAP / Redis Stream 摄入
                                  v
+---------------------------------+---------------------------------+
|  K3s 业务集群: Flink Cluster (Application Mode / GitOps 托管)     |
|                                                                   |
|  [ Flink JobManager ] <---> [ Flink TaskManager ]                 |
|                                                                   |
|  DataStream 流处理流水线:                                          |
|  1. Source: Email / Redis Stream Source (带 Checkpoint 容错)      |
|  2. Clean & Deduplicate: 基于 email_uid 幂等过滤                  |
|  3. Regex Pattern FlatMap: 提取金额、商户、卡号与动账分类         |
|  4. Sink: Flink JDBC Sink (Upsert into raw_sms_records)           |
+---------------------------------+---------------------------------+
                                  | JDBC 批量/流式写入
                                  v
+---------------------------------+---------------------------------+
|  数据湖/底座: OCI MySQL HeatWave (raw_sms_records) / Neon PG      |
+-------------------------------------------------------------------+
```

---

## 🗄️ 数据模型 (Raw 短信明细表)

详见 [`docs/schema.sql`](docs/schema.sql)。

核心字段涵盖：
* `email_uid`: 邮箱唯一 Message-ID (幂等防重键)
* `source_type`: 数据源类型 (`SMS` / `APP_NOTIFICATION`)
* `sender`: 发送方 (`95508`, `com.tencent.mm` 等)
* `received_at`: 实际交易/短信接收时间
* `raw_content`: 原始全文未清洗报文
* `sms_category`: 动账分类 (`EXPENSE`, `BILL`, `AUTH_CODE`, `NOTICE`)
* `parsed_amount`: 提取金额 (如 `18.00`, `62.00`)
* `parsed_merchant`: 交易商户 (如 `易放停车`, `人民公社饭堂`)
* `parsed_card_no`: 涉及卡号尾号 (如 `3342`)

---

## 🚀 快速上手与编译运行

### 1. 运行单元测试
```bash
mvn test
```

### 2. 构建可执行 Fat JAR
```bash
mvn clean package -DskipTests
```
产物输出路径：`target/sms-flink-etl-1.0.0.jar`

### 3. 本地或远程直接提交 Flink 执行
```bash
java -jar target/sms-flink-etl-1.0.0.jar \
  --imap.user alice.h.y.he@gmail.com \
  --imap.password <APP_PASSWORD> \
  --db.url "jdbc:mysql://100.122.84.84:3306/litellm_db" \
  --db.user litellm_user \
  --db.password <DB_PASSWORD>
```

### 4. 部署至 K3s 集群 (Flink on K3s)
```bash
kubectl apply -f k8s/flink-deployment.yaml
```

---

## 📁 目录结构

```
sms-flink-etl/
├── docs/                      # 架构设计图与数据库 DDL
│   ├── architecture.md
│   └── schema.sql
├── k8s/                       # Flink on K3s 部署清单 (JobManager, TaskManager, Service)
│   └── flink-deployment.yaml
├── src/
│   ├── main/java/com/jppwl/etl/
│   │   ├── SmsFlinkEtlApp.java     # Flink 主流水线入口与 Checkpoint 配置
│   │   ├── model/
│   │   │   ├── EmailMessage.java   # 邮件 DTO
│   │   │   └── SmsRecord.java      # 短信/动账数据实体
│   │   ├── parser/
│   │   │   └── SmsParser.java      # 广发银行/微信/支付宝动账正则抽取器
│   │   ├── sink/
│   │   │   └── SmsJdbcSink.java    # Flink JDBC 幂等写入 Sink
│   │   └── source/
│   │       └── EmailImapSource.java # Flink IMAP 邮件长轮询 Source (含状态恢复)
│   └── test/java/com/jppwl/etl/
│       └── parser/SmsParserTest.java # 动账正则提取全覆盖单元测试
├── Dockerfile                 # 多架构容器构建文件
└── pom.xml                    # Maven 构建文件
```
