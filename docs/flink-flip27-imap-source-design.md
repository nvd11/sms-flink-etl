# Flink FLIP-27 规范: SMS IMAP 自定义 Source 连接器设计规格书

本文档作为 **`sms-flink-etl`** 核心连接器设计的正式规格说明，指导实现严格符合 **Apache Flink FLIP-27 现代流批统一 Source API** 规范的自定义数据源连接器（`ImapSource`）。

---

## 1. 设计目标与学习定位

* **学习定位**：深入探究并彻底掌握 Apache Flink 生产级连接器架构，克服“单机黑盒抽取”的局限，学习 Flink 底层关于分布式任务分片、调度协同、状态快照与字节序列化的核心实现机制。
* **业务定位**：将邮件协议拉取过程（Jakarta Mail over SSL）完全委托给 Flink TaskManager Worker 算子，在批处理（`BATCH`）模式下优雅完成短连接拉取、数据发射（Emit）并自动发送结束信号（`END_OF_INPUT`）。

---

## 2. FLIP-27 核心协作架构时序图

在 Flink 现代 Source 体系中，各组件在 Master（JobManager）与 Worker（TaskManager）之间的交互协作如下：

```text
       【JobManager 端】                                    【TaskManager 端】
   (协调控制面 · 并发度 = 1)                              (数据计算面 · 并发度 = N)
 ┌───────────────────────────┐                         ┌──────────────────────────┐
 │    ImapSplitEnumerator    │                         │     ImapSourceReader     │
 └─────────────┬─────────────┘                         └────────────┬─────────────┘
               │                                                    │
               │ 1. start(): 建立一次性 IMAP 连接                    │
               │    探测未读邮件 UID 列表 (例如: 1~20)                 │
               │    切分为工单 ImapSplit(1~20)                      │
               │                                                    │
               │ 2. addReader(): Worker 启动就绪, 发起拉单请求        │
               │◀───────────────────────────────────────────────────│
               │                                                    │
               │ 3. 派发工单: assignSplit(ImapSplit)                │
               │    (经 ImapSplitSerializer 序列化为 byte[])         │
               │───────────────────────────────────────────────────▶│
               │                                                    │ 4. addSplits(): 接收工单并反序列化
               │ 4. 派发完所有工单后                                  │    建立短连接读取邮件
               │    通知所有 Reader: noMoreSplits()                 │    在 pollNext() 中逐条发射:
               │───────────────────────────────────────────────────▶│    output.collect(RawEmail)
               │                                                    │
               │                                                    │ 5. 邮件读取完毕且收到 noMoreSplits
               │                                                    │    返回 InputStatus.END_OF_INPUT
               │                                                    │    批处理任务正常收工退出
```

---

## 3. 类拓扑与职责设计矩阵

连接器将集中收敛在 **`com.finance.etl.source.imap`** 包下：

```text
com.finance.etl.source.imap
├── ImapSource.java               <-- 顶层门面工厂 (实现 Source<RawEmail, ImapSplit, ImapCheckpoint>)
├── ImapSplit.java                <-- 数据分片工单 POJO (实现 SourceSplit)
├── ImapSplitSerializer.java      <-- 工单二进制版本化编解码器 (实现 SimpleVersionedSerializer<ImapSplit>)
├── ImapSourceReader.java         <-- TaskManager 执行工人 (实现 SourceReader<RawEmail, ImapSplit>)
├── ImapSplitEnumerator.java      <-- JobManager 调度主管 (实现 SplitEnumerator<ImapSplit, Void>)
└── ImapSourceBuilder.java        <-- 链式参数构造器 (便于 Job/Pipeline 注入凭据与代理参数)
```

### 3.1 关键协议规范：Gmail 场景下的 3 种邮件 ID 辨析

在生产环境中处理 Gmail 动账邮件时，存在 3 种截然不同的标识符，架构设计中必须严格界定分工，杜绝混淆：

| 标识符类型 | 协议规范 | 示例值 | 特性与稳定性 | 在系统中的物理分工 |
| :--- | :--- | :--- | :--- | :--- |
| **RFC 3501 IMAP UID** | IMAP4rev1 规范 | `343`, `352` (32位递增整型) | **在当前文件夹（如 INBOX）内单调递增且永久不变**。受 `UIDVALIDITY` 保护。 | **`ImapSplit` 工单调度与增量水位线游标（Offset Cursor）**。用于 Reader 范围扫描与精准点名拉取。*(严禁使用临时的 `getMessageNumber()`)* |
| **RFC 2822 Message-ID** | Internet 邮件头标准 | `<CAGH1234@mail.gmail.com>` | **发信端生成，全球永久唯一**。跨文件夹移动或标记已读均不改变。 | **Lakehouse ODS 表防重键**。结合报文体生成 SHA-256 唯一幂等指纹（`msg_uid`），保障 Exactly-Once 入湖。 |
| **Google X-GM-MSGID** | Gmail IMAP 专有扩展 | `1790514450322` (64位整型) | **Google 底层分布式数据库全局唯一**。对应 Gmail Web 端的网址访问 ID。 | 可选辅助溯源标识，用于对账排查与直接定位 Web 界面邮件。 |

---

### 3.2 详细类职责规范

#### ① 工单分片载体：`ImapSplit`
* **接口**：`implements SourceSplit, Serializable`
* **内部字段**：
  * `String splitId`：分片唯一标识（例如：`split-inbox-0`）；
  * `String folderName`：邮件文件夹名（默认 `"INBOX"`）；
  * `long startUid` / `long endUid`：分片负责的 RFC 3501 IMAP UID 起止边界（历史全量回溯模式下的低/高水位）；
  * `List<Long> specificUids`：具体指定的未读邮件 RFC 3501 IMAP UID 列表（定时增量模式下的核心字段）。
* **核心方法**：
  * `splitId()`：返回唯一 ID。

#### ② 版本化序列化器：`ImapSplitSerializer`
* **接口**：`implements SimpleVersionedSerializer<ImapSplit>`
* **核心方法**：
  * `int getVersion()`：返回版本号（`1`），保证向前兼容；
  * `byte[] serialize(ImapSplit split)`：利用 `DataOutputStream` 将分片字段编码为精简二进制；
  * `ImapSplit deserialize(int version, byte[] bytes)`：利用 `DataInputStream` 从二进制还原分片对象。

#### ③ 任务调度协调官：`ImapSplitEnumerator`
* **接口**：`implements SplitEnumerator<ImapSplit, Void>`
* **所在位置**：JobManager（Master 节点）
* **增量水位机制 (Lakehouse-Native Watermark Discovery · 方案 1)**：
  * 在无常驻内存的批处理（Mode B）下，调度主管启动时**优先读取 Cloudflare R2 上 Iceberg 表的最新元数据**，获取已成功落盘的物理最大时间戳 `MAX(received_at)`；
  * 以此高水位线（配合轻量 Lookback 窗口，如往前宽限 10 分钟）作为本次 IMAP 检索的起始条件，结合 `UNSEEN` 邮件标签，双重杜绝“手抖误读导致的漏拉”或“跨批次重启状态丢失”；
* **核心生命周期**：
  * `start()`：
    1. 探查 Iceberg 湖仓高水位（若首次启动或空表，则采用默认全量/7天回溯策略）；
    2. 建立一次性轻量 IMAP 探查，查询符合水位范围的未读/增量邮件 UID 列表；
    3. 若有匹配邮件，生成单个或多个 `ImapSplit`（携带具体 `specificUids` 或 `startUid~endUid`）放入待分配队列 `pendingSplits`；
    4. 若无增量邮件，生成一条带特殊标记的心跳空工单（保证下游链路保活探测）；
  * `handleSplitRequest(int subtaskId, String requesterHostname)`：当 TaskManager 的 Reader 索要工单时，从队列出队并分配（`context.assignSplit()`）；
  * 当所有分片分配完毕后，向对应 Subtask 发送 `context.signalNoMoreSplits(subtaskId)`。

#### ④ 真实拉取执行人：`ImapSourceReader`
* **接口**：`implements SourceReader<RawEmail, ImapSplit>`
* **所在位置**：TaskManager（Worker Slot）
* **生产级网络吞吐与抗超时调优策略 (Dual-Trophy Network Strategy)**：
  1. **正文批量整包预取 (消灭 N+1 次串行 RTT)**：
     传统 JavaMail 在循环读取 `msg.getContent()` 时会产生多达 N 次跨洋公网请求。本实现强制在 `FetchProfile` 中挂载 `IMAPFolder.FetchProfileItem.MESSAGE` 指令，驱动远程 IMAP 服务端一次性将整批邮件的全部报文体（Body/Header）打包下发，网络请求从 N 次缩减至 1 次；
  2. **短超时与流控保护 (消灭 10 秒假死挂起)**：
     通过专职工具类 `ImapUtils` 将 TCP 连接与读取超时统一收敛至 5 秒（`timeout=5000`），并配置 1MB 预取流式缓冲区（`fetchsize=1048576`）和整包获取（`partialfetch=false`），杜绝因小包频发被 Gmail 服务端掐断连接导致的长时间挂起。
* **核心生命周期**：
  * `start()`：向 Master 发送 `context.sendSplitRequest()` 索要工单；
  * `addSplits(List<ImapSplit> splits)`：接收分配给自己的工单，加入待处理列表；
  * `pollNext(ReaderOutput<RawEmail> output)`：
    * 建立网络短连接（支持 SOCKS5 代理挂载）；
    * 按照工单中的 UID 读取真实邮件，递归解析纯文本正文并装配为 `RawEmail` 实体；
    * 调用 `output.collect(rawEmail)` 吐出给 Flink 算子链；
    * 当所有分配的分片处理完且收到 `noMoreSplits` 信号时，返回 `InputStatus.END_OF_INPUT`，通知批处理节点完成生命周期；
  * `close()`：优雅关闭 IMAP 邮件连接与网络资源。

#### ⑤ 顶层连接器门面：`ImapSource`
* **接口**：`implements Source<RawEmail, ImapSplit, Void>`
* **核心方法**：
  * `getBoundedness()`：返回 `Boundedness.BOUNDED`（显式声明为有界批处理数据源）；
  * `createEnumerator(SplitEnumeratorContext<ImapSplit> enumContext)`：生产 `ImapSplitEnumerator`；
  * `createReader(SourceReaderContext readerContext)`：生产 `ImapSourceReader`；
  * `getSplitSerializer()`：返回 `ImapSplitSerializer` 实例。

---

## 4. Pipeline 与 Job 的优雅调用方式

实现该 Source 后，`SmsGmailR2Pipeline` 将能够使用 100% 官方正统的标准语法组装流图：

```java
// 1. 构建正统 FLIP-27 Source
ImapSource imapSource = ImapSource.builder()
    .host("imap.gmail.com")
    .port(993)
    .user("alice.h.y.he@gmail.com")
    .password(password)
    .proxy("10.0.1.105", 7890)
    .build();

// 2. 正统 env.fromSource 接入 Flink 数据流
DataStream<RawEmail> emailStream = env.fromSource(
    imapSource,
    WatermarkStrategy.noWatermarks(),
    "Gmail-IMAP-FLIP27-Source"
);

// 3. 业务算子链 (FlatMap 转换)
DataStream<SmsRecord> smsStream = emailStream
    .flatMap(new SmsRecordFlatMapParser());
```

---

## 5. 预期收益与工程验证标准

1. **掌握工业级标准**：彻底吃透 Flink FLIP-27 内部两阶段交互、Enumerator 状态与 Reader 事件机制；
2. **极速单测覆盖**：支持通过 Mock Split 或本地邮件 Stub 对 `ImapSourceReader` 和 `ImapSplitEnumerator` 分别进行纯 POJO 单测；
3. **完美融入 Mode B**：在单 Pod MiniCluster 下，Enumerator 与 Reader 协同步调完全闭环，算完即发 `END_OF_INPUT` 优雅退出。
