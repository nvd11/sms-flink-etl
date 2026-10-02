# 从 192 秒到 13 秒：Apache Flink 批处理极速调优实战记录

## 1. 背景与技术栈

在轻量化金融级湖仓批处理场景中，我们基于 **Apache Flink 1.19 (FLIP-27 Source API)** 搭建了一套无常驻状态的 Mode B 批处理数据提取任务（`sms-flink-etl`）：

```
[ Gmail IMAP (SmsForwarder) ]
              │
              ▼  (TLS / SOCKS5 代理)
[ Flink 1.19 FLIP-27 Source ] ── (MiniCluster / BATCH 模式)
              │
              ▼
[ FlatMap 实体清洗 & SHA-256 指纹去重 ]
              │
              ▼
[ Cloudflare R2 / Apache Iceberg Lakehouse ]
```

- **执行模式**：Mode B（Run-to-Completion 批处理，单进程内嵌 MiniCluster，算完即毁，零常驻集群成本）。
- **任务目标**：每次定时触发，拉取增量动账报文，完成解包、实体清洗与 SHA-256 指纹提取，批量落盘入湖。
- **初始瓶颈**：在冷启动拉取 89 ~ 200 封邮件时，作业端到端耗时居然高达 **192 秒**。对于一个设计为每小时/每天定时拉起的轻量批任务来说，3分多钟的延迟不仅不可接受，更潜藏着严重的跨洋超时中断风险。

经过四轮针对协议层、调度层、拓扑层和云端网络的深度排查与重构，我们最终将端到端耗时收敛到了 **13.6 秒**（性能提升 **14 倍**），单测试方法耗时压至 **8.9 秒**。

以下是完整的故障排查链路与重构实录。

---

## 2. 优化历程全景对比

| 阶段 | 瓶颈特征 | 根本原因 | 关键改动 | 端到端耗时 |
|---|---|---|---|---|
| **基线 (v0.1)** | 192s (89 封) | JavaMail N+1 串行网络 RTT；默认超时导致假死；错误直连 CockroachDB 探查湖仓表 | 单邮件逐封 `getContent()` | 192.3s |
| **Phase 1** | 32s (89 封) | 跨洋小包高频交互；缓冲区仅 8KB；单次拉取往返过多 | 引入 `FetchProfileItem.MESSAGE` 整包批量预取；调优 1MB Buffer 与 5s 超时 | 32.1s |
| **Phase 2** | 31s (200 封) | 并发调度竞态：Worker 0 极速启动垄断两个工单，Worker 1 空转退出 | 专属邮箱制（Dedicated Mailbox Fair Dispatching）：`splitIndex % parallelism` 确定性绑定 | 31.3s (单Worker) |
| **Phase 3** | 27s (200 封) | **伪并行陷阱**：MiniCluster 默认仅 1 个 Slot，同算子双 Subtask 排队接力（共用 AllocationID） | `TaskManagerOptions.NUM_TASK_SLOTS` 与并行度物理对齐 | 27.1s (真双并发) |
| **Phase 4** | **13.6s** (200 封) | `discoverSplits()` 服务端 `search(UNSEEN)` 全表遍历阻塞 14 秒；本地 SOCKS5 握手抖动 | 废除 `UNSEEN`，改物理序号倒序截取（400ms出单）；CI 云端直连 Google | **13.6s** (本地 8.9s) |

---

## 3. 第一阶段：消灭 JavaMail N+1 次跨洋 RTT（192s -> 32s）

### 3.1 现象与分析
在基线版本中，拉取 89 封邮件耗时接近 3 分 12 秒。通过在 `ImapSourceReader` 中打点发现，大量时间被耗费在以下代码片段中：

```java
// ❌ 灾难性的旧代码：伪“批量”，实则逐封远程交互
Message[] messages = inbox.getMessagesByUID(uids);
for (Message msg : messages) {
    // 每次调用 getContent() 都会向远程 imap.gmail.com:993 发送一次 FETCH (BODY[TEXT]) 请求！
    Object content = msg.getContent(); 
    String body = parseBody(content);
    // ...
}
```

**问题根因**：
1. **JavaMail 的惰性加载陷阱（N+1 RTT）**：`Folder.getMessages()` 或 `getMessagesByUID()` 返回的 `Message[]` 数组本质上仅仅是轻量级的代理句柄（Message Proxy）。当业务逻辑在循环中调用 `msg.getContent()`、`msg.getSubject()` 或 `msg.getHeader()` 时，JavaMail 底层会通过 Socket 发送独立的 IMAP `FETCH` 指令拉取报文体。
2. 跨洋网络（经 SOCKS5 代理访问 Gmail）单次 RTT 约 200~400ms。拉取 89 封邮件意味着至少要进行 `89 * 2 = 178` 次串行 TCP 往返，仅纯网络等待时间就超过 60 秒，若叠加代理丢包重传，耗时直奔 3 分钟。
3. 此外，旧代码在 `fetchLastSyncedImapUidFromLakehouse()` 中误将 Iceberg 湖仓表当作 CockroachDB 的元数据物理表直接尝试 JDBC 查询，由于网络防火墙与连接超时，在启动阶段就白白浪费了数秒等待。

### 3.2 解决方案：整包批量预取与流控参数固化

我们在 `ImapSourceReader` 拉取邮件的核心路径上挂载了 **`IMAPFolder.FetchProfileItem.MESSAGE`**，同时抽离了专职网络配置类 `ImapUtils`：

```java
// src/main/java/com/finance/etl/source/imap/ImapUtils.java
public static Properties createImapsProperties(String host, int port, @Nullable String proxyHost, int proxyPort) {
    Properties props = new Properties();
    props.put("mail.store.protocol", "imaps");
    props.put("mail.imaps.host", host);
    props.put("mail.imaps.port", String.valueOf(port));
    props.put("mail.imaps.ssl.enable", "true");
    
    // 1. 5秒快速超时熔断，杜绝 TCP 假死阻塞 Flink 线程
    props.put("mail.imaps.connectiontimeout", "5000");
    props.put("mail.imaps.timeout", "5000");
    
    // 2. 扩容传输缓冲区至 1MB（默认仅 8KB），适应大邮件一次性倾泻
    props.put("mail.imaps.fetchsize", "1048576");
    
    // 3. 强制整包抓取，严禁拆包片段拉取（Partial Fetch）
    props.put("mail.imaps.partialfetch", "false");
    props.put("mail.imaps.ssl.socketFactory.fallback", "false");

    if (proxyHost != null && !proxyHost.trim().isEmpty()) {
        props.put("mail.imaps.socks.host", proxyHost.trim());
        props.put("mail.imaps.socks.port", String.valueOf(proxyPort));
    }
    return props;
}
```

在 `ImapSourceReader.fetchEmailsForSplit()` 中执行原子级预取：

```java
// src/main/java/com/finance/etl/source/imap/ImapSourceReader.java
FetchProfile fp = new FetchProfile();
fp.add(UIDFolder.FetchProfileItem.UID);
// 核心杀招：挂载 MESSAGE 属性，驱动服务端一次性把 Header + MIME 完整正文打包下推
fp.add(IMAPFolder.FetchProfileItem.MESSAGE);

// 此时只触发 1 次网络批量通信！
inbox.fetch(messages, fp);

// 后续循环中的 getContent() / getSubject() 全部从本地客户端缓存内存读取，0 网络交互
for (Message msg : messages) {
    RawEmail email = parseMessage(msg);
    output.collect(email);
}
```

**效果**：89 封邮件的总处理时间直接从 **192 秒断崖式下降至 32 秒**，提速 6 倍。

---

## 4. 第二阶段：从单 Worker 到并行切片，与竞态派单陷阱（32s -> 31s）

### 4.1 引入真·动态并发分片
当批量增大到 200 封邮件时，单线程跑完需要约 31 秒。为了压榨本地 CPU 与多路并发能力，我们在 Flink 作业中设置 `FLINK_PARALLELISM=2`，并在 `ImapSplitEnumerator` 中引入动态切片算法：

```java
// 切片逻辑：根据 Flink 上下文注入的物理并发度动态切分为 N 张工单
int parallelism = Math.max(1, context.currentParallelism());
int chunkSize = (int) Math.ceil((double) uids.size() / parallelism);

for (int i = 0; i < uids.size(); i += chunkSize) {
    int end = Math.min(i + chunkSize, uids.size());
    List<Long> subList = new ArrayList<>(uids.subList(i, end));
    ImapSplit split = new ImapSplit("split-imap-" + timestamp + "-" + splitIndex, "INBOX", subList);
    pendingSplits.add(split);
    splitIndex++;
}
```

### 4.2 惊魂一刻：Worker 0 极速抢光工单，Worker 1 饥饿空转
配置了并发度 2 之后，测试日志却出现了极度诡异的现象：**200 封邮件依然花了 31 秒，毫无提速！**

翻阅详细日志，发现了 Flink FLIP-27 内部异步启动时的时间差竞态（Race Condition）：
```log
# Worker 0 启动极快，在 Worker 1 尚未完成 RPC 注册前连续索要工单
[SourceCoordinator] Received split request from Worker Subtask 0.
[SourceCoordinator] Assigning split-0 (100 UIDs) to Worker Subtask 0.
[SourceCoordinator] Received split request from Worker Subtask 0.
[SourceCoordinator] Assigning split-1 (100 UIDs) to Worker Subtask 0.  <-- Worker 0 把两个工单全部吃完！

# 14 秒后，Worker 1 终于完成注册并索要工单
[SourceCoordinator] Received split request from Worker Subtask 1.
[SourceCoordinator] No more splits for Worker Subtask 1. Sending signalNoMoreSplits. <-- 扑了个空，直接退出！
```

**根因剖析**：
旧实现采用了全局单一的 `Queue<ImapSplit> pendingSplits` 结构（基于 FIFO 争抢）。在分布式/多线程异步启动场景下，一旦 Worker 0 启动稍快，它可以在 Worker 1 注册进 Master 的等待室前连续发出多次 `sendSplitRequest`，将全局队列扫荡一空。这导致 Worker 1 完全处于饥饿状态，所谓的双并发名存实亡。

### 4.3 解决方案：专属邮箱制（Dedicated Mailbox Fair Dispatching）
为什么切片时可以提前知道归属哪个工位？
**因为 subtaskId 根本不需要运行时动态探测——Flink `ExecutionGraph` 在作业提交期（编译期）就根据并发度 N 静态确定了顶点编号必为 `0, 1, ..., N-1`。**

我们将公共争抢队列推倒，重构成按工位划分的专属邮箱字典：

```java
// src/main/java/com/finance/etl/source/imap/ImapSplitEnumerator.java

// 彻底告别全局共享 Queue，换为 Subtask 专属邮箱
private final Map<Integer, Queue<ImapSplit>> splitsBySubtask = new HashMap<>();

// 1. 切片阶段：纯数学公式静态绑定归属工位
int ownerSubtask = splitIndex % parallelism;
splitsBySubtask.computeIfAbsent(ownerSubtask, k -> new ArrayDeque<>()).add(split);

// 2. 派单阶段：严格实行专属邮箱制，只准领自己名下的工单
private synchronized void assignNextSplit(int subtaskId) {
    Queue<ImapSplit> dedicatedQueue = splitsBySubtask.get(subtaskId);
    ImapSplit split = (dedicatedQueue != null) ? dedicatedQueue.poll() : null;
    if (split != null) {
        LOG.info("🚀 Assigning split {} to Worker Subtask {}.", split.splitId(), subtaskId);
        context.assignSplit(split, subtaskId);
    } else {
        // 属于该工位的工单消耗殆尽，发出终态信号
        LOG.info("📢 No more splits for Worker Subtask {}. Sending signalNoMoreSplits.", subtaskId);
        context.signalNoMoreSplits(subtaskId);
    }
}

// 3. 容错回退阶段：保持亲和性归还原主人
@Override
public void addSplitsBack(List<ImapSplit> splits, int subtaskId) {
    splitsBySubtask.computeIfAbsent(subtaskId, k -> new ArrayDeque<>()).addAll(splits);
    assignNextSplit(subtaskId);
}
```

改造后，Worker 0 即使发 100 次请求，也绝不可能动 Worker 1 名下的工单一根毫毛！

---

## 5. 第三阶段：戳破“伪并行”泡沫——MiniCluster TaskSlot 物理陷阱（31s -> 27s）

### 5.1 抓现场：为什么公平派单后依然跑了 31 秒？
实现了专属邮箱后，本地 JUnit 测试 `SmsGmailR2JobTest` 依然稳定耗时 31.78 秒。
我们抓取了 Flink `ExecutionGraph` 内部任务部署的时序：

```log
2026-10-02 02:29:07.367 INFO ExecutionGraph - Deploying Source ... (1/2) with allocation id 7e3fe8cf1aae05eb300af84cc97b6cad
2026-10-02 02:29:07.545 INFO ImapSourceReader - 👷 [Worker Slot 0] started...
... (Worker 0 处理了 100 封邮件，耗时 5 秒) ...
2026-10-02 02:29:27.530 INFO ImapSourceReader - 🏁 [Worker Slot 0] Terminal state reached.

# 观察下一行的时间戳与 allocation id！！
2026-10-02 02:29:27.547 INFO ExecutionGraph - Deploying Source ... (2/2) with allocation id 7e3fe8cf1aae05eb300af84cc97b6cad
2026-10-02 02:29:27.566 INFO ImapSourceReader - 👷 [Worker Slot 1] started...
```

**决定性铁证**：
1. **时间差高达 20 秒**：Subtask (2/2) 根本不是在作业启动时部署的，它整整等了 20 秒，直到 Worker 0 发出 `END_OF_INPUT` 退出后才部署！
2. **Allocation ID 完全一致**：两个 Subtask 携带的 `allocation id` 竟然都是 `7e3fe8cf1aae05eb300af84cc97b6cad`！

### 5.2 根因定位：Flink 的逻辑并发 vs 物理资源割裂
在 `SmsGmailR2Job` 的最初代码中：
```java
// ❌ 埋雷代码
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(2);
```

- **逻辑层**：`env.setParallelism(2)` 仅仅修改了 JobGraph 顶点的逻辑并发度；
- **物理层**：通过裸方法 `getExecutionEnvironment()` 启动的单机内嵌 MiniCluster，其底层 `TaskExecutor` 默认配置参数 `TaskManagerOptions.NUM_TASK_SLOTS` 值为 **1**！
- **调度契约冲突**：Flink 严格规定，**同一个 Operator Vertex 的多个并行实例（Subtask 0 和 Subtask 1）绝对不允许复用同一个 TaskSlot！**
- **退化为物理排队**：集群只有一个物理 Slot，Subtask 0 抢占了该 Slot；Subtask 1 因无物理资源可用，被挂起在 SlotManager 的等待队列中。直到 Subtask 0 彻底执行完毕释放资源，Subtask 1 才能搬进该 Slot 接着跑。

**所谓的“并行”，在底层变成了彻头彻尾的串行接力赛！**

### 5.3 解决方案：通过 Configuration 强制对齐物理槽位
在获取执行环境时，显式配置 `TaskManagerOptions.NUM_TASK_SLOTS`：

```java
// src/main/java/com/finance/etl/jobs/SmsGmailR2Job.java
int parallelism = ConfigUtils.getInt("FLINK_PARALLELISM", 2);

Configuration flinkConf = new Configuration();
// 🎯 核心修复：强制将本地 TaskManager 的物理 Slot 槽位扩容对齐至并发度
flinkConf.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
env.setParallelism(parallelism);
```

修改后重跑，日志瞬间转正：
```log
02:32:43.118 INFO ExecutionGraph - Deploying Source ... (1/2) with allocation id 3e6b...
02:32:43.125 INFO ExecutionGraph - Deploying Source ... (2/2) with allocation id 8a4c...  <-- 仅相差 7 毫秒，分配给不同物理 Slot！
02:32:43.272 INFO ImapSourceReader - 👷 [Worker Slot 0] started.
02:32:43.272 INFO ImapSourceReader - 👷 [Worker Slot 1] started.                         <-- 真正同毫秒并发启动！
02:32:57.408 INFO ImapSourceReader - 📥 [Worker Slot 0] Received 1 split.
02:32:57.408 INFO ImapSourceReader - 📥 [Worker Slot 1] Received 1 split.                <-- 同一毫秒各领 100 封！
```

---

## 6. 第四阶段：拔除 `search(UNSEEN)` 毒瘤，迈入 13 秒新时代（27s -> 13s）

### 6.1 抓出最后 14 秒长尾：自相矛盾的 `UNSEEN`
在实现了真·双 Slot 并行后，双 Worker 各拉取 100 封邮件的耗时已收敛到 6~9 秒，但整个测试依然要跑 27 秒。
通过排查时间轴发现，在 Master 启动与 Worker 领单之间，存在长达 14 秒的死寂空白期：

```log
02:29:44.609 👑 [JobManager Master] ImapSplitEnumerator starting. Probing IMAP...
02:29:57.268 ❄️ [JobManager Master] Cold start mode (watermark = 0)... 
             └─ 中间整整卡死了 12.66 秒！
```

审查 `discoverSplits()` 代码，发现了严重的架构自相矛盾：
```java
// ❌ 架构设计倒退的毒瘤代码
Message[] unreadMessages = inbox.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
```

**痛点反思**：
- 我们的技术方案明确规定：**“彻底无视邮件是否已读，纯基于湖仓 Offset 水位直扫，配合 SHA-256 业务指纹幂等去重，防止误读漏单”**；
- 然而在冷启动（`watermark = 0`）分支中，却残留了旧思维的 `inbox.search(UNSEEN)`；
- Gmail 邮箱中堆积了数万封邮件，服务端被强迫进行全箱遍历检索未读标记，再通过跨洋代理将巨量匹配项送回，活生生将 `SourceCoordinator` 主事件循环线程阻塞了 **14 秒**！

### 6.2 彻底除根：物理序号倒序截取（0 搜索网络开销）
既然我们根本不关心已读未读，冷启动只需要拉取最新的一批动账报文，为什么要在服务端做昂贵的全量 search？直接利用物理序号（Sequence Number）从收件箱尾部截取：

```java
// src/main/java/com/finance/etl/source/imap/ImapSplitEnumerator.java

// 1. 彻底删除 import jakarta.mail.search.FlagTerm;

// 2. 模式 B：首次冷启动模式 (Cold Start / Initial Sync)
int totalCount = inbox.getMessageCount(); // ⚡ 纯内存元数据读取，0 网络 RTT 开销！
LOG.info("❄️ [JobManager Master] Cold start mode (total inbox messages: {}). Fetching latest batch (limit: {})...",
        totalCount, maxBatchSize);

if (totalCount > 0) {
    // 基于收件箱物理末尾倒序截取最新窗口（纯客户端指针切片，无搜索往返）
    int start = Math.max(1, totalCount - maxBatchSize + 1);
    Message[] latestMessages = inbox.getMessages(start, totalCount);

    FetchProfile fp = new FetchProfile();
    fp.add(UIDFolder.FetchProfileItem.UID);
    inbox.fetch(latestMessages, fp); // 仅需一次极速 UID 批量查询

    for (Message msg : latestMessages) {
        uids.add(uidFolder.getUID(msg));
    }
}
```

**实测性能反差**：
```log
2026-10-02 02:41:39.438 ❄️ Cold start mode (watermark = 0, total inbox messages: 432). Fetching latest batch (limit: 200)...
2026-10-02 02:41:39.841 📦 Sliced 200 UIDs into 2 parallel splits (chunkSize: 100) across parallelism 2.
                         └─ 耗时仅 403 毫秒（从 14 秒骤降至 0.4 秒，提速 35 倍！）
```

---

## 7. 最终战果与 GitHub Actions 云端实测

为了彻底摆脱本地 LAN 代理（`10.0.1.105:7890`）的不可控网络抖动，我们将全部敏感凭据安全移至 GitHub Secrets（`ENV_FILE`），并构建了专属的 GitHub Actions 流水线（`.github/workflows/run-junit-tests.yml`）。

在云端环境直接向 Google IMAP 发起直连拉取，实测数据如下：

```
==================== [JUnit Test Timing Summary] ====================
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.006 s -- in com.finance.etl.ConfigUtilsTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.776 s -- in com.finance.etl.SmsPipelineTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.713 s -- in com.finance.etl.jobs.HelloWorldJobTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13.60 s -- in com.finance.etl.jobs.SmsGmailR2JobTest  <-- 🎯 13.6 秒全流程跑完！
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.934 s -- in com.finance.etl.pipeline.SmsGmailR2PipelineTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.191 s -- in com.finance.etl.source.imap.ImapSourceReaderTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.830 s -- in com.finance.etl.source.imap.ImapSourceTest
... (全量 24 个单测全部通过)
=====================================================================
```

- **`SmsGmailR2JobTest` 耗时**：从原始的 **192 秒** $\rightarrow$ **13.60 秒**（本地好网条件下跑出 **8.91 秒**）；
- **`discoverSplits` UID 探测**：云端直连仅耗时 **49 毫秒**；
- **Worker 并发拉取 100 封邮件**：单 Slot 最快仅耗时 **1.7 秒**；
- **24 个单元与集成测试全部绿灯通过**。

---

## 8. 经验总结与工程排坑要点

1. **JavaMail 绝不可裸循环读取**：
   在任何涉及跨公网 IMAP 协议的生产代码中，必须显式配置 `FetchProfileItem.MESSAGE` 进行批量整包抓取，严防隐式 N+1 RTT 把批处理拉垮。
2. **Flink FLIP-27 派单严防“单共享队列”**：
   异步启动环境下，启动较快的 Worker 线程必然导致工作窃取失衡甚至独占。善用 Flink 编译期确定性的 `subtaskId`，采用**专属邮箱制（`splitIndex % parallelism`）**是保证多工位负载均衡的最坚固防线。
3. **MiniCluster 并发度不等于物理槽位**：
   单机内嵌 MiniCluster 中，`env.setParallelism(N)` 仅改变逻辑拓扑，TaskExecutor 默认依旧只有 1 个 TaskSlot。同一算子的多个子任务严禁共享 Slot，必须显式配置 `TaskManagerOptions.NUM_TASK_SLOTS`，否则多并发必然退化为隐式串行排队。
4. **坚守架构原则，杜绝无意义搜索**：
   既然湖仓管道以 Offset 水位和指纹去重为基石，就绝不要在数据源层发起全量 `search(UNSEEN)`。通过物理序号直接倒序切片，是兼顾吞吐与低开销的最优解。
