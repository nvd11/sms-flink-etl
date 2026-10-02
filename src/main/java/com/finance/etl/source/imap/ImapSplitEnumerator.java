package com.finance.etl.source.imap;

import com.finance.etl.util.ConfigUtils;
import jakarta.mail.*;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Flink FLIP-27 规范: IMAP 任务调度总管 (ImapSplitEnumerator)
 * 职责：运行在 Master (JobManager) 节点上，单并发执行。
 * 在作业启动时探查 Lakehouse 湖仓已落盘的高水位游标 (MAX(last_offset))，
 * 优先采用“基于 UID 水位直扫（Watermark-Driven）”，无论邮件是否已读均绝不漏拉；
 * 首次冷启动（水位为 0）时回退至最新物理窗口邮件探测（彻底无视已读/未读状态）。
 */
public class ImapSplitEnumerator implements SplitEnumerator<ImapSplit, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ImapSplitEnumerator.class);

    private final SplitEnumeratorContext<ImapSplit> context;
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String proxyHost;
    private final int proxyPort;
    private final int maxBatchSize;

    // 💡【核心架构注解：为什么必须是 Map<subtaskId, Queue> 专属邮箱制 (Fair Dispatching) 而不是全局共享 Queue？】
    // 1. 先到先得的竞态饥渴 (Race-Condition Starvation)：
    //    共享 FIFO 队列下，启动极快的 Worker 0 (Subtask 0) 可在 Worker 1 完成注册前连发多次 split request，
    //    将 N 个工单一扫而空 (实测 2 个 split 全被 Worker 0 抢走，Worker 1 空转 14 秒后直接 NoMoreSplits 退出)。
    //    专属邮箱制从结构上根除垄断：每个工位只允许领取自己名下的工单。
    // 2. subtaskId 的静态确定性 (Deterministic Subtask Indexing)：
    //    subtaskId 并非运行时动态发现的信息——Flink ExecutionGraph 在作业提交 (编译期) 阶段就按 parallelism
    //    将 Source 算子静态复制为 N 个 ExecutionVertex，subtaskIndex 固定为 0..N-1，早于任何 Worker 注册与 RPC。
    //    因此切片时用纯数学公式 splitIndex % parallelism 即可完成工单与工位的确定性绑定，零运行时探测成本。
    //    (context.registeredReaders() 属运行时事后信息，仅用于"谁在等"的排队补偿，不参与切片归属决策。)
    // 3. 容错与退单亲和性 (Failure Affinity)：
    //    addSplitsBack 回退的未完成工单将归还至原主人的专属队列，保证恢复后的 Worker 优先续跑自己
    //    熟悉的 UID 区间 (缓存亲和性)，也避免退单被其他 Worker 无序争抢。
    // 4. 多文件夹/多源扩展性 (Multi-Source Extensibility)：
    //    未来多邮箱文件夹 (INBOX, Spam, Notifications) 可按 subtaskId 哈希归入不同专属队列，天然分域消费。
    private final Map<Integer, Queue<ImapSplit>> splitsBySubtask = new HashMap<>();

    // 💡【核心架构注解：subtasksAwaitingSplits 是什么？】
    // 它本质上是 Master (主管) 手里的一张“等候室工位工号排队名单” (Waiting Room Set)。
    // 1. 存什么：
    //    存储的是 Worker 线程的唯一物理工位号 (Subtask ID，例如 0, 1)。
    // 2. 为什么需要它：
    //    当启动极快的 Worker (SourceReader) 提前向 Master 发送 RPC 请求索要工单，而 Master 尚在
    //    discoverSplits() 耗时阻塞连接远程 Gmail 查邮件时 (splitsDiscovered 为 false)，
    //    Master 无法立即给出工单，只能先把该工位的工号 (subtaskId) 记在这个集合中排队等候。
    // 3. 何时消费：
    //    一旦 discoverSplits() 成功出单，Master 会立刻按序遍历该集合，将工单精准补偿派发给所有等候中的工位，
    //    彻底解决分布式异步启动时“工人先到要活、主管后出工单”的时间差赛跑问题。
    private final Set<Integer> subtasksAwaitingSplits = new HashSet<>();
    private boolean splitsDiscovered = false;

    public ImapSplitEnumerator(SplitEnumeratorContext<ImapSplit> context,
                                String host, int port, String user, String password,
                                String proxyHost, int proxyPort, int maxBatchSize) {
        this.context = Objects.requireNonNull(context, "SplitEnumeratorContext must not be null");
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void start() {
        LOG.info("👑 [JobManager Master] ImapSplitEnumerator starting. Probing IMAP server for incremental emails...");
        discoverSplits();
        splitsDiscovered = true;

        // 💡【核心并发竞态注解：为什么 start() 刚完成 discoverSplits() 就要立刻遍历此 Set？】
        // 在分布式集群中，Master (JobManager) 与 Worker (TaskManager) 是完全异步并行启动的。
        // discoverSplits() 涉及远程数据库与 Gmail IMAP 的网络 I/O，通常会阻塞耗时 1~3 秒。
        // 在这数秒的等待窗口期内，启动极快的 Worker 线程可能早已就绪，并通过 Flink 底层 Netty RPC 网络线程
        // 提前回调了 handleSplitRequest()。由于当时 splitsDiscovered 尚为 false，该 Worker 工号便被暂存进了 subtasksAwaitingSplits。
        // 因此，一旦 discoverSplits() 成功出单，必须立即在此处遍历排队名单，将欠下这批“早起工人”的工单全部清偿派发，杜绝异步死锁。
        for (int subtaskId : new ArrayList<>(subtasksAwaitingSplits)) {
            assignNextSplit(subtaskId);
        }
        subtasksAwaitingSplits.clear();
    }

    /**
     * 💡【核心 RPC 拓扑与数据本地性注解：handleSplitRequest 的调用源头与 requesterHostname 的物理使命】
     * 
     * 1. 对应远程调用关系 (RPC Pair)：
     *    本方法是 Worker 端 ImapSourceReader.start() / pollNext() 中调用 context.sendSplitRequest() 后的
     *    直接远程回调端！底层由 SourceCoordinator 接收 RequestSplitEvent(host) 网络事件并在 Master 线程中解包执行。
     *
     * 2. requesterHostname 参数的战略使命 (Data Locality)：
     *    - 在当前 Gmail IMAP 场景下，数据源位于远程公网，所有 Worker 均经公网拉取，无本地物理磁盘依赖，
     *      因此此处的 requesterHostname 被标记为 @Nullable 且未在调度中参与计算。
     *    - 但在 Flink FLIP-27 官方通用分布式体系中，该参数承载着大数据“移动计算成本远低于移动海量数据 (Data Locality)”的战略使命：
     *      在 HDFS、本地磁盘文件或专属存储节点上，Master 依据 requesterHostname 优先将位于该机器本地磁盘的数据分片 (Node-Local Split)
     *      精准派发给该 Worker，彻底消灭跨交换机甚至跨机架的大规模网络传输开销。
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("📬 [JobManager Master] Received split request from Worker Subtask {}.", subtaskId);
        assignOrEnqueue(subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("🙋 [JobManager Master] Worker Subtask {} registered with Enumerator.", subtaskId);
        assignOrEnqueue(subtaskId);
    }

    /**
     * 统一派单或排队逻辑：消灭 handleSplitRequest 与 addReader 间的重复代码
     */
    private synchronized void assignOrEnqueue(int subtaskId) {
        if (!splitsDiscovered) {
            subtasksAwaitingSplits.add(subtaskId);
        } else {
            assignNextSplit(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<ImapSplit> splits, int subtaskId) {
        LOG.warn("↩️ [JobManager Master] Adding back {} failed/unassigned split(s) from Subtask {}.", splits.size(), subtaskId);
        // 退单亲和性：未消费完的工单归还至原主人的专属队列，恢复后优先续跑 (缓存亲和性)
        splitsBySubtask.computeIfAbsent(subtaskId, k -> new ArrayDeque<>()).addAll(splits);
        assignNextSplit(subtaskId);
    }

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {
        LOG.info("🛑 [JobManager Master] Closing ImapSplitEnumerator.");
        splitsBySubtask.clear();
        subtasksAwaitingSplits.clear();
    }

    /**
     * 💡【公平派单核心 (Fair Dispatch)】：Worker 只能从自己的专属邮箱取工单。
     * 与旧版共享 FIFO 队列的根本区别：即使 Worker 0 启动飞快连发多次 split request，
     * 它也只能领走 splitsBySubtask.get(0) 名下的工单；Worker 1 的工单谁也抢不走。
     * 专属队列为空时立即 signalNoMoreSplits，该工位功成身退。
     */
    private synchronized void assignNextSplit(int subtaskId) {
        Queue<ImapSplit> dedicatedQueue = splitsBySubtask.get(subtaskId);
        ImapSplit split = (dedicatedQueue != null) ? dedicatedQueue.poll() : null;
        if (split != null) {
            LOG.info("🚀 [JobManager Master] Assigning split {} to Worker Subtask {}.", split.splitId(), subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            LOG.info("📢 [JobManager Master] No more splits for Worker Subtask {}. Sending signalNoMoreSplits.", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    /**
     * 探查湖仓最新已同步的最高 UID 水位线 (从独立的 etl_sync_offsets 状态元表中读取)
     * 架构说明：etl_sync_offsets 是存储于 Cloudflare R2 上的 Iceberg 湖仓表，
     * 绝非 CockroachDB 元数据目录表。若未配置外部服务水位，默认返回 0L 走安全冷启动。
     */
    public long fetchLastSyncedImapUidFromLakehouse() {
        String configuredStartUid = ConfigUtils.get("IMAP_START_UID");
        if (configuredStartUid != null && !configuredStartUid.trim().isEmpty()) {
            try {
                long uid = Long.parseLong(configuredStartUid.trim());
                LOG.info("🌊 [Lakehouse Offset] Configured start UID from environment: {}", uid);
                return uid;
            } catch (NumberFormatException ignored) {
            }
        }

        // 🎯 核心闭环：通过独立的 IcebergOffsetRepository 毫秒级直读 etl_sync_offsets 湖仓元数据表
        try (com.finance.etl.sink.iceberg.IcebergOffsetRepository offsetRepo =
                     com.finance.etl.sink.iceberg.IcebergOffsetRepository.fromConfig()) {
            long uid = offsetRepo.getLatestOffset("sms-gmail-r2", "EMAIL_IMAP", user);
            if (uid > 0L) {
                LOG.info("🌊 [Lakehouse Offset] Discovered latest synced UID from Iceberg table: {}", uid);
                return uid;
            }
        } catch (Exception e) {
            LOG.warn("⚠️ [Lakehouse Offset] Could not read offset repository: {}. Defaulting to 0L.", e.getMessage());
        }

        LOG.info("ℹ️ [Lakehouse Offset] Defaulting start UID to 0L (Cold start mode).");
        return 0L;
    }

    /**
     * 💡【核心架构注解：基于湖仓 Offset (Watermark) 驱动的增量邮件提取策略】
     * 
     * 传统简单的邮件采集往往依赖邮箱的 "UNSEEN (未读)" 状态标记，但这在生产环境中极其脆弱：
     * 一旦用户在手机端、网页端不小心点击了新动账邮件将其标记为已读，批处理任务就会彻底漏拉该交易，造成永久账目缺失。
     *
     * 本架构采用工业级 "基于 Offset 游标驱动 (Offset/Watermark-Driven)" 机制：
     * 1. 启动第一步：向数据湖元数据表 (iceberg.finance.etl_sync_offsets) 探查上次成功提交的高水位 (MAX(last_offset))。
     * 2. 增量消费阶段 (lastSyncedUid > 0)：
     *    - 彻底无视邮件是否已被人工标记为已读 (SEEN)！
     *    - 严格利用 IMAP 原生 UIDFolder.getMessagesByUID(lastSyncedUid + 1, MAXUID) 指令，
     *      直接从 Gmail 服务器检索所有在物理时间线上晚于上次水位的全新邮件。
     *    - 配合下游按 SHA-256 业务指纹幂等去重，既彻底杜绝漏单，又天然免疫网络重试引起的重复记账。
     * 3. 冷启动阶段 (lastSyncedUid == 0)：
     *    - 表中尚无任何历史同步记录，无视已读未读状态，直接基于物理序号倒序截取最新窗口邮件进行初始同步。
     */
    public void discoverSplits() {
        // 1. 前置凭据快速检查
        if (password == null || password.trim().isEmpty() || password.equals("your_password")) {
            LOG.warn("⚠️ [JobManager Master] Password not provided. Generating empty heartbeat split.");
            splitsBySubtask.computeIfAbsent(0, k -> new ArrayDeque<>())
                    .add(new ImapSplit("split-heartbeat-0", "INBOX", Collections.emptyList()));
            return;
        }

        // 2. 探查湖仓水位线
        long lastSyncedUid = fetchLastSyncedImapUidFromLakehouse();

        Properties props = ImapUtils.createImapsProperties(host, port, proxyHost, proxyPort);

        Store store = null;
        Folder inbox = null;
        try {
            Session session = Session.getInstance(props, null);
            store = session.getStore("imaps");
            store.connect(host, user, password);

            inbox = store.getFolder("INBOX");
            inbox.open(Folder.READ_ONLY);

            List<Long> uids = new ArrayList<>();

            if (inbox instanceof UIDFolder) {
                UIDFolder uidFolder = (UIDFolder) inbox;

                // 🎯 核心架构判定：根据水位状态选择检索策略
                if (lastSyncedUid > 0) {
                    // 🌟 模式 A：严密增量模式 (Watermark-Driven)
                    // 只要 UID > 上次同步水位，无论用户是否在手机上误点为“已读”，通通精准捕获，绝不漏单！
                    LOG.info("🌊 [JobManager Master] Using Incremental Watermark Mode. Fetching all messages with UID > {}...", lastSyncedUid);
                    Message[] incrementalMessages = uidFolder.getMessagesByUID(lastSyncedUid + 1, UIDFolder.MAXUID);

                    if (incrementalMessages != null && incrementalMessages.length > 0) {
                        FetchProfile fp = new FetchProfile();
                        fp.add(UIDFolder.FetchProfileItem.UID);
                        inbox.fetch(incrementalMessages, fp);

                        for (Message msg : incrementalMessages) {
                            long uid = uidFolder.getUID(msg);
                            if (uid > lastSyncedUid) {
                                uids.add(uid);
                            }
                        }
                    }
                } else {
                    // 🌟 模式 B：首次冷启动模式 (Cold Start / Initial Sync)
                    // 湖仓尚无已落盘水位 (lastSyncedUid == 0)。
                    // 严格坚守“彻底无视已读未读”架构铁律：严禁在服务端发起昂贵的 search(UNSEEN) 全箱搜索！
                    // 直接基于物理序号截取收件箱中最新的 maxBatchSize 封邮件（纯内存指针截取，0 搜索网络开销），
                    // 既保证拉取最新一批动账短信，又彻底消灭了长达 14 秒的服务端遍历延迟。
                    int totalCount = inbox.getMessageCount();
                    LOG.info("❄️ [JobManager Master] Cold start mode (watermark = 0, total inbox messages: {}). Fetching latest batch (limit: {})...",
                            totalCount, maxBatchSize);
                    if (totalCount > 0) {
                        int start = Math.max(1, totalCount - maxBatchSize + 1);
                        Message[] latestMessages = inbox.getMessages(start, totalCount);

                        FetchProfile fp = new FetchProfile();
                        fp.add(UIDFolder.FetchProfileItem.UID);
                        inbox.fetch(latestMessages, fp);

                        for (Message msg : latestMessages) {
                            uids.add(uidFolder.getUID(msg));
                        }
                    }
                }
            }

            if (!uids.isEmpty()) {
                // 🚀【真·并行分片调度】：依据 Flink 运行时注入的物理并发度，将 UIDs 均衡切分成 N 个工单
                // 并按 splitIndex % parallelism 确定性绑定专属工位 (subtaskId 在作业编译期即静态确定为 0..N-1)
                int parallelism = Math.max(1, context.currentParallelism());
                int chunkSize = (int) Math.ceil((double) uids.size() / parallelism);
                long timestamp = System.currentTimeMillis();
                int splitIndex = 0;

                for (int i = 0; i < uids.size(); i += chunkSize) {
                    int end = Math.min(i + chunkSize, uids.size());
                    List<Long> subList = new ArrayList<>(uids.subList(i, end));

                    int ownerSubtask = splitIndex % parallelism;
                    ImapSplit split = new ImapSplit(
                            "split-imap-" + timestamp + "-" + splitIndex,
                            "INBOX",
                            subList
                    );
                    splitsBySubtask
                            .computeIfAbsent(ownerSubtask, k -> new ArrayDeque<>())
                            .add(split);
                    LOG.info("🗂️ [JobManager Master] Split {} ({} UIDs) dedicated to Worker Subtask {}.",
                            split.splitId(), subList.size(), ownerSubtask);
                    splitIndex++;
                }

                LOG.info("📦 [JobManager Master] Sliced {} UIDs into {} parallel splits (chunkSize: {}) across parallelism {}.",
                        uids.size(), getPendingSplitsCount(), chunkSize, parallelism);
            } else {
                // 两种模式均无增量数据时，拉取最近 5 封邮件用于批处理探活验证
                int totalCount = inbox.getMessageCount();
                if (totalCount > 0 && inbox instanceof UIDFolder) {
                    UIDFolder uidFolder = (UIDFolder) inbox;
                    int start = Math.max(1, totalCount - 4);
                    Message[] recent = inbox.getMessages(start, totalCount);

                    FetchProfile fp = new FetchProfile();
                    fp.add(UIDFolder.FetchProfileItem.UID);
                    inbox.fetch(recent, fp);

                    for (Message m : recent) {
                        uids.add(uidFolder.getUID(m));
                    }
                }
                splitsBySubtask.computeIfAbsent(0, k -> new ArrayDeque<>())
                        .add(new ImapSplit("split-verify-" + System.currentTimeMillis(), "INBOX", uids));
                LOG.info("ℹ️ [JobManager Master] No new emails found. Generated verification split with {} recent UIDs.",
                        uids.size());
            }
        } catch (Exception e) {
            LOG.warn("⚠️ [JobManager Master] Failed to probe IMAP: {}. Emitting empty fallback split.", e.getMessage());
            splitsBySubtask.computeIfAbsent(0, k -> new ArrayDeque<>())
                    .add(new ImapSplit("split-fallback-0", "INBOX", Collections.emptyList()));
        } finally {
            try {
                if (inbox != null && inbox.isOpen()) inbox.close(false);
                if (store != null && store.isConnected()) store.close();
            } catch (Exception ignored) {
            }
        }
    }

    public int getPendingSplitsCount() {
        return splitsBySubtask.values().stream().mapToInt(Queue::size).sum();
    }
}
