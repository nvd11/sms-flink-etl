package com.finance.etl.source.imap;

import com.finance.etl.util.ConfigUtils;
import jakarta.mail.*;
import jakarta.mail.search.FlagTerm;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Flink FLIP-27 规范: IMAP 任务调度总管 (ImapSplitEnumerator)
 * 职责：运行在 Master (JobManager) 节点上，单并发执行。
 * 在作业启动时探查 Lakehouse 湖仓已落盘的高水位游标 (MAX(last_offset))，
 * 优先采用“基于 UID 水位直扫（Watermark-Driven）”，无论邮件是否已读均绝不漏拉；
 * 首次冷启动（水位为 0）时回退至未读邮件与最近窗口探测。
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

    // 💡【核心架构注解：为什么 pendingSplits 必须设计为 Queue 而不是单个变量？】
    // 在当前单邮箱日常轻量增量批处理场景下，单次 discoverSplits() 通常仅产出 1 个工单 (Queue 长度为 1)。
    // 之所以严格采用 Queue<ImapSplit> FIFO 队列结构，是基于 Flink FLIP-27 核心规范的三大必然考量：
    // 1. 容错与退单重试契约 (Fault-Tolerance & addSplitsBack):
    //    当下游某个 Worker (TaskManager) 发生网络抖动或崩溃时，Flink 框架会回调 addSplitsBack(List<ImapSplit>, subtaskId)，
    //    将未消费完毕的多个工单原样退回。必须依赖队列结构才能完整承接退单并重新调度给活着的 Worker。
    // 2. 多并发任务切分支持 (Work Stealing & Concurrency Scaling):
    //    未来大批量历史补录时，discoverSplits() 可按 UID 区间切分成 N 个工单推入队列，供多个 Worker 线程并行争抢消费。
    // 3. 多文件夹/多源扩展性 (Multi-Source Extensibility):
    //    支持未来同时挂载多个邮箱文件夹 (如 INBOX, Spam, Notifications) 生成多工单排队有序消费。
    private final Queue<ImapSplit> pendingSplits = new ArrayDeque<>();

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
        pendingSplits.addAll(splits);
        assignNextSplit(subtaskId);
    }

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {
        LOG.info("🛑 [JobManager Master] Closing ImapSplitEnumerator.");
        pendingSplits.clear();
        subtasksAwaitingSplits.clear();
    }

    private synchronized void assignNextSplit(int subtaskId) {
        if (!pendingSplits.isEmpty()) {
            ImapSplit split = pendingSplits.poll();
            LOG.info("🚀 [JobManager Master] Assigning split {} to Worker Subtask {}.", split.splitId(), subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            LOG.info("📢 [JobManager Master] No more splits for Worker Subtask {}. Sending signalNoMoreSplits.", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    /**
     * 探查湖仓最新已同步的最高 UID 水位线 (从独立的 etl_sync_offsets 状态元表中读取)
     */
    public long fetchLastSyncedImapUidFromLakehouse() {
        String jdbcUri = ConfigUtils.get("ICEBERG_CATALOG_URI");
        String jdbcUser = ConfigUtils.get("ICEBERG_CATALOG_USER");
        String jdbcPassword = ConfigUtils.get("ICEBERG_CATALOG_PASSWORD");

        if (jdbcUri == null || jdbcUri.trim().isEmpty() || !jdbcUri.startsWith("jdbc:")) {
            LOG.info("ℹ️ [Lakehouse Offset] No Iceberg Catalog JDBC URI configured. Defaulting start UID to 0.");
            return 0L;
        }

        String sql = "SELECT COALESCE(MAX(last_offset), 0) AS max_offset FROM etl_sync_offsets WHERE job_name = 'sms-gmail-r2' AND channel = 'EMAIL_IMAP'";
        try (Connection conn = DriverManager.getConnection(jdbcUri, jdbcUser, jdbcPassword);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                long lastUid = rs.getLong("max_offset");
                LOG.info("🌊 [Lakehouse Offset] Discovered highest synced offset from etl_sync_offsets: {}", lastUid);
                return lastUid;
            }
        } catch (Exception e) {
            LOG.warn("⚠️ [Lakehouse Offset] Failed to query max offset from etl_sync_offsets ({}: {}). Defaulting to 0.",
                    e.getClass().getSimpleName(), e.getMessage());
        }
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
     *    - 表中尚无任何历史同步记录，安全降级为按 UNSEEN 未读邮件进行初始全量拉取。
     */
    public void discoverSplits() {
        // 1. 前置凭据快速检查
        if (password == null || password.trim().isEmpty() || password.equals("your_password")) {
            LOG.warn("⚠️ [JobManager Master] Password not provided. Generating empty heartbeat split.");
            pendingSplits.add(new ImapSplit("split-heartbeat-0", "INBOX", Collections.emptyList()));
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
                    // 🌟 模式 B：首次冷启动模式 (Cold Start)
                    // 水位为 0，拉取最近批次的未读 UNSEEN 邮件作为安全启动基线 (默认最多 30 封，防止首次拉取几万封老邮件撑爆)
                    LOG.info("❄️ [JobManager Master] Cold start mode (watermark = 0). Probing unread UNSEEN messages...");
                    Message[] unreadMessages = inbox.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
                    if (unreadMessages != null && unreadMessages.length > 0) {
                        int total = unreadMessages.length;
                        int limit = Math.min(total, 30); // 首次冷启动最多取最新 30 封
                        Message[] trimmed = Arrays.copyOfRange(unreadMessages, total - limit, total);

                        FetchProfile fp = new FetchProfile();
                        fp.add(UIDFolder.FetchProfileItem.UID);
                        inbox.fetch(trimmed, fp);

                        for (Message msg : trimmed) {
                            uids.add(uidFolder.getUID(msg));
                        }
                    }
                }
            }

            if (!uids.isEmpty()) {
                ImapSplit split = new ImapSplit("split-imap-" + System.currentTimeMillis(), "INBOX", uids);
                pendingSplits.add(split);
                LOG.info("📦 [JobManager Master] Generated active split with {} email UIDs (above watermark {}): {}",
                        uids.size(), lastSyncedUid, uids);
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
                ImapSplit split = new ImapSplit("split-verify-" + System.currentTimeMillis(), "INBOX", uids);
                pendingSplits.add(split);
                LOG.info("ℹ️ [JobManager Master] No new emails found. Generated verification split with {} recent UIDs.",
                        uids.size());
            }
        } catch (Exception e) {
            LOG.warn("⚠️ [JobManager Master] Failed to probe IMAP: {}. Emitting empty fallback split.", e.getMessage());
            pendingSplits.add(new ImapSplit("split-fallback-0", "INBOX", Collections.emptyList()));
        } finally {
            try {
                if (inbox != null && inbox.isOpen()) inbox.close(false);
                if (store != null && store.isConnected()) store.close();
            } catch (Exception ignored) {
            }
        }
    }

    public int getPendingSplitsCount() {
        return pendingSplits.size();
    }
}
