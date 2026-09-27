package com.finance.etl.source.imap;

import jakarta.mail.*;
import jakarta.mail.search.FlagTerm;
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
 * 在作业启动时探查 Gmail IMAP 邮箱未读/增量邮件，生成 ImapSplit 工单，
 * 响应 Worker (TaskManager) 的工单索要请求，并在工单分发完毕后发出 signalNoMoreSplits 终止信号。
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

    private final Queue<ImapSplit> pendingSplits = new ArrayDeque<>();
    private final Set<Integer> readersAwaitingSplits = new HashSet<>();
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

        // 如果在分片探查完成前已有 Worker 注册索取，立即向其派发
        for (int subtaskId : new ArrayList<>(readersAwaitingSplits)) {
            assignNextSplit(subtaskId);
        }
        readersAwaitingSplits.clear();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("📬 [JobManager Master] Received split request from Worker Subtask {}.", subtaskId);
        if (!splitsDiscovered) {
            readersAwaitingSplits.add(subtaskId);
        } else {
            assignNextSplit(subtaskId);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("🙋 [JobManager Master] Worker Subtask {} registered with Enumerator.", subtaskId);
        if (!splitsDiscovered) {
            readersAwaitingSplits.add(subtaskId);
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
        // 批处理模式下无需跨快照持久化 Enumerator 状态
        return null;
    }

    @Override
    public void close() throws IOException {
        LOG.info("🛑 [JobManager Master] Closing ImapSplitEnumerator.");
        pendingSplits.clear();
        readersAwaitingSplits.clear();
    }

    /**
     * 核心派单逻辑：有工单派工单；无工单则向该 Subtask 发送 signalNoMoreSplits
     */
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
     * 探测 IMAP 邮箱并生成工单
     */
    public void discoverSplits() {
        if (password == null || password.trim().isEmpty() || password.equals("your_password")) {
            LOG.warn("⚠️ [JobManager Master] Password not provided. Generating empty heartbeat split.");
            pendingSplits.add(new ImapSplit("split-heartbeat-0", "INBOX", Collections.emptyList()));
            return;
        }

        Properties props = new Properties();
        props.put("mail.store.protocol", "imaps");
        props.put("mail.imaps.host", host);
        props.put("mail.imaps.port", String.valueOf(port));
        props.put("mail.imaps.ssl.enable", "true");
        props.put("mail.imaps.connectiontimeout", "10000");
        props.put("mail.imaps.timeout", "10000");

        if (proxyHost != null && !proxyHost.trim().isEmpty()) {
            props.put("mail.imaps.socks.host", proxyHost.trim());
            props.put("mail.imaps.socks.port", String.valueOf(proxyPort));
        }

        Store store = null;
        Folder inbox = null;
        try {
            Session session = Session.getInstance(props, null);
            store = session.getStore("imaps");
            store.connect(host, user, password);

            inbox = store.getFolder("INBOX");
            inbox.open(Folder.READ_ONLY);

            Message[] unreadMessages = inbox.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
            LOG.info("🔍 [JobManager Master] Probed {} unread messages in Gmail INBOX.", unreadMessages.length);

            List<Long> uids = new ArrayList<>();
            if (inbox instanceof UIDFolder && unreadMessages.length > 0) {
                UIDFolder uidFolder = (UIDFolder) inbox;
                // 限制单批拉取上限，先截取后抓取 UID，避免对上百封邮件做逐个网络往返
                int total = unreadMessages.length;
                int startIdx = Math.max(0, total - maxBatchSize);
                Message[] trimmed = Arrays.copyOfRange(unreadMessages, startIdx, total);

                FetchProfile fp = new FetchProfile();
                fp.add(UIDFolder.FetchProfileItem.UID);
                inbox.fetch(trimmed, fp);

                for (Message msg : trimmed) {
                    uids.add(uidFolder.getUID(msg));
                }
            }

            if (!uids.isEmpty()) {
                // 如果发现未读邮件，生成工单
                ImapSplit split = new ImapSplit("split-imap-" + System.currentTimeMillis(), "INBOX", uids);
                pendingSplits.add(split);
                LOG.info("📦 [JobManager Master] Generated active split with {} email UIDs: {}", uids.size(), uids);
            } else {
                // 若无未读邮件，拉取最近的 5 封进行最新状态核验与探活
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
                LOG.info("ℹ️ [JobManager Master] No unread emails. Generated verification split with {} recent UIDs.", uids.size());
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
