package com.finance.etl.source.imap;

import com.finance.etl.model.RawEmail;
import jakarta.mail.*;
import jakarta.mail.internet.MimeMultipart;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Flink FLIP-27 规范: IMAP 邮件读取工人 (ImapSourceReader)
 * 职责：运行在 TaskManager Worker Slot 中，接收来自 Master 的 ImapSplit 工单，
 * 建立与 IMAP 服务器的短连接拉取邮件并解构成 RawEmail 实体，逐条推送给下游 Flink 算子链。
 */
public class ImapSourceReader implements SourceReader<RawEmail, ImapSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(ImapSourceReader.class);

    private final SourceReaderContext context;
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String proxyHost;
    private final int proxyPort;

    private final Queue<ImapSplit> splitsQueue = new ArrayDeque<>();
    private final Queue<RawEmail> bufferedEmails = new ArrayDeque<>();
    private boolean noMoreSplits = false;
    private CompletableFuture<Void> availability = new CompletableFuture<>();

    public ImapSourceReader(SourceReaderContext context,
                            String host, int port, String user, String password,
                            String proxyHost, int proxyPort) {
        this.context = Objects.requireNonNull(context, "SourceReaderContext must not be null");
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
    }

    /**
     * 💡【核心 RPC 拓扑注解：context.sendSplitRequest() 的远程调用映射关系】
     * 
     * 此处是 Worker (TaskManager) 与 Master (JobManager) 之间建立拉模式 (Pull-Based) 调度的触发引信：
     * 1. 物理链路：当前 Worker 线程调用 context.sendSplitRequest() 时，底层 SourceReaderContextImpl 会
     *    自动获取本机主机名并封装为 RequestSplitEvent(host) RPC 事件，通过 Netty / Pekko 发送给 JobManager。
     * 2. 遥控端回调：JobManager 端的 SourceCoordinator 收到该 RPC 事件后，会精准回调触发
     *    ImapSplitEnumerator.handleSplitRequest(int subtaskId, String requesterHostname)。
     * 3. 闭环协作：Worker 借此向 Master 索要工单切片 (ImapSplit)，从而驱动数据流拉取与流转。
     */
    @Override
    public void start() {
        LOG.info("👷 [Worker Slot {}] ImapSourceReader started. Requesting split from JobManager...",
                context.getIndexOfSubtask());
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RawEmail> output) throws Exception {
        // 1. 如果缓冲区内有已解析好的邮件，优先发射一条
        if (!bufferedEmails.isEmpty()) {
            output.collect(bufferedEmails.poll());
            return InputStatus.MORE_AVAILABLE;
        }

        // 2. 如果工单队列非空，取出下一张工单并执行网络读取
        if (!splitsQueue.isEmpty()) {
            ImapSplit split = splitsQueue.poll();
            LOG.info("📋 [Worker Slot {}] Processing split: [ID: {}, Folder: {}, UIDs: {}]",
                    context.getIndexOfSubtask(), split.splitId(), split.getFolderName(), split.getSpecificUids());

            List<RawEmail> fetched = fetchEmailsForSplit(split);
            bufferedEmails.addAll(fetched);

            if (!bufferedEmails.isEmpty()) {
                output.collect(bufferedEmails.poll());
                return InputStatus.MORE_AVAILABLE;
            } else {
                // 本工单未拉取到邮件，若已收到 noMoreSplits 且无待处理工单，则宣告批处理结束
                if (splitsQueue.isEmpty() && noMoreSplits) {
                    LOG.info("🏁 [Worker Slot {}] All splits processed and no more splits incoming. Emitting END_OF_INPUT.",
                            context.getIndexOfSubtask());
                    return InputStatus.END_OF_INPUT;
                }
                return InputStatus.MORE_AVAILABLE;
            }
        }

        // 3. 工单队列为空，若收到领导通告无新工单，宣告作业完工 (END_OF_INPUT)
        if (noMoreSplits) {
            LOG.info("🏁 [Worker Slot {}] No more splits. Terminal state reached.", context.getIndexOfSubtask());
            return InputStatus.END_OF_INPUT;
        }

        // 4. 工单暂时为空但未宣布结束，向 Master 索要工单并等待
        context.sendSplitRequest();
        synchronized (this) {
            if (availability.isDone()) {
                availability = new CompletableFuture<>();
            }
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<ImapSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(splitsQueue);
    }

    @Override
    public synchronized CompletableFuture<Void> isAvailable() {
        if (!bufferedEmails.isEmpty() || !splitsQueue.isEmpty() || noMoreSplits) {
            return CompletableFuture.completedFuture(null);
        }
        return availability;
    }

    @Override
    public synchronized void addSplits(List<ImapSplit> splits) {
        LOG.info("📥 [Worker Slot {}] Received {} new split(s).", context.getIndexOfSubtask(), splits.size());
        splitsQueue.addAll(splits);
        availability.complete(null);
    }

    @Override
    public synchronized void notifyNoMoreSplits() {
        LOG.info("📢 [Worker Slot {}] Received noMoreSplits signal from JobManager.", context.getIndexOfSubtask());
        noMoreSplits = true;
        availability.complete(null);
    }

    @Override
    public void close() throws Exception {
        LOG.info("🛑 [Worker Slot {}] Closing ImapSourceReader and releasing resources.", context.getIndexOfSubtask());
        bufferedEmails.clear();
        splitsQueue.clear();
    }

    /**
     * 💡【核心分布式架构注解：为什么此处 Worker 还要连一次 Gmail？（全链路连了两次 Gmail 的设计权衡）】
     * 
     * 在全链路中，系统确实先后对 Gmail 发起了两次连接：
     * 1. 第一次在 Master (ImapSplitEnumerator): 仅探查增量邮件元数据，抓取纯数字的邮件编号列表 (UIDs)。
     * 2. 第二次在 Worker (此处 fetchEmailsForSplit): 依据工单派发的 UIDs，再次建连下载完整邮件报文体 (Body/Header)。
     * 
     * 之所以采用看似多一次网络握手的“保守派”工业级设计，是基于大数据分布式体系的两大铁律防线：
     * 1. 绝对杜绝 Master (JobManager) 内存雪崩 (OOM 防线):
     *    在大数据与 Flink 规范中，Master 严禁触碰实体大数据（Data/砖头），只能掌管元数据指针（Metadata/账本）。
     *    若全量历史补录积压 10 万封邮件，若 Master 直接抓取邮件全文并塞入 Split，将瞬间挤爆 JobManager 堆内存造成集群崩溃；
     *    而仅抓取 UID 列表（10 万个 Long 仅占不足 1MB），Master 稳如泰山，真正搬运几百兆数据的繁重网络/IO负载被均匀分摊到下游多并发 Worker。
     * 2. Split 跨网络 RPC 与快照存储的轻量化契约:
     *    工单 (ImapSplit) 需要经历序列化并通过 Pekko 网络分发，且需要参与 Flink State Snapshot 持久化。
     *    工单只记录“去哪拉、拉哪几条”，不夹带沉重数据资产，符合分布式高可用与轻量容错契约。
     */
    public List<RawEmail> fetchEmailsForSplit(ImapSplit split) {
        List<RawEmail> emails = new ArrayList<>();
        if (password == null || password.trim().isEmpty() || password.equals("your_password")) {
            LOG.warn("⚠️ [IMAP] Password not provided, skipping network fetch for split {}", split.splitId());
            return emails;
        }

        Properties props = ImapUtils.createImapsProperties(host, port, proxyHost, proxyPort);

        Store store = null;
        Folder folder = null;
        try {
            Session session = Session.getInstance(props, null);
            store = session.getStore("imaps");
            store.connect(host, user, password);

            folder = store.getFolder(split.getFolderName());
            folder.open(Folder.READ_ONLY);

            Message[] messages = new Message[0];
            if (folder instanceof UIDFolder) {
                UIDFolder uidFolder = (UIDFolder) folder;
                if (!split.getSpecificUids().isEmpty()) {
                    long[] uids = split.getSpecificUids().stream().mapToLong(Long::longValue).toArray();
                    messages = uidFolder.getMessagesByUID(uids);
                } else if (split.getStartUid() > 0 && split.getEndUid() >= split.getStartUid()) {
                    messages = uidFolder.getMessagesByUID(split.getStartUid(), split.getEndUid());
                }
            }

            if (messages != null && messages.length > 0) {
                // 🚀【性能王牌优化：正文整包预取】
                // 传统 JavaMail 默认只抓信封，访问 msg.getContent() 时会产生 N 次网络阻塞往返；
                // 此处强制注入 IMAPFolder.FetchProfileItem.MESSAGE 与 BODY[] 预取指令，
                // 让 Gmail 服务器一次性将整批邮件的全部报文体 (Body/Header) 打包下发，将网络往返从 N 次压缩为 1 次！
                FetchProfile fp = new FetchProfile();
                fp.add(FetchProfile.Item.ENVELOPE);
                fp.add(FetchProfile.Item.CONTENT_INFO);
                fp.add(UIDFolder.FetchProfileItem.UID);
                if (folder instanceof com.sun.mail.imap.IMAPFolder) {
                    fp.add(com.sun.mail.imap.IMAPFolder.FetchProfileItem.MESSAGE);
                }
                folder.fetch(messages, fp);

                for (Message msg : messages) {
                    if (msg == null) continue;
                    try {
                        RawEmail rawEmail = new RawEmail();
                        // 🎯 核心规范：使用 uidFolder.getUID(msg) 提取 RFC 3501 永久 UID，严禁使用临时的 getMessageNumber()
                        long permanentUid = (folder instanceof UIDFolder) ? ((UIDFolder) folder).getUID(msg) : msg.getMessageNumber();
                        rawEmail.setImapUid(permanentUid);

                        String[] messageIdHeader = msg.getHeader("Message-ID");
                        rawEmail.setMessageId(messageIdHeader != null && messageIdHeader.length > 0 ?
                                messageIdHeader[0] : UUID.randomUUID().toString());

                        rawEmail.setSubject(msg.getSubject() != null ? msg.getSubject() : "");
                        rawEmail.setFrom(msg.getFrom() != null && msg.getFrom().length > 0 ?
                                msg.getFrom()[0].toString() : "UNKNOWN");
                        rawEmail.setTo(user);

                        rawEmail.setSentAt(msg.getSentDate() != null ? msg.getSentDate().toInstant() : Instant.now());
                        rawEmail.setReceivedAt(msg.getReceivedDate() != null ? msg.getReceivedDate().toInstant() : Instant.now());
                        rawEmail.setFolderName(split.getFolderName());
                        rawEmail.setBody(extractTextFromPart(msg));

                        emails.add(rawEmail);
                    } catch (Exception ex) {
                        LOG.error("❌ Failed to parse message in split: {}", ex.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("❌ [Worker Slot {}] Failed to execute IMAP split fetch for split {}: {}",
                    context.getIndexOfSubtask(), split.splitId(), e.getMessage(), e);
            throw new RuntimeException("IMAP split fetch failed for " + split.splitId(), e);
        } finally {
            try {
                if (folder != null && folder.isOpen()) folder.close(false);
                if (store != null && store.isConnected()) store.close();
            } catch (Exception ignored) {
            }
        }
        return emails;
    }

    private String extractTextFromPart(Part part) throws MessagingException, IOException {
        if (part.isMimeType("text/plain")) {
            return (String) part.getContent();
        }
        if (part.isMimeType("text/html")) {
            String html = (String) part.getContent();
            return html.replaceAll("<[^>]*>", "").trim();
        }
        if (part.isMimeType("multipart/*")) {
            MimeMultipart mimeMultipart = (MimeMultipart) part.getContent();
            StringBuilder result = new StringBuilder();
            int count = mimeMultipart.getCount();
            for (int i = 0; i < count; i++) {
                BodyPart bodyPart = mimeMultipart.getBodyPart(i);
                result.append(extractTextFromPart(bodyPart)).append("\n");
            }
            return result.toString().trim();
        }
        return "";
    }
}
