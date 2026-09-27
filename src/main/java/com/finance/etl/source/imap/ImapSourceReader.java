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
     * 针对单个 ImapSplit 建立 IMAP 短连接并拉取指定邮件
     */
    public List<RawEmail> fetchEmailsForSplit(ImapSplit split) {
        List<RawEmail> emails = new ArrayList<>();
        if (password == null || password.trim().isEmpty() || password.equals("your_password")) {
            LOG.warn("⚠️ [IMAP] Password not provided, skipping network fetch for split {}", split.splitId());
            return emails;
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
                FetchProfile fp = new FetchProfile();
                fp.add(FetchProfile.Item.ENVELOPE);
                fp.add(FetchProfile.Item.CONTENT_INFO);
                fp.add(UIDFolder.FetchProfileItem.UID); // 预取真正的 IMAP UID
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
            LOG.warn("⚠️ Failed to execute IMAP split fetch: {}", e.getMessage());
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
