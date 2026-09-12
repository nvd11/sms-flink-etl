package com.jppwl.etl.source;

import com.jppwl.etl.model.EmailMessage;
import jakarta.mail.*;
import jakarta.mail.internet.MimeMultipart;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class EmailImapSource extends RichSourceFunction<EmailMessage> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EmailImapSource.class);

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final long pollIntervalMs;

    private volatile boolean isRunning = true;
    private transient ListState<String> processedIdsState;
    private final Set<String> processedMessageIds = new HashSet<>();

    public EmailImapSource(String host, int port, String username, String password, long pollIntervalMs) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("EmailImapSource initialized for account: {}", username);
    }

    @Override
    public void run(SourceContext<EmailMessage> ctx) throws Exception {
        Properties props = new Properties();
        props.put("mail.store.protocol", "imaps");
        props.put("mail.imaps.host", host);
        props.put("mail.imaps.port", String.valueOf(port));
        props.put("mail.imaps.ssl.enable", "true");
        props.put("mail.imaps.timeout", "15000");
        props.put("mail.imaps.connectiontimeout", "15000");

        while (isRunning) {
            Store store = null;
            Folder inbox = null;
            try {
                Session session = Session.getInstance(props);
                store = session.getStore("imaps");
                store.connect(host, port, username, password);

                inbox = store.getFolder("INBOX");
                inbox.open(Folder.READ_ONLY);

                int total = inbox.getMessageCount();
                int fetchCount = Math.min(total, 50); // 每次最多检索最近50封
                int start = Math.max(1, total - fetchCount + 1);

                Message[] messages = inbox.getMessages(start, total);
                for (Message msg : messages) {
                    if (!isRunning) break;

                    String messageId = getMessageId(msg);
                    if (messageId == null) {
                        messageId = "UID-" + msg.getMessageNumber() + "-" + (msg.getReceivedDate() != null ? msg.getReceivedDate().getTime() : System.currentTimeMillis());
                    }

                    synchronized (ctx.getCheckpointLock()) {
                        if (processedMessageIds.contains(messageId)) {
                            continue;
                        }

                        EmailMessage emailMsg = extractEmail(msg, messageId);
                        ctx.collect(emailMsg);
                        processedMessageIds.add(messageId);

                        // 防止内存集合无限膨胀，保留最近10000条ID
                        if (processedMessageIds.size() > 10000) {
                            processedMessageIds.clear();
                            processedMessageIds.add(messageId);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error reading IMAP inbox: {}", e.getMessage(), e);
            } finally {
                if (inbox != null && inbox.isOpen()) {
                    try { inbox.close(false); } catch (Exception ignored) {}
                }
                if (store != null && store.isConnected()) {
                    try { store.close(); } catch (Exception ignored) {}
                }
            }

            if (isRunning) {
                Thread.sleep(pollIntervalMs);
            }
        }
    }

    private String getMessageId(Message msg) {
        try {
            String[] headers = msg.getHeader("Message-ID");
            if (headers != null && headers.length > 0 && headers[0] != null) {
                return headers[0].trim().replaceAll("[<>]", "");
            }
        } catch (Exception ignored) {}
        return null;
    }

    private EmailMessage extractEmail(Message msg, String messageId) throws Exception {
        String subject = msg.getSubject();
        String from = msg.getFrom() != null && msg.getFrom().length > 0 ? msg.getFrom()[0].toString() : "UNKNOWN";
        Date receivedDate = msg.getReceivedDate() != null ? msg.getReceivedDate() : msg.getSentDate();
        LocalDateTime ldt = receivedDate != null ?
                receivedDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime() :
                LocalDateTime.now();

        String body = extractBody(msg);
        return new EmailMessage(messageId, subject, from, ldt, body);
    }

    private String extractBody(Part part) throws Exception {
        if (part.isMimeType("text/plain")) {
            return (String) part.getContent();
        } else if (part.isMimeType("text/html")) {
            String html = (String) part.getContent();
            return html.replaceAll("<br\\s*/?>", "\n").replaceAll("<[^>]+>", "");
        } else if (part.isMimeType("multipart/*")) {
            MimeMultipart mp = (MimeMultipart) part.getContent();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < mp.getCount(); i++) {
                sb.append(extractBody(mp.getBodyPart(i))).append("\n");
            }
            return sb.toString().trim();
        }
        return "";
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        processedIdsState.clear();
        for (String id : processedMessageIds) {
            processedIdsState.add(id);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("processed-email-ids", Types.STRING);
        processedIdsState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            for (String id : processedIdsState.get()) {
                processedMessageIds.add(id);
            }
            LOG.info("Restored {} processed message IDs from Flink Checkpoint", processedMessageIds.size());
        }
    }
}
