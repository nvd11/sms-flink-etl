package com.finance.etl.reader;

import com.finance.etl.model.RawEmail;
import com.finance.etl.util.ConfigUtils;
import jakarta.mail.*;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.search.FlagTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * IMAP 协议交互实体对象 (GmailImapReader)
 * 职责：负责以短连接方式安全连接 Gmail IMAP 服务器，获取未读邮件并递归解析为纯文本 RawEmail DTO。
 * 纯 Java 实现，脱离 Flink 流计算运行时，支持通过构造函数或 ConfigUtils 注入配置。
 */
public class GmailImapReader {
    private static final Logger LOG = LoggerFactory.getLogger(GmailImapReader.class);

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String proxyHost;
    private final int proxyPort;
    private final int maxBatchSize;

    public GmailImapReader(String host, int port, String user, String password,
                           String proxyHost, int proxyPort, int maxBatchSize) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * 工厂方法：从环境变量与 .env 配置中初始化 Reader
     */
    public static GmailImapReader fromConfig() {
        String host = ConfigUtils.get("GMAIL_IMAP_HOST", "imap.gmail.com");
        int port = ConfigUtils.getInt("GMAIL_IMAP_PORT", 993);
        String user = ConfigUtils.get("GMAIL_IMAP_USER", "alice.h.y.he@gmail.com");
        String password = ConfigUtils.get("GMAIL_IMAP_PASS", ConfigUtils.get("GMAIL_IMAP_PASSWORD", ""));
        String proxyHost = ConfigUtils.get("IMAP_PROXY_HOST");
        int proxyPort = ConfigUtils.getInt("IMAP_PROXY_PORT", 7890);
        int maxBatchSize = ConfigUtils.getInt("IMAP_MAX_BATCH_SIZE", 20);

        return new GmailImapReader(host, port, user, password, proxyHost, proxyPort, maxBatchSize);
    }

    /**
     * 拉取未读邮件并转换为 RawEmail 实体列表 (短连接，读取完毕后立即断开网络)
     */
    public List<RawEmail> fetchEmails() {
        List<RawEmail> emailList = new ArrayList<>();

        if (password == null || password.trim().isEmpty() || password.equals("your_password")) {
            LOG.warn("⚠️ [IMAP] Gmail password not provided, skipping remote IMAP fetch.");
            return emailList;
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
            LOG.info("🌐 [IMAP] Connecting via SOCKS5 proxy: {}:{}", proxyHost, proxyPort);
        }

        Store store = null;
        Folder inbox = null;
        try {
            Session session = Session.getInstance(props, null);
            store = session.getStore("imaps");

            LOG.info("🔌 [IMAP] Connecting to {}:{} as {}...", host, port, user);
            store.connect(host, user, password);

            inbox = store.getFolder("INBOX");
            inbox.open(Folder.READ_ONLY); // 以只读模式安全读取，保证无侵入

            // 搜索所有未读邮件 (UNSEEN)
            Message[] messages = inbox.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
            LOG.info("📬 [IMAP] Found {} unread messages in INBOX.", messages.length);

            // 限制单批拉取上限，防止网络超时
            if (messages.length > maxBatchSize) {
                LOG.info("✂️ Limiting current batch to latest {} messages.", maxBatchSize);
                Message[] trimmed = new Message[maxBatchSize];
                System.arraycopy(messages, messages.length - maxBatchSize, trimmed, 0, maxBatchSize);
                messages = trimmed;
            }

            // 如果没有未读邮件，拉取最近的 5 封进行最新状态核验
            if (messages.length == 0) {
                int totalCount = inbox.getMessageCount();
                int start = Math.max(1, totalCount - 4);
                if (totalCount > 0) {
                    messages = inbox.getMessages(start, totalCount);
                    LOG.info("ℹ️ [IMAP] No unread messages, inspecting latest {} messages for verification.", messages.length);
                }
            }

            for (Message msg : messages) {
                try {
                    RawEmail rawEmail = new RawEmail();
                    rawEmail.setImapUid((long) msg.getMessageNumber());

                    String[] messageIdHeader = msg.getHeader("Message-ID");
                    rawEmail.setMessageId(messageIdHeader != null && messageIdHeader.length > 0 ? messageIdHeader[0] : UUID.randomUUID().toString());

                    rawEmail.setSubject(msg.getSubject() != null ? msg.getSubject() : "");
                    rawEmail.setFrom(msg.getFrom() != null && msg.getFrom().length > 0 ? msg.getFrom()[0].toString() : "UNKNOWN");
                    rawEmail.setTo(user);

                    rawEmail.setSentAt(msg.getSentDate() != null ? msg.getSentDate().toInstant() : Instant.now());
                    rawEmail.setReceivedAt(msg.getReceivedDate() != null ? msg.getReceivedDate().toInstant() : Instant.now());
                    rawEmail.setFolderName("INBOX");

                    String body = extractTextFromPart(msg);
                    rawEmail.setBody(body);

                    emailList.add(rawEmail);
                } catch (Exception e) {
                    LOG.error("❌ Failed to parse single email message: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("⚠️ [IMAP] Failed to fetch emails via IMAP: {}. (Returning collected count: {})", e.getMessage(), emailList.size());
        } finally {
            try {
                if (inbox != null && inbox.isOpen()) inbox.close(false);
                if (store != null && store.isConnected()) store.close();
                LOG.info("🔌 [IMAP] Disconnected and released network resources successfully.");
            } catch (Exception ignored) {
            }
        }

        return emailList;
    }

    /**
     * 辅助工具：递归提取 MimePart 里的纯文本正文内容
     */
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

    public String getUser() {
        return user;
    }
}
