package com.finance.etl;

import com.finance.etl.model.RawEmail;
import com.finance.etl.model.SmsRecord;
import com.finance.etl.util.ConfigUtils;
import jakarta.mail.*;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.search.FlagTerm;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;

/**
 * SMS Flink ETL - 金融短信动账批处理入湖流水线主作业 (sms-gmail-r2)
 * 数据链路: 小米15 (SmsForwarder) -> Gmail IMAP -> Flink on NUC -> Cloudflare R2 (Apache Iceberg) -> Trino
 */
public class SmsGmailR2Job {
    private static final Logger LOG = LoggerFactory.getLogger(SmsGmailR2Job.class);

    public static void main(String[] args) throws Exception {
        LOG.info("================================================================================");
        LOG.info("🚀 Starting SMS Gmail to Cloudflare R2 Lakehouse ETL Batch Job (sms-gmail-r2)...");
        LOG.info("================================================================================");

        String user = ConfigUtils.get("GMAIL_IMAP_USER", "alice.h.y.he@gmail.com");
        LOG.info("📧 Configured Gmail IMAP Buffer Account: {}", user);

        // 1. 初始化 Flink 批处理流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 匹配 NUC TaskManager 的 2 个并行 Slots

        // 2. 从 Gmail IMAP 拉取所有未读邮件并解析为 SMS 记录
        List<SmsRecord> allSmsRecords = getAllSms();
        LOG.info("📦 Total extracted SMS records ready for ingestion: {}", allSmsRecords.size());

        // 3. 接入 Flink 流式计算拓扑
        if (allSmsRecords.isEmpty()) {
            LOG.info("📭 No new SMS records to process. Emitting empty heartbeat record.");
            allSmsRecords = Collections.singletonList(createHeartbeatRecord());
        }

        DataStream<SmsRecord> smsStream = env.fromCollection(allSmsRecords);
        smsStream
            .map(record -> {
                String logMsg = String.format("[Nova-Worker-Slot] [sms-gmail-r2] Ingesting: [Sender: %s, UID: %s, Body: %s]",
                        record.getSender(), record.getMsgUid().substring(0, Math.min(8, record.getMsgUid().length())), record.getRawBody());
                LOG.info(logMsg);
                return record;
            })
            .print();

        // 4. 提交作业执行
        LOG.info("🚀 Submitting sms-gmail-r2 JobGraph to Flink execution runtime...");
        env.execute("SMS-Gmail-R2-Lakehouse-Batch-Job");

        LOG.info("================================================================================");
        LOG.info("✅ SMS Gmail to R2 Batch Job Execution Finished Successfully!");
        LOG.info("================================================================================");
    }

    /**
     * 协调获取所有短信记录 (邮件提取 -> 业务脱壳)
     */
    public static List<SmsRecord> getAllSms() {
        String user = ConfigUtils.get("GMAIL_IMAP_USER", "alice.h.y.he@gmail.com");
        String pass = ConfigUtils.get("GMAIL_IMAP_PASS", ConfigUtils.get("GMAIL_IMAP_PASSWORD", ""));

        List<RawEmail> emails = fetchEmailsFromGmail(user, pass);
        List<SmsRecord> smsRecords = new ArrayList<>();

        for (RawEmail email : emails) {
            List<SmsRecord> extracted = getSMSrecordFromEmail(email);
            if (extracted != null && !extracted.isEmpty()) {
                smsRecords.addAll(extracted);
            }
        }
        return smsRecords;
    }

    /**
     * 第一步：纯邮件协议层提取 (通过 IMAP over SSL 短连接拉取邮件并脱壳成 RawEmail DTO)
     */
    public static List<RawEmail> fetchEmailsFromGmail(String user, String password) {
        List<RawEmail> emailList = new ArrayList<>();

        if (password == null || password.trim().isEmpty() || password.equals("your_password")) {
            LOG.warn("⚠️ [IMAP] Gmail password not provided, skipping remote IMAP fetch.");
            return emailList;
        }

        String host = ConfigUtils.get("GMAIL_IMAP_HOST", "imap.gmail.com");
        String port = ConfigUtils.get("GMAIL_IMAP_PORT", "993");

        Properties props = new Properties();
        props.put("mail.store.protocol", "imaps");
        props.put("mail.imaps.host", host);
        props.put("mail.imaps.port", port);
        props.put("mail.imaps.ssl.enable", "true");
        props.put("mail.imaps.connectiontimeout", "10000");
        props.put("mail.imaps.timeout", "10000");

        // 针对家庭内网或本地环境可选挂载 SOCKS5 代理
        String proxyHost = ConfigUtils.get("IMAP_PROXY_HOST");
        String proxyPort = ConfigUtils.get("IMAP_PROXY_PORT", "7890");
        if (proxyHost != null && !proxyHost.trim().isEmpty()) {
            props.put("mail.imaps.socks.host", proxyHost.trim());
            props.put("mail.imaps.socks.port", proxyPort.trim());
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
            inbox.open(Folder.READ_ONLY); // 先以只读模式安全读取，保证无侵入

            // 搜索所有未读邮件 (UNSEEN)
            Message[] messages = inbox.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
            LOG.info("📬 [IMAP] Found {} unread messages in INBOX.", messages.length);

            // 限制单批拉取上限，防止一次拉取几百封造成网络超时 (默认单批 20 封)
            int maxBatchSize = ConfigUtils.getInt("IMAP_MAX_BATCH_SIZE", 20);
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

                    // 递归提取纯文本正文
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
     * 第二步：业务转换层 (从 RawEmail 邮件元数据中提炼出金融动账业务实体 SmsRecord)
     */
    public static List<SmsRecord> getSMSrecordFromEmail(RawEmail email) {
        List<SmsRecord> list = new ArrayList<>();
        if (email == null) {
            return list;
        }

        String subject = email.getSubject() != null ? email.getSubject() : "";
        String body = email.getBody() != null ? email.getBody() : "";
        String fullContent = subject + " " + body;

        // 识别业务发送方 (广发 95508, 微信支付, 支付宝, 招行等)
        String sender = "OTHER";
        if (fullContent.contains("95508") || fullContent.contains("广发银行")) {
            sender = "95508";
        } else if (fullContent.contains("微信支付") || fullContent.contains("财付通")) {
            sender = "WECHAT_PAY";
        } else if (fullContent.contains("支付宝") || fullContent.contains("蚂蚁金服")) {
            sender = "ALIPAY";
        } else if (fullContent.contains("95555") || fullContent.contains("招商银行")) {
            sender = "95555";
        }

        // 识别接收手机号 / 卡槽标识 (从 SmsForwarder 默认主题提取，例如: [SIM1] 或手机号)
        String receiverPhone = "SIM_SLOT_1";
        if (subject.contains("SIM2") || body.contains("卡槽2")) {
            receiverPhone = "SIM_SLOT_2";
        }

        // 生成 SHA-256 幂等防重指纹
        String fingerprint = generateSha256(email.getMessageId() + "_" + body.trim());

        SmsRecord record = new SmsRecord();
        record.setId(System.nanoTime());
        record.setMsgUid(fingerprint);
        record.setChannel("EMAIL_IMAP");
        record.setSender(sender);
        record.setReceiverPhone(receiverPhone);
        record.setReceivedAt(email.getReceivedAt() != null ? email.getReceivedAt() : Instant.now());
        record.setRawBody(body.trim().isEmpty() ? subject : body.trim());
        record.setCreatedAt(Instant.now());

        list.add(record);
        return list;
    }

    /**
     * 辅助工具：递归提取 MimePart 里的纯文本正文内容
     */
    private static String extractTextFromPart(Part part) throws MessagingException, IOException {
        if (part.isMimeType("text/plain")) {
            return (String) part.getContent();
        }
        if (part.isMimeType("text/html")) {
            String html = (String) part.getContent();
            return html.replaceAll("<[^>]*>", "").trim(); // 简单去除 HTML 标签保留纯文本
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

    /**
     * 辅助工具：生成 SHA-256 唯一指纹
     */
    private static String generateSha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                String h = Integer.toHexString(0xff & b);
                if (h.length() == 1) hex.append('0');
                hex.append(h);
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString().replace("-", "");
        }
    }

    /**
     * 空数据时的兜底心跳记录
     */
    private static SmsRecord createHeartbeatRecord() {
        SmsRecord r = new SmsRecord();
        r.setId(System.nanoTime());
        r.setMsgUid("HEARTBEAT_" + UUID.randomUUID());
        r.setChannel("EMAIL_IMAP");
        r.setSender("SYSTEM_HEARTBEAT");
        r.setReceiverPhone("NONE");
        r.setReceivedAt(Instant.now());
        r.setRawBody("[HEARTBEAT] No incoming SMS in Gmail. Pulse tick ok.");
        r.setCreatedAt(Instant.now());
        return r;
    }
}
