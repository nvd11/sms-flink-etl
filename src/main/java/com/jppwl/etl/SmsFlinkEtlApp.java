package com.jppwl.etl;

import com.jppwl.etl.model.EmailMessage;
import com.jppwl.etl.model.SmsRecord;
import com.jppwl.etl.parser.SmsParser;
import com.jppwl.etl.sink.SmsJdbcSink;
import com.jppwl.etl.source.EmailImapSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmsFlinkEtlApp {
    private static final Logger LOG = LoggerFactory.getLogger(SmsFlinkEtlApp.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting SMS Flink ETL Stream Pipeline on K3s...");

        ParameterTool params = ParameterTool.fromArgs(args);

        // IMAP 邮件配置 (支持命令行参数或环境变量注入)
        String imapHost = params.get("imap.host", getEnv("IMAP_HOST", "imap.gmail.com"));
        int imapPort = params.getInt("imap.port", Integer.parseInt(getEnv("IMAP_PORT", "993")));
        String imapUser = params.get("imap.user", getEnv("IMAP_USER", "alice.h.y.he@gmail.com"));
        String imapPass = params.get("imap.password", getEnv("IMAP_PASSWORD", "gudhdbrswjdvysfv"));
        long pollIntervalMs = params.getLong("poll.interval.ms", Long.parseLong(getEnv("POLL_INTERVAL_MS", "10000")));

        // 目标数据库配置
        String dbUrl = params.get("db.url", getEnv("DB_URL", "jdbc:mysql://100.122.84.84:3306/litellm_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"));
        String dbUser = params.get("db.user", getEnv("DB_USER", "litellm_user"));
        String dbPass = params.get("db.password", getEnv("DB_PASSWORD", "Hsbc1234!"));

        // 初始化 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 Checkpoint 容错保障 (每15秒一次 Checkpoint，Exactly-Once)
        env.enableCheckpointing(15000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        LOG.info("Flink Environment configured. Source IMAP: {} / {}", imapHost, imapUser);
        LOG.info("Target DB Sink: {}", dbUrl);

        // 1. 数据摄入 Source (Email IMAP Source)
        DataStream<EmailMessage> emailStream = env.addSource(
                new EmailImapSource(imapHost, imapPort, imapUser, imapPass, pollIntervalMs)
        ).name("Email-IMAP-Source");

        // 2. 数据清洗与正则抽取 MapFunction (SmsParser)
        final SmsParser parser = new SmsParser();
        DataStream<SmsRecord> smsStream = emailStream.map(new MapFunction<EmailMessage, SmsRecord>() {
            private static final long serialVersionUID = 1L;
            @Override
            public SmsRecord map(EmailMessage email) throws Exception {
                SmsRecord record = parser.parse(email);
                LOG.info("Parsed Record: category={}, amount={}, merchant={}, card={}",
                        record.getSmsCategory(), record.getParsedAmount(), record.getParsedMerchant(), record.getParsedCardNo());
                return record;
            }
        }).name("Regex-Sms-Parser");

        // 3. 数据落地 Sink (JDBC Upsert Sink)
        smsStream.addSink(
                SmsJdbcSink.createSink(dbUrl, dbUser, dbPass)
        ).name("Raw-Sms-MySQL-Sink");

        env.execute("SMS-Bookkeeping-Flink-ETL");
    }

    private static String getEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        return (val != null && !val.trim().isEmpty()) ? val.trim() : defaultValue;
    }
}
