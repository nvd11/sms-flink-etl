package com.finance.etl.source.imap;

import com.finance.etl.model.RawEmail;
import com.finance.etl.util.ConfigUtils;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 ImapSourceReader 状态机与真实/模拟拉取能力的单元测试
 */
public class ImapSourceReaderTest {
    private static final Logger LOG = LoggerFactory.getLogger(ImapSourceReaderTest.class);

    private SourceReaderContext createMockContext(AtomicBoolean splitRequested) {
        return new SourceReaderContext() {
            @Override
            public SourceReaderMetricGroup metricGroup() {
                return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
            }

            @Override
            public Configuration getConfiguration() {
                return new Configuration();
            }

            @Override
            public String getLocalHostName() {
                return "localhost";
            }

            @Override
            public int getIndexOfSubtask() {
                return 0;
            }

            @Override
            public void sendSplitRequest() {
                if (splitRequested != null) splitRequested.set(true);
            }

            @Override
            public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return null;
            }
        };
    }

    private ReaderOutput<RawEmail> createMockOutput(List<RawEmail> collected) {
        return new ReaderOutput<>() {
            @Override
            public void collect(RawEmail record) {
                collected.add(record);
            }

            @Override
            public void collect(RawEmail record, long timestamp) {
                collected.add(record);
            }

            @Override
            public void emitWatermark(Watermark watermark) {}

            @Override
            public void markIdle() {}

            @Override
            public void markActive() {}

            @Override
            public SourceOutput<RawEmail> createOutputForSplit(String splitId) {
                return this;
            }

            @Override
            public void releaseOutputForSplit(String splitId) {}
        };
    }

    @Test
    @DisplayName("测试 1: Reader 状态机流转 (start -> pollNext -> addSplits -> noMoreSplits -> END_OF_INPUT)")
    public void testReaderLifecycleAndTermination() throws Exception {
        AtomicBoolean splitRequested = new AtomicBoolean(false);
        SourceReaderContext mockContext = createMockContext(splitRequested);

        ImapSourceReader reader = new ImapSourceReader(
                mockContext, "imap.gmail.com", 993, "test@gmail.com", "", null, 7890);

        // 1. 验证 start() 触发 sendSplitRequest
        reader.start();
        assertTrue(splitRequested.get(), "Reader 启动时应主动向协调器请求 Split");

        // 2. 初始队列为空且未结束时，pollNext 应返回 NOTHING_AVAILABLE
        List<RawEmail> collected = new ArrayList<>();
        ReaderOutput<RawEmail> output = createMockOutput(collected);
        InputStatus status1 = reader.pollNext(output);
        assertEquals(InputStatus.NOTHING_AVAILABLE, status1);

        // 3. 模拟投递一张工单并核验状态
        ImapSplit split = new ImapSplit("split-test-1", "INBOX", Collections.emptyList());
        reader.addSplits(Collections.singletonList(split));
        assertEquals(1, reader.snapshotState(1L).size(), "快照中应保留未消费的工单");

        // 4. 模拟消费完工单并通知结束
        reader.notifyNoMoreSplits();
        reader.pollNext(output); // 消费空 split
        InputStatus terminalStatus = reader.pollNext(output); // 宣告结束

        assertEquals(InputStatus.END_OF_INPUT, terminalStatus, "处理完所有分片并收到 noMoreSplits 后必须返回 END_OF_INPUT");
        reader.close();
    }

    @Test
    @DisplayName("测试 2: 真实 Gmail IMAP 端到端拉取测试 (基于 .env 凭据执行真实邮件读取与 UID 解析)")
    public void testRealGmailFetchForSplit() throws Exception {
        String user = ConfigUtils.get("GMAIL_IMAP_USER");
        String pass = ConfigUtils.get("GMAIL_IMAP_PASS");
        String proxyHost = ConfigUtils.get("IMAP_PROXY_HOST");
        int proxyPort = ConfigUtils.getInt("IMAP_PROXY_PORT", 7890);

        if (pass == null || pass.trim().isEmpty() || pass.equals("your_password")) {
            LOG.warn("⚠️ [Test] No real Gmail credentials found. Skipping real network test.");
            return;
        }

        LOG.info("🧪 [Test] Starting Real Gmail IMAP Reader Fetch Test for {}", user);
        SourceReaderContext context = createMockContext(new AtomicBoolean());
        ImapSourceReader reader = new ImapSourceReader(
                context, "imap.gmail.com", 993, user, pass, proxyHost, proxyPort);

        // 构造一张包含真实 UID 或范围的工单进行现场取信 (比如指定拉取最新的 UID 361 和 362)
        ImapSplit split = new ImapSplit("split-real-test", "INBOX", Arrays.asList(361L, 362L));
        List<RawEmail> emails = reader.fetchEmailsForSplit(split);

        LOG.info("📬 [Test] Successfully fetched {} email(s) directly from Gmail via ImapSourceReader!", emails.size());
        for (RawEmail email : emails) {
            LOG.info("  ✉️ [UID: {}, Subject: {}, Sender: {}, SentAt: {}]",
                    email.getImapUid(), email.getSubject(), email.getFrom(), email.getSentAt());
            LOG.info("  📄 [Body Content Preview]: >>>{}<<<", email.getBody());
            assertNotNull(email.getImapUid(), "真实拉取的邮件必须带有 RFC 3501 永久 UID");
            assertNotNull(email.getMessageId(), "Message-ID 不能为空");
            assertNotNull(email.getBody(), "正文文本不能为空");
        }
        reader.close();
    }
}
