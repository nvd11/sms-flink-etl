package com.finance.etl.source.imap;

import com.finance.etl.model.RawEmail;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 针对 ImapSourceReader 状态机与生命周期的单元测试
 */
public class ImapSourceReaderTest {

    @Test
    @DisplayName("测试 Reader 启动、工单请求与 noMoreSplits 终止状态流转")
    public void testReaderLifecycleAndTermination() throws Exception {
        AtomicBoolean splitRequested = new AtomicBoolean(false);

        SourceReaderContext mockContext = new SourceReaderContext() {
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
                splitRequested.set(true);
            }

            @Override
            public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return null;
            }
        };

        ImapSourceReader reader = new ImapSourceReader(
                mockContext, "imap.gmail.com", 993, "test@gmail.com", "", null, 7890);

        // 1. 验证 start() 触发 sendSplitRequest
        reader.start();
        assertTrue(splitRequested.get(), "Reader 启动时应主动向协调器请求 Split");

        // 2. 模拟 ReaderOutput
        List<RawEmail> collected = new ArrayList<>();
        ReaderOutput<RawEmail> output = new ReaderOutput<>() {
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

        // 3. 初始队列为空且未结束时，pollNext 应返回 NOTHING_AVAILABLE
        InputStatus status1 = reader.pollNext(output);
        assertEquals(InputStatus.NOTHING_AVAILABLE, status1);

        // 4. 模拟投递一张空工单
        ImapSplit split = new ImapSplit("split-test-1", "INBOX", Collections.emptyList());
        reader.addSplits(Collections.singletonList(split));
        assertEquals(1, reader.snapshotState(1L).size(), "快照中应保留未消费的工单");

        // 5. 模拟消费完工单，通知无更多工单
        reader.notifyNoMoreSplits();
        InputStatus status2 = reader.pollNext(output); // 消费该 split (空邮件)
        InputStatus status3 = reader.pollNext(output); // 队列空且 noMoreSplits -> END_OF_INPUT

        assertEquals(InputStatus.END_OF_INPUT, status3, "处理完所有分片并收到 noMoreSplits 后必须返回 END_OF_INPUT");
        reader.close();
    }
}
